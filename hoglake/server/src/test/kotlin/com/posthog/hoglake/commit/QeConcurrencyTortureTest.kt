package com.posthog.hoglake.commit

import com.posthog.hoglake.model.AlterOp
import com.posthog.hoglake.model.ColType
import com.posthog.hoglake.model.ColumnDef
import com.posthog.hoglake.model.CommitRequest
import com.posthog.hoglake.model.DeleteFileRegistration
import com.posthog.hoglake.model.FileRegistration
import com.posthog.hoglake.model.HoglakeException
import com.posthog.hoglake.model.TableAppend
import com.posthog.hoglake.model.TableDeletes
import com.posthog.hoglake.service.AlterService
import com.posthog.hoglake.service.CatalogService
import com.posthog.hoglake.service.CleanupService
import com.posthog.hoglake.service.ExpiryService
import com.posthog.hoglake.service.RemovalStore
import com.posthog.hoglake.testing.PgTestSupport
import org.assertj.core.api.Assertions.assertThat
import org.jdbi.v3.core.Handle
import org.jdbi.v3.core.kotlin.withHandleUnchecked
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import org.testcontainers.containers.MinIOContainer
import java.util.Random
import java.util.UUID
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.CountDownLatch
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger
import kotlin.concurrent.thread

/**
 * Concurrency torture (fuzzing.md layer 3): storms of mixed
 * DDL/commit/DV-delete/offset traffic against one catalog, plus expiry
 * and cleanup running CONCURRENTLY with commits. Individual operations
 * may fail with catalog semantics (conflict/not-found/validation — the
 * storm races on purpose); anything else is a test failure. Afterwards
 * the GLOBAL invariants are asserted straight from SQL:
 *
 *  - snapshot ids dense 0..head (or earliest..head once expiry ran)
 *  - every change row's snapshot exists; only snapshot 0 is change-free
 *  - at most one live DV per data file; DV chains are monotone
 *  - at most one live (namespace, name) per table / view
 *  - per-table row-id ranges tile [0, next_row_id) with no overlap
 *  - hog_table_stats.next_row_id == sum of record_counts ever appended
 *  - expiry never advanced past a consumer's committed offset
 *  - queued removal paths are never still referenced by file rows
 */
@Tag("integration")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class QeConcurrencyTortureTest {
    private val db = PgTestSupport.freshDatabase()
    private val catalogs = CatalogService(db.jdbi)
    private val commits = CommitService(db.jdbi)
    private val alters = AlterService(db.jdbi)
    private val expiry = ExpiryService(db.jdbi)
    private val fileSeq = AtomicInteger(0)

    @AfterAll
    fun tearDown() = db.close()

    // ---- helpers ---------------------------------------------------------

    private fun catalogId(name: String): Long =
        db.jdbi.withHandleUnchecked { h ->
            h.createQuery("SELECT catalog_id FROM hog_catalog WHERE name = ?")
                .bind(0, name).mapTo(Long::class.java).one()
        }

    private fun head(catalogId: Long): Long =
        db.jdbi.withHandleUnchecked { h ->
            h.createQuery("SELECT last_snapshot_id FROM hog_catalog WHERE catalog_id = ?")
                .bind(0, catalogId).mapTo(Long::class.java).one()
        }

    private fun newPath(catalog: String) = "s3://qe-storm/$catalog/f${fileSeq.incrementAndGet()}.parquet"

    private fun newDvPath(catalog: String) = "s3://qe-storm/$catalog/dv${fileSeq.incrementAndGet()}.puffin"

    private fun appendReq(
        catalog: String,
        table: String,
        counts: List<Long>,
    ) = CommitRequest(
        appends =
            listOf(
                TableAppend(
                    "ns",
                    table,
                    counts.map { FileRegistration(newPath(catalog), it, it * 8) },
                ),
            ),
    )

    /** Run [body] swallowing catalog semantics; anything else is recorded. */
    private fun tolerate(
        errors: ConcurrentLinkedQueue<Throwable>,
        body: () -> Unit,
    ) {
        try {
            body()
        } catch (_: HoglakeException) {
            // conflict/not-found/validation/regression: expected in a storm
        } catch (t: Throwable) {
            errors += t
        }
    }

    // ---- test A: mixed DDL + commit + delete + offset storm ---------------

    @Test
    fun `mixed ddl-commit-delete-offset storm preserves global invariants`() {
        val cat = "qe-storm"
        catalogs.createCatalog(cat, "s3://qe-storm/$cat")
        catalogs.createNamespace(cat, "ns")
        val pool = (0..5).map { "s$it" }
        for (t in pool.take(4)) {
            catalogs.createTable(cat, "ns", t, listOf(ColumnDef("id", ColType.LONG)))
            commits.commit(cat, appendReq(cat, t, listOf(10L, 0L, 25L)))
        }
        val cid = catalogId(cat)
        val headBeforeStorm = head(cid)

        val errors = ConcurrentLinkedQueue<Throwable>()
        val start = CountDownLatch(1)
        val threads =
            (0 until 6).map { tid ->
                thread(name = "qe-storm-$tid") {
                    val rnd = Random(1_000L + tid)
                    start.await()
                    repeat(30) {
                        val table = pool[rnd.nextInt(pool.size)]
                        when (rnd.nextInt(10)) {
                            0 ->
                                tolerate(errors) {
                                    catalogs.createTable(
                                        cat,
                                        "ns",
                                        table,
                                        listOf(ColumnDef("id", ColType.LONG)),
                                    )
                                }
                            1 -> tolerate(errors) { catalogs.dropTable(cat, "ns", table) }
                            2 ->
                                tolerate(errors) {
                                    alters.alterTable(
                                        cat,
                                        "ns",
                                        table,
                                        listOf(
                                            AlterOp.AddColumn(
                                                ColumnDef("c${tid}_${rnd.nextInt(1_000_000)}", ColType.STRING),
                                            ),
                                        ),
                                    )
                                }
                            3, 4, 5 ->
                                tolerate(errors) {
                                    val counts = (0..rnd.nextInt(2)).map { rnd.nextInt(50).toLong() }
                                    val read = if (rnd.nextBoolean()) head(cid) else null
                                    commits.commit(
                                        cat,
                                        appendReq(cat, table, counts).copy(readSnapshot = read),
                                    )
                                }
                            6, 7 ->
                                tolerate(errors) {
                                    // Supersede the live DV (or mint the first) on a
                                    // random live data file of a live table.
                                    val target =
                                        db.jdbi.withHandleUnchecked { h ->
                                            h.createQuery(
                                                """
                                    SELECT df.data_file_id, df.record_count, tv.name
                                      FROM hog_data_file df
                                      JOIN hog_table t
                                        ON t.catalog_id = df.catalog_id AND t.table_id = df.table_id
                                      JOIN hog_table_version tv
                                        ON tv.catalog_id = df.catalog_id AND tv.table_id = df.table_id
                                       AND tv.end_snapshot IS NULL
                                     WHERE df.catalog_id = :c AND df.end_snapshot IS NULL
                                       AND df.record_count > 0 AND t.dropped_snapshot IS NULL
                                     ORDER BY random() LIMIT 1
                                    """,
                                            )
                                                .bind("c", cid)
                                                .map { rs, _ ->
                                                    Triple(rs.getLong(1), rs.getLong(2), rs.getString(3))
                                                }
                                                .findOne().orElse(null)
                                        } ?: return@tolerate
                                    val (fileId, recordCount, liveName) = target
                                    commits.commit(
                                        cat,
                                        CommitRequest(
                                            readSnapshot = head(cid),
                                            deletes =
                                                listOf(
                                                    TableDeletes(
                                                        "ns",
                                                        liveName,
                                                        listOf(
                                                            DeleteFileRegistration(
                                                                dataFileId = fileId,
                                                                path = newDvPath(cat),
                                                                deleteCount = recordCount,
                                                                fileSizeBytes = 16,
                                                            ),
                                                        ),
                                                    ),
                                                ),
                                        ),
                                    )
                                }
                            else ->
                                tolerate(errors) {
                                    val uuid =
                                        db.jdbi.withHandleUnchecked { h ->
                                            h.createQuery(
                                                "SELECT table_uuid FROM hog_table " +
                                                    "WHERE catalog_id = ? ORDER BY random() LIMIT 1",
                                            ).bind(0, cid).mapTo(UUID::class.java).findOne().orElse(null)
                                        } ?: return@tolerate
                                    catalogs.commitOffset(cat, "consumer-$tid", uuid, head(cid))
                                }
                        }
                    }
                }
            }
        start.countDown()
        threads.forEach { it.join(120_000) }
        assertThat(threads.none { it.isAlive }).describedAs("no wedged threads").isTrue()
        assertThat(errors).describedAs("non-catalog exceptions from the storm").isEmpty()
        // The storm must have actually landed work, not silently failed
        // every op into the tolerated-exception bucket.
        assertThat(head(cid))
            .describedAs("storm produced a substantial snapshot history")
            .isGreaterThan(headBeforeStorm + 50)

        db.jdbi.withHandleUnchecked { h -> assertGlobalInvariants(h, cid, earliest = 0L) }
    }

    // ---- test B: expiry concurrent with commits, drops, offsets -----------

    @Test
    fun `expiry racing commits and offset advances never eats a consumer's range`() {
        val cat = "qe-race"
        catalogs.createCatalog(cat, "s3://qe-storm/$cat")
        catalogs.createNamespace(cat, "ns")
        val t = catalogs.createTable(cat, "ns", "t", listOf(ColumnDef("id", ColType.LONG)))
        val cid = catalogId(cat)
        db.jdbi.withHandleUnchecked { h ->
            h.createUpdate(
                "UPDATE hog_catalog SET snapshot_retention_seconds = 1, consumer_floor = true WHERE catalog_id = ?",
            ).bind(0, cid).execute()
        }
        catalogs.commitOffset(cat, "lagger", t.tableUuid, 0)

        val errors = ConcurrentLinkedQueue<Throwable>()
        val running = AtomicBoolean(true)
        val committer =
            thread(name = "qe-race-commit") {
                var i = 0
                while (running.get()) {
                    tolerate(errors) {
                        commits.commit(cat, appendReq(cat, "t", listOf((i++ % 5).toLong())))
                    }
                    Thread.sleep(15)
                }
            }
        val dropper =
            thread(name = "qe-race-drop") {
                var gen = 0
                while (running.get()) {
                    tolerate(errors) {
                        catalogs.createTable(cat, "ns", "d", listOf(ColumnDef("id", ColType.LONG)))
                        commits.commit(cat, appendReq(cat, "d", listOf(5L)))
                        catalogs.dropTable(cat, "ns", "d")
                        gen++
                    }
                    Thread.sleep(40)
                }
            }
        val consumer =
            thread(name = "qe-race-offset") {
                while (running.get()) {
                    tolerate(errors) { catalogs.commitOffset(cat, "lagger", t.tableUuid, head(cid)) }
                    Thread.sleep(35)
                }
            }
        val expirer =
            thread(name = "qe-race-expiry") {
                while (running.get()) {
                    tolerate(errors) { expiry.runOnce(cat, batchSize = 10) }
                    Thread.sleep(10)
                }
            }

        Thread.sleep(3_000)
        running.set(false)
        for (th in listOf(committer, dropper, consumer, expirer)) {
            th.join(30_000)
            assertThat(th.isAlive).describedAs("thread ${th.name} deadlocked").isFalse()
        }
        assertThat(errors).describedAs("non-catalog exceptions").isEmpty()

        // One more sweep from a quiet catalog, then the invariants.
        expiry.runOnce(cat, batchSize = 1_000)
        db.jdbi.withHandleUnchecked { h ->
            val (earliest, headNow) =
                h.createQuery(
                    "SELECT earliest_snapshot_id, last_snapshot_id FROM hog_catalog WHERE catalog_id = ?",
                ).bind(0, cid).map { rs, _ -> rs.getLong(1) to rs.getLong(2) }.one()

            val minOffset =
                h.createQuery(
                    "SELECT min(committed_snapshot) FROM hog_consumer_offset WHERE catalog_id = ?",
                ).bind(0, cid).mapTo(Long::class.javaObjectType).one()
            assertThat(earliest)
                .describedAs("floor never passes the min consumer offset")
                .isLessThanOrEqualTo(minOffset)

            val ids =
                h.createQuery(
                    "SELECT snapshot_id FROM hog_snapshot WHERE catalog_id = ? ORDER BY snapshot_id",
                ).bind(0, cid).mapTo(Long::class.java).list()
            assertThat(ids)
                .describedAs("surviving snapshots are a dense [earliest, head] range")
                .isEqualTo((earliest..headNow).toList())

            // Every surviving ended file row is still reachable; everything
            // expiry deleted was queued (and is no longer a row).
            val danglingEnded =
                h.createQuery(
                    """
                SELECT count(*) FROM hog_data_file
                WHERE catalog_id = ? AND end_snapshot IS NOT NULL AND end_snapshot <= ?
                """,
                ).bind(0, cid).bind(1, earliest).mapTo(Long::class.java).one()
            assertThat(danglingEnded)
                .describedAs("no unreachable file rows survive a sweep")
                .isEqualTo(0)

            val queuedStillReferenced =
                h.createQuery(
                    """
                SELECT count(*) FROM hog_file_removal q
                WHERE q.catalog_id = :c AND q.drained_at IS NULL AND EXISTS (
                    SELECT 1 FROM hog_data_file f
                    WHERE f.catalog_id = q.catalog_id AND f.path = q.path
                    UNION
                    SELECT 1 FROM hog_delete_file d
                    WHERE d.catalog_id = q.catalog_id AND d.path = q.path)
                """,
                ).bind("c", cid).mapTo(Long::class.java).one()
            assertThat(queuedStillReferenced)
                .describedAs("queued removals must be unreferenced")
                .isEqualTo(0)

            assertGlobalInvariants(h, cid, earliest)
        }
    }

    // ---- test C: cleanup draining concurrently with expiry + commits ------

    @Test
    fun `cleanup racing expiry and drop-recreate churn drains without violations`() {
        val cat = "qe-drain"
        catalogs.createCatalog(cat, "s3://$DRAIN_BUCKET/$cat")
        catalogs.createNamespace(cat, "ns")
        val cid = catalogId(cat)
        db.jdbi.withHandleUnchecked { h ->
            h.createUpdate(
                "UPDATE hog_catalog SET snapshot_retention_seconds = 1, consumer_floor = false WHERE catalog_id = ?",
            ).bind(0, cid).execute()
        }
        val cleanup = CleanupService(db.jdbi, removals, subBatchSize = 7)

        val errors = ConcurrentLinkedQueue<Throwable>()
        val running = AtomicBoolean(true)
        val violations = ConcurrentLinkedQueue<String>()
        val churner =
            thread(name = "qe-drain-churn") {
                while (running.get()) {
                    tolerate(errors) {
                        catalogs.createTable(cat, "ns", "d", listOf(ColumnDef("id", ColType.LONG)))
                        commits.commit(cat, appendReq(cat, "d", listOf(3L, 0L)))
                        catalogs.dropTable(cat, "ns", "d")
                    }
                    Thread.sleep(25)
                }
            }
        val expirer =
            thread(name = "qe-drain-expiry") {
                while (running.get()) {
                    tolerate(errors) { expiry.runOnce(cat, batchSize = 50) }
                    Thread.sleep(20)
                }
            }
        val drainer =
            thread(name = "qe-drain-cleanup") {
                while (running.get()) {
                    tolerate(errors) {
                        val r = cleanup.runOnce(cat, batchSize = 100)
                        if (r.stillReferenced > 0) {
                            violations += "cleanup saw still_referenced=${r.stillReferenced}"
                        }
                    }
                    Thread.sleep(20)
                }
            }

        Thread.sleep(3_000)
        running.set(false)
        for (th in listOf(churner, expirer, drainer)) {
            th.join(30_000)
            assertThat(th.isAlive).describedAs("thread ${th.name} deadlocked").isFalse()
        }
        assertThat(errors).describedAs("non-catalog exceptions").isEmpty()
        assertThat(violations)
            .describedAs("cleanup must never see a still-referenced queue entry here")
            .isEmpty()

        // Quiesce: final expiry (catalog now idle; everything ended is old
        // after a beat) then a final drain leaves the queue empty.
        Thread.sleep(1_100)
        expiry.runOnce(cat, batchSize = 10_000)
        val last = cleanup.runOnce(cat, batchSize = 10_000)
        assertThat(last.stillReferenced).isEqualTo(0)
        db.jdbi.withHandleUnchecked { h ->
            // Draining soft-deletes: rows persist as the ledger, so "fully
            // drained" means no UNDRAINED entries remain.
            val left =
                h.createQuery(
                    "SELECT count(*) FROM hog_file_removal WHERE catalog_id = ? AND drained_at IS NULL",
                ).bind(0, cid).mapTo(Long::class.java).one()
            assertThat(left).describedAs("queue fully drained").isEqualTo(0)
            val earliest =
                h.createQuery(
                    "SELECT earliest_snapshot_id FROM hog_catalog WHERE catalog_id = ?",
                ).bind(0, cid).mapTo(Long::class.java).one()
            assertGlobalInvariants(h, cid, earliest, expiryRan = true)
        }
    }

    // ---- shared invariant sweep -------------------------------------------

    /**
     * The SQL-level invariant audit. [earliest] is the expected floor;
     * [expiryRan] relaxes the row-id "sum of everything ever appended"
     * check to reachable rows only (expiry legitimately deletes
     * unreachable file rows, so the historical sum is no longer in the
     * table — tiling of the SURVIVING live rows is still checked).
     */
    private fun assertGlobalInvariants(
        h: Handle,
        cid: Long,
        earliest: Long,
        expiryRan: Boolean = earliest > 0,
    ) {
        val headNow =
            h.createQuery(
                "SELECT last_snapshot_id FROM hog_catalog WHERE catalog_id = ?",
            ).bind(0, cid).mapTo(Long::class.java).one()

        // Snapshots dense.
        val ids =
            h.createQuery(
                "SELECT snapshot_id FROM hog_snapshot WHERE catalog_id = ? ORDER BY snapshot_id",
            ).bind(0, cid).mapTo(Long::class.java).list()
        assertThat(ids).isEqualTo((earliest..headNow).toList())

        // Change rows reference existing snapshots.
        val orphanChanges =
            h.createQuery(
                """
            SELECT count(*) FROM hog_snapshot_change c
            WHERE c.catalog_id = :c AND NOT EXISTS (
                SELECT 1 FROM hog_snapshot s
                WHERE s.catalog_id = c.catalog_id AND s.snapshot_id = c.snapshot_id)
            """,
            ).bind("c", cid).mapTo(Long::class.java).one()
        assertThat(orphanChanges).isEqualTo(0)

        // Only snapshot 0 may be change-free (when retained).
        val changeFree =
            h.createQuery(
                """
            SELECT s.snapshot_id FROM hog_snapshot s
            WHERE s.catalog_id = :c AND NOT EXISTS (
                SELECT 1 FROM hog_snapshot_change ch
                WHERE ch.catalog_id = s.catalog_id AND ch.snapshot_id = s.snapshot_id)
            """,
            ).bind("c", cid).mapTo(Long::class.java).list()
        assertThat(changeFree).isSubsetOf(listOf(0L))

        // At most one live DV per data file.
        val dupLiveDvs =
            h.createQuery(
                """
            SELECT count(*) FROM (
                SELECT data_file_id FROM hog_delete_file
                WHERE catalog_id = :c AND end_snapshot IS NULL
                GROUP BY data_file_id HAVING count(*) > 1) x
            """,
            ).bind("c", cid).mapTo(Long::class.java).one()
        assertThat(dupLiveDvs).isEqualTo(0)

        // DV chains: counts monotone along begin_snapshot, count bounded by
        // the data file's record_count.
        val badDvs =
            h.createQuery(
                """
            SELECT count(*) FROM hog_delete_file dv
            JOIN hog_data_file df
              ON df.catalog_id = dv.catalog_id AND df.data_file_id = dv.data_file_id
            WHERE dv.catalog_id = :c AND dv.delete_count > df.record_count
            """,
            ).bind("c", cid).mapTo(Long::class.java).one()
        assertThat(badDvs).describedAs("DV never exceeds record_count").isEqualTo(0)
        val shrinkingChains =
            h.createQuery(
                """
            SELECT count(*) FROM hog_delete_file a
            JOIN hog_delete_file b
              ON b.catalog_id = a.catalog_id AND b.data_file_id = a.data_file_id
             AND b.begin_snapshot > a.begin_snapshot AND b.delete_count < a.delete_count
            WHERE a.catalog_id = :c
            """,
            ).bind("c", cid).mapTo(Long::class.java).one()
        assertThat(shrinkingChains).describedAs("vectors only grow").isEqualTo(0)

        // One live name per (namespace, name) — tables and views.
        val dupNames =
            h.createQuery(
                """
            SELECT count(*) FROM (
                SELECT namespace_id, name FROM hog_table_version
                WHERE catalog_id = :c AND end_snapshot IS NULL
                GROUP BY namespace_id, name HAVING count(*) > 1) x
            """,
            ).bind("c", cid).mapTo(Long::class.java).one()
        assertThat(dupNames).isEqualTo(0)

        // Row-id tiling per table over the surviving rows.
        val tableIds =
            h.createQuery(
                "SELECT table_id FROM hog_table WHERE catalog_id = ?",
            ).bind(0, cid).mapTo(Long::class.java).list()
        for (tableId in tableIds) {
            val files =
                h.createQuery(
                    """
                SELECT row_id_start, record_count FROM hog_data_file
                WHERE catalog_id = :c AND table_id = :t
                ORDER BY data_file_id
                """,
                ).bind("c", cid).bind("t", tableId)
                    .map { rs, _ -> rs.getLong(1) to rs.getLong(2) }.list()
            // No overlap ever: each file's range starts at or after the
            // previous file's end (equality = adjacent; gaps only when
            // expiry deleted rows in between).
            var prevEnd = -1L
            for ((start, rc) in files) {
                assertThat(start)
                    .describedAs("row ranges never overlap (table %d)", tableId)
                    .isGreaterThanOrEqualTo(maxOf(prevEnd, 0))
                prevEnd = start + rc
            }
            val stats =
                h.createQuery(
                    """
                SELECT next_row_id, record_count, file_size_bytes FROM hog_table_stats
                WHERE catalog_id = :c AND table_id = :t
                """,
                ).bind("c", cid).bind("t", tableId)
                    .map { rs, _ -> Triple(rs.getLong(1), rs.getLong(2), rs.getLong(3)) }
                    .findOne().orElse(null) ?: continue
            if (!expiryRan) {
                assertThat(stats.second)
                    .describedAs("gross record rollup == sum of appended counts (table %d)", tableId)
                    .isEqualTo(files.sumOf { it.second })
                assertThat(stats.first)
                    .describedAs("next_row_id == sum of appended counts (table %d)", tableId)
                    .isEqualTo(files.sumOf { it.second })
            } else {
                assertThat(stats.first)
                    .describedAs("allocator at or past the last surviving range (table %d)", tableId)
                    .isGreaterThanOrEqualTo(maxOf(prevEnd, 0))
            }
        }
    }

    private companion object {
        const val DRAIN_BUCKET = "qe-drain"

        val minio: MinIOContainer by lazy {
            MinIOContainer("minio/minio:RELEASE.2023-09-04T19-57-37Z").also { it.start() }
        }

        val removals: RemovalStore by lazy {
            RemovalStore(
                endpoint = minio.s3URL,
                region = "us-east-1",
                accessKey = minio.userName,
                secretKey = minio.password,
                pathStyle = true,
            )
        }
    }
}
