package com.posthog.hoglake.service

import com.posthog.hoglake.testing.PgTestSupport
import io.kotest.property.Arb
import io.kotest.property.arbitrary.arbitrary
import io.kotest.property.arbitrary.boolean
import io.kotest.property.arbitrary.int
import io.kotest.property.arbitrary.list
import io.kotest.property.arbitrary.long
import io.kotest.property.checkAll
import kotlinx.coroutines.runBlocking
import org.assertj.core.api.Assertions.assertThat
import org.jdbi.v3.core.Handle
import org.jdbi.v3.core.kotlin.withHandleUnchecked
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import java.util.UUID
import java.util.concurrent.atomic.AtomicInteger

/**
 * Property-based assault on the expiry floor math (fuzzing.md layer 1):
 * generated (head, freshness cut, retention, consumer offsets, files,
 * batch, floor flag) configurations are seeded into a REAL Postgres
 * catalog via SQL, ExpiryService.runOnce is executed, and the sweep's
 * outcome is checked against independently computed expectations plus
 * the service's stated invariants:
 *
 *  - newEarliest <= head (head NEVER expires)
 *  - newEarliest <= earliest + batch (incremental)
 *  - newEarliest <= min consumer offset when the floor is on
 *  - no snapshot any consumer offset still needs is expired
 *  - surviving snapshots are exactly [newEarliest, head]
 *  - queued file paths are exactly the independently computed
 *    unreachable set (end_snapshot <= newEarliest), for data files AND
 *    delete-vector files (including live DVs riding an expiring file)
 *  - repeated sweeps are monotone non-decreasing in the floor
 *
 * Timing margins are coarse (stale snapshots are > 1h beyond the
 * retention cutoff, fresh ones are seconds old vs retention >= 2min) so
 * DB-vs-JVM clock skew cannot flip a case.
 */
@Tag("integration")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class QeExpiryPropertyTest {
    private val db = PgTestSupport.freshDatabase()
    private val expiry = ExpiryService(db.jdbi)
    private val caseCounter = AtomicInteger(0)

    @AfterAll
    fun tearDown() = db.close()

    // ---- the generated case ---------------------------------------------

    private data class FileSeed(val id: Long, val begin: Long, val end: Long?, val dv: DvSeed?)

    private data class DvSeed(val begin: Long, val end: Long?)

    private data class ExpiryCase(
        val head: Long,
        /** Snapshots with id < cut are stale (older than retention). */
        val cut: Long,
        val retentionSeconds: Long,
        val offsets: List<Long>,
        val batch: Int,
        val consumerFloor: Boolean,
        val files: List<FileSeed>,
    )

    private val arbCase: Arb<ExpiryCase> =
        arbitrary {
            val head = Arb.long(1L..40L).bind()
            val cut = Arb.long(0L..head + 1).bind()
            val retention = Arb.long(120L..86_400L).bind()
            val offsets = Arb.list(Arb.long(0L..head), 0..3).bind()
            val batch = Arb.int(1..(head + 5).toInt()).bind()
            val floor = Arb.boolean().bind()
            val fileCount = Arb.int(0..12).bind()
            var id = 1L
            val files =
                (0 until fileCount).map {
                    val begin = Arb.long(0L..head).bind()
                    val end =
                        if (begin < head && Arb.boolean().bind()) {
                            Arb.long(begin + 1..head).bind()
                        } else {
                            null
                        }
                    val dv =
                        if (Arb.boolean().bind()) {
                            val dvBegin = Arb.long(0L..head).bind()
                            val dvEnd =
                                if (dvBegin < head && Arb.boolean().bind()) {
                                    Arb.long(dvBegin + 1..head).bind()
                                } else {
                                    null
                                }
                            DvSeed(dvBegin, dvEnd)
                        } else {
                            null
                        }
                    FileSeed(id++, begin, end, dv)
                }
            ExpiryCase(head, cut, retention, offsets, batch, floor, files)
        }

    // ---- seeding ---------------------------------------------------------

    private fun seed(
        h: Handle,
        name: String,
        c: ExpiryCase,
    ): Long {
        val catalogId =
            h.createQuery(
                """
            INSERT INTO hog_catalog
                (name, data_path, last_snapshot_id, snapshot_retention_seconds, consumer_floor)
            VALUES (:name, 's3://qe-expiry/', :head, :retention, :floor)
            RETURNING catalog_id
            """,
            )
                .bind("name", name)
                .bind("head", c.head)
                .bind("retention", c.retentionSeconds)
                .bind("floor", c.consumerFloor)
                .mapTo(Long::class.java)
                .one()

        val snapBatch =
            h.prepareBatch(
                """
            INSERT INTO hog_snapshot (catalog_id, snapshot_id, snapshot_time, schema_version)
            VALUES (:catalogId, :id, now() - make_interval(secs => :age), 0)
            """,
            )
        for (i in 0..c.head) {
            // Stale snapshots sit > 1h past the cutoff; fresh ones are
            // seconds old. Ages strictly decrease with id (monotone times).
            val age: Long =
                if (i < c.cut) {
                    c.retentionSeconds + 3_600 + (c.cut - i) * 10
                } else {
                    c.head - i
                }
            snapBatch.bind("catalogId", catalogId).bind("id", i).bind("age", age).add()
        }
        snapBatch.execute()

        val tableUuid =
            h.createQuery(
                """
                INSERT INTO hog_table (catalog_id, table_id, created_snapshot) VALUES (:c, 1, 0)
                RETURNING table_uuid
                """,
            ).bind("c", catalogId).mapTo(UUID::class.java).one()

        for (f in c.files) {
            h.createUpdate(
                """
                INSERT INTO hog_data_file (catalog_id, data_file_id, table_id, begin_snapshot,
                                           end_snapshot, path, record_count, file_size_bytes,
                                           row_id_start)
                VALUES (:c, :id, 1, :begin, :end, :path, 0, 0, 0)
                """,
            )
                .bind("c", catalogId)
                .bind("id", f.id)
                .bind("begin", f.begin)
                .bind("end", f.end)
                .bind("path", dataPath(name, f.id))
                .execute()
            val dv = f.dv ?: continue
            h.createUpdate(
                """
                INSERT INTO hog_delete_file (catalog_id, delete_file_id, table_id, data_file_id,
                                             begin_snapshot, end_snapshot, path, delete_count,
                                             file_size_bytes)
                VALUES (:c, :id, 1, :dataFileId, :begin, :end, :path, 1, 0)
                """,
            )
                .bind("c", catalogId)
                .bind("id", 100_000 + f.id)
                .bind("dataFileId", f.id)
                .bind("begin", dv.begin)
                .bind("end", dv.end)
                .bind("path", dvPath(name, f.id))
                .execute()
        }

        // Offsets reference the seeded table's uuid: the floor query joins
        // hog_table, so only offsets on an extant table identity pin.
        c.offsets.forEachIndexed { k, committed ->
            h.createUpdate(
                """
                INSERT INTO hog_consumer_offset
                    (catalog_id, consumer_id, table_uuid, committed_snapshot)
                VALUES (:c, :consumer, :uuid, :committed)
                """,
            )
                .bind("c", catalogId)
                .bind("consumer", "qe-consumer-$k")
                .bind("uuid", tableUuid)
                .bind("committed", committed)
                .execute()
        }
        return catalogId
    }

    private fun dataPath(
        name: String,
        id: Long,
    ) = "s3://qe-expiry/$name/f$id.parquet"

    private fun dvPath(
        name: String,
        id: Long,
    ) = "s3://qe-expiry/$name/dv$id.puffin"

    // ---- the property ----------------------------------------------------

    @Test
    fun `expiry floor math holds for generated catalogs`(): Unit =
        runBlocking {
            checkAll(25, arbCase) { c ->
                val name = "qe-exp-${caseCounter.incrementAndGet()}"
                val catalogId = db.jdbi.withHandleUnchecked { h -> seed(h, name, c) }

                // Independently computed expectation (mirrors the documented
                // floor math, computed from the generated inputs, not the DB).
                val firstFresh = if (c.cut <= c.head) c.cut else Long.MAX_VALUE
                val unfloored = minOf(firstFresh, c.head, 0L + c.batch)
                val minOffset = c.offsets.minOrNull()
                val expectedNewEarliest =
                    maxOf(
                        0L,
                        minOf(
                            unfloored,
                            if (c.consumerFloor && minOffset != null) minOffset else Long.MAX_VALUE,
                        ),
                    )

                val result = expiry.runOnce(name, c.batch)

                // Core invariants.
                assertThat(result.newEarliestSnapshotId)
                    .describedAs("head never expires (case %s)", c)
                    .isLessThanOrEqualTo(c.head)
                assertThat(result.newEarliestSnapshotId)
                    .describedAs("bounded by batch")
                    .isLessThanOrEqualTo(c.batch.toLong())
                if (c.consumerFloor && minOffset != null) {
                    assertThat(result.newEarliestSnapshotId)
                        .describedAs("consumer floor respected (min offset %d)", minOffset)
                        .isLessThanOrEqualTo(minOffset)
                }
                // Exact floor (safe: timing margins are hours vs seconds).
                assertThat(result.newEarliestSnapshotId)
                    .describedAs("exact floor math for %s", c)
                    .isEqualTo(expectedNewEarliest)
                assertThat(result.snapshotsExpired).isEqualTo(expectedNewEarliest)

                // flooredByConsumer names a pinning consumer exactly when the
                // consumer bound the sweep below what time/head/batch allowed.
                val shouldFloor =
                    c.consumerFloor && minOffset != null &&
                        minOffset < unfloored && unfloored > 0
                if (shouldFloor) {
                    assertThat(result.flooredByConsumer)
                        .describedAs("consumer should be named as pinning")
                        .isNotNull()
                } else {
                    assertThat(result.flooredByConsumer).isNull()
                }

                db.jdbi.withHandleUnchecked { h ->
                    // Survivors are exactly [newEarliest, head].
                    val ids =
                        h.createQuery(
                            "SELECT snapshot_id FROM hog_snapshot WHERE catalog_id = ? ORDER BY snapshot_id",
                        ).bind(0, catalogId).mapTo(Long::class.java).list()
                    assertThat(ids)
                        .describedAs("surviving snapshots")
                        .isEqualTo((result.newEarliestSnapshotId..c.head).toList())

                    // No snapshot a consumer offset needs is gone.
                    for (committed in c.offsets) {
                        if (c.consumerFloor) {
                            assertThat(ids)
                                .describedAs("offset %d must remain readable", committed)
                                .contains(maxOf(committed, result.newEarliestSnapshotId))
                            assertThat(result.newEarliestSnapshotId).isLessThanOrEqualTo(committed)
                        }
                    }

                    // Queued paths == the independently computed unreachable set.
                    val newE = result.newEarliestSnapshotId
                    val expectedDataQueued =
                        c.files
                            .filter { it.end != null && it.end <= newE }
                            .map { dataPath(name, it.id) }
                            .toSet()
                    val fileExpired =
                        c.files
                            .filter { it.end != null && it.end <= newE }
                            .map { it.id }
                            .toSet()
                    val expectedDvQueued =
                        c.files
                            .filter { f ->
                                f.dv != null &&
                                    ((f.dv.end != null && f.dv.end <= newE) || f.id in fileExpired)
                            }
                            .map { dvPath(name, it.id) }
                            .toSet()
                    val queued =
                        h.createQuery(
                            "SELECT path, file_kind FROM hog_file_removal WHERE catalog_id = ?",
                        ).bind(0, catalogId).map { rs, _ -> rs.getString(1) to rs.getString(2) }.list()
                    assertThat(queued.filter { it.second == "data" }.map { it.first }.toSet())
                        .describedAs("queued data files == unreachable set")
                        .isEqualTo(expectedDataQueued)
                    assertThat(queued.filter { it.second == "delete" }.map { it.first }.toSet())
                        .describedAs("queued DV files == unreachable set")
                        .isEqualTo(expectedDvQueued)
                    assertThat(result.dataFilesQueued).isEqualTo(expectedDataQueued.size.toLong())
                    assertThat(result.deleteFilesQueued).isEqualTo(expectedDvQueued.size.toLong())

                    // Expired file rows are gone; surviving rows untouched.
                    val remaining =
                        h.createQuery(
                            "SELECT path FROM hog_data_file WHERE catalog_id = ?",
                        ).bind(0, catalogId).mapTo(String::class.java).list().toSet()
                    assertThat(remaining)
                        .isEqualTo(
                            c.files.map { dataPath(name, it.id) }.toSet() - expectedDataQueued,
                        )
                }

                // Monotonicity across repeated sweeps: the floor never regresses,
                // and a second sweep from the same inputs can only advance it
                // when the first was batch-limited.
                val second = expiry.runOnce(name, c.batch)
                assertThat(second.newEarliestSnapshotId)
                    .isGreaterThanOrEqualTo(result.newEarliestSnapshotId)
                val expectedSecond =
                    maxOf(
                        result.newEarliestSnapshotId,
                        minOf(
                            minOf(firstFresh, c.head, result.newEarliestSnapshotId + c.batch),
                            if (c.consumerFloor && minOffset != null) minOffset else Long.MAX_VALUE,
                        ),
                    )
                assertThat(second.newEarliestSnapshotId)
                    .describedAs("second sweep floor math")
                    .isEqualTo(expectedSecond)
            }
        }
}
