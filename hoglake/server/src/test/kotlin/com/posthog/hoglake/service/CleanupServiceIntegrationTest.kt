package com.posthog.hoglake.service

import com.posthog.hoglake.hydrator.ObjectStore
import com.posthog.hoglake.model.CleanupResult
import com.posthog.hoglake.model.HoglakeException
import com.posthog.hoglake.persistence.Locks
import com.posthog.hoglake.testing.PgTestSupport
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.awaitility.Awaitility.await
import org.jdbi.v3.core.kotlin.useHandleUnchecked
import org.jdbi.v3.core.kotlin.withHandleUnchecked
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import org.testcontainers.containers.MinIOContainer
import java.time.Duration

/**
 * Cleanup drain semantics against a real MinIO: physical deletion is
 * always liveness-checked (a queue entry is a suggestion, never an
 * authorization), missing objects drain as success, still-referenced
 * paths are skipped with the object AND queue row surviving, and
 * sub-batches commit independently. Ends with the full lifecycle:
 * expire -> cleanup -> the queued objects are gone from S3.
 */
@Tag("integration")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class CleanupServiceIntegrationTest {
    private val db = PgTestSupport.freshDatabase()
    private val jdbi get() = db.jdbi
    private val svc by lazy { CleanupService(jdbi, removals) }

    @AfterAll
    fun tearDown() = db.close()

    private companion object {
        const val BUCKET = "hoglake-cleanup"

        val minio: MinIOContainer by lazy {
            MinIOContainer("minio/minio:RELEASE.2023-09-04T19-57-37Z").also { it.start() }
        }

        /** Read/put side (bucket bootstrap + object seeding). */
        val objects: ObjectStore by lazy {
            ObjectStore(
                endpoint = minio.s3URL,
                region = "us-east-1",
                accessKey = minio.userName,
                secretKey = minio.password,
                pathStyle = true,
            ).also { it.createBucket(BUCKET) }
        }

        /** Delete side under test. */
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

    // ---- seeding -----------------------------------------------------------

    private fun seedCatalog(name: String): Long =
        jdbi.withHandleUnchecked { h ->
            h.createQuery(
                "INSERT INTO hog_catalog (name, data_path) VALUES (:name, 's3://$BUCKET/') RETURNING catalog_id",
            )
                .bind("name", name)
                .mapTo(Long::class.java)
                .one()
        }

    private fun queue(
        catalogId: Long,
        path: String,
        kind: String = "data",
    ): Long =
        jdbi.withHandleUnchecked { h ->
            h.createQuery(
                """
                INSERT INTO hog_file_removal (catalog_id, path, file_kind, reason)
                VALUES (:catalogId, :path, :kind, 'snapshot_expiry')
                RETURNING removal_id
                """,
            )
                .bind("catalogId", catalogId)
                .bind("path", path)
                .bind("kind", kind)
                .mapTo(Long::class.java)
                .one()
        }

    private fun putObject(path: String) = objects.put(path, "bytes".toByteArray())

    /** Paths still awaiting drain (soft-deleted ledger rows excluded). */
    private fun queuedPaths(catalogId: Long): List<String> =
        jdbi.withHandleUnchecked { h ->
            h.createQuery(
                "SELECT path FROM hog_file_removal " +
                    "WHERE catalog_id = ? AND drained_at IS NULL ORDER BY removal_id",
            )
                .bind(0, catalogId).mapTo(String::class.java).list()
        }

    private data class LedgerRow(
        val path: String,
        val attempts: Int,
        val lastAttemptAt: java.time.OffsetDateTime?,
        val drainedAt: java.time.OffsetDateTime?,
        val drainedOutcome: String?,
    )

    /** Every ledger row (drained or not), in queue order. */
    private fun ledgerRows(catalogId: Long): List<LedgerRow> =
        jdbi.withHandleUnchecked { h ->
            h.createQuery(
                """
                SELECT path, attempts, last_attempt_at, drained_at, drained_outcome
                FROM hog_file_removal WHERE catalog_id = ? ORDER BY removal_id
                """,
            )
                .bind(0, catalogId)
                .map { rs, _ ->
                    LedgerRow(
                        path = rs.getString("path"),
                        attempts = rs.getInt("attempts"),
                        lastAttemptAt = rs.getObject("last_attempt_at", java.time.OffsetDateTime::class.java),
                        drainedAt = rs.getObject("drained_at", java.time.OffsetDateTime::class.java),
                        drainedOutcome = rs.getString("drained_outcome"),
                    )
                }
                .list()
        }

    // ---- tests -------------------------------------------------------------

    @Test
    fun `happy drain deletes objects and empties the queue`() {
        val catalogId = seedCatalog("cl-happy")
        val paths = (1..3).map { "s3://$BUCKET/cl-happy/f$it.parquet" }
        paths.forEach {
            putObject(it)
            queue(catalogId, it)
        }

        val result = svc.runOnce("cl-happy", batchSize = 100)
        assertThat(result.removed).isEqualTo(3)
        assertThat(result.missing).isEqualTo(0)
        assertThat(result.stillReferenced).isEqualTo(0)
        assertThat(queuedPaths(catalogId)).isEmpty()
        paths.forEach { assertThat(removals.exists(it)).isFalse() }

        // Soft-delete: the rows SURVIVE as the ledger, marked drained.
        val ledger = ledgerRows(catalogId)
        assertThat(ledger).hasSize(3)
        for (row in ledger) {
            assertThat(row.drainedAt).isNotNull()
            assertThat(row.drainedOutcome).isEqualTo("deleted")
            assertThat(row.lastAttemptAt).isNotNull()
            assertThat(row.attempts).isEqualTo(0) // settled first touch: no failed attempts
        }
    }

    @Test
    fun `missing object counts as missing and drains its queue row as absent`() {
        val catalogId = seedCatalog("cl-missing")
        queue(catalogId, "s3://$BUCKET/cl-missing/never-existed.parquet")

        val result = svc.runOnce("cl-missing", batchSize = 100)
        assertThat(result.removed).isEqualTo(0)
        assertThat(result.missing).isEqualTo(1)
        assertThat(result.stillReferenced).isEqualTo(0)
        assertThat(queuedPaths(catalogId)).isEmpty()
        val row = ledgerRows(catalogId).single()
        assertThat(row.drainedAt).isNotNull()
        assertThat(row.drainedOutcome).isEqualTo("absent")
    }

    @Test
    fun `still-referenced path is never deleted - object and queue row survive`() {
        val catalogId = seedCatalog("cl-live")
        jdbi.useHandleUnchecked { h ->
            h.execute(
                "INSERT INTO hog_table (catalog_id, table_id, created_snapshot) VALUES (?, 1, 0)",
                catalogId,
            )
            // A HISTORICAL data-file row (end-snapshotted) still counts as a reference.
            h.execute(
                """
                INSERT INTO hog_data_file (catalog_id, data_file_id, table_id, begin_snapshot,
                    end_snapshot, path, record_count, file_size_bytes, row_id_start)
                VALUES (?, 1, 1, 1, 2, 's3://$BUCKET/cl-live/df.parquet', 10, 100, 0)
                """,
                catalogId,
            )
            h.execute(
                """
                INSERT INTO hog_delete_file (catalog_id, delete_file_id, table_id, data_file_id,
                    begin_snapshot, path, delete_count, file_size_bytes)
                VALUES (?, 10, 1, 1, 1, 's3://$BUCKET/cl-live/dv.puffin', 1, 10)
                """,
                catalogId,
            )
        }
        val dataPath = "s3://$BUCKET/cl-live/df.parquet"
        val dvPath = "s3://$BUCKET/cl-live/dv.puffin"
        putObject(dataPath)
        putObject(dvPath)
        queue(catalogId, dataPath, kind = "data")
        queue(catalogId, dvPath, kind = "delete")

        val result = svc.runOnce("cl-live", batchSize = 100)
        assertThat(result.removed).isEqualTo(0)
        assertThat(result.missing).isEqualTo(0)
        assertThat(result.stillReferenced).isEqualTo(2)
        // Nothing deleted, entries left for a later run (the reference may go away).
        assertThat(removals.exists(dataPath)).isTrue()
        assertThat(removals.exists(dvPath)).isTrue()
        assertThat(queuedPaths(catalogId)).containsExactly(dataPath, dvPath)
        // Each skip recorded an attempt; a second run records another.
        for (row in ledgerRows(catalogId)) {
            assertThat(row.attempts).isEqualTo(1)
            assertThat(row.lastAttemptAt).isNotNull()
            assertThat(row.drainedAt).isNull()
        }
        svc.runOnce("cl-live", batchSize = 100)
        assertThat(ledgerRows(catalogId).map { it.attempts }).containsOnly(2)
    }

    @Test
    fun `purge removes only drained rows older than the ledger retention`() {
        val catalogId = seedCatalog("cl-purge")
        // Three ledger states: an OLD drained row (past retention), a fresh
        // drained row, and an undrained entry (whose object is absent, so
        // this run drains it as 'absent').
        jdbi.useHandleUnchecked { h ->
            h.execute(
                "INSERT INTO hog_file_removal (catalog_id, path, file_kind, reason, drained_at, drained_outcome) " +
                    "VALUES (?, 's3://$BUCKET/cl-purge/old-drained', 'data', 'snapshot_expiry', " +
                    "now() - interval '2 hours', 'deleted')",
                catalogId,
            )
            h.execute(
                "INSERT INTO hog_file_removal (catalog_id, path, file_kind, reason, drained_at, drained_outcome) " +
                    "VALUES (?, 's3://$BUCKET/cl-purge/fresh-drained', 'data', 'snapshot_expiry', " +
                    "now(), 'absent')",
                catalogId,
            )
        }
        queue(catalogId, "s3://$BUCKET/cl-purge/pending")

        // Retention of one hour: only the 2-hours-old drained row purges.
        val shortRetention = CleanupService(jdbi, removals, ledgerRetentionSeconds = 3600)
        val result = shortRetention.runOnce("cl-purge", batchSize = 100)
        assertThat(result.missing).isEqualTo(1) // the pending entry drained as absent

        val paths = ledgerRows(catalogId).map { it.path }
        assertThat(paths).containsExactlyInAnyOrder(
            "s3://$BUCKET/cl-purge/fresh-drained",
            "s3://$BUCKET/cl-purge/pending",
        )
    }

    @Test
    fun `sub-batches commit independently - a skip in one never undoes another`() {
        val catalogId = seedCatalog("cl-subbatch")
        jdbi.useHandleUnchecked { h ->
            h.execute(
                "INSERT INTO hog_table (catalog_id, table_id, created_snapshot) VALUES (?, 1, 0)",
                catalogId,
            )
            h.execute(
                """
                INSERT INTO hog_data_file (catalog_id, data_file_id, table_id, begin_snapshot,
                    path, record_count, file_size_bytes, row_id_start)
                VALUES (?, 1, 1, 1, 's3://$BUCKET/cl-subbatch/c.parquet', 10, 100, 0)
                """,
                catalogId,
            )
        }
        // Sub-batches of 2 over [a, b, c(referenced), d, e]: [a,b] drains,
        // [c,d] drains d only, [e] drains.
        val paths = listOf("a", "b", "c", "d", "e").map { "s3://$BUCKET/cl-subbatch/$it.parquet" }
        paths.forEach {
            putObject(it)
            queue(catalogId, it)
        }

        val result =
            CleanupService(jdbi, removals, subBatchSize = 2)
                .runOnce("cl-subbatch", batchSize = 100)
        assertThat(result.removed).isEqualTo(4)
        assertThat(result.stillReferenced).isEqualTo(1)
        assertThat(queuedPaths(catalogId)).containsExactly("s3://$BUCKET/cl-subbatch/c.parquet")
        assertThat(removals.exists("s3://$BUCKET/cl-subbatch/c.parquet")).isTrue()
        for (p in paths - "s3://$BUCKET/cl-subbatch/c.parquet") {
            assertThat(removals.exists(p)).isFalse()
        }
    }

    @Test
    fun `an undeletable entry stays queued without wedging the rest`() {
        val catalogId = seedCatalog("cl-badpath")
        // Not an s3:// URI: the per-object delete throws, the row stays.
        jdbi.useHandleUnchecked { h ->
            h.execute(
                "INSERT INTO hog_file_removal (catalog_id, path, file_kind, reason) " +
                    "VALUES (?, 'file:///not-s3', 'data', 'table_drop_gc')",
                catalogId,
            )
        }
        val good = "s3://$BUCKET/cl-badpath/good.parquet"
        putObject(good)
        queue(catalogId, good)

        val result = svc.runOnce("cl-badpath", batchSize = 100)
        assertThat(result.removed).isEqualTo(1)
        assertThat(result.missing).isEqualTo(0)
        assertThat(result.stillReferenced).isEqualTo(0)
        assertThat(removals.exists(good)).isFalse()
        assertThat(queuedPaths(catalogId)).containsExactly("file:///not-s3")
        // The transient failure recorded an attempt on the surviving entry.
        val badRow = ledgerRows(catalogId).single { it.path == "file:///not-s3" }
        assertThat(badRow.attempts).isEqualTo(1)
        assertThat(badRow.drainedAt).isNull()
    }

    @Test
    fun `batch size bounds one run`() {
        val catalogId = seedCatalog("cl-batch")
        val paths = (1..3).map { "s3://$BUCKET/cl-batch/f$it.parquet" }
        paths.forEach {
            putObject(it)
            queue(catalogId, it)
        }

        val result = svc.runOnce("cl-batch", batchSize = 2)
        assertThat(result.removed).isEqualTo(2)
        assertThat(queuedPaths(catalogId)).containsExactly(paths[2]) // lowest removal_id first

        val rest = svc.runOnce("cl-batch", batchSize = 2)
        assertThat(rest.removed).isEqualTo(1)
        assertThat(queuedPaths(catalogId)).isEmpty()
    }

    @Test
    fun `drain sub-batches take the per-catalog commit lock`() {
        // Pinned regression (bug hunt #2, locking half): the reference
        // check and the physical delete must be serialized against the
        // commit tail via the SAME advisory lock every commit takes.
        // Without it, an in-flight commit past its own removal-queue
        // check could insert a hog_data_file row for a queued path that
        // referencedPaths (READ COMMITTED) cannot see yet — the drain
        // would delete the object under the about-to-commit live row.
        // With the lock, the drain waits for the commit to finish (and
        // then sees its rows), or the commit waits for the sub-batch.
        // This test asserts the lock is actually taken: while a fake
        // "commit" holds it, the drain makes no progress.
        val catalogId = seedCatalog("cl-lock")
        val path = "s3://$BUCKET/cl-lock/f.parquet"
        putObject(path)
        queue(catalogId, path)

        val holder = jdbi.open()
        val executor = java.util.concurrent.Executors.newSingleThreadExecutor()
        try {
            holder.begin()
            Locks.acquireCatalogCommitLock(holder, catalogId)

            val drain = executor.submit<CleanupResult> { svc.runOnce("cl-lock", batchSize = 100) }
            Thread.sleep(500)
            // Blocked behind the "commit": nothing settled, object intact.
            assertThat(drain.isDone).isFalse()
            assertThat(queuedPaths(catalogId)).containsExactly(path)
            assertThat(removals.exists(path)).isTrue()

            holder.rollback() // the "commit" finishes; the drain proceeds
            val result = drain.get(30, java.util.concurrent.TimeUnit.SECONDS)
            assertThat(result.removed).isEqualTo(1)
            assertThat(queuedPaths(catalogId)).isEmpty()
            assertThat(removals.exists(path)).isFalse()
        } finally {
            if (holder.isInTransaction) holder.rollback()
            holder.close()
            executor.shutdownNow()
        }
    }

    @Test
    fun `invalid inputs - unknown catalog and non-positive batch`() {
        assertThatThrownBy { svc.runOnce("cl-nope", 100) }
            .isInstanceOf(HoglakeException.NotFound::class.java)
        assertThatThrownBy { svc.runOnce("cl-nope", 0) }
            .isInstanceOf(HoglakeException.Validation::class.java)
    }

    @Test
    fun `background loop drains all catalogs and a non-positive interval is a no-op`() {
        com.posthog.hoglake.BackgroundLoops().use { it.register("cleanup", 0) { svc.runOnceAllCatalogs(100) } }
        com.posthog.hoglake.BackgroundLoops().use { it.register("cleanup", -1) { svc.runOnceAllCatalogs(100) } }

        val idA = seedCatalog("cl-loop-a")
        val idB = seedCatalog("cl-loop-b")
        val pathA = "s3://$BUCKET/cl-loop-a/f.parquet"
        val pathB = "s3://$BUCKET/cl-loop-b/f.parquet"
        putObject(pathA)
        queue(idA, pathA)
        putObject(pathB)
        queue(idB, pathB)

        com.posthog.hoglake.BackgroundLoops().use { loops ->
            loops.register("cleanup", 50) { svc.runOnceAllCatalogs(100) }
            await().atMost(Duration.ofSeconds(30)).untilAsserted {
                assertThat(queuedPaths(idA)).isEmpty()
                assertThat(queuedPaths(idB)).isEmpty()
            }
        }
        assertThat(removals.exists(pathA)).isFalse()
        assertThat(removals.exists(pathB)).isFalse()
    }

    /** Captures audit-logger events; assertions read the structured kv args. */
    private class AuditCapture : ch.qos.logback.core.AppenderBase<ch.qos.logback.classic.spi.ILoggingEvent>() {
        val events = java.util.concurrent.CopyOnWriteArrayList<ch.qos.logback.classic.spi.ILoggingEvent>()

        override fun append(event: ch.qos.logback.classic.spi.ILoggingEvent) {
            events += event
        }

        fun lines(): List<String> = events.map { e -> e.argumentArray.orEmpty().joinToString(" ") { it.toString() } }
    }

    private fun <T> withAuditCapture(block: (AuditCapture) -> T): T {
        val ctx = org.slf4j.LoggerFactory.getILoggerFactory() as ch.qos.logback.classic.LoggerContext
        val capture = AuditCapture().apply { context = ctx }
        capture.start()
        val logger =
            org.slf4j.LoggerFactory.getLogger(com.posthog.hoglake.observability.Audit.LOGGER_NAME)
                as ch.qos.logback.classic.Logger
        logger.addAppender(capture)
        try {
            return block(capture)
        } finally {
            logger.detachAppender(capture)
            capture.stop()
        }
    }

    @Test
    fun `audit trail - per-path file_deleted and cleanup_violation events, silence when idle`() {
        val catalogId = seedCatalog("cl-audit")
        // One referenced path (violation) + two deletable ones.
        jdbi.useHandleUnchecked { h ->
            h.execute(
                "INSERT INTO hog_table (catalog_id, table_id, created_snapshot) VALUES (?, 1, 0)",
                catalogId,
            )
            h.execute(
                """
                INSERT INTO hog_data_file (catalog_id, data_file_id, table_id, begin_snapshot,
                    path, record_count, file_size_bytes, row_id_start)
                VALUES (?, 1, 1, 1, 's3://$BUCKET/cl-audit/live.parquet', 10, 100, 0)
                """,
                catalogId,
            )
        }
        val live = "s3://$BUCKET/cl-audit/live.parquet"
        val dead1 = "s3://$BUCKET/cl-audit/dead1.parquet"
        val dead2 = "s3://$BUCKET/cl-audit/dead2.parquet"
        for (p in listOf(live, dead1, dead2)) {
            putObject(p)
            queue(catalogId, p)
        }

        withAuditCapture { capture ->
            val result = svc.runOnce("cl-audit", batchSize = 100)
            assertThat(result.removed).isEqualTo(2)
            assertThat(result.stillReferenced).isEqualTo(1)
            val lines = capture.lines()
            // One file_deleted event per physically removed object, keyed
            // by path.
            for (p in listOf(dead1, dead2)) {
                assertThat(lines)
                    .describedAs("file_deleted event for %s", p)
                    .anySatisfy {
                        assertThat(it).contains("action=file_deleted").contains("object=$p")
                    }
            }
            // The invariant violation names its path too.
            assertThat(lines).anySatisfy {
                assertThat(it)
                    .contains("action=cleanup_violation")
                    .contains("outcome=invariant_violation")
                    .contains("object=$live")
            }
            // Summary event flags the violation.
            assertThat(lines).anySatisfy {
                assertThat(it).contains("action=cleanup").contains("outcome=invariant_violation")
            }
        }

        // Zero-work drain (only the still-referenced entry remains — and it
        // still counts as work): drain a catalog with an EMPTY queue and
        // expect audit silence.
        seedCatalog("cl-audit-idle")
        withAuditCapture { capture ->
            val idle = svc.runOnce("cl-audit-idle", batchSize = 100)
            assertThat(idle).isEqualTo(com.posthog.hoglake.model.CleanupResult(0, 0, 0))
            assertThat(capture.lines())
                .describedAs("zero-work drain stays out of the audit stream")
                .noneSatisfy { assertThat(it).contains("action=cleanup") }
        }
    }

    @Test
    fun `end to end - expiry queues unreachable files and cleanup removes them from S3`() {
        // Catalog with old snapshots 0..4 (head 4), retention 60s.
        val catalogId =
            jdbi.withHandleUnchecked { h ->
                val id =
                    h.createQuery(
                        """
                INSERT INTO hog_catalog
                    (name, data_path, last_snapshot_id, snapshot_retention_seconds)
                VALUES ('cl-e2e', 's3://$BUCKET/', 4, 60)
                RETURNING catalog_id
                """,
                    ).mapTo(Long::class.java).one()
                for (s in 0..4) {
                    h.execute(
                        """
                    INSERT INTO hog_snapshot (catalog_id, snapshot_id, snapshot_time, schema_version)
                    VALUES (?, ?, now() - make_interval(secs => 3600), 0)
                    """,
                        id,
                        s,
                    )
                }
                h.execute("INSERT INTO hog_table (catalog_id, table_id, created_snapshot) VALUES (?, 1, 0)", id)
                // Rewritten at snapshot 2 -> unreachable once the floor passes it.
                h.execute(
                    """
                INSERT INTO hog_data_file (catalog_id, data_file_id, table_id, begin_snapshot,
                    end_snapshot, path, record_count, file_size_bytes, row_id_start)
                VALUES (?, 1, 1, 1, 2, 's3://$BUCKET/cl-e2e/old.parquet', 10, 100, 0)
                """,
                    id,
                )
                // Its live replacement survives everything.
                h.execute(
                    """
                INSERT INTO hog_data_file (catalog_id, data_file_id, table_id, begin_snapshot,
                    path, record_count, file_size_bytes, row_id_start)
                VALUES (?, 2, 1, 2, 's3://$BUCKET/cl-e2e/live.parquet', 10, 100, 10)
                """,
                    id,
                )
                id
            }
        val oldPath = "s3://$BUCKET/cl-e2e/old.parquet"
        val livePath = "s3://$BUCKET/cl-e2e/live.parquet"
        putObject(oldPath)
        putObject(livePath)

        val expiry = ExpiryService(jdbi).runOnce("cl-e2e", batchSize = 100)
        assertThat(expiry.newEarliestSnapshotId).isEqualTo(4)
        assertThat(expiry.snapshotsExpired).isEqualTo(4)
        assertThat(expiry.dataFilesQueued).isEqualTo(1)
        assertThat(queuedPaths(catalogId)).containsExactly(oldPath)

        val cleanup = svc.runOnce("cl-e2e", batchSize = 100)
        assertThat(cleanup.removed).isEqualTo(1)
        assertThat(cleanup.missing).isEqualTo(0)
        assertThat(cleanup.stillReferenced).isEqualTo(0)
        assertThat(queuedPaths(catalogId)).isEmpty()
        assertThat(removals.exists(oldPath)).isFalse() // physically gone
        assertThat(removals.exists(livePath)).isTrue() // live data untouched
    }
}
