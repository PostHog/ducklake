package com.posthog.hoglake.service

import com.posthog.hoglake.hydrator.ObjectStore
import com.posthog.hoglake.model.HoglakeException
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

    private fun queuedPaths(catalogId: Long): List<String> =
        jdbi.withHandleUnchecked { h ->
            h.createQuery("SELECT path FROM hog_file_removal WHERE catalog_id = ? ORDER BY removal_id")
                .bind(0, catalogId).mapTo(String::class.java).list()
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
    }

    @Test
    fun `missing object counts as missing and drains its queue row`() {
        val catalogId = seedCatalog("cl-missing")
        queue(catalogId, "s3://$BUCKET/cl-missing/never-existed.parquet")

        val result = svc.runOnce("cl-missing", batchSize = 100)
        assertThat(result.removed).isEqualTo(0)
        assertThat(result.missing).isEqualTo(1)
        assertThat(result.stillReferenced).isEqualTo(0)
        assertThat(queuedPaths(catalogId)).isEmpty()
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
    fun `invalid inputs - unknown catalog and non-positive batch`() {
        assertThatThrownBy { svc.runOnce("cl-nope", 100) }
            .isInstanceOf(HoglakeException.NotFound::class.java)
        assertThatThrownBy { svc.runOnce("cl-nope", 0) }
            .isInstanceOf(HoglakeException.Validation::class.java)
    }

    @Test
    fun `background loop drains all catalogs and a non-positive interval is a no-op`() {
        svc.startLoop(0, 100).close()
        svc.startLoop(-1, 100).close()

        val idA = seedCatalog("cl-loop-a")
        val idB = seedCatalog("cl-loop-b")
        val pathA = "s3://$BUCKET/cl-loop-a/f.parquet"
        val pathB = "s3://$BUCKET/cl-loop-b/f.parquet"
        putObject(pathA)
        queue(idA, pathA)
        putObject(pathB)
        queue(idB, pathB)

        svc.startLoop(50, 100).use {
            await().atMost(Duration.ofSeconds(30)).untilAsserted {
                assertThat(queuedPaths(idA)).isEmpty()
                assertThat(queuedPaths(idB)).isEmpty()
            }
        }
        assertThat(removals.exists(pathA)).isFalse()
        assertThat(removals.exists(pathB)).isFalse()
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
