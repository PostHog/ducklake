package com.posthog.hoglake.service

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
import java.time.Duration

/**
 * Expiry semantics: the newEarliest floor math (time / head / consumer /
 * batch constraints), range deletes of snapshots + change rows, and
 * unreachable-file queueing into hog_file_removal. All seeding is
 * direct SQL so snapshot ages and file lineage are exact.
 */
@Tag("integration")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class ExpiryServiceIntegrationTest {
    private val db = PgTestSupport.freshDatabase()
    private val jdbi get() = db.jdbi
    private val svc = ExpiryService(jdbi)

    @AfterAll
    fun tearDown() = db.close()

    // ---- seeding -----------------------------------------------------------

    /** Catalog with snapshots 0..head, each aged per [ageSeconds]. */
    private fun seedCatalog(
        name: String,
        head: Long,
        retentionSeconds: Long?,
        consumerFloor: Boolean = true,
        ageSeconds: (Long) -> Long = { 3_600 },
    ): Long =
        jdbi.withHandleUnchecked { h ->
            val catalogId =
                h.createQuery(
                    """
            INSERT INTO hog_catalog
                (name, data_path, last_snapshot_id, snapshot_retention_seconds, consumer_floor)
            VALUES (:name, 's3://bucket/p', :head, :retention, :floor)
            RETURNING catalog_id
            """,
                )
                    .bind("name", name)
                    .bind("head", head)
                    .apply {
                        if (retentionSeconds == null) {
                            bindNull("retention", java.sql.Types.BIGINT)
                        } else {
                            bind("retention", retentionSeconds)
                        }
                    }
                    .bind("floor", consumerFloor)
                    .mapTo(Long::class.java)
                    .one()
            for (s in 0..head) {
                h.execute(
                    """
                INSERT INTO hog_snapshot (catalog_id, snapshot_id, snapshot_time, schema_version)
                VALUES (?, ?, now() - make_interval(secs => ?), 0)
                """,
                    catalogId,
                    s,
                    ageSeconds(s),
                )
            }
            catalogId
        }

    private fun seedChange(
        catalogId: Long,
        snapshotId: Long,
    ) = jdbi.useHandleUnchecked { h ->
        h.execute(
            "INSERT INTO hog_snapshot_change (catalog_id, snapshot_id, kind, object_id) " +
                "VALUES (?, ?, 'table_created', 1)",
            catalogId,
            snapshotId,
        )
    }

    private fun seedTable(
        catalogId: Long,
        tableId: Long = 1L,
    ) = jdbi.useHandleUnchecked { h ->
        h.execute(
            "INSERT INTO hog_table (catalog_id, table_id, created_snapshot) VALUES (?, ?, 0)",
            catalogId,
            tableId,
        )
    }

    private fun seedDataFile(
        catalogId: Long,
        fileId: Long,
        endSnapshot: Long?,
        path: String,
    ) = jdbi.useHandleUnchecked { h ->
        h.execute(
            """
                INSERT INTO hog_data_file
                    (catalog_id, data_file_id, table_id, begin_snapshot, end_snapshot, path,
                     record_count, file_size_bytes, row_id_start)
                VALUES (?, ?, 1, 1, ?, ?, 10, 100, 0)
                """,
            catalogId,
            fileId,
            endSnapshot,
            path,
        )
    }

    private fun seedDeleteFile(
        catalogId: Long,
        deleteFileId: Long,
        dataFileId: Long,
        endSnapshot: Long?,
        path: String,
    ) = jdbi.useHandleUnchecked { h ->
        h.execute(
            """
            INSERT INTO hog_delete_file
                (catalog_id, delete_file_id, table_id, data_file_id, begin_snapshot,
                 end_snapshot, path, delete_count, file_size_bytes)
            VALUES (?, ?, 1, ?, 2, ?, ?, 1, 10)
            """,
            catalogId,
            deleteFileId,
            dataFileId,
            endSnapshot,
            path,
        )
    }

    /**
     * Offset whose table_uuid names a real hog_table row (the floor query
     * joins hog_table, so an offset only pins when its table identity
     * still exists — any incarnation, dropped included).
     */
    private fun seedOffset(
        catalogId: Long,
        consumerId: String,
        committed: Long,
        dropped: Boolean = false,
    ) = jdbi.useHandleUnchecked { h ->
        val uuid =
            h.createQuery(
                """
            INSERT INTO hog_table (catalog_id, table_id, created_snapshot, dropped_snapshot)
            SELECT :c, COALESCE(MAX(table_id), 0) + 1, 0, :dropped
              FROM hog_table WHERE catalog_id = :c
            RETURNING table_uuid
            """,
            )
                .bind("c", catalogId)
                .apply {
                    if (dropped) bind("dropped", 1L) else bindNull("dropped", java.sql.Types.BIGINT)
                }
                .mapTo(java.util.UUID::class.java)
                .one()
        h.execute(
            """
            INSERT INTO hog_consumer_offset (catalog_id, consumer_id, table_uuid, committed_snapshot)
            VALUES (?, ?, ?, ?)
            """,
            catalogId,
            consumerId,
            uuid,
            committed,
        )
    }

    /** Offset whose table row does NOT exist (expired away entirely). */
    private fun seedOrphanOffset(
        catalogId: Long,
        consumerId: String,
        committed: Long,
    ) = jdbi.useHandleUnchecked { h ->
        h.execute(
            """
                INSERT INTO hog_consumer_offset (catalog_id, consumer_id, table_uuid, committed_snapshot)
                VALUES (?, ?, gen_random_uuid(), ?)
                """,
            catalogId,
            consumerId,
            committed,
        )
    }

    // ---- readbacks ---------------------------------------------------------

    private fun snapshotIds(catalogId: Long): List<Long> =
        jdbi.withHandleUnchecked { h ->
            h.createQuery("SELECT snapshot_id FROM hog_snapshot WHERE catalog_id = ? ORDER BY snapshot_id")
                .bind(0, catalogId).mapTo(Long::class.java).list()
        }

    private fun changeCount(catalogId: Long): Long =
        jdbi.withHandleUnchecked { h ->
            h.createQuery("SELECT count(*) FROM hog_snapshot_change WHERE catalog_id = ?")
                .bind(0, catalogId).mapTo(Long::class.java).one()
        }

    private fun earliest(catalogId: Long): Long =
        jdbi.withHandleUnchecked { h ->
            h.createQuery("SELECT earliest_snapshot_id FROM hog_catalog WHERE catalog_id = ?")
                .bind(0, catalogId).mapTo(Long::class.java).one()
        }

    private fun earliestTime(catalogId: Long): java.time.Instant? =
        jdbi.withHandleUnchecked { h ->
            h.createQuery("SELECT earliest_snapshot_time FROM hog_catalog WHERE catalog_id = ?")
                .bind(0, catalogId)
                .map { rs, _ ->
                    rs.getObject("earliest_snapshot_time", java.time.OffsetDateTime::class.java)?.toInstant()
                }
                .one()
        }

    /** (path, file_kind, reason) of every queue entry for the catalog, in queue order. */
    private fun removalQueue(catalogId: Long): List<Triple<String, String, String>> =
        jdbi.withHandleUnchecked { h ->
            h.createQuery(
                "SELECT path, file_kind, reason FROM hog_file_removal WHERE catalog_id = ? ORDER BY removal_id",
            )
                .bind(0, catalogId)
                .map { rs, _ ->
                    Triple(rs.getString("path"), rs.getString("file_kind"), rs.getString("reason"))
                }
                .list()
        }

    private fun dataFilePaths(catalogId: Long): List<String> =
        jdbi.withHandleUnchecked { h ->
            h.createQuery("SELECT path FROM hog_data_file WHERE catalog_id = ? ORDER BY data_file_id")
                .bind(0, catalogId).mapTo(String::class.java).list()
        }

    private fun deleteFilePaths(catalogId: Long): List<String> =
        jdbi.withHandleUnchecked { h ->
            h.createQuery("SELECT path FROM hog_delete_file WHERE catalog_id = ? ORDER BY delete_file_id")
                .bind(0, catalogId).mapTo(String::class.java).list()
        }

    // ---- tests -------------------------------------------------------------

    @Test
    fun `null retention is a no-op sweep`() {
        val catalogId = seedCatalog("exp-null", head = 5, retentionSeconds = null)
        val result = svc.runOnce("exp-null", batchSize = 100)
        assertThat(result.snapshotsExpired).isEqualTo(0)
        assertThat(result.dataFilesQueued).isEqualTo(0)
        assertThat(result.deleteFilesQueued).isEqualTo(0)
        assertThat(result.newEarliestSnapshotId).isEqualTo(0)
        assertThat(result.flooredByConsumer).isNull()
        assertThat(snapshotIds(catalogId)).hasSize(6)
        assertThat(earliest(catalogId)).isEqualTo(0)
    }

    @Test
    fun `time-based expiry removes old snapshots but never head`() {
        // Everything an hour old, retention 60s: all expirable, head survives.
        val catalogId = seedCatalog("exp-time", head = 5, retentionSeconds = 60)
        seedChange(catalogId, 2) // must cascade with its snapshot
        seedChange(catalogId, 5) // head's change survives

        val result = svc.runOnce("exp-time", batchSize = 100)
        assertThat(result.snapshotsExpired).isEqualTo(5)
        assertThat(result.newEarliestSnapshotId).isEqualTo(5)
        assertThat(result.flooredByConsumer).isNull()
        assertThat(snapshotIds(catalogId)).containsExactly(5L)
        assertThat(changeCount(catalogId)).isEqualTo(1)
        assertThat(earliest(catalogId)).isEqualTo(5)

        // Idempotent: a second sweep finds nothing.
        val again = svc.runOnce("exp-time", batchSize = 100)
        assertThat(again.snapshotsExpired).isEqualTo(0)
        assertThat(again.newEarliestSnapshotId).isEqualTo(5)
    }

    @Test
    fun `snapshots inside the retention window survive`() {
        // 0..2 an hour old, 3..5 fresh; retention 60s -> floor stops at 3.
        val catalogId =
            seedCatalog("exp-window", head = 5, retentionSeconds = 60) { s ->
                if (s <= 2) 3_600 else 0
            }
        val result = svc.runOnce("exp-window", batchSize = 100)
        assertThat(result.snapshotsExpired).isEqualTo(3)
        assertThat(result.newEarliestSnapshotId).isEqualTo(3)
        assertThat(snapshotIds(catalogId)).containsExactly(3L, 4L, 5L)
    }

    @Test
    fun `nothing old enough is a zero-work sweep`() {
        val catalogId = seedCatalog("exp-fresh", head = 5, retentionSeconds = 86_400) { 0 }
        val result = svc.runOnce("exp-fresh", batchSize = 100)
        assertThat(result.snapshotsExpired).isEqualTo(0)
        assertThat(result.newEarliestSnapshotId).isEqualTo(0)
        assertThat(result.flooredByConsumer).isNull()
        assertThat(snapshotIds(catalogId)).hasSize(6)
    }

    @Test
    fun `consumer floor pins the sweep and names the consumer`() {
        val catalogId = seedCatalog("exp-floor", head = 5, retentionSeconds = 60)
        seedOffset(catalogId, "cdc-lagging", 3)
        seedOffset(catalogId, "cdc-current", 5)

        val result = svc.runOnce("exp-floor", batchSize = 100)
        assertThat(result.snapshotsExpired).isEqualTo(3)
        assertThat(result.newEarliestSnapshotId).isEqualTo(3)
        assertThat(result.flooredByConsumer).isEqualTo("cdc-lagging")
        assertThat(snapshotIds(catalogId)).containsExactly(3L, 4L, 5L)

        // Still pinned: the next sweep does zero work but keeps naming
        // the pinning consumer (page-worthy).
        val pinned = svc.runOnce("exp-floor", batchSize = 100)
        assertThat(pinned.snapshotsExpired).isEqualTo(0)
        assertThat(pinned.newEarliestSnapshotId).isEqualTo(3)
        assertThat(pinned.flooredByConsumer).isEqualTo("cdc-lagging")

        // Consumer advances -> the floor follows.
        jdbi.useHandleUnchecked { h ->
            h.execute(
                "UPDATE hog_consumer_offset SET committed_snapshot = 5 " +
                    "WHERE catalog_id = ? AND consumer_id = 'cdc-lagging'",
                catalogId,
            )
        }
        val advanced = svc.runOnce("exp-floor", batchSize = 100)
        assertThat(advanced.newEarliestSnapshotId).isEqualTo(5)
        assertThat(advanced.flooredByConsumer).isNull() // ties with head don't count as floored
    }

    @Test
    fun `consumer_floor false ignores offsets`() {
        val catalogId = seedCatalog("exp-nofloor", head = 5, retentionSeconds = 60, consumerFloor = false)
        seedOffset(catalogId, "cdc-ignored", 2)

        val result = svc.runOnce("exp-nofloor", batchSize = 100)
        assertThat(result.snapshotsExpired).isEqualTo(5)
        assertThat(result.newEarliestSnapshotId).isEqualTo(5)
        assertThat(result.flooredByConsumer).isNull()
        assertThat(snapshotIds(catalogId)).containsExactly(5L)
    }

    @Test
    fun `offset on a dropped-but-extant table still pins the floor`() {
        // Documented semantics: offsets survive drops (consumers must SEE
        // incarnation changes), so a dropped table's offset keeps pinning
        // retention as long as the hog_table identity row exists.
        val catalogId = seedCatalog("exp-dropped-pin", head = 5, retentionSeconds = 60)
        seedOffset(catalogId, "cdc-on-dropped", 2, dropped = true)

        val result = svc.runOnce("exp-dropped-pin", batchSize = 100)
        assertThat(result.newEarliestSnapshotId).isEqualTo(2)
        assertThat(result.flooredByConsumer).isEqualTo("cdc-on-dropped")
        assertThat(snapshotIds(catalogId)).containsExactly(2L, 3L, 4L, 5L)
    }

    @Test
    fun `offset whose table row no longer exists cannot pin the floor`() {
        // The floor query joins hog_consumer_offset to hog_table: an
        // offset stranded on a table identity that expired away entirely
        // must not hold retention hostage forever.
        val catalogId = seedCatalog("exp-orphan-nopin", head = 5, retentionSeconds = 60)
        seedOrphanOffset(catalogId, "cdc-orphaned", 1)

        val result = svc.runOnce("exp-orphan-nopin", batchSize = 100)
        assertThat(result.newEarliestSnapshotId).isEqualTo(5)
        assertThat(result.flooredByConsumer).isNull()
        assertThat(snapshotIds(catalogId)).containsExactly(5L)
    }

    @Test
    fun `batch size makes expiry incremental across sweeps`() {
        val catalogId = seedCatalog("exp-batch", head = 5, retentionSeconds = 60)

        val r1 = svc.runOnce("exp-batch", batchSize = 2)
        assertThat(r1.snapshotsExpired).isEqualTo(2)
        assertThat(r1.newEarliestSnapshotId).isEqualTo(2)
        assertThat(earliest(catalogId)).isEqualTo(2)

        val r2 = svc.runOnce("exp-batch", batchSize = 2)
        assertThat(r2.snapshotsExpired).isEqualTo(2)
        assertThat(r2.newEarliestSnapshotId).isEqualTo(4)

        val r3 = svc.runOnce("exp-batch", batchSize = 2)
        assertThat(r3.snapshotsExpired).isEqualTo(1) // head caps below the batch
        assertThat(r3.newEarliestSnapshotId).isEqualTo(5)

        val r4 = svc.runOnce("exp-batch", batchSize = 2)
        assertThat(r4.snapshotsExpired).isEqualTo(0)
        assertThat(snapshotIds(catalogId)).containsExactly(5L)
    }

    @Test
    fun `unreachable files are queued and their rows deleted - live files untouched`() {
        val catalogId = seedCatalog("exp-files", head = 5, retentionSeconds = 60)
        seedTable(catalogId)
        // df1: end-snapshotted below the new floor -> queued.
        seedDataFile(catalogId, 1, endSnapshot = 3, path = "s3://b/df1")
        // df2: live -> never queued.
        seedDataFile(catalogId, 2, endSnapshot = null, path = "s3://b/df2")
        // df3: expiring data file carrying a still-live DV -> both queued
        // (the DV would otherwise be cascade-deleted un-queued).
        seedDataFile(catalogId, 3, endSnapshot = 3, path = "s3://b/df3")
        seedDeleteFile(catalogId, 30, dataFileId = 3, endSnapshot = null, path = "s3://b/dv30")
        // dv1: superseded DV on the live df2 -> queued; dv2: its live successor -> stays.
        seedDeleteFile(catalogId, 10, dataFileId = 2, endSnapshot = 3, path = "s3://b/dv10")
        seedDeleteFile(catalogId, 11, dataFileId = 2, endSnapshot = null, path = "s3://b/dv11")
        // Dependent rows that must cascade with df1.
        jdbi.useHandleUnchecked { h ->
            h.execute(
                "INSERT INTO hog_file_column_stats (catalog_id, data_file_id, field_id, value_count, null_count) " +
                    "VALUES (?, 1, 1, 10, 0)",
                catalogId,
            )
            h.execute(
                "INSERT INTO hog_file_partition_value (catalog_id, data_file_id, key_index, value) " +
                    "VALUES (?, 1, 0, 'x')",
                catalogId,
            )
        }

        val result = svc.runOnce("exp-files", batchSize = 100)
        assertThat(result.newEarliestSnapshotId).isEqualTo(5)
        assertThat(result.dataFilesQueued).isEqualTo(2)
        assertThat(result.deleteFilesQueued).isEqualTo(2)

        val queue = removalQueue(catalogId)
        assertThat(queue.filter { it.second == "data" }.map { it.first })
            .containsExactlyInAnyOrder("s3://b/df1", "s3://b/df3")
        assertThat(queue.filter { it.second == "delete" }.map { it.first })
            .containsExactlyInAnyOrder("s3://b/dv30", "s3://b/dv10")
        assertThat(queue.map { it.third }).containsOnly("snapshot_expiry")

        // Rows gone, live rows intact, dependents cascaded.
        assertThat(dataFilePaths(catalogId)).containsExactly("s3://b/df2")
        assertThat(deleteFilePaths(catalogId)).containsExactly("s3://b/dv11")
        jdbi.withHandleUnchecked { h ->
            val stats =
                h.createQuery("SELECT count(*) FROM hog_file_column_stats WHERE catalog_id = ?")
                    .bind(0, catalogId).mapTo(Long::class.java).one()
            val pv =
                h.createQuery("SELECT count(*) FROM hog_file_partition_value WHERE catalog_id = ?")
                    .bind(0, catalogId).mapTo(Long::class.java).one()
            assertThat(stats).isEqualTo(0)
            assertThat(pv).isEqualTo(0)
        }
    }

    @Test
    fun `file whose end_snapshot equals the new floor is unreachable and queued`() {
        val catalogId = seedCatalog("exp-boundary", head = 5, retentionSeconds = 60)
        seedTable(catalogId)
        seedDataFile(catalogId, 1, endSnapshot = 5, path = "s3://b/at-floor") // invisible at S>=5
        seedDataFile(catalogId, 2, endSnapshot = 6, path = "s3://b/past-floor") // visible at S=5

        val result = svc.runOnce("exp-boundary", batchSize = 100)
        assertThat(result.newEarliestSnapshotId).isEqualTo(5)
        assertThat(result.dataFilesQueued).isEqualTo(1)
        assertThat(removalQueue(catalogId).map { it.first }).containsExactly("s3://b/at-floor")
        assertThat(dataFilePaths(catalogId)).containsExactly("s3://b/past-floor")
    }

    @Test
    fun `floor advance captures the new floor snapshot's time`() {
        val catalogId = seedCatalog("exp-floortime", head = 5, retentionSeconds = 60)
        // Never advanced -> null (the "expiry never ran" contract).
        assertThat(earliestTime(catalogId)).isNull()

        val result = svc.runOnce("exp-floortime", batchSize = 100)
        assertThat(result.newEarliestSnapshotId).isEqualTo(5)
        val expected =
            jdbi.withHandleUnchecked { h ->
                h.createQuery(
                    "SELECT snapshot_time FROM hog_snapshot WHERE catalog_id = ? AND snapshot_id = 5",
                ).bind(0, catalogId).mapTo(java.time.OffsetDateTime::class.java).one()
            }
        assertThat(earliestTime(catalogId)).isEqualTo(expected.toInstant())

        // A zero-work sweep leaves it untouched.
        svc.runOnce("exp-floortime", batchSize = 100)
        assertThat(earliestTime(catalogId)).isEqualTo(expected.toInstant())
    }

    /**
     * The fifth expiry step: versioned DDL rows with end_snapshot <= the
     * new floor are unreachable (visible at S iff S < end; every valid
     * S >= floor) and deleted; live rows and rows ending above the floor
     * survive, so time travel at every retained snapshot is intact.
     */
    @Test
    fun `versioned-row retention deletes below-floor corpses and cascades spec fields`() {
        val catalogId = seedCatalog("exp-vrows", head = 5, retentionSeconds = 60)
        seedTable(catalogId)
        jdbi.useHandleUnchecked { h ->
            h.execute(
                "INSERT INTO hog_namespace (catalog_id, namespace_id, name) VALUES (?, 1, 'ns')",
                catalogId,
            )
            // hog_table_version: corpse (ends at 3 <= floor 5), history row
            // ending above the floor, and the live row.
            h.execute(
                "INSERT INTO hog_table_version " +
                    "(catalog_id, table_id, begin_snapshot, end_snapshot, namespace_id, name) " +
                    "VALUES (?, 1, 1, 3, 1, 'old_name'), (?, 1, 3, 6, 1, 'mid_name')",
                catalogId,
                catalogId,
            )
            h.execute(
                "INSERT INTO hog_table_version (catalog_id, table_id, begin_snapshot, namespace_id, name) " +
                    "VALUES (?, 1, 6, 1, 't')",
                catalogId,
            )
            // hog_column: corpse + live (distinct ordinals — live unique index).
            h.execute(
                "INSERT INTO hog_column " +
                    "(catalog_id, table_id, field_id, begin_snapshot, end_snapshot, name, col_type, ordinal) " +
                    "VALUES (?, 1, 1, 1, 4, 'dropped_col', 'long', 0)",
                catalogId,
            )
            h.execute(
                "INSERT INTO hog_column (catalog_id, table_id, field_id, begin_snapshot, name, col_type, ordinal) " +
                    "VALUES (?, 1, 2, 4, 'id', 'long', 1)",
                catalogId,
            )
            // hog_partition_spec corpse WITH a field row that must CASCADE.
            h.execute(
                "INSERT INTO hog_partition_spec (catalog_id, table_id, spec_id, begin_snapshot, end_snapshot) " +
                    "VALUES (?, 1, 1, 1, 5)",
                catalogId,
            )
            h.execute(
                "INSERT INTO hog_partition_field " +
                    "(catalog_id, table_id, spec_id, key_index, source_field_id, transform) " +
                    "VALUES (?, 1, 1, 0, 2, 'identity')",
                catalogId,
            )
            // hog_sort_spec corpse WITH a field row, plus a live sort spec.
            h.execute(
                "INSERT INTO hog_sort_spec (catalog_id, table_id, sort_id, begin_snapshot, end_snapshot) " +
                    "VALUES (?, 1, 1, 1, 2)",
                catalogId,
            )
            h.execute(
                "INSERT INTO hog_sort_field " +
                    "(catalog_id, table_id, sort_id, key_index, source_field_id, direction, null_order) " +
                    "VALUES (?, 1, 1, 0, 2, 'asc', 'nulls_first')",
                catalogId,
            )
            h.execute(
                "INSERT INTO hog_sort_spec (catalog_id, table_id, sort_id, begin_snapshot) VALUES (?, 1, 2, 2)",
                catalogId,
            )
            // hog_view corpse + live view.
            h.execute(
                "INSERT INTO hog_view (catalog_id, view_id, namespace_id, name, sql, begin_snapshot, end_snapshot) " +
                    "VALUES (?, 1, 1, 'dead_v', 'SELECT 1', 1, 2)",
                catalogId,
            )
            h.execute(
                "INSERT INTO hog_view (catalog_id, view_id, namespace_id, name, sql, begin_snapshot) " +
                    "VALUES (?, 2, 1, 'v', 'SELECT 2', 2)",
                catalogId,
            )
        }

        val result = svc.runOnce("exp-vrows", batchSize = 100)
        assertThat(result.newEarliestSnapshotId).isEqualTo(5)

        fun names(query: String): List<String> =
            jdbi.withHandleUnchecked { h ->
                h.createQuery(query).bind(0, catalogId).mapTo(String::class.java).list()
            }
        // Corpses gone; the row ending ABOVE the floor (visible at S=5) and
        // live rows survive — time travel at every retained snapshot intact.
        assertThat(names("SELECT name FROM hog_table_version WHERE catalog_id = ? ORDER BY begin_snapshot"))
            .containsExactly("mid_name", "t")
        assertThat(names("SELECT name FROM hog_column WHERE catalog_id = ? ORDER BY field_id"))
            .containsExactly("id")
        assertThat(names("SELECT name FROM hog_view WHERE catalog_id = ? ORDER BY view_id"))
            .containsExactly("v")
        jdbi.withHandleUnchecked { h ->
            fun count(table: String): Long =
                h.createQuery("SELECT count(*) FROM $table WHERE catalog_id = ?")
                    .bind(0, catalogId).mapTo(Long::class.java).one()
            assertThat(count("hog_partition_spec")).isZero()
            assertThat(count("hog_partition_field")).describedAs("cascaded with its spec").isZero()
            assertThat(count("hog_sort_spec")).isEqualTo(1)
            assertThat(count("hog_sort_field")).describedAs("cascaded with its spec").isZero()
        }
    }

    @Test
    fun `a sweep that does not advance the floor deletes no versioned rows`() {
        // Everything fresh: newEarliest == earliest, zero work — the
        // below-floor corpse (end_snapshot 0... none possible) and any
        // end-snapshotted row must survive untouched.
        val catalogId = seedCatalog("exp-vrows-noop", head = 5, retentionSeconds = 86_400) { 0 }
        seedTable(catalogId)
        jdbi.useHandleUnchecked { h ->
            h.execute(
                "INSERT INTO hog_column " +
                    "(catalog_id, table_id, field_id, begin_snapshot, end_snapshot, name, col_type, ordinal) " +
                    "VALUES (?, 1, 1, 0, 1, 'ended_early', 'long', 0)",
                catalogId,
            )
        }
        val result = svc.runOnce("exp-vrows-noop", batchSize = 100)
        assertThat(result.snapshotsExpired).isEqualTo(0)
        jdbi.withHandleUnchecked { h ->
            val cols =
                h.createQuery("SELECT count(*) FROM hog_column WHERE catalog_id = ?")
                    .bind(0, catalogId).mapTo(Long::class.java).one()
            assertThat(cols).isEqualTo(1)
        }
    }

    /**
     * Service-level DDL churn: repeated renames pile up end-snapshotted
     * hog_table_version / hog_column rows; expiry past the churn deletes
     * exactly the unreachable ones while every retained snapshot still
     * resolves the correct historical shape.
     */
    @Test
    fun `DDL churn corpses are reclaimed and retained-snapshot time travel is intact`() {
        val catalogs = CatalogService(jdbi)
        val alter = AlterService(jdbi)
        val cat = "exp-churn"
        val catalogId = catalogs.createCatalog(cat, "s3://bucket/churn").catalogId // S0
        catalogs.createNamespace(cat, "ns") // S1
        catalogs.createTable(
            cat,
            "ns",
            "t0",
            listOf(
                com.posthog.hoglake.model.ColumnDef("c0", com.posthog.hoglake.model.ColType.LONG),
            ),
        ) // S2
        // Rename storm: table t0->t1->...->t5, column c0->c1->...->c5 (S3..S12).
        var table = "t0"
        for (i in 1..5) {
            alter.alterTable(cat, "ns", table, listOf(com.posthog.hoglake.model.AlterOp.RenameTable("t$i")))
            table = "t$i"
            alter.alterTable(
                cat,
                "ns",
                table,
                listOf(com.posthog.hoglake.model.AlterOp.RenameColumn("c${i - 1}", "c$i")),
            )
        }
        check(catalogs.getCatalog(cat).headSnapshotId == 12L)

        // Age snapshots 0..9 out of retention; keep 10..12 fresh.
        jdbi.useHandleUnchecked { h ->
            h.execute(
                "UPDATE hog_snapshot SET snapshot_time = now() - interval '1 hour' " +
                    "WHERE catalog_id = ? AND snapshot_id <= 9",
                catalogId,
            )
            h.execute(
                "UPDATE hog_catalog SET snapshot_retention_seconds = 60 WHERE catalog_id = ?",
                catalogId,
            )
        }
        val result = svc.runOnce(cat, batchSize = 100)
        assertThat(result.newEarliestSnapshotId).isEqualTo(10)

        // Corpses below the floor are gone...
        jdbi.withHandleUnchecked { h ->
            val versionEnds =
                h.createQuery(
                    "SELECT end_snapshot FROM hog_table_version WHERE catalog_id = ? AND end_snapshot IS NOT NULL",
                ).bind(0, catalogId).mapTo(Long::class.java).list()
            assertThat(versionEnds).allSatisfy { assertThat(it).isGreaterThan(10L) }
            val columnEnds =
                h.createQuery(
                    "SELECT end_snapshot FROM hog_column WHERE catalog_id = ? AND end_snapshot IS NOT NULL",
                ).bind(0, catalogId).mapTo(Long::class.java).list()
            assertThat(columnEnds).allSatisfy { assertThat(it).isGreaterThan(10L) }
        }
        // ...and every retained snapshot still resolves its exact shape
        // (timeline: table renames at S3,5,7,9,11; column renames at
        // S4,6,8,10,12) — asserted via the service, which applies the
        // visibility rule.
        assertThat(catalogs.getTable(cat, "ns", "t5", snapshot = 12).columns.single().def.name)
            .isEqualTo("c5")
        val at10 = catalogs.getTable(cat, "ns", "t4", snapshot = 10) // t4->t5 lands at S11
        assertThat(at10.columns.single().def.name).isEqualTo("c4")
        val at11 = catalogs.getTable(cat, "ns", "t5", snapshot = 11) // c4->c5 lands at S12
        assertThat(at11.columns.single().def.name).isEqualTo("c4")
        // Below the floor: 410, not silent wrong answers.
        assertThatThrownBy { catalogs.getTable(cat, "ns", "t5", snapshot = 9) }
            .isInstanceOf(HoglakeException.Expired::class.java)
    }

    @Test
    fun `invalid inputs - unknown catalog and non-positive batch`() {
        assertThatThrownBy { svc.runOnce("exp-nope", 100) }
            .isInstanceOf(HoglakeException.NotFound::class.java)
        assertThatThrownBy { svc.runOnce("exp-nope", 0) }
            .isInstanceOf(HoglakeException.Validation::class.java)
    }

    @Test
    fun `runOnceAllCatalogs sweeps every catalog independently`() {
        val idA = seedCatalog("exp-all-a", head = 3, retentionSeconds = 60)
        val idB = seedCatalog("exp-all-b", head = 4, retentionSeconds = null)

        val results = svc.runOnceAllCatalogs(batchSize = 100).toMap()
        assertThat(results["exp-all-a"]!!.newEarliestSnapshotId).isEqualTo(3)
        assertThat(results["exp-all-b"]!!.snapshotsExpired).isEqualTo(0)
        assertThat(earliest(idA)).isEqualTo(3)
        assertThat(earliest(idB)).isEqualTo(0)
    }

    @Test
    fun `background loop expires and a non-positive interval is a no-op`() {
        com.posthog.hoglake.BackgroundLoops().use { it.register("expiry", 0) { svc.runOnceAllCatalogs(100) } }
        com.posthog.hoglake.BackgroundLoops().use { it.register("expiry", -1) { svc.runOnceAllCatalogs(100) } }

        val catalogId = seedCatalog("exp-loop", head = 4, retentionSeconds = 60)
        com.posthog.hoglake.BackgroundLoops().use { loops ->
            loops.register("expiry", 50) { svc.runOnceAllCatalogs(100) }
            await().atMost(Duration.ofSeconds(30)).untilAsserted {
                assertThat(earliest(catalogId)).isEqualTo(4)
            }
        }
    }
}
