package com.posthog.hoglake.service

import com.posthog.hoglake.model.ColType
import com.posthog.hoglake.model.ColumnDef
import com.posthog.hoglake.model.HoglakeException
import com.posthog.hoglake.model.StatsState
import com.posthog.hoglake.testing.PgTestSupport
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.jdbi.v3.core.kotlin.withHandleUnchecked
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance

/**
 * listFiles + changes() range semantics. Data files are inserted
 * directly via SQL (the commit service is another agent's scope);
 * snapshots are advanced with real DDL so head stays honest.
 *
 * Timeline built in [setUp]:
 *   S0 catalog, S1 namespace, S2 table t, S3-S5 padding DDL.
 *   Files: f1@S3 (rows 0..), f2@S4 (rows 100..), f4@S4 (rows 150..),
 *          f3@S5 (rows 200..).
 */
@Tag("integration")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class ChangefeedIntegrationTest {
    private val db = PgTestSupport.freshDatabase()
    private val svc = CatalogService(db.jdbi)
    private val idCol = ColumnDef("id", ColType.LONG, nullable = false)

    private val cat = "feed-cat"
    private var catalogId = 0L
    private var tableId = 0L

    @BeforeAll
    fun setUp() {
        catalogId = svc.createCatalog(cat, "s3://bucket/feed").catalogId // S0
        svc.createNamespace(cat, "ns") // S1
        tableId = svc.createTable(cat, "ns", "t", listOf(idCol)).tableId // S2
        // Padding DDL to mint S3..S5.
        svc.createTable(cat, "ns", "pad3", listOf(idCol))
        svc.createTable(cat, "ns", "pad4", listOf(idCol))
        svc.createTable(cat, "ns", "pad5", listOf(idCol))
        check(svc.getCatalog(cat).headSnapshotId == 5L)

        insertFile(fileId = 1, begin = 3, rowIdStart = 0)
        insertFile(fileId = 2, begin = 4, rowIdStart = 100)
        insertFile(fileId = 4, begin = 4, rowIdStart = 150)
        insertFile(fileId = 3, begin = 5, rowIdStart = 200)
    }

    @AfterAll
    fun tearDown() = db.close()

    private fun insertFile(
        fileId: Long,
        begin: Long,
        rowIdStart: Long,
    ) {
        db.jdbi.withHandleUnchecked { h ->
            h.createUpdate(
                """
                INSERT INTO hog_data_file
                    (catalog_id, data_file_id, table_id, begin_snapshot, path,
                     record_count, file_size_bytes, row_id_start)
                VALUES (:cid, :fid, :tid, :begin, :path, 50, 4096, :rowIdStart)
                """,
            )
                .bind("cid", catalogId)
                .bind("fid", fileId)
                .bind("tid", tableId)
                .bind("begin", begin)
                .bind("path", "s3://bucket/feed/f$fileId.parquet")
                .bind("rowIdStart", rowIdStart)
                .execute()
        }
    }

    @Test
    fun `listFiles honors time travel and orders by append position`() {
        // Head (S5): everything, ordered by (begin_snapshot, row_id_start).
        val atHead = svc.listFiles(cat, "ns", "t")
        assertThat(atHead.map { it.dataFileId }).containsExactly(1L, 2L, 4L, 3L)
        assertThat(atHead).allSatisfy {
            assertThat(it.statsState).isEqualTo(StatsState.PROVIDED)
            assertThat(it.tableId).isEqualTo(tableId)
        }

        assertThat(svc.listFiles(cat, "ns", "t", snapshot = 3).map { it.dataFileId })
            .containsExactly(1L)
        assertThat(svc.listFiles(cat, "ns", "t", snapshot = 4).map { it.dataFileId })
            .containsExactly(1L, 2L, 4L)
        assertThat(svc.listFiles(cat, "ns", "t", snapshot = 2)).isEmpty()
    }

    @Test
    fun `changes returns files in the half-open range from-exclusive to-inclusive`() {
        val head = svc.getCatalog(cat).headSnapshotId

        // Default to = head; from is exclusive.
        // [changes() now returns ChangesPlan instead of Triple/LongRange.]
        val plan = svc.changes(cat, "ns", "t", fromSnapshot = 3)
        assertThat(plan.tableUuid).isEqualTo(svc.getTable(cat, "ns", "t").tableUuid)
        assertThat(plan.fromSnapshot).isEqualTo(3L)
        assertThat(plan.toSnapshot).isEqualTo(head)
        assertThat(plan.files.map { it.dataFileId }).containsExactly(2L, 4L, 3L)

        // Explicit sub-range.
        val sub = svc.changes(cat, "ns", "t", fromSnapshot = 3, toSnapshot = 4)
        assertThat(sub.fromSnapshot).isEqualTo(3L)
        assertThat(sub.toSnapshot).isEqualTo(4L)
        assertThat(sub.files.map { it.dataFileId }).containsExactly(2L, 4L)

        // From 0 catches everything appended so far.
        val all = svc.changes(cat, "ns", "t", fromSnapshot = 0)
        assertThat(all.files.map { it.dataFileId }).containsExactly(1L, 2L, 4L, 3L)

        // Empty range: from == to.
        val none = svc.changes(cat, "ns", "t", fromSnapshot = head, toSnapshot = head)
        assertThat(none.files).isEmpty()
    }

    @Test
    fun `changes validates its range`() {
        val head = svc.getCatalog(cat).headSnapshotId
        assertThatThrownBy { svc.changes(cat, "ns", "t", fromSnapshot = -1) }
            .isInstanceOf(HoglakeException.Validation::class.java)
        assertThatThrownBy { svc.changes(cat, "ns", "t", fromSnapshot = head + 1) }
            .isInstanceOf(HoglakeException.Validation::class.java)
        assertThatThrownBy { svc.changes(cat, "ns", "t", fromSnapshot = 0, toSnapshot = head + 1) }
            .isInstanceOf(HoglakeException.Validation::class.java)
        assertThatThrownBy { svc.changes(cat, "ns", "t", fromSnapshot = 4, toSnapshot = 3) }
            .isInstanceOf(HoglakeException.Validation::class.java)
        assertThatThrownBy { svc.changes(cat, "ns", "nope", fromSnapshot = 0) }
            .isInstanceOf(HoglakeException.NotFound::class.java)
    }

    @Test
    fun `dropped table stays readable in the past but not at head`() {
        // Own fixture so the shared timeline stays intact.
        svc.createNamespace(cat, "drop-ns")
        val t = svc.createTable(cat, "drop-ns", "d", listOf(idCol))
        val createdAt = svc.getCatalog(cat).headSnapshotId
        db.jdbi.withHandleUnchecked { h ->
            h.createUpdate(
                """
                INSERT INTO hog_data_file
                    (catalog_id, data_file_id, table_id, begin_snapshot, path,
                     record_count, file_size_bytes, row_id_start)
                VALUES (:cid, 900, :tid, :begin, 's3://bucket/feed/d.parquet', 5, 128, 0)
                """,
            ).bind("cid", catalogId).bind("tid", t.tableId).bind("begin", createdAt).execute()
        }
        val drop = svc.dropTable(cat, "drop-ns", "d")

        // At head the table no longer resolves, for files or changes.
        assertThatThrownBy { svc.listFiles(cat, "drop-ns", "d") }
            .isInstanceOf(HoglakeException.NotFound::class.java)
        assertThatThrownBy { svc.changes(cat, "drop-ns", "d", fromSnapshot = 0) }
            .isInstanceOf(HoglakeException.NotFound::class.java)

        // At the pre-drop snapshot everything is still visible, even
        // though the drop end-snapshotted the file rows.
        assertThat(svc.listFiles(cat, "drop-ns", "d", snapshot = createdAt)).hasSize(1)
        // [changes() now returns ChangesPlan instead of Triple.]
        val past =
            svc.changes(cat, "drop-ns", "d", fromSnapshot = 0, toSnapshot = drop.snapshotId - 1)
        assertThat(past.tableUuid).isEqualTo(t.tableUuid)
        assertThat(past.files).hasSize(1)
    }
}
