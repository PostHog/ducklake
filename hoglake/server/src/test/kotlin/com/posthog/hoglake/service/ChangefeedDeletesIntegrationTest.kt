package com.posthog.hoglake.service

import com.posthog.hoglake.model.ColType
import com.posthog.hoglake.model.ColumnDef
import com.posthog.hoglake.testing.PgTestSupport
import org.assertj.core.api.Assertions.assertThat
import org.jdbi.v3.core.kotlin.withHandleUnchecked
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance

/**
 * Deletions-aware changefeed: ChangesPlan.deleteFiles carries the DVs
 * registered in (from, to], split from the appended files, both lists
 * honoring the same half-open boundaries. Rows are inserted directly
 * via SQL (the commit service has its own coverage); snapshots are
 * advanced with real DDL so head stays honest.
 *
 * Timeline: S0 catalog, S1 namespace, S2 table t, S3-S5 padding DDL.
 *   Data files: f1@S3, f2@S4.
 *   DVs: dv10 on f1 @S4 (superseded at S5), dv11 on f1 @S5, dv12 on f2 @S5.
 */
@Tag("integration")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class ChangefeedDeletesIntegrationTest {
    private val db = PgTestSupport.freshDatabase()
    private val svc = CatalogService(db.jdbi)
    private val idCol = ColumnDef("id", ColType.LONG, nullable = false)

    private val cat = "dv-feed-cat"
    private var catalogId = 0L
    private var tableId = 0L

    @BeforeAll
    fun setUp() {
        catalogId = svc.createCatalog(cat, "s3://bucket/dvfeed").catalogId // S0
        svc.createNamespace(cat, "ns") // S1
        tableId = svc.createTable(cat, "ns", "t", listOf(idCol)).tableId // S2
        svc.createTable(cat, "ns", "pad3", listOf(idCol))
        svc.createTable(cat, "ns", "pad4", listOf(idCol))
        svc.createTable(cat, "ns", "pad5", listOf(idCol))
        check(svc.getCatalog(cat).headSnapshotId == 5L)

        insertDataFile(fileId = 1, begin = 3, rowIdStart = 0)
        insertDataFile(fileId = 2, begin = 4, rowIdStart = 100)
        // dv10 was live from S4 until superseded by dv11 at S5.
        insertDeleteFile(deleteFileId = 10, dataFileId = 1, begin = 4, end = 5, deleteCount = 3)
        insertDeleteFile(deleteFileId = 11, dataFileId = 1, begin = 5, end = null, deleteCount = 7)
        insertDeleteFile(deleteFileId = 12, dataFileId = 2, begin = 5, end = null, deleteCount = 2)
    }

    @AfterAll
    fun tearDown() = db.close()

    private fun insertDataFile(
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
                .bind("path", "s3://bucket/dvfeed/f$fileId.parquet")
                .bind("rowIdStart", rowIdStart)
                .execute()
        }
    }

    private fun insertDeleteFile(
        deleteFileId: Long,
        dataFileId: Long,
        begin: Long,
        end: Long?,
        deleteCount: Long,
    ) {
        db.jdbi.withHandleUnchecked { h ->
            h.createUpdate(
                """
                INSERT INTO hog_delete_file
                    (catalog_id, delete_file_id, table_id, data_file_id,
                     begin_snapshot, end_snapshot, path, delete_count, file_size_bytes)
                VALUES (:cid, :dfid, :tid, :dataFid, :begin, :end, :path, :count, 64)
                """,
            )
                .bind("cid", catalogId)
                .bind("dfid", deleteFileId)
                .bind("tid", tableId)
                .bind("dataFid", dataFileId)
                .bind("begin", begin)
                .bind("end", end)
                .bind("path", "s3://bucket/dvfeed/dv$deleteFileId.puffin")
                .bind("count", deleteCount)
                .execute()
        }
    }

    @Test
    fun `plan splits appends and deletes and orders deletes by begin then id`() {
        val plan = svc.changes(cat, "ns", "t", fromSnapshot = 2)
        assertThat(plan.tableUuid).isEqualTo(svc.getTable(cat, "ns", "t").tableUuid)
        assertThat(plan.fromSnapshot).isEqualTo(2L)
        assertThat(plan.toSnapshot).isEqualTo(5L)
        assertThat(plan.files.map { it.dataFileId }).containsExactly(1L, 2L)
        assertThat(plan.deleteFiles.map { it.deleteFileId }).containsExactly(10L, 11L, 12L)
        // The DV rows carry their registration payloads.
        plan.deleteFiles.first().let { dv ->
            assertThat(dv.dataFileId).isEqualTo(1L)
            assertThat(dv.path).isEqualTo("s3://bucket/dvfeed/dv10.puffin")
            assertThat(dv.fileFormat).isEqualTo("puffin-dv")
            assertThat(dv.deleteCount).isEqualTo(3L)
            assertThat(dv.beginSnapshot).isEqualTo(4L)
        }
    }

    @Test
    fun `both lists honor the half-open from-exclusive to-inclusive range`() {
        // (3, 4]: f2 appended at 4; only dv10 registered at 4.
        val mid = svc.changes(cat, "ns", "t", fromSnapshot = 3, toSnapshot = 4)
        assertThat(mid.files.map { it.dataFileId }).containsExactly(2L)
        assertThat(mid.deleteFiles.map { it.deleteFileId }).containsExactly(10L)

        // (4, 5]: no appends; the two DVs registered at 5.
        val tail = svc.changes(cat, "ns", "t", fromSnapshot = 4, toSnapshot = 5)
        assertThat(tail.files).isEmpty()
        assertThat(tail.deleteFiles.map { it.deleteFileId }).containsExactly(11L, 12L)

        // (5, 5]: empty on both sides.
        val none = svc.changes(cat, "ns", "t", fromSnapshot = 5, toSnapshot = 5)
        assertThat(none.files).isEmpty()
        assertThat(none.deleteFiles).isEmpty()

        // (2, 3]: the append at 3 predates every DV.
        val early = svc.changes(cat, "ns", "t", fromSnapshot = 2, toSnapshot = 3)
        assertThat(early.files.map { it.dataFileId }).containsExactly(1L)
        assertThat(early.deleteFiles).isEmpty()
    }

    @Test
    fun `supersession does not hide a DV from the range it was registered in`() {
        // dv10 was end-snapshotted at S5, but the (3, 4] feed still
        // reports it: the feed is registration history, not liveness.
        val plan = svc.changes(cat, "ns", "t", fromSnapshot = 3, toSnapshot = 4)
        assertThat(plan.deleteFiles.map { it.deleteFileId }).containsExactly(10L)
    }
}
