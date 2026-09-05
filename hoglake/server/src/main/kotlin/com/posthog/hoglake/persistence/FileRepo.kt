package com.posthog.hoglake.persistence

import com.posthog.hoglake.model.DataFile
import com.posthog.hoglake.model.StatsState
import org.jdbi.v3.core.Handle
import org.jdbi.v3.core.mapper.RowMapper

/**
 * hog_data_file reads for planning (listFiles) and the changefeed, plus
 * the drop-time end-snapshotting. File INSERTs belong to the commit
 * service, not here.
 */
object FileRepo {

    private val fileMapper = RowMapper { rs, _ ->
        DataFile(
            dataFileId = rs.getLong("data_file_id"),
            tableId = rs.getLong("table_id"),
            path = rs.getString("path"),
            fileFormat = rs.getString("file_format"),
            recordCount = rs.getLong("record_count"),
            fileSizeBytes = rs.getLong("file_size_bytes"),
            footerSize = rs.getObject("footer_size")?.let { (it as Number).toLong() },
            rowIdStart = rs.getLong("row_id_start"),
            statsState = StatsState.fromWire(rs.getString("stats_state")),
            beginSnapshot = rs.getLong("begin_snapshot"),
            specId = rs.getObject("spec_id")?.let { (it as Number).toLong() },
            partitionValues = (rs.getArray("partition_values")?.array as? Array<*>)
                ?.map { it as String? }
                ?.takeIf { it.isNotEmpty() },
        )
    }

    private const val COLUMNS =
        """f.data_file_id, f.table_id, f.path, f.file_format, f.record_count,
           f.file_size_bytes, f.footer_size, f.row_id_start, f.stats_state,
           f.begin_snapshot, f.spec_id,
           (SELECT array_agg(pv.value ORDER BY pv.key_index)
            FROM hog_file_partition_value pv
            WHERE pv.catalog_id = f.catalog_id
              AND pv.data_file_id = f.data_file_id) AS partition_values"""

    /** Files visible at [snapshot], in row-id order. */
    fun listAt(handle: Handle, catalogId: Long, tableId: Long, snapshot: Long): List<DataFile> =
        handle.createQuery(
            """
            SELECT $COLUMNS
            FROM hog_data_file f
            WHERE f.catalog_id = :catalogId AND f.table_id = :tableId
              AND f.begin_snapshot <= :snapshot
              AND (f.end_snapshot IS NULL OR :snapshot < f.end_snapshot)
            ORDER BY begin_snapshot, row_id_start, data_file_id
            """,
        )
            .bind("catalogId", catalogId)
            .bind("tableId", tableId)
            .bind("snapshot", snapshot)
            .map(fileMapper)
            .list()

    /** Count of files visible at [snapshot]. */
    fun countAt(handle: Handle, catalogId: Long, tableId: Long, snapshot: Long): Long =
        handle.createQuery(
            """
            SELECT count(*) FROM hog_data_file
            WHERE catalog_id = :catalogId AND table_id = :tableId
              AND begin_snapshot <= :snapshot
              AND (end_snapshot IS NULL OR :snapshot < end_snapshot)
            """,
        )
            .bind("catalogId", catalogId)
            .bind("tableId", tableId)
            .bind("snapshot", snapshot)
            .mapTo(Long::class.javaObjectType)
            .one()

    /**
     * Changefeed range read: files appended in (from, to] — i.e.
     * begin_snapshot > [fromSnapshot] AND begin_snapshot <= [toSnapshot]
     * — ordered by begin_snapshot, row_id_start. No end_snapshot filter:
     * the feed reports what was appended in the range, regardless of
     * later lifecycle.
     */
    fun changedIn(
        handle: Handle,
        catalogId: Long,
        tableId: Long,
        fromSnapshot: Long,
        toSnapshot: Long,
    ): List<DataFile> =
        handle.createQuery(
            """
            SELECT $COLUMNS
            FROM hog_data_file f
            WHERE f.catalog_id = :catalogId AND f.table_id = :tableId
              AND f.begin_snapshot > :fromSnapshot
              AND f.begin_snapshot <= :toSnapshot
            ORDER BY begin_snapshot, row_id_start, data_file_id
            """,
        )
            .bind("catalogId", catalogId)
            .bind("tableId", tableId)
            .bind("fromSnapshot", fromSnapshot)
            .bind("toSnapshot", toSnapshot)
            .map(fileMapper)
            .list()

    /** Drop tail: end-snapshot every live file of the table. */
    fun endLiveFiles(handle: Handle, catalogId: Long, tableId: Long, snapshot: Long): Int =
        handle.createUpdate(
            """
            UPDATE hog_data_file SET end_snapshot = :snapshot
            WHERE catalog_id = :catalogId AND table_id = :tableId AND end_snapshot IS NULL
            """,
        )
            .bind("catalogId", catalogId)
            .bind("tableId", tableId)
            .bind("snapshot", snapshot)
            .execute()
}
