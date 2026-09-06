package com.posthog.hoglake.persistence

import com.posthog.hoglake.model.DeleteFile
import org.jdbi.v3.core.Handle
import org.jdbi.v3.core.mapper.RowMapper

/**
 * hog_delete_file range reads for the changefeed. DV INSERTs and the
 * supersession chain belong to the commit service; the per-snapshot
 * live-DV join belongs to ScanService. This repo only answers "which
 * DVs were registered in a snapshot range".
 */
object DeleteFileReadRepo {
    private val deleteFileMapper =
        RowMapper { rs, _ ->
            DeleteFile(
                deleteFileId = rs.getLong("delete_file_id"),
                dataFileId = rs.getLong("data_file_id"),
                path = rs.getString("path"),
                fileFormat = rs.getString("file_format"),
                deleteCount = rs.getLong("delete_count"),
                fileSizeBytes = rs.getLong("file_size_bytes"),
                beginSnapshot = rs.getLong("begin_snapshot"),
            )
        }

    /**
     * Changefeed deletions feed: DVs registered in (from, to] —
     * begin_snapshot > [fromSnapshot] AND begin_snapshot <= [toSnapshot]
     * — ordered by begin_snapshot, delete_file_id. No end_snapshot
     * filter, mirroring [FileRepo.changedIn]: the feed reports what was
     * registered in the range regardless of later supersession.
     */
    fun changedIn(
        handle: Handle,
        catalogId: Long,
        tableId: Long,
        fromSnapshot: Long,
        toSnapshot: Long,
    ): List<DeleteFile> =
        handle.createQuery(
            """
            SELECT delete_file_id, data_file_id, path, file_format,
                   delete_count, file_size_bytes, begin_snapshot
            FROM hog_delete_file
            WHERE catalog_id = :catalogId AND table_id = :tableId
              AND begin_snapshot > :fromSnapshot
              AND begin_snapshot <= :toSnapshot
            ORDER BY begin_snapshot, delete_file_id
            """,
        )
            .bind("catalogId", catalogId)
            .bind("tableId", tableId)
            .bind("fromSnapshot", fromSnapshot)
            .bind("toSnapshot", toSnapshot)
            .map(deleteFileMapper)
            .list()
}
