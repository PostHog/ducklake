package com.posthog.hoglake.commit

import com.posthog.hoglake.model.CommitRequest
import com.posthog.hoglake.model.CommitResult
import com.posthog.hoglake.model.FileRegistration
import com.posthog.hoglake.model.HoglakeException
import com.posthog.hoglake.persistence.Locks
import org.jdbi.v3.core.Handle
import org.jdbi.v3.core.Jdbi

/**
 * The append commit (README.md commit-protocol / commit-serialization
 * sections): register client-written parquet files with footer-derived
 * stats, producing one snapshot.
 *
 * One SQL transaction per commit. The tail is serialized per catalog by
 * a Postgres advisory xact lock (v1 takes it up front, before any
 * allocation), so snapshot ids, file ids, and per-table row-id ranges
 * are dense and ordered with commit order — id order = commit order by
 * construction, and per-file row-id ranges are never reused (THE
 * lineage guarantee).
 *
 * Conflict rule (append fast path): appends only conflict with DDL on
 * the tables they touch. With a non-null readSnapshot we look for
 * 'table_dropped' / 'table_altered' changes on the touched tables since
 * that snapshot — one indexed lookup. A null readSnapshot is a blind
 * append with no conflict window.
 */
class CommitService(private val jdbi: Jdbi) {

    fun commit(catalog: String, request: CommitRequest): CommitResult =
        jdbi.inTransaction<CommitResult, RuntimeException> { handle ->
            doCommit(handle, catalog, request)
        }

    private data class ResolvedAppend(
        val namespace: String,
        val table: String,
        val tableId: Long,
        val files: List<FileRegistration>,
    )

    private fun doCommit(h: Handle, catalogName: String, req: CommitRequest): CommitResult {
        // 1. Resolve catalog, then serialize the commit tail.
        val catalogId = h.createQuery("SELECT catalog_id FROM hog_catalog WHERE name = ?")
            .bind(0, catalogName)
            .mapTo(Long::class.java)
            .findOne()
            .orElseThrow { HoglakeException.NotFound("catalog '$catalogName'") }
        Locks.acquireCatalogCommitLock(h, catalogId)

        val (head, schemaVersion) = h.createQuery(
            "SELECT last_snapshot_id, schema_version FROM hog_catalog WHERE catalog_id = ?",
        )
            .bind(0, catalogId)
            .map { rs, _ -> rs.getLong(1) to rs.getLong(2) }
            .one()

        // 2. Merge duplicate (namespace, table) appends, preserving request
        // order (first occurrence for tables, concatenation for files), then
        // resolve each to a live table at head.
        val merged = LinkedHashMap<Pair<String, String>, MutableList<FileRegistration>>()
        for (append in req.appends) {
            merged.getOrPut(append.namespace to append.table) { mutableListOf() }
                .addAll(append.files)
        }
        if (merged.isEmpty()) {
            throw HoglakeException.Validation("commit has no appends")
        }
        val resolved = merged.map { (key, files) ->
            val (namespace, table) = key
            val tableId = resolveLiveTable(h, catalogId, namespace, table)
                ?: throw HoglakeException.Validation("unknown table $namespace.$table")
            ResolvedAppend(namespace, table, tableId, files)
        }

        // 3. Structural validation. Nothing is written unless all of it passes.
        for (append in resolved) {
            validateFiles(h, catalogId, append)
        }

        // 4. Conflict check (append fast path).
        val readSnapshot = req.readSnapshot
        if (readSnapshot != null) {
            if (readSnapshot > head) {
                throw HoglakeException.Validation(
                    "readSnapshot $readSnapshot is ahead of catalog head $head",
                )
            }
            checkConflicts(h, catalogId, readSnapshot, resolved)
        }

        // 5. Allocations, all under the lock via UPDATE..RETURNING. Appends
        // are not DDL: schema_version is NOT bumped.
        val totalFiles = resolved.sumOf { it.files.size }
        val (snapshotId, firstFileId) = h.createQuery(
            """
            UPDATE hog_catalog
               SET last_snapshot_id = last_snapshot_id + 1,
                   next_file_id = next_file_id + ?
             WHERE catalog_id = ?
            RETURNING last_snapshot_id, next_file_id
            """,
        )
            .bind(0, totalFiles)
            .bind(1, catalogId)
            .map { rs, _ -> rs.getLong(1) to (rs.getLong(2) - totalFiles) }
            .one()

        // 6. Writes.
        h.createUpdate(
            """
            INSERT INTO hog_snapshot (catalog_id, snapshot_id, schema_version, author, commit_message)
            VALUES (:catalogId, :snapshotId, :schemaVersion, :author, :message)
            """,
        )
            .bind("catalogId", catalogId)
            .bind("snapshotId", snapshotId)
            .bind("schemaVersion", schemaVersion)
            .bind("author", req.author)
            .bind("message", req.message)
            .execute()

        val changeBatch = h.prepareBatch(
            """
            INSERT INTO hog_snapshot_change (catalog_id, snapshot_id, kind, object_id)
            VALUES (:catalogId, :snapshotId, 'table_inserted_into', :tableId)
            """,
        )
        for (append in resolved) {
            changeBatch
                .bind("catalogId", catalogId)
                .bind("snapshotId", snapshotId)
                .bind("tableId", append.tableId)
                .add()
        }
        changeBatch.execute()

        val fileBatch = h.prepareBatch(
            """
            INSERT INTO hog_data_file (catalog_id, data_file_id, table_id, begin_snapshot,
                                       path, record_count, file_size_bytes, footer_size,
                                       row_id_start, stats_state)
            VALUES (:catalogId, :dataFileId, :tableId, :beginSnapshot,
                    :path, :recordCount, :fileSizeBytes, :footerSize,
                    :rowIdStart, :statsState)
            """,
        )
        val statsBatch = h.prepareBatch(
            """
            INSERT INTO hog_file_column_stats (catalog_id, data_file_id, field_id, value_count,
                                               null_count, nan_count, size_bytes,
                                               lower_bound, upper_bound)
            VALUES (:catalogId, :dataFileId, :fieldId, :valueCount,
                    :nullCount, :nanCount, :sizeBytes,
                    :lowerBound, :upperBound)
            """,
        )
        var nextFileId = firstFileId
        var haveStats = false
        for (append in resolved) {
            val recordSum = append.files.sumOf { it.recordCount }
            val byteSum = append.files.sumOf { it.fileSizeBytes }
            // Per-table row-id range, advanced with the table's rollup in one
            // statement; each file gets a contiguous slice in request order.
            var rowId = h.createQuery(
                """
                UPDATE hog_table_stats
                   SET next_row_id = next_row_id + :records,
                       record_count = record_count + :records,
                       file_size_bytes = file_size_bytes + :bytes
                 WHERE catalog_id = :catalogId AND table_id = :tableId
                RETURNING next_row_id
                """,
            )
                .bind("records", recordSum)
                .bind("bytes", byteSum)
                .bind("catalogId", catalogId)
                .bind("tableId", append.tableId)
                .mapTo(Long::class.java)
                .findOne()
                .orElseThrow {
                    IllegalStateException(
                        "missing hog_table_stats row for table_id=${append.tableId}",
                    )
                } - recordSum

            for (file in append.files) {
                val dataFileId = nextFileId++
                fileBatch
                    .bind("catalogId", catalogId)
                    .bind("dataFileId", dataFileId)
                    .bind("tableId", append.tableId)
                    .bind("beginSnapshot", snapshotId)
                    .bind("path", file.path)
                    .bind("recordCount", file.recordCount)
                    .bind("fileSizeBytes", file.fileSizeBytes)
                    .bind("footerSize", file.footerSize)
                    .bind("rowIdStart", rowId)
                    .bind("statsState", if (file.columnStats != null) "provided" else "pending")
                    .add()
                rowId += file.recordCount
                for (stats in file.columnStats.orEmpty()) {
                    haveStats = true
                    statsBatch
                        .bind("catalogId", catalogId)
                        .bind("dataFileId", dataFileId)
                        .bind("fieldId", stats.fieldId)
                        .bind("valueCount", stats.valueCount)
                        .bind("nullCount", stats.nullCount)
                        .bind("nanCount", stats.nanCount)
                        .bind("sizeBytes", stats.sizeBytes)
                        .bind("lowerBound", stats.lowerBound)
                        .bind("upperBound", stats.upperBound)
                        .add()
                }
            }
        }
        fileBatch.execute()
        if (haveStats) statsBatch.execute()

        return CommitResult(snapshotId, schemaVersion)
    }

    /** A table is live iff its head version row is open and neither it nor its namespace is dropped. */
    private fun resolveLiveTable(
        h: Handle,
        catalogId: Long,
        namespace: String,
        table: String,
    ): Long? = h.createQuery(
        """
        SELECT tv.table_id
          FROM hog_table_version tv
          JOIN hog_namespace ns
            ON ns.catalog_id = tv.catalog_id AND ns.namespace_id = tv.namespace_id
          JOIN hog_table t
            ON t.catalog_id = tv.catalog_id AND t.table_id = tv.table_id
         WHERE tv.catalog_id = :catalogId
           AND tv.end_snapshot IS NULL
           AND tv.name = :table
           AND ns.name = :namespace
           AND NOT ns.dropped
           AND t.dropped_snapshot IS NULL
        """,
    )
        .bind("catalogId", catalogId)
        .bind("namespace", namespace)
        .bind("table", table)
        .mapTo(Long::class.java)
        .findOne()
        .orElse(null)

    private fun validateFiles(h: Handle, catalogId: Long, append: ResolvedAppend) {
        val qualified = "${append.namespace}.${append.table}"
        val liveFieldIds: Set<Long> by lazy {
            h.createQuery(
                """
                SELECT field_id FROM hog_column
                 WHERE catalog_id = ? AND table_id = ? AND end_snapshot IS NULL
                """,
            )
                .bind(0, catalogId)
                .bind(1, append.tableId)
                .mapTo(Long::class.java)
                .toSet()
        }
        for (file in append.files) {
            if (file.path.isBlank()) {
                throw HoglakeException.Validation("blank file path in append to $qualified")
            }
            if (file.recordCount < 0) {
                throw HoglakeException.Validation(
                    "negative record_count for ${file.path} in $qualified",
                )
            }
            if (file.fileSizeBytes < 0) {
                throw HoglakeException.Validation(
                    "negative file_size_bytes for ${file.path} in $qualified",
                )
            }
            val stats = file.columnStats ?: continue
            val seenFieldIds = HashSet<Long>()
            for (stat in stats) {
                if (stat.fieldId !in liveFieldIds) {
                    throw HoglakeException.Validation(
                        "unknown field_id ${stat.fieldId} in stats for ${file.path} in $qualified",
                    )
                }
                if (!seenFieldIds.add(stat.fieldId)) {
                    throw HoglakeException.Validation(
                        "duplicate field_id ${stat.fieldId} in stats for ${file.path} in $qualified",
                    )
                }
                if (stat.valueCount < 0 || stat.nullCount < 0) {
                    throw HoglakeException.Validation(
                        "negative value_count/null_count for field_id ${stat.fieldId} " +
                            "in stats for ${file.path} in $qualified",
                    )
                }
            }
        }
    }

    /**
     * The append fast path: DDL-vs-append is the only conflict class for an
     * append commit, so one indexed lookup over the typed change table covers
     * it. 'table_inserted_into' changes never conflict with an append.
     */
    private fun checkConflicts(
        h: Handle,
        catalogId: Long,
        readSnapshot: Long,
        resolved: List<ResolvedAppend>,
    ) {
        val nameByTableId = resolved.associate { it.tableId to "${it.namespace}.${it.table}" }
        val conflicted = h.createQuery(
            """
            SELECT DISTINCT object_id FROM hog_snapshot_change
             WHERE catalog_id = :catalogId
               AND kind IN ('table_dropped', 'table_altered')
               AND object_id IN (<tableIds>)
               AND snapshot_id > :readSnapshot
            """,
        )
            .bind("catalogId", catalogId)
            .bind("readSnapshot", readSnapshot)
            .bindList("tableIds", nameByTableId.keys.toList())
            .mapTo(Long::class.java)
            .list()
        if (conflicted.isNotEmpty()) {
            val names = conflicted.mapNotNull(nameByTableId::get).sorted()
            throw HoglakeException.CommitConflict(
                "concurrent DDL since snapshot $readSnapshot on table(s): " +
                    names.joinToString(", "),
            )
        }
    }
}
