package com.posthog.hoglake.commit

import com.posthog.hoglake.model.CommitRequest
import com.posthog.hoglake.model.CommitResult
import com.posthog.hoglake.model.DeleteFileRegistration
import com.posthog.hoglake.model.FileRegistration
import com.posthog.hoglake.model.HoglakeException
import com.posthog.hoglake.persistence.Locks
import org.jdbi.v3.core.Handle
import org.jdbi.v3.core.Jdbi

/**
 * The commit endpoint (README.md commit-protocol / commit-serialization
 * sections): register client-written parquet files with footer-derived
 * stats and/or deletion-vector (DV) files, producing one snapshot.
 *
 * One SQL transaction per commit. The tail is serialized per catalog by
 * a Postgres advisory xact lock (taken up front, before any allocation),
 * so snapshot ids, file ids, and per-table row-id ranges are dense and
 * ordered with commit order — id order = commit order by construction,
 * and per-file row-id ranges are never reused (THE lineage guarantee).
 *
 * Appends and partitioning: if the touched table has a live partition
 * spec, every registered file must carry partition_values with arity
 * matching the spec's field count; the values are opaque TRANSFORMED
 * strings (no server-side transform computation in this milestone) and
 * land in hog_file_partition_value, with hog_data_file.spec_id binding
 * the file to the spec it was written under. On an unpartitioned table
 * partition_values are forbidden.
 *
 * Deletes (deletion vectors): each DeleteFileRegistration supersedes the
 * target data file's current live DV (there is at most one, enforced by
 * a unique partial index). Vectors only grow — the new delete_count must
 * cover the superseded one — and a DV registered after the writer's
 * readSnapshot means the writer built on a stale vector, which is a
 * CommitConflict (retryable), so readSnapshot is REQUIRED whenever
 * deletes are present. Deleted-from tables get the same table-level
 * DDL conflict check as appends ('table_dropped'/'table_altered' after
 * readSnapshot).
 *
 * A commit may mix appends and deletes, including on the same table;
 * appends are applied first, and a delete may NOT target a data file
 * created in the same commit (Validation — keeps the readSnapshot
 * semantics coherent: you cannot have read a file this commit creates).
 *
 * NOTE on stats: deletes do NOT touch hog_table_stats. record_count /
 * file_size_bytes remain the gross append counters (and next_row_id the
 * lineage allocator); net row liveness is computed at read time from the
 * live DVs, never denormalized here.
 *
 * Conflict rule (append fast path): appends only conflict with DDL on
 * the tables they touch. With a non-null readSnapshot we look for
 * 'table_dropped' / 'table_altered' changes on the touched tables since
 * that snapshot — one indexed lookup. A null readSnapshot is a blind
 * append with no conflict window (only legal when the commit has no
 * deletes).
 */
class CommitService(private val jdbi: Jdbi) {

    fun commit(catalog: String, request: CommitRequest): CommitResult =
        jdbi.inTransaction<CommitResult, RuntimeException> { handle ->
            doCommit(handle, catalog, request)
        }

    /** Live partition spec header: id + field arity. */
    private data class LiveSpec(val specId: Long, val fieldCount: Int)

    private data class ResolvedAppend(
        val namespace: String,
        val table: String,
        val tableId: Long,
        val files: List<FileRegistration>,
        /** Live spec with >= 1 field, or null for an unpartitioned table. */
        val spec: LiveSpec?,
    )

    private data class ResolvedDeletes(
        val namespace: String,
        val table: String,
        val tableId: Long,
        val files: List<DeleteFileRegistration>,
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

        // 2. Merge duplicate (namespace, table) appends/deletes, preserving
        // request order (first occurrence for tables, concatenation for
        // files), then resolve each to a live table at head.
        val mergedAppends = LinkedHashMap<Pair<String, String>, MutableList<FileRegistration>>()
        for (append in req.appends) {
            mergedAppends.getOrPut(append.namespace to append.table) { mutableListOf() }
                .addAll(append.files)
        }
        val mergedDeletes = LinkedHashMap<Pair<String, String>, MutableList<DeleteFileRegistration>>()
        for (deletes in req.deletes) {
            mergedDeletes.getOrPut(deletes.namespace to deletes.table) { mutableListOf() }
                .addAll(deletes.files)
        }
        if (mergedAppends.isEmpty() && mergedDeletes.isEmpty()) {
            throw HoglakeException.Validation("commit has no appends or deletes")
        }
        val readSnapshot = req.readSnapshot
        if (mergedDeletes.isNotEmpty() && readSnapshot == null) {
            throw HoglakeException.Validation(
                "read_snapshot is required when the commit contains deletes",
            )
        }
        val resolvedAppends = mergedAppends.map { (key, files) ->
            val (namespace, table) = key
            val tableId = resolveLiveTable(h, catalogId, namespace, table)
                ?: throw HoglakeException.Validation("unknown table $namespace.$table")
            ResolvedAppend(namespace, table, tableId, files, liveSpec(h, catalogId, tableId))
        }
        val appendTableIdByName =
            resolvedAppends.associate { (it.namespace to it.table) to it.tableId }
        val resolvedDeletes = mergedDeletes.map { (key, files) ->
            val (namespace, table) = key
            val tableId = appendTableIdByName[key]
                ?: resolveLiveTable(h, catalogId, namespace, table)
                ?: throw HoglakeException.Validation("unknown table $namespace.$table")
            ResolvedDeletes(namespace, table, tableId, files)
        }

        // 3. Structural validation. Nothing is written unless all of it passes.
        for (append in resolvedAppends) {
            validateFiles(h, catalogId, append)
        }
        validateDeleteRegistrations(resolvedDeletes)

        // 4. Conflict check ('table_dropped'/'table_altered' since
        // readSnapshot on every touched table — appends and deletes share
        // the one-query pattern). readSnapshot is non-null whenever deletes
        // exist; a null readSnapshot is an appends-only blind commit.
        if (readSnapshot != null) {
            if (readSnapshot > head) {
                throw HoglakeException.Validation(
                    "readSnapshot $readSnapshot is ahead of catalog head $head",
                )
            }
            val names = HashMap<Long, String>()
            resolvedAppends.forEach { names[it.tableId] = "${it.namespace}.${it.table}" }
            resolvedDeletes.forEach { names[it.tableId] = "${it.namespace}.${it.table}" }
            checkConflicts(h, catalogId, readSnapshot, names)
        }

        // 5. Allocations, all under the lock via UPDATE..RETURNING. Data
        // files and DV files share the next_file_id allocator. Neither
        // appends nor deletes are DDL: schema_version is NOT bumped.
        val appendFileCount = resolvedAppends.sumOf { it.files.size }
        val deleteFileCount = resolvedDeletes.sumOf { it.files.size }
        val totalFiles = appendFileCount + deleteFileCount
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

        // 6. Writes: snapshot, change rows, then appends before deletes (a
        // delete targeting a same-commit data file is detected below by its
        // begin_snapshot and rejected, rolling the whole commit back).
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
            VALUES (:catalogId, :snapshotId, :kind, :tableId)
            """,
        )
        for (append in resolvedAppends) {
            changeBatch
                .bind("catalogId", catalogId)
                .bind("snapshotId", snapshotId)
                .bind("kind", "table_inserted_into")
                .bind("tableId", append.tableId)
                .add()
        }
        for (deletes in resolvedDeletes) {
            changeBatch
                .bind("catalogId", catalogId)
                .bind("snapshotId", snapshotId)
                .bind("kind", "table_deleted_from")
                .bind("tableId", deletes.tableId)
                .add()
        }
        changeBatch.execute()

        var nextFileId = firstFileId
        nextFileId = writeAppends(h, catalogId, snapshotId, nextFileId, resolvedAppends)
        applyDeletes(h, catalogId, snapshotId, readSnapshot, nextFileId, resolvedDeletes)

        return CommitResult(snapshotId, schemaVersion)
    }

    /** hog_data_file / hog_file_partition_value / hog_file_column_stats writes. Returns the next free file id. */
    private fun writeAppends(
        h: Handle,
        catalogId: Long,
        snapshotId: Long,
        firstFileId: Long,
        resolved: List<ResolvedAppend>,
    ): Long {
        if (resolved.isEmpty()) return firstFileId
        val fileBatch = h.prepareBatch(
            """
            INSERT INTO hog_data_file (catalog_id, data_file_id, table_id, begin_snapshot,
                                       path, record_count, file_size_bytes, footer_size,
                                       row_id_start, stats_state, spec_id)
            VALUES (:catalogId, :dataFileId, :tableId, :beginSnapshot,
                    :path, :recordCount, :fileSizeBytes, :footerSize,
                    :rowIdStart, :statsState, :specId)
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
        val partitionBatch = h.prepareBatch(
            """
            INSERT INTO hog_file_partition_value (catalog_id, data_file_id, key_index, value)
            VALUES (:catalogId, :dataFileId, :keyIndex, :value)
            """,
        )
        var nextFileId = firstFileId
        var haveStats = false
        var havePartitionValues = false
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
                    .bind("specId", append.spec?.specId)
                    .add()
                rowId += file.recordCount
                if (append.spec != null) {
                    for ((keyIndex, value) in file.partitionValues!!.withIndex()) {
                        havePartitionValues = true
                        partitionBatch
                            .bind("catalogId", catalogId)
                            .bind("dataFileId", dataFileId)
                            .bind("keyIndex", keyIndex)
                            .bind("value", value)
                            .add()
                    }
                }
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
        if (havePartitionValues) partitionBatch.execute()
        if (haveStats) statsBatch.execute()
        return nextFileId
    }

    /**
     * DV registrations: per-target existence/ownership/liveness checks,
     * the supersession chain (stale-vector -> CommitConflict, shrink ->
     * Validation), then hog_delete_file inserts. Runs AFTER appends are
     * written so a same-commit target is visible here and rejected by its
     * begin_snapshot. hog_table_stats is deliberately untouched (see class
     * KDoc).
     */
    private fun applyDeletes(
        h: Handle,
        catalogId: Long,
        snapshotId: Long,
        readSnapshot: Long?,
        firstFileId: Long,
        resolved: List<ResolvedDeletes>,
    ) {
        if (resolved.isEmpty()) return
        checkNotNull(readSnapshot) { "deletes require a readSnapshot (validated earlier)" }
        var nextFileId = firstFileId
        val insertBatch = h.prepareBatch(
            """
            INSERT INTO hog_delete_file (catalog_id, delete_file_id, table_id, data_file_id,
                                         begin_snapshot, path, delete_count, file_size_bytes)
            VALUES (:catalogId, :deleteFileId, :tableId, :dataFileId,
                    :beginSnapshot, :path, :deleteCount, :fileSizeBytes)
            """,
        )
        for (deletes in resolved) {
            val qualified = "${deletes.namespace}.${deletes.table}"
            for (reg in deletes.files) {
                val target = h.createQuery(
                    """
                    SELECT table_id, record_count, begin_snapshot,
                           (end_snapshot IS NULL) AS live
                      FROM hog_data_file
                     WHERE catalog_id = :catalogId AND data_file_id = :dataFileId
                    """,
                )
                    .bind("catalogId", catalogId)
                    .bind("dataFileId", reg.dataFileId)
                    .map { rs, _ ->
                        Triple(rs.getLong(1), rs.getLong(2), rs.getLong(3)) to rs.getBoolean(4)
                    }
                    .findOne()
                    .orElseThrow {
                        HoglakeException.Validation(
                            "delete for $qualified targets unknown data_file_id ${reg.dataFileId}",
                        )
                    }
                val (tableIdRecordsBegin, live) = target
                val (targetTableId, recordCount, targetBegin) = tableIdRecordsBegin
                if (targetBegin == snapshotId) {
                    throw HoglakeException.Validation(
                        "delete for $qualified targets data_file_id ${reg.dataFileId} " +
                            "created in this same commit",
                    )
                }
                if (targetTableId != deletes.tableId) {
                    throw HoglakeException.Validation(
                        "delete for $qualified targets data_file_id ${reg.dataFileId} " +
                            "which belongs to another table",
                    )
                }
                if (!live) {
                    throw HoglakeException.Validation(
                        "delete for $qualified targets data_file_id ${reg.dataFileId} " +
                            "which is no longer live",
                    )
                }
                if (reg.deleteCount > recordCount) {
                    throw HoglakeException.Validation(
                        "delete_count ${reg.deleteCount} exceeds record_count $recordCount " +
                            "of data_file_id ${reg.dataFileId} in $qualified",
                    )
                }
                // Supersession chain: at most one live DV per data file (unique
                // partial index). Staleness is checked before monotonicity: a
                // DV registered after the writer's readSnapshot means the new
                // vector was built without seeing it — retryable conflict,
                // regardless of counts.
                val current = h.createQuery(
                    """
                    SELECT delete_file_id, delete_count, begin_snapshot
                      FROM hog_delete_file
                     WHERE catalog_id = :catalogId AND data_file_id = :dataFileId
                       AND end_snapshot IS NULL
                    """,
                )
                    .bind("catalogId", catalogId)
                    .bind("dataFileId", reg.dataFileId)
                    .map { rs, _ -> Triple(rs.getLong(1), rs.getLong(2), rs.getLong(3)) }
                    .findOne()
                    .orElse(null)
                if (current != null) {
                    val (currentId, currentCount, currentBegin) = current
                    if (currentBegin > readSnapshot) {
                        throw HoglakeException.CommitConflict(
                            "deletion vector for data_file_id ${reg.dataFileId} in $qualified " +
                                "was superseded at snapshot $currentBegin, after read snapshot " +
                                "$readSnapshot",
                        )
                    }
                    if (currentCount > reg.deleteCount) {
                        throw HoglakeException.Validation(
                            "delete_count ${reg.deleteCount} for data_file_id ${reg.dataFileId} " +
                                "in $qualified shrinks the live deletion vector " +
                                "(delete_count $currentCount) — vectors only grow",
                        )
                    }
                    h.createUpdate(
                        """
                        UPDATE hog_delete_file SET end_snapshot = :snapshotId
                         WHERE catalog_id = :catalogId AND delete_file_id = :deleteFileId
                        """,
                    )
                        .bind("snapshotId", snapshotId)
                        .bind("catalogId", catalogId)
                        .bind("deleteFileId", currentId)
                        .execute()
                }
                insertBatch
                    .bind("catalogId", catalogId)
                    .bind("deleteFileId", nextFileId++)
                    .bind("tableId", deletes.tableId)
                    .bind("dataFileId", reg.dataFileId)
                    .bind("beginSnapshot", snapshotId)
                    .bind("path", reg.path)
                    .bind("deleteCount", reg.deleteCount)
                    .bind("fileSizeBytes", reg.fileSizeBytes)
                    .add()
            }
        }
        insertBatch.execute()
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

    /**
     * The table's live partition spec, or null when unpartitioned. A spec
     * with zero fields (SetPartitionSpec([])) is unpartitioned too.
     */
    private fun liveSpec(h: Handle, catalogId: Long, tableId: Long): LiveSpec? =
        h.createQuery(
            """
            SELECT ps.spec_id,
                   (SELECT count(*) FROM hog_partition_field pf
                     WHERE pf.catalog_id = ps.catalog_id AND pf.table_id = ps.table_id
                       AND pf.spec_id = ps.spec_id) AS field_count
              FROM hog_partition_spec ps
             WHERE ps.catalog_id = :catalogId AND ps.table_id = :tableId
               AND ps.end_snapshot IS NULL
            """,
        )
            .bind("catalogId", catalogId)
            .bind("tableId", tableId)
            .map { rs, _ -> LiveSpec(rs.getLong(1), rs.getInt(2)) }
            .findOne()
            .orElse(null)
            ?.takeIf { it.fieldCount > 0 }

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
            val spec = append.spec
            val values = file.partitionValues
            if (spec != null) {
                if (values == null) {
                    throw HoglakeException.Validation(
                        "$qualified is partitioned (${spec.fieldCount} field(s)) but " +
                            "${file.path} has no partition_values",
                    )
                }
                if (values.size != spec.fieldCount) {
                    throw HoglakeException.Validation(
                        "partition_values arity ${values.size} for ${file.path} in $qualified " +
                            "does not match the live spec's ${spec.fieldCount} field(s)",
                    )
                }
            } else if (values != null) {
                throw HoglakeException.Validation(
                    "partition_values given for ${file.path} but $qualified is not partitioned",
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

    /** DB-independent DV registration checks: shapes, ranges, duplicate targets. */
    private fun validateDeleteRegistrations(resolved: List<ResolvedDeletes>) {
        val seenTargets = HashSet<Long>()
        for (deletes in resolved) {
            val qualified = "${deletes.namespace}.${deletes.table}"
            for (reg in deletes.files) {
                if (reg.path.isBlank()) {
                    throw HoglakeException.Validation("blank delete file path in $qualified")
                }
                if (reg.deleteCount <= 0) {
                    throw HoglakeException.Validation(
                        "delete_count must be > 0 for data_file_id ${reg.dataFileId} " +
                            "in $qualified, got ${reg.deleteCount}",
                    )
                }
                if (reg.fileSizeBytes < 0) {
                    throw HoglakeException.Validation(
                        "negative file_size_bytes for delete of data_file_id " +
                            "${reg.dataFileId} in $qualified",
                    )
                }
                if (!seenTargets.add(reg.dataFileId)) {
                    throw HoglakeException.Validation(
                        "duplicate delete target data_file_id ${reg.dataFileId} in one commit",
                    )
                }
            }
        }
    }

    /**
     * DDL-vs-write is the only table-level conflict class for a commit, so
     * one indexed lookup over the typed change table covers appends and
     * deletes alike. 'table_inserted_into' / 'table_deleted_from' changes
     * never conflict at table level (DV-level staleness is handled per
     * target in [applyDeletes]).
     */
    private fun checkConflicts(
        h: Handle,
        catalogId: Long,
        readSnapshot: Long,
        nameByTableId: Map<Long, String>,
    ) {
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
