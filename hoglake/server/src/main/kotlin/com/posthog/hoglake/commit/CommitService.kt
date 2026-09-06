package com.posthog.hoglake.commit

import com.posthog.hoglake.model.CommitRequest
import com.posthog.hoglake.model.CommitResult
import com.posthog.hoglake.model.DeleteFileRegistration
import com.posthog.hoglake.model.FileRegistration
import com.posthog.hoglake.model.HoglakeException
import com.posthog.hoglake.observability.Audit
import com.posthog.hoglake.observability.Metrics
import com.posthog.hoglake.persistence.Locks
import org.jdbi.v3.core.Handle
import org.jdbi.v3.core.Jdbi
import java.util.UUID

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
class CommitService(
    private val jdbi: Jdbi,
    /**
     * lock_timeout for the commit transaction's advisory-lock wait
     * (HOGLAKE_COMMIT_LOCK_TIMEOUT_MS; B2 admission control). 0 =
     * unbounded. Expiry surfaces as CommitQueueTimeout -> 503.
     */
    private val commitLockTimeoutMs: Long = DEFAULT_COMMIT_LOCK_TIMEOUT_MS,
) {
    companion object {
        /** Default commit admission bound: 30s (Config's default mirrors it). */
        const val DEFAULT_COMMIT_LOCK_TIMEOUT_MS: Long = 30_000

        /**
         * Per-file record_count sanity cap (2^48 ≈ 281T rows). No real
         * parquet file gets anywhere close; the cap keeps a hostile
         * registration from racing the row-id allocator toward overflow.
         */
        private const val MAX_FILE_RECORD_COUNT: Long = 1L shl 48
    }

    /**
     * Commit entry point. Metrics + audit are emitted HERE, after the
     * transaction has committed (or rolled back on the way out as an
     * exception) — never inside it.
     */
    fun commit(
        catalog: String,
        request: CommitRequest,
    ): CommitResult {
        val files = request.appends.sumOf { it.files.size }
        val deletes = request.deletes.sumOf { it.files.size }
        val result =
            try {
                jdbi.inTransaction<CommitResult, RuntimeException> { handle ->
                    doCommit(handle, catalog, request)
                }
            } catch (e: HoglakeException) {
                Metrics.commitFailureResult(e)?.let { Metrics.commitRecorded(catalog, it) }
                Audit.event(
                    action = "commit",
                    catalog = catalog,
                    obj = null,
                    outcome = Audit.failureOutcome(e),
                    detail = e.message,
                )
                throw e
            } catch (e: Throwable) {
                // An unexpected failure mid-commit (bug, dead pool, broken
                // state) must still leave an audit trace and a counter tick
                // before it becomes the API's 500 — silence here was the
                // adversarial-review finding.
                Metrics.commitRecorded(catalog, "error")
                Audit.event(
                    action = "commit",
                    catalog = catalog,
                    obj = null,
                    outcome = "error",
                    detail = e.message,
                )
                throw e
            }
        Metrics.commitRecorded(catalog, "committed")
        Audit.event(
            action = "commit",
            catalog = catalog,
            obj = null,
            outcome = "committed",
            detail = "snapshot=${result.snapshotId} files=$files deletes=$deletes",
        )
        return result
    }

    /** hog_catalog head-of-line state, read once under the commit lock. */
    private data class CatalogHead(
        val head: Long,
        val schemaVersion: Long,
        val earliestSnapshotId: Long,
        /** Expiry-floor anchor time; null until expiry first advances the floor. */
        val earliestSnapshotTime: java.time.Instant?,
    ) {
        /** ", reached at <time>" suffix for 410 messages (TimeTravelRepo's convention). */
        fun reachedAtSuffix(): String = earliestSnapshotTime?.let { ", reached at $it" } ?: ""
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

    private fun doCommit(
        h: Handle,
        catalogName: String,
        req: CommitRequest,
    ): CommitResult {
        // 1. Resolve catalog, then serialize the commit tail.
        val catalogId =
            h.createQuery("SELECT catalog_id FROM hog_catalog WHERE name = ?")
                .bind(0, catalogName)
                .mapTo(Long::class.java)
                .findOne()
                .orElseThrow { HoglakeException.NotFound("catalog '$catalogName'") }
        Locks.acquireCatalogCommitLock(h, catalogId, commitLockTimeoutMs)

        val catalogHead =
            h.createQuery(
                """
                SELECT last_snapshot_id, schema_version, earliest_snapshot_id, earliest_snapshot_time
                FROM hog_catalog WHERE catalog_id = ?
                """,
            )
                .bind(0, catalogId)
                .map { rs, _ ->
                    CatalogHead(
                        head = rs.getLong(1),
                        schemaVersion = rs.getLong(2),
                        earliestSnapshotId = rs.getLong(3),
                        earliestSnapshotTime =
                            rs.getObject(4, java.time.OffsetDateTime::class.java)?.toInstant(),
                    )
                }
                .one()
        val (head, schemaVersion) = catalogHead.head to catalogHead.schemaVersion

        // 2. Merge duplicate (namespace, table) appends/deletes, preserving
        // request order (first occurrence for tables, concatenation for
        // files), then resolve each to a live table at head. Every
        // expected_table_uuid supplied for a table is collected and checked
        // against the resolved incarnation — merged duplicates that
        // disagree cannot both match, so a stale one still conflicts.
        val mergedAppends = LinkedHashMap<Pair<String, String>, MutableList<FileRegistration>>()
        val expectedUuids = HashMap<Pair<String, String>, MutableSet<UUID>>()
        for (append in req.appends) {
            mergedAppends.getOrPut(append.namespace to append.table) { mutableListOf() }
                .addAll(append.files)
            append.expectedTableUuid?.let {
                expectedUuids.getOrPut(append.namespace to append.table) { mutableSetOf() } += it
            }
        }
        val mergedDeletes = LinkedHashMap<Pair<String, String>, MutableList<DeleteFileRegistration>>()
        for (deletes in req.deletes) {
            mergedDeletes.getOrPut(deletes.namespace to deletes.table) { mutableListOf() }
                .addAll(deletes.files)
            deletes.expectedTableUuid?.let {
                expectedUuids.getOrPut(deletes.namespace to deletes.table) { mutableSetOf() } += it
            }
        }
        if (mergedAppends.isEmpty() && mergedDeletes.isEmpty()) {
            throw HoglakeException.Validation("commit has no appends or deletes")
        }
        val readSnapshot = req.readSnapshot
        if (readSnapshot != null && readSnapshot < 0) {
            throw HoglakeException.Validation(
                "read_snapshot must be >= 0, got $readSnapshot",
            )
        }
        if (mergedDeletes.isNotEmpty() && readSnapshot == null) {
            throw HoglakeException.Validation(
                "read_snapshot is required when the commit contains deletes",
            )
        }

        fun resolveGuarded(key: Pair<String, String>): LiveTable {
            val (namespace, table) = key
            val live =
                resolveLiveTable(h, catalogId, namespace, table)
                    ?: throw HoglakeException.Validation("unknown table $namespace.$table")
            for (expected in expectedUuids[key].orEmpty()) {
                // The atomic incarnation guard: the name resolved, but to a
                // different incarnation than the writer planned against —
                // a drop+recreate happened. Retryable conflict, never a
                // silent write into the wrong table.
                if (expected != live.tableUuid) {
                    throw HoglakeException.CommitConflict(
                        "table '$namespace.$table' is uuid ${live.tableUuid}, " +
                            "expected $expected: the table was recreated",
                    )
                }
            }
            return live
        }
        val resolvedAppends =
            mergedAppends.map { (key, files) ->
                val (namespace, table) = key
                val live = resolveGuarded(key)
                ResolvedAppend(namespace, table, live.tableId, files, liveSpec(h, catalogId, live.tableId))
            }
        val appendTableIdByName =
            resolvedAppends.associate { (it.namespace to it.table) to it.tableId }
        val resolvedDeletes =
            mergedDeletes.map { (key, files) ->
                val (namespace, table) = key
                val tableId = appendTableIdByName[key] ?: resolveGuarded(key).tableId
                ResolvedDeletes(namespace, table, tableId, files)
            }

        // 3. Structural validation. Nothing is written unless all of it passes.
        for (append in resolvedAppends) {
            validateFiles(h, catalogId, append)
        }
        validateDeleteRegistrations(resolvedDeletes)

        // 3b. Removal-queue collision check (under the commit lock, so it
        // serializes with cleanup's drain sub-batches, which take the same
        // lock): a registered path with an UNDRAINED hog_file_removal row
        // is scheduled for physical deletion — accepting it would let the
        // cleanup drain delete the object out from under the new live row
        // (path reuse under a deterministic path scheme / writer retry).
        // Duplicate paths against live/historical file rows stay legal —
        // this rejects only paths the cleanup queue currently owns; once
        // the entry drains (drained_at set) the path is registrable again.
        checkRemovalQueueCollisions(h, catalogId, resolvedAppends, resolvedDeletes)

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
            // The floor guard: expiry cascade-deletes hog_snapshot_change
            // rows below earliest_snapshot_id, so a readSnapshot under the
            // floor has a partly-expired conflict window — checkConflicts
            // below could silently miss a 'table_altered'/'table_dropped'
            // in the expired range. 410, matching every read path's
            // below-floor contract: the writer's conflict basis is gone;
            // it must re-read at a retained snapshot and re-commit.
            // readSnapshot == earliest is fine (the window (earliest, head]
            // is fully retained).
            if (readSnapshot < catalogHead.earliestSnapshotId) {
                throw HoglakeException.Expired(
                    "read_snapshot $readSnapshot is below the expiry floor (earliest " +
                        "retained snapshot is ${catalogHead.earliestSnapshotId}" +
                        "${catalogHead.reachedAtSuffix()}): the conflict window since " +
                        "that snapshot is partly expired; re-read at a retained " +
                        "snapshot and re-commit",
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
        val (snapshotId, firstFileId) =
            h.createQuery(
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
        // snapshot_time = clock_timestamp(), NOT the column default now():
        // now() is the TRANSACTION start time, which predates the advisory
        // lock wait, so defaulting it would let a queued commit record a
        // time older than an earlier-committed snapshot's. clock_timestamp()
        // executes here, under the lock, keeping snapshot_time monotone
        // with snapshot id (modulo the DB clock stepping backwards) — the
        // premise resolveTimestamp relies on.
        h.createUpdate(
            """
            INSERT INTO hog_snapshot (catalog_id, snapshot_id, snapshot_time, schema_version, author, commit_message)
            VALUES (:catalogId, :snapshotId, clock_timestamp(), :schemaVersion, :author, :message)
            """,
        )
            .bind("catalogId", catalogId)
            .bind("snapshotId", snapshotId)
            .bind("schemaVersion", schemaVersion)
            .bind("author", req.author)
            .bind("message", req.message)
            .execute()

        val changeBatch =
            h.prepareBatch(
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

    /**
     * Reject any registered path (data or DV) that has an undrained
     * hog_file_removal row in this catalog: the path is scheduled for
     * physical deletion and the cleanup drain (serialized against this
     * check by the shared per-catalog advisory lock) would delete the
     * object out from under the new row. Typed 409 — the writer retries
     * with a fresh path, or after the drain settles the entry.
     */
    private fun checkRemovalQueueCollisions(
        h: Handle,
        catalogId: Long,
        appends: List<ResolvedAppend>,
        deletes: List<ResolvedDeletes>,
    ) {
        val paths =
            (appends.flatMap { a -> a.files.map { it.path } } + deletes.flatMap { d -> d.files.map { it.path } })
                .distinct()
        if (paths.isEmpty()) return
        val queued =
            h.createQuery(
                """
                SELECT DISTINCT path FROM hog_file_removal
                WHERE catalog_id = :catalogId AND drained_at IS NULL AND path = ANY(:paths)
                """,
            )
                .bind("catalogId", catalogId)
                .bindArray("paths", String::class.java, paths)
                .mapTo(String::class.java)
                .list()
        if (queued.isNotEmpty()) {
            throw HoglakeException.CommitConflict(
                "path(s) scheduled for deletion by the cleanup queue: " +
                    queued.sorted().joinToString(", ") +
                    " — registering them would race the physical delete; use fresh " +
                    "paths, or retry after the removal queue drains",
            )
        }
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
        val fileBatch =
            h.prepareBatch(
                """
            INSERT INTO hog_data_file (catalog_id, data_file_id, table_id, begin_snapshot,
                                       path, record_count, file_size_bytes, footer_size,
                                       row_id_start, stats_state, spec_id)
            VALUES (:catalogId, :dataFileId, :tableId, :beginSnapshot,
                    :path, :recordCount, :fileSizeBytes, :footerSize,
                    :rowIdStart, :statsState, :specId)
            """,
            )
        val statsBatch =
            h.prepareBatch(
                """
            INSERT INTO hog_file_column_stats (catalog_id, data_file_id, field_id, value_count,
                                               null_count, nan_count, size_bytes,
                                               lower_bound, upper_bound)
            VALUES (:catalogId, :dataFileId, :fieldId, :valueCount,
                    :nullCount, :nanCount, :sizeBytes,
                    :lowerBound, :upperBound)
            """,
            )
        val partitionBatch =
            h.prepareBatch(
                """
            INSERT INTO hog_file_partition_value (catalog_id, data_file_id, key_index, value)
            VALUES (:catalogId, :dataFileId, :keyIndex, :value)
            """,
            )
        var nextFileId = firstFileId
        var haveStats = false
        var havePartitionValues = false
        for (append in resolved) {
            // The per-table record sum and the allocator advancement are
            // both overflow-checked (Math.addExact): a wrapped sum would
            // hand out negative / reused row-id ranges, silently breaking
            // THE lineage guarantee. The CHECK (next_row_id >= 0) on
            // hog_table_stats is the DB backstop for the same invariant.
            val recordSum =
                try {
                    append.files.fold(0L) { acc, f -> Math.addExact(acc, f.recordCount) }
                } catch (_: ArithmeticException) {
                    throw HoglakeException.Validation("record_count sum overflows row-id space")
                }
            // Overflow-checked like recordSum: a wrapped byte sum would
            // corrupt the hog_table_stats rollup (and the CHECK
            // (file_size_bytes >= 0) on hog_table_stats is the DB backstop).
            val byteSum =
                try {
                    append.files.fold(0L) { acc, f -> Math.addExact(acc, f.fileSizeBytes) }
                } catch (_: ArithmeticException) {
                    throw HoglakeException.Validation(
                        "file_size_bytes sum overflows for ${append.namespace}.${append.table}",
                    )
                }
            // Per-table row-id range: read the allocator, advance it with
            // the table's rollup (safe read-then-write — the per-catalog
            // advisory lock serializes every writer); each file gets a
            // contiguous slice in request order.
            val rowIdStart =
                h.createQuery(
                    """
                SELECT next_row_id FROM hog_table_stats
                 WHERE catalog_id = :catalogId AND table_id = :tableId
                """,
                )
                    .bind("catalogId", catalogId)
                    .bind("tableId", append.tableId)
                    .mapTo(Long::class.java)
                    .findOne()
                    .orElseThrow {
                        IllegalStateException(
                            "missing hog_table_stats row for table_id=${append.tableId}",
                        )
                    }
            val newNextRowId =
                try {
                    Math.addExact(rowIdStart, recordSum)
                } catch (_: ArithmeticException) {
                    throw HoglakeException.Validation("record_count sum overflows row-id space")
                }
            h.createUpdate(
                """
                UPDATE hog_table_stats
                   SET next_row_id = :newNextRowId,
                       record_count = record_count + :records,
                       file_size_bytes = file_size_bytes + :bytes
                 WHERE catalog_id = :catalogId AND table_id = :tableId
                """,
            )
                .bind("newNextRowId", newNextRowId)
                .bind("records", recordSum)
                .bind("bytes", byteSum)
                .bind("catalogId", catalogId)
                .bind("tableId", append.tableId)
                .execute()
            var rowId = rowIdStart

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
        val insertBatch =
            h.prepareBatch(
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
                val target =
                    h.createQuery(
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
                val current =
                    h.createQuery(
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

    /** A live table's id + identity uuid (the incarnation the name currently binds to). */
    private data class LiveTable(val tableId: Long, val tableUuid: UUID)

    /** A table is live iff its head version row is open and neither it nor its namespace is dropped. */
    private fun resolveLiveTable(
        h: Handle,
        catalogId: Long,
        namespace: String,
        table: String,
    ): LiveTable? =
        h.createQuery(
            """
        SELECT tv.table_id, t.table_uuid
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
            .map { rs, _ -> LiveTable(rs.getLong(1), rs.getObject(2) as UUID) }
            .findOne()
            .orElse(null)

    /**
     * The table's live partition spec, or null when unpartitioned. A spec
     * with zero fields (SetPartitionSpec([])) is unpartitioned too.
     * Deliberately inline rather than persistence/SpecRepo.specAt: the
     * commit tail only needs the spec header + field COUNT (one query),
     * not the full field list.
     */
    private fun liveSpec(
        h: Handle,
        catalogId: Long,
        tableId: Long,
    ): LiveSpec? =
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

    private fun validateFiles(
        h: Handle,
        catalogId: Long,
        append: ResolvedAppend,
    ) {
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
            if (file.recordCount > MAX_FILE_RECORD_COUNT) {
                throw HoglakeException.Validation(
                    "record_count ${file.recordCount} for ${file.path} in $qualified exceeds " +
                        "the per-file cap $MAX_FILE_RECORD_COUNT (2^48); no real file has that many rows",
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
        val conflicted =
            h.createQuery(
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
