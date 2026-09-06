package com.posthog.hoglake.compaction

import com.posthog.hoglake.hydrator.ObjectStore
import com.posthog.hoglake.model.ChangeKind
import com.posthog.hoglake.model.ColType
import com.posthog.hoglake.model.ColumnStats
import com.posthog.hoglake.model.CompactionResult
import com.posthog.hoglake.model.HoglakeException
import com.posthog.hoglake.model.SortFieldDef
import com.posthog.hoglake.observability.Audit
import com.posthog.hoglake.observability.Metrics
import com.posthog.hoglake.persistence.CatalogRepo
import com.posthog.hoglake.persistence.Locks
import com.posthog.hoglake.persistence.NamespaceRepo
import com.posthog.hoglake.persistence.SnapshotRepo
import com.posthog.hoglake.persistence.SortRepo
import com.posthog.hoglake.persistence.TableRepo
import com.posthog.hoglake.stats.IcebergSingleValue
import io.github.oshai.kotlinlogging.KotlinLogging
import org.jdbi.v3.core.Handle
import org.jdbi.v3.core.Jdbi
import org.jdbi.v3.core.kotlin.inTransactionUnchecked
import org.jdbi.v3.core.kotlin.withHandleUnchecked
import java.nio.ByteBuffer
import java.nio.ByteOrder
import java.nio.file.Files
import java.nio.file.Path
import java.util.UUID
import kotlin.io.path.deleteIfExists
import kotlin.io.path.fileSize

/** Per-run compaction knobs (Config's HOGLAKE_COMPACTION_* env surface). */
data class CompactionConfig(
    /** Output target size; also the "small file" input threshold. */
    val targetBytes: Long,
    /** Smallest group worth rewriting. */
    val minInputFiles: Int,
    /** Groups rewritten per run per catalog — tiny bites, never a storm. */
    val maxGroupsPerRun: Int,
)

/** One live small file eligible for merging. */
data class CompactionCandidate(
    val dataFileId: Long,
    val path: String,
    val recordCount: Long,
    val fileSizeBytes: Long,
    val rowIdStart: Long,
    val statsProvided: Boolean,
)

/** A greedy run of candidates sharing (spec_id, partition_values). */
data class CompactionGroup(
    val files: List<CompactionCandidate>,
    val specId: Long?,
    val partitionValues: List<String?>?,
) {
    val totalBytes: Long get() = files.sumOf { it.fileSizeBytes }
    val totalRecords: Long get() = files.sumOf { it.recordCount }
}

/** The metadata-only plan for one table. */
data class CompactionPlan(
    val tableId: Long,
    val namespace: String,
    val table: String,
    val groups: List<CompactionGroup>,
)

/**
 * Server-side compaction (M4 — the maintenance-parity item the
 * predecessor never survived in production; README.md §4's
 * commit-storm history is the design constraint here).
 *
 * PLANNING is metadata-only, per table: candidates are LIVE data files
 * under the target size with NO live deletion vector (v1 never rewrites
 * DV-bearing files — rewriting deleted rows away would change row-id
 * semantics; DV-aware compaction is future work), bucketed by
 * (spec_id, identical partition_values), ordered by row_id_start, then
 * grouped greedily into runs whose summed bytes stay <= target and
 * whose file count reaches min_input_files. UNLIKE the predecessor's
 * merge_adjacent_files, row-id ADJACENCY IS NOT REQUIRED — which is
 * exactly why outputs must materialize ids explicitly (ParquetRewriter).
 *
 * EXECUTION happens entirely OUTSIDE any catalog transaction: inputs
 * are fetched from the object store, merged/sorted/rewritten locally,
 * and the output uploaded — only then does the group COMMIT open a
 * transaction. The commit is small metadata under the per-catalog
 * commit lock, following CommitService's tail shape (allocate under
 * lock via UPDATE..RETURNING, snapshot + typed change row, no
 * schema_version bump): foreground writers wait milliseconds, never on
 * S3 IO. Rate-awareness is by construction: max_groups_per_run
 * (default 1) per catalog per sweep — tiny bites, never the 60-180s
 * commit convoys of the 2026-09-04 incident.
 *
 * Plan-to-commit races: appenders never conflict with compaction (its
 * change kind, 'table_compacted', is invisible to the append conflict
 * check), but a DV registered against an input after planning would be
 * silently orphaned by the rewrite. The unique live-DV index cannot
 * catch that (the inputs are being end-snapshotted, not DV'd), so the
 * commit transaction RE-VERIFIES every input is still live and still
 * DV-free; any miss aborts just that group (skipped, logged, counted in
 * skipped_conflicts) and the next run re-plans. The uploaded output of
 * an aborted group is an orphaned object (never referenced by the
 * catalog, so cleanup's liveness check would pass it; v1 leaves it and
 * logs — bucket lifecycle rules are the backstop).
 *
 * Inputs are END-SNAPSHOTTED, never deleted: they remain visible to
 * time travel below the compaction snapshot, and expiry queues their
 * paths for physical removal once end_snapshot falls under the
 * retention floor — exactly the superseded-DV lifecycle. Nothing is
 * queued here.
 *
 * Stats for the output are aggregated server-side from the inputs'
 * hog_file_column_stats: counts sum; bounds are recomputed from the
 * TYPED decoded bounds (IcebergSingleValue.decode + compareValues +
 * re-encode) — a raw binary min/max of the encodings would be wrong for
 * signed little-endian types. If any input lacks provided stats the
 * output registers as 'pending' and the hydrator fills it from the
 * footer.
 */
class CompactionService(
    private val jdbi: Jdbi,
    private val store: ObjectStore,
    private val defaults: CompactionConfig,
) {
    private val log = KotlinLogging.logger {}

    /** Everything execution needs beyond the group list. */
    private data class TableContext(
        val catalogId: Long,
        val dataPath: String,
        val namespace: String,
        val table: String,
        val tableId: Long,
        /** Live column types by field id (stats aggregation). */
        val columnTypes: Map<Long, ColType>,
        /** Live column field ids by name (rewriter id fallback). */
        val fieldIdsByName: Map<String, Long>,
        /** Live sort order — BINDING for the rewrite. Empty = row-id order. */
        val sortFields: List<SortFieldDef>,
    )

    private data class PlanWithContext(val ctx: TableContext, val plan: CompactionPlan)

    // ---- planning --------------------------------------------------------

    /** Public metadata-only planning for one table (also the test surface). */
    fun planTable(
        catalog: String,
        namespace: String,
        table: String,
        cfg: CompactionConfig = defaults,
    ): CompactionPlan = jdbi.withHandleUnchecked { h -> planWithContext(h, catalog, namespace, table, cfg).plan }

    private fun planWithContext(
        h: Handle,
        catalog: String,
        namespace: String,
        table: String,
        cfg: CompactionConfig,
    ): PlanWithContext {
        val cat =
            CatalogRepo.findByName(h, catalog)
                ?: throw HoglakeException.NotFound("catalog '$catalog'")
        val ns =
            NamespaceRepo.findLiveByName(h, cat.catalogId, namespace)
                ?: throw HoglakeException.NotFound("namespace '$namespace' in catalog '$catalog'")
        val t =
            TableRepo.findLive(h, cat.catalogId, ns.namespaceId, table)
                ?: throw HoglakeException.NotFound("table '$namespace.$table' in catalog '$catalog'")
        val columns = TableRepo.columnsAt(h, cat.catalogId, t.tableId, cat.headSnapshotId)
        val ctx =
            TableContext(
                catalogId = cat.catalogId,
                dataPath = cat.dataPath,
                namespace = ns.name,
                table = t.name,
                tableId = t.tableId,
                columnTypes = columns.associate { it.fieldId to it.def.type },
                fieldIdsByName = columns.associate { it.def.name to it.fieldId },
                sortFields =
                    SortRepo.sortSpecAt(h, cat.catalogId, t.tableId, cat.headSnapshotId)
                        ?.fields ?: emptyList(),
            )
        return PlanWithContext(ctx, CompactionPlan(t.tableId, ns.name, t.name, groups(h, ctx, cfg)))
    }

    private fun groups(
        h: Handle,
        ctx: TableContext,
        cfg: CompactionConfig,
    ): List<CompactionGroup> {
        data class Bucket(val specId: Long?, val values: List<String?>?)

        data class Row(val candidate: CompactionCandidate, val bucket: Bucket)

        val rows =
            h.createQuery(
                """
            SELECT f.data_file_id, f.path, f.record_count, f.file_size_bytes,
                   f.row_id_start, f.spec_id, f.stats_state,
                   (SELECT array_agg(pv.value ORDER BY pv.key_index)
                    FROM hog_file_partition_value pv
                    WHERE pv.catalog_id = f.catalog_id
                      AND pv.data_file_id = f.data_file_id) AS partition_values
            FROM hog_data_file f
            WHERE f.catalog_id = :catalogId AND f.table_id = :tableId
              AND f.end_snapshot IS NULL
              AND f.file_size_bytes < :targetBytes
              AND NOT EXISTS (
                    SELECT 1 FROM hog_delete_file dv
                    WHERE dv.catalog_id = f.catalog_id
                      AND dv.data_file_id = f.data_file_id
                      AND dv.end_snapshot IS NULL)
            ORDER BY f.row_id_start, f.data_file_id
            """,
            )
                .bind("catalogId", ctx.catalogId)
                .bind("tableId", ctx.tableId)
                .bind("targetBytes", cfg.targetBytes)
                .map { rs, _ ->
                    Row(
                        CompactionCandidate(
                            dataFileId = rs.getLong("data_file_id"),
                            path = rs.getString("path"),
                            recordCount = rs.getLong("record_count"),
                            fileSizeBytes = rs.getLong("file_size_bytes"),
                            rowIdStart = rs.getLong("row_id_start"),
                            statsProvided = rs.getString("stats_state") == "provided",
                        ),
                        Bucket(
                            specId = rs.getObject("spec_id")?.let { (it as Number).toLong() },
                            values =
                                (rs.getArray("partition_values")?.array as? Array<*>)
                                    ?.map { it as String? },
                        ),
                    )
                }
                .list()

        val out = mutableListOf<CompactionGroup>()
        for ((bucket, bucketRows) in rows.groupBy { it.bucket }) {
            var run = mutableListOf<CompactionCandidate>()
            var runBytes = 0L

            fun close() {
                if (run.size >= cfg.minInputFiles) {
                    out += CompactionGroup(run, bucket.specId, bucket.values)
                }
                run = mutableListOf()
                runBytes = 0
            }
            for (row in bucketRows) {
                if (run.isNotEmpty() && runBytes + row.candidate.fileSizeBytes > cfg.targetBytes) close()
                run += row.candidate
                runBytes += row.candidate.fileSizeBytes
            }
            close()
        }
        return out
    }

    // ---- one run ---------------------------------------------------------

    /** Manual-trigger convenience: defaults with an optional groups-per-run override. */
    fun runOnce(
        catalog: String,
        batchOverride: Int? = null,
    ): CompactionResult = runOnce(catalog, defaults.copy(maxGroupsPerRun = batchOverride ?: defaults.maxGroupsPerRun))

    /**
     * One compaction sweep over [catalog]: plan tables in name order and
     * rewrite at most cfg.maxGroupsPerRun groups. Metrics and the audit
     * event are emitted here, after all group transactions resolved.
     */
    fun runOnce(
        catalog: String,
        cfg: CompactionConfig,
    ): CompactionResult =
        Audit.audited(
            "compaction",
            catalog,
            null,
            detail = { r ->
                "groups_compacted=${r.groupsCompacted} files_in=${r.filesIn} files_out=${r.filesOut} " +
                    "bytes_in=${r.bytesIn} bytes_out=${r.bytesOut} skipped_conflicts=${r.skippedConflicts}"
            },
        ) {
            if (cfg.maxGroupsPerRun <= 0) {
                throw HoglakeException.Validation(
                    "batch (groups per run) must be positive (got ${cfg.maxGroupsPerRun})",
                )
            }
            val result = doRunOnce(catalog, cfg)
            Metrics.compactionGroups(catalog, result.groupsCompacted)
            Metrics.compactionFilesRewritten(catalog, result.filesIn)
            result
        }

    private fun doRunOnce(
        catalog: String,
        cfg: CompactionConfig,
    ): CompactionResult {
        val tables = jdbi.withHandleUnchecked { h -> liveTables(h, catalog) }
        var groupsCompacted = 0L
        var filesIn = 0L
        var bytesIn = 0L
        var bytesOut = 0L
        var skipped = 0L
        outer@ for ((namespace, table) in tables) {
            if (groupsCompacted + skipped >= cfg.maxGroupsPerRun) break
            val (ctx, plan) =
                jdbi.withHandleUnchecked { h -> planWithContext(h, catalog, namespace, table, cfg) }
            for (group in plan.groups) {
                if (groupsCompacted + skipped >= cfg.maxGroupsPerRun) break@outer
                try {
                    val committed = compactGroup(ctx, group)
                    if (committed == null) {
                        skipped++
                    } else {
                        groupsCompacted++
                        filesIn += group.files.size
                        bytesIn += group.totalBytes
                        bytesOut += committed
                    }
                } catch (e: Exception) {
                    // One bad group (unreadable input, mixed schema
                    // vintages, S3 hiccup) never wedges the sweep.
                    log.error(e) {
                        "compaction group of ${group.files.size} files failed for " +
                            "$catalog/$namespace.$table; continuing"
                    }
                }
            }
        }
        return CompactionResult(
            groupsCompacted = groupsCompacted,
            filesIn = filesIn,
            filesOut = groupsCompacted,
            bytesIn = bytesIn,
            bytesOut = bytesOut,
            skippedConflicts = skipped,
        )
    }

    private fun liveTables(
        h: Handle,
        catalog: String,
    ): List<Pair<String, String>> {
        val cat =
            CatalogRepo.findByName(h, catalog)
                ?: throw HoglakeException.NotFound("catalog '$catalog'")
        return h.createQuery(
            """
            SELECT ns.name AS namespace, tv.name AS table_name
            FROM hog_table_version tv
            JOIN hog_namespace ns
              ON ns.catalog_id = tv.catalog_id AND ns.namespace_id = tv.namespace_id
            JOIN hog_table t
              ON t.catalog_id = tv.catalog_id AND t.table_id = tv.table_id
            WHERE tv.catalog_id = :catalogId AND tv.end_snapshot IS NULL
              AND NOT ns.dropped AND t.dropped_snapshot IS NULL
            ORDER BY ns.name, tv.name
            """,
        )
            .bind("catalogId", cat.catalogId)
            .map { rs, _ -> rs.getString("namespace") to rs.getString("table_name") }
            .list()
    }

    // ---- group execution + commit ----------------------------------------

    /**
     * Execute + commit one ALREADY-PLANNED group, without re-planning.
     * Test surface for the plan-to-commit race (a DV registered between
     * planning and here must abort the commit); production traffic goes
     * through [runOnce], which plans and executes in one sweep.
     */
    internal fun compactPlannedGroup(
        catalog: String,
        namespace: String,
        table: String,
        group: CompactionGroup,
    ): Long? {
        val ctx =
            jdbi.withHandleUnchecked { h ->
                planWithContext(h, catalog, namespace, table, defaults).ctx
            }
        return compactGroup(ctx, group)
    }

    /**
     * Rewrite one group (all IO outside any transaction) and commit it.
     * Returns output bytes, or null when the commit-time re-verification
     * found an input no longer live / no longer DV-free (skip; re-plan
     * next run).
     */
    private fun compactGroup(
        ctx: TableContext,
        group: CompactionGroup,
    ): Long? {
        val tmpDir = Files.createTempDirectory("hoglake-compaction")
        val tmpFiles = mutableListOf<Path>()
        try {
            val inputs =
                group.files.map { f ->
                    val local = tmpDir.resolve("in-${f.dataFileId}.parquet")
                    Files.write(local, store.get(f.path))
                    tmpFiles.add(local)
                    ParquetRewriter.Input(local, f.rowIdStart)
                }
            val outLocal = tmpDir.resolve("out.parquet")
            tmpFiles.add(outLocal)
            val rowsWritten =
                ParquetRewriter.rewrite(inputs, ctx.fieldIdsByName, ctx.sortFields, outLocal)
            check(rowsWritten == group.totalRecords) {
                "rewrite produced $rowsWritten rows but inputs registered ${group.totalRecords} — " +
                    "refusing to commit a lossy compaction"
            }
            val outputBytes = outLocal.fileSize()
            val footerSize = footerSize(outLocal)
            val outputPath =
                "${ctx.dataPath.trimEnd('/')}/data/${ctx.namespace}/${ctx.table}/" +
                    "compacted-${UUID.randomUUID()}.parquet"
            store.put(outputPath, Files.readAllBytes(outLocal))

            val stats =
                if (group.files.all { it.statsProvided }) {
                    aggregateStats(group.files.map { it.dataFileId }, ctx)
                } else {
                    null
                }
            val snapshotId =
                commitGroup(ctx, group, outputPath, outputBytes, footerSize, stats)
            if (snapshotId == null) {
                log.warn {
                    "compaction group for ${ctx.namespace}.${ctx.table} lost a plan-to-commit " +
                        "race (input dropped or gained a DV); skipping — uploaded output " +
                        "$outputPath is orphaned and left to bucket lifecycle"
                }
                return null
            }
            log.info {
                "compacted ${group.files.size} files (${group.totalBytes} B) of " +
                    "${ctx.namespace}.${ctx.table} into $outputPath ($outputBytes B) " +
                    "at snapshot $snapshotId"
            }
            return outputBytes
        } finally {
            tmpFiles.forEach { it.deleteIfExists() }
            tmpDir.deleteIfExists()
        }
    }

    /** Thrift footer length from the 4 LE bytes before the trailing "PAR1". */
    private fun footerSize(file: Path): Long {
        val bytes = Files.readAllBytes(file)
        require(bytes.size >= 8) { "output too small to be parquet" }
        return ByteBuffer.wrap(bytes, bytes.size - 8, 4).order(ByteOrder.LITTLE_ENDIAN).int.toLong()
    }

    /**
     * The metadata commit for one group: one transaction under the
     * per-catalog commit lock, CommitService's tail shape (parallel
     * code by design — its helpers are private and shaped around
     * appends; the comments here mark each mirrored step). Returns the
     * minted snapshot id, or null when re-verification failed.
     */
    private fun commitGroup(
        ctx: TableContext,
        group: CompactionGroup,
        outputPath: String,
        outputBytes: Long,
        footerSize: Long,
        stats: List<ColumnStats>?,
    ): Long? =
        jdbi.inTransactionUnchecked { h ->
            Locks.acquireCatalogCommitLock(h, ctx.catalogId)

            // Re-verify under the lock: every input must still be live and
            // still DV-free. A miss = plan-to-commit race; abort the group.
            val ids = group.files.map { it.dataFileId }
            val stillCompactable =
                h.createQuery(
                    """
                SELECT count(*) FROM hog_data_file f
                WHERE f.catalog_id = :catalogId AND f.table_id = :tableId
                  AND f.data_file_id IN (<ids>)
                  AND f.end_snapshot IS NULL
                  AND NOT EXISTS (
                        SELECT 1 FROM hog_delete_file dv
                        WHERE dv.catalog_id = f.catalog_id
                          AND dv.data_file_id = f.data_file_id
                          AND dv.end_snapshot IS NULL)
                """,
                )
                    .bind("catalogId", ctx.catalogId)
                    .bind("tableId", ctx.tableId)
                    .bindList("ids", ids)
                    .mapTo(Long::class.javaObjectType)
                    .one()
            if (stillCompactable != ids.size.toLong()) {
                return@inTransactionUnchecked null
            }

            // Allocate snapshot + file id under the lock (CommitService
            // step 5); compaction is not DDL: schema_version untouched.
            val (snapshotId, dataFileId, schemaVersion) =
                h.createQuery(
                    """
                UPDATE hog_catalog
                   SET last_snapshot_id = last_snapshot_id + 1,
                       next_file_id = next_file_id + 1
                 WHERE catalog_id = :catalogId
                RETURNING last_snapshot_id, next_file_id - 1 AS file_id, schema_version
                """,
                )
                    .bind("catalogId", ctx.catalogId)
                    .map { rs, _ -> Triple(rs.getLong(1), rs.getLong(2), rs.getLong(3)) }
                    .one()

            SnapshotRepo.insert(
                h,
                ctx.catalogId,
                snapshotId,
                schemaVersion,
                author = "compaction",
                message = "compacted ${group.files.size} files of ${ctx.namespace}.${ctx.table}",
            )
            SnapshotRepo.insertChange(h, ctx.catalogId, snapshotId, ChangeKind.TABLE_COMPACTED, ctx.tableId)

            // The output row. record_count = sum of inputs; row_id_start =
            // MIN of the inputs' — with explicit_row_ids its positional
            // meaning is void (the ids live in the _hog_row_id column);
            // it survives as the range-min for ordering and diagnostics.
            h.createUpdate(
                """
                INSERT INTO hog_data_file (catalog_id, data_file_id, table_id, begin_snapshot,
                                           path, record_count, file_size_bytes, footer_size,
                                           row_id_start, stats_state, spec_id, explicit_row_ids)
                VALUES (:catalogId, :dataFileId, :tableId, :beginSnapshot,
                        :path, :recordCount, :fileSizeBytes, :footerSize,
                        :rowIdStart, :statsState, :specId, true)
                """,
            )
                .bind("catalogId", ctx.catalogId)
                .bind("dataFileId", dataFileId)
                .bind("tableId", ctx.tableId)
                .bind("beginSnapshot", snapshotId)
                .bind("path", outputPath)
                .bind("recordCount", group.totalRecords)
                .bind("fileSizeBytes", outputBytes)
                .bind("footerSize", footerSize)
                .bind("rowIdStart", group.files.minOf { it.rowIdStart })
                .bind("statsState", if (stats != null) "provided" else "pending")
                .bind("specId", group.specId)
                .execute()

            val values = group.partitionValues
            if (values != null) {
                val batch =
                    h.prepareBatch(
                        """
                    INSERT INTO hog_file_partition_value (catalog_id, data_file_id, key_index, value)
                    VALUES (:catalogId, :dataFileId, :keyIndex, :value)
                    """,
                    )
                values.forEachIndexed { keyIndex, value ->
                    batch
                        .bind("catalogId", ctx.catalogId)
                        .bind("dataFileId", dataFileId)
                        .bind("keyIndex", keyIndex)
                        .bind("value", value)
                        .add()
                }
                batch.execute()
            }

            if (stats != null && stats.isNotEmpty()) {
                val batch =
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
                for (s in stats) {
                    batch
                        .bind("catalogId", ctx.catalogId)
                        .bind("dataFileId", dataFileId)
                        .bind("fieldId", s.fieldId)
                        .bind("valueCount", s.valueCount)
                        .bind("nullCount", s.nullCount)
                        .bind("nanCount", s.nanCount)
                        .bind("sizeBytes", s.sizeBytes)
                        .bind("lowerBound", s.lowerBound)
                        .bind("upperBound", s.upperBound)
                        .add()
                }
                batch.execute()
            }

            // End-snapshot the inputs — NOT delete: they stay readable at
            // every snapshot below this one, and expiry queues their paths
            // once end_snapshot sinks under the retention floor (the
            // superseded-DV lifecycle). Nothing enters hog_file_removal
            // here. hog_table_stats is untouched (gross append counters;
            // visible-file aggregates are unchanged by construction).
            h.createUpdate(
                """
                UPDATE hog_data_file SET end_snapshot = :snapshotId
                WHERE catalog_id = :catalogId AND data_file_id IN (<ids>)
                """,
            )
                .bind("snapshotId", snapshotId)
                .bind("catalogId", ctx.catalogId)
                .bindList("ids", ids)
                .execute()

            snapshotId
        }

    // ---- stats aggregation -----------------------------------------------

    /**
     * Merge the inputs' per-column stats into the output's: counts sum;
     * bounds are recomputed by DECODING each input bound to its typed
     * value, comparing typed, and re-encoding the winner — never a raw
     * binary compare of the encodings (wrong for signed little-endian
     * types). A field only gets a stats row when EVERY input has one;
     * within a field, nan/size/bounds go null if any input's is null.
     */
    private fun aggregateStats(
        inputIds: List<Long>,
        ctx: TableContext,
    ): List<ColumnStats> {
        data class StatsRow(
            val fieldId: Long,
            val valueCount: Long,
            val nullCount: Long,
            val nanCount: Long?,
            val sizeBytes: Long?,
            val lower: ByteArray?,
            val upper: ByteArray?,
        )

        val rows =
            jdbi.withHandleUnchecked { h ->
                h.createQuery(
                    """
                SELECT field_id, value_count, null_count, nan_count, size_bytes,
                       lower_bound, upper_bound
                FROM hog_file_column_stats
                WHERE catalog_id = :catalogId AND data_file_id IN (<ids>)
                """,
                )
                    .bind("catalogId", ctx.catalogId)
                    .bindList("ids", inputIds)
                    .map { rs, _ ->
                        StatsRow(
                            fieldId = rs.getLong("field_id"),
                            valueCount = rs.getLong("value_count"),
                            nullCount = rs.getLong("null_count"),
                            nanCount = rs.getObject("nan_count")?.let { (it as Number).toLong() },
                            sizeBytes = rs.getObject("size_bytes")?.let { (it as Number).toLong() },
                            lower = rs.getBytes("lower_bound"),
                            upper = rs.getBytes("upper_bound"),
                        )
                    }
                    .list()
            }

        val out = mutableListOf<ColumnStats>()
        for ((fieldId, fieldRows) in rows.groupBy { it.fieldId }) {
            if (fieldRows.size != inputIds.size) continue // not every input covered the field
            val type = ctx.columnTypes[fieldId] ?: continue // column dropped since the inputs landed
            out +=
                ColumnStats(
                    fieldId = fieldId,
                    valueCount = fieldRows.sumOf { it.valueCount },
                    nullCount = fieldRows.sumOf { it.nullCount },
                    nanCount =
                        if (fieldRows.any { it.nanCount == null }) null else fieldRows.sumOf { it.nanCount!! },
                    sizeBytes =
                        if (fieldRows.any { it.sizeBytes == null }) null else fieldRows.sumOf { it.sizeBytes!! },
                    lowerBound = mergeBound(type, fieldRows.map { it.lower }, takeUpper = false),
                    upperBound = mergeBound(type, fieldRows.map { it.upper }, takeUpper = true),
                )
        }
        return out.sortedBy { it.fieldId }
    }

    private fun mergeBound(
        type: ColType,
        bounds: List<ByteArray?>,
        takeUpper: Boolean,
    ): ByteArray? {
        if (bounds.any { it == null }) return null
        val decoded = bounds.map { IcebergSingleValue.decode(type, it!!) }
        val winner =
            decoded.reduce { a, b ->
                val cmp = IcebergSingleValue.compareValues(type, a, b)
                if ((cmp < 0) != takeUpper) a else b
            }
        return IcebergSingleValue.encode(type, winner)
    }

    // ---- loops -----------------------------------------------------------

    /**
     * One sweep across every catalog, for the background loop
     * (BackgroundLoops in App.kt); per-catalog failure isolation
     * (Expiry pattern).
     */
    fun runOnceAllCatalogs(cfg: CompactionConfig = defaults): List<Pair<String, CompactionResult>> {
        val names = jdbi.withHandleUnchecked { h -> CatalogRepo.listAll(h) }.map { it.name }
        val results = mutableListOf<Pair<String, CompactionResult>>()
        for (name in names) {
            try {
                results += name to runOnce(name, cfg)
            } catch (e: Exception) {
                log.error(e) { "compaction sweep failed for catalog '$name'; continuing" }
            }
        }
        return results
    }
}
