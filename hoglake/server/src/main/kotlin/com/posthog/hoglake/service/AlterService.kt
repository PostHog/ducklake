package com.posthog.hoglake.service

import com.posthog.hoglake.model.AlterOp
import com.posthog.hoglake.model.ChangeKind
import com.posthog.hoglake.model.ColType
import com.posthog.hoglake.model.Column
import com.posthog.hoglake.model.HoglakeException
import com.posthog.hoglake.model.PartitionSpec
import com.posthog.hoglake.model.SortSpec
import com.posthog.hoglake.model.TableInfo
import com.posthog.hoglake.model.Transform
import com.posthog.hoglake.model.canPromoteTo
import com.posthog.hoglake.observability.Audit
import com.posthog.hoglake.persistence.CatalogRepo
import com.posthog.hoglake.persistence.FileRepo
import com.posthog.hoglake.persistence.Locks
import com.posthog.hoglake.persistence.NamespaceRepo
import com.posthog.hoglake.persistence.SnapshotRepo
import com.posthog.hoglake.persistence.SortRepo
import com.posthog.hoglake.persistence.SpecRepo
import com.posthog.hoglake.persistence.TableRepo
import com.posthog.hoglake.stats.IcebergSingleValue
import org.jdbi.v3.core.Handle
import org.jdbi.v3.core.Jdbi
import org.jdbi.v3.core.kotlin.inTransactionUnchecked

/**
 * Schema evolution: one ALTER request is one DDL commit. The whole op
 * list applies atomically in a single transaction under the per-catalog
 * commit lock, minting exactly one snapshot with exactly one
 * `table_altered` change row regardless of op count.
 *
 * Ops apply IN ORDER against the evolving table state: each op is
 * validated against the shape produced by the ops before it, so e.g.
 * `[add c2, rename c2 -> c3]` works in one request and `[drop x,
 * rename x -> y]` fails Validation on the second op.
 *
 * Versioned-row mechanics follow the schema's rule (visible at S iff
 * begin_snapshot <= S AND (end_snapshot IS NULL OR S < end_snapshot)):
 * a mutation ends the live row at the new snapshot and inserts a
 * replacement beginning there. When an op mutates a row that an earlier
 * op in the SAME request created (begin_snapshot == new snapshot), the
 * row is deleted instead of ended — it was never visible at any
 * snapshot, and end == begin would violate the schema CHECK.
 */
class AlterService(private val jdbi: Jdbi) {
    fun alterTable(
        catalog: String,
        namespace: String,
        table: String,
        ops: List<AlterOp>,
    ): TableInfo =
        Audit.audited(
            "table_alter",
            catalog,
            "$namespace.$table",
            detail = { "ops=${summarize(ops)}" },
        ) {
            if (ops.isEmpty()) {
                throw HoglakeException.Validation("ops must contain at least one operation")
            }
            jdbi.inTransactionUnchecked { h ->
                val cat =
                    CatalogRepo.findByName(h, catalog)
                        ?: throw HoglakeException.NotFound("catalog '$catalog'")
                Locks.acquireCatalogCommitLock(h, cat.catalogId)
                val ns =
                    NamespaceRepo.findLiveByName(h, cat.catalogId, namespace)
                        ?: throw HoglakeException.NotFound("namespace '$namespace' in catalog '$catalog'")
                val t =
                    TableRepo.findLive(h, cat.catalogId, ns.namespaceId, table)
                        ?: throw HoglakeException.NotFound("table '$namespace.$table' in catalog '$catalog'")

                val alloc = CatalogRepo.allocateSnapshot(h, cat.catalogId)
                SnapshotRepo.insert(h, cat.catalogId, alloc.snapshotId, alloc.schemaVersion)
                SnapshotRepo.insertChange(
                    h,
                    cat.catalogId,
                    alloc.snapshotId,
                    ChangeKind.TABLE_ALTERED,
                    t.tableId,
                )

                // Live shape as of the pre-alter head (read under the lock).
                val state =
                    TableState(
                        cols =
                            TableRepo.columnsAt(h, cat.catalogId, t.tableId, alloc.snapshotId - 1)
                                .toMutableList(),
                        name = t.name,
                        spec = SpecRepo.specAt(h, cat.catalogId, t.tableId, alloc.snapshotId - 1),
                        sortSpec = SortRepo.sortSpecAt(h, cat.catalogId, t.tableId, alloc.snapshotId - 1),
                    )
                for (op in ops) {
                    applyOp(h, cat.catalogId, t.tableId, ns.namespaceId, namespace, alloc.snapshotId, state, op)
                }

                val agg = FileRepo.aggregateAt(h, cat.catalogId, t.tableId, alloc.snapshotId)
                TableInfo(
                    tableId = t.tableId,
                    tableUuid = t.tableUuid,
                    namespace = ns.name,
                    name = state.name,
                    columns = state.cols.sortedBy { it.ordinal },
                    recordCount = agg.recordCount,
                    fileCount = agg.fileCount,
                    fileSizeBytes = agg.fileSizeBytes,
                    partitionSpec = state.spec,
                    sortSpec = state.sortSpec,
                )
            }
        }

    /** Audit-detail summary of an op list: op kinds with counts, in order of first appearance. */
    private fun summarize(ops: List<AlterOp>): String =
        ops.groupingBy { it::class.simpleName ?: "Op" }
            .eachCount()
            .entries
            .joinToString(",") { (kind, n) -> if (n == 1) kind else "${kind}x$n" }

    // ---- op application --------------------------------------------------

    /** The evolving in-request view of the table's live shape. */
    private class TableState(
        val cols: MutableList<Column>,
        var name: String,
        var spec: PartitionSpec?,
        var sortSpec: SortSpec?,
    )

    private fun applyOp(
        h: Handle,
        catalogId: Long,
        tableId: Long,
        namespaceId: Long,
        namespaceName: String,
        snapshot: Long,
        state: TableState,
        op: AlterOp,
    ) = when (op) {
        is AlterOp.AddColumn -> addColumn(h, catalogId, tableId, snapshot, state, op)
        is AlterOp.DropColumn -> dropColumn(h, catalogId, tableId, snapshot, state, op)
        is AlterOp.RenameColumn -> renameColumn(h, catalogId, tableId, snapshot, state, op)
        is AlterOp.PromoteColumn -> promoteColumn(h, catalogId, tableId, snapshot, state, op)
        is AlterOp.RenameTable ->
            renameTable(h, catalogId, tableId, namespaceId, namespaceName, snapshot, state, op)
        is AlterOp.SetPartitionSpec -> setPartitionSpec(h, catalogId, tableId, snapshot, state, op)
        is AlterOp.SetSortOrder -> setSortOrder(h, catalogId, tableId, snapshot, state, op)
    }

    private fun addColumn(
        h: Handle,
        catalogId: Long,
        tableId: Long,
        snapshot: Long,
        state: TableState,
        op: AlterOp.AddColumn,
    ) {
        Identifiers.validate("column", op.def.name)
        if (state.cols.any { it.def.name == op.def.name }) {
            throw HoglakeException.Validation("column '${op.def.name}' already exists")
        }
        val fieldId = TableRepo.allocateFieldIds(h, catalogId, tableId, 1)
        val col =
            Column(
                fieldId = fieldId,
                ordinal = (state.cols.maxOfOrNull { it.ordinal } ?: -1) + 1,
                def = op.def,
            )
        TableRepo.insertColumns(h, catalogId, tableId, snapshot, listOf(col))
        state.cols += col
    }

    private fun dropColumn(
        h: Handle,
        catalogId: Long,
        tableId: Long,
        snapshot: Long,
        state: TableState,
        op: AlterOp.DropColumn,
    ) {
        val col = requireColumn(state, op.name)
        if (state.cols.size == 1) {
            throw HoglakeException.Validation("cannot drop '${op.name}': it is the last column")
        }
        val spec = state.spec
        if (spec != null && spec.fields.any { it.sourceFieldId == col.fieldId }) {
            throw HoglakeException.Validation(
                "cannot drop column '${op.name}': it is a source of the live partition spec",
            )
        }
        val sortSpec = state.sortSpec
        if (sortSpec != null && sortSpec.fields.any { it.sourceFieldId == col.fieldId }) {
            throw HoglakeException.Validation(
                "cannot drop column '${op.name}': it is a source of the live sort order",
            )
        }
        endOrDeleteColumnRow(h, catalogId, tableId, col.fieldId, snapshot)
        state.cols.remove(col)
    }

    private fun renameColumn(
        h: Handle,
        catalogId: Long,
        tableId: Long,
        snapshot: Long,
        state: TableState,
        op: AlterOp.RenameColumn,
    ) {
        Identifiers.validate("column", op.to)
        val col = requireColumn(state, op.from)
        if (state.cols.any { it.def.name == op.to }) {
            throw HoglakeException.Validation("column '${op.to}' already exists")
        }
        // The field-id contract guard: files whose parquet schema carries
        // no field ids bind columns by NAME. Renaming while any such file
        // is live (visible at head) would silently NULL that column's
        // history in readers, so the rename is refused (409) until the
        // id-less files are compacted, expired, or otherwise retired.
        // 'pending' files count too — missing_field_ids is only written by
        // the hydrator's footer read, so a not-yet-hydrated file's id state
        // is UNKNOWN and must be treated as id-less until proven otherwise
        // (the TOCTOU: commit deferred-stats id-less file -> rename slips
        // through before the sweep -> the flag arrives too late).
        // RenameTable is unaffected — table binding rides table_uuid.
        val (idlessLive, pendingLive) =
            h.createQuery(
                """
                SELECT count(*) FILTER (WHERE missing_field_ids) AS idless,
                       count(*) FILTER (WHERE stats_state = 'pending' AND NOT missing_field_ids) AS pending
                FROM hog_data_file
                WHERE catalog_id = :catalogId AND table_id = :tableId
                  AND end_snapshot IS NULL
                  AND (missing_field_ids OR stats_state = 'pending')
                """,
            )
                .bind("catalogId", catalogId)
                .bind("tableId", tableId)
                .map { rs, _ -> rs.getLong("idless") to rs.getLong("pending") }
                .one()
        if (idlessLive > 0 || pendingLive > 0) {
            val blockers =
                buildList {
                    if (idlessLive > 0) add("$idlessLive id-less")
                    if (pendingLive > 0) {
                        add(
                            "$pendingLive not-yet-hydrated (id state unknown until the footer is read)",
                        )
                    }
                }.joinToString(" and ")
            throw HoglakeException.IdlessFilesPresent(
                "cannot rename column '${op.from}' to '${op.to}': $blockers live data " +
                    "file(s) may bind columns by name — renaming would silently NULL " +
                    "their history in readers; hydrate, rewrite, or retire them first",
            )
        }
        endOrDeleteColumnRow(h, catalogId, tableId, col.fieldId, snapshot)
        val renamed = col.copy(def = col.def.copy(name = op.to))
        TableRepo.insertColumns(h, catalogId, tableId, snapshot, listOf(renamed))
        state.cols[state.cols.indexOf(col)] = renamed
    }

    private fun promoteColumn(
        h: Handle,
        catalogId: Long,
        tableId: Long,
        snapshot: Long,
        state: TableState,
        op: AlterOp.PromoteColumn,
    ) {
        val col = requireColumn(state, op.name)
        if (!col.def.type.canPromoteTo(op.to)) {
            throw HoglakeException.Validation(
                "cannot promote column '${op.name}' from '${col.def.type.wire}' to '${op.to.wire}'",
            )
        }
        endOrDeleteColumnRow(h, catalogId, tableId, col.fieldId, snapshot)
        val promoted = col.copy(def = col.def.copy(type = op.to))
        TableRepo.insertColumns(h, catalogId, tableId, snapshot, listOf(promoted))
        state.cols[state.cols.indexOf(col)] = promoted
        reencodeStatsOnPromote(h, catalogId, tableId, col.fieldId, col.def.type, op.to)
    }

    /**
     * Same-transaction stats re-encode for a width-changing promote:
     * existing hog_file_column_stats bounds for the column were written
     * in the OLD type's 4-byte Iceberg encoding; readers and compaction's
     * bound-merge decode bounds under the LIVE type (8 bytes after
     * int->long / float->double), so stale-width rows would either fail
     * decoding forever (the poison-group compaction wedge) or be skipped.
     * Values are preserved exactly — both promotions are lossless widens.
     *
     * Caveat, stated: a time-travel reader decoding these bounds at a
     * PRE-promote snapshot (column type still int/float there) sees
     * 8-byte encodings. Bounds are advisory pruning metadata, and the
     * head-correctness + never-wedge-compaction trade wins; clients that
     * cannot decode a bound must treat it as absent (the "NULL, never
     * guessed" contract's read-side dual).
     *
     * Bounded work: only this column's rows with 4-byte bounds, under the
     * catalog lock — promote is rare DDL. Rows hydrated concurrently under
     * the old type can still slip in AFTER this (hydrator race);
     * compaction's bound-merge skips undecodable widths as the backstop.
     */
    private fun reencodeStatsOnPromote(
        h: Handle,
        catalogId: Long,
        tableId: Long,
        fieldId: Long,
        from: ColType,
        to: ColType,
    ) {
        val widen: (ByteArray) -> ByteArray =
            when {
                from == ColType.INT && to == ColType.LONG -> ::widenIntToLong
                from == ColType.FLOAT && to == ColType.DOUBLE -> ::widenFloatToDouble
                else -> return
            }

        data class Stale(val dataFileId: Long, val lower: ByteArray?, val upper: ByteArray?)

        val stale =
            h.createQuery(
                """
                SELECT s.data_file_id, s.lower_bound, s.upper_bound
                FROM hog_file_column_stats s
                JOIN hog_data_file f
                  ON f.catalog_id = s.catalog_id AND f.data_file_id = s.data_file_id
                WHERE s.catalog_id = :catalogId AND f.table_id = :tableId
                  AND s.field_id = :fieldId
                  AND (octet_length(s.lower_bound) = 4 OR octet_length(s.upper_bound) = 4)
                """,
            )
                .bind("catalogId", catalogId)
                .bind("tableId", tableId)
                .bind("fieldId", fieldId)
                .map {
                        rs,
                        _,
                    ->
                    Stale(rs.getLong("data_file_id"), rs.getBytes("lower_bound"), rs.getBytes("upper_bound"))
                }
                .list()
        if (stale.isEmpty()) return
        val batch =
            h.prepareBatch(
                """
                UPDATE hog_file_column_stats
                   SET lower_bound = :lower, upper_bound = :upper
                 WHERE catalog_id = :catalogId AND data_file_id = :dataFileId AND field_id = :fieldId
                """,
            )
        for (row in stale) {
            batch
                .bind("lower", row.lower?.let { if (it.size == 4) widen(it) else it })
                .bind("upper", row.upper?.let { if (it.size == 4) widen(it) else it })
                .bind("catalogId", catalogId)
                .bind("dataFileId", row.dataFileId)
                .bind("fieldId", fieldId)
                .add()
        }
        batch.execute()
    }

    /** Lossless width promotion of one 4-byte int bound to the long encoding. */
    private fun widenIntToLong(b: ByteArray): ByteArray =
        IcebergSingleValue.encode(ColType.LONG, (IcebergSingleValue.decode(ColType.INT, b) as Int).toLong())

    /** Lossless width promotion of one 4-byte float bound to the double encoding. */
    private fun widenFloatToDouble(b: ByteArray): ByteArray =
        IcebergSingleValue.encode(ColType.DOUBLE, (IcebergSingleValue.decode(ColType.FLOAT, b) as Float).toDouble())

    private fun renameTable(
        h: Handle,
        catalogId: Long,
        tableId: Long,
        namespaceId: Long,
        namespaceName: String,
        snapshot: Long,
        state: TableState,
        op: AlterOp.RenameTable,
    ) {
        Identifiers.validate("table", op.newName)
        val existing = TableRepo.findLive(h, catalogId, namespaceId, op.newName)
        if (existing != null && existing.tableId != tableId) {
            throw HoglakeException.AlreadyExists(
                "table '${op.newName}' already exists in namespace '$namespaceName'",
            )
        }
        endOrDeleteVersionRow(h, catalogId, tableId, snapshot)
        TableRepo.insertVersion(h, catalogId, tableId, snapshot, namespaceId, op.newName)
        state.name = op.newName
    }

    private fun setPartitionSpec(
        h: Handle,
        catalogId: Long,
        tableId: Long,
        snapshot: Long,
        state: TableState,
        op: AlterOp.SetPartitionSpec,
    ) {
        for (f in op.fields) {
            val col =
                state.cols.find { it.fieldId == f.sourceFieldId }
                    ?: throw HoglakeException.Validation(
                        "partition source field_id ${f.sourceFieldId} is not a live column",
                    )
            when (f.transform) {
                Transform.BUCKET ->
                    if (f.transformParam == null || f.transformParam < 1) {
                        throw HoglakeException.Validation(
                            "bucket transform requires transform_param >= 1",
                        )
                    }
                else ->
                    if (f.transformParam != null) {
                        throw HoglakeException.Validation(
                            "transform '${f.transform.wire}' does not take transform_param",
                        )
                    }
            }
            if (f.transform in TEMPORAL_TRANSFORMS && col.def.type !in TEMPORAL_TYPES) {
                throw HoglakeException.Validation(
                    "transform '${f.transform.wire}' requires a date/timestamp/timestamptz " +
                        "column; '${col.def.name}' is '${col.def.type.wire}'",
                )
            }
        }
        endOrDeleteSpec(h, catalogId, tableId, snapshot)
        if (op.fields.isEmpty()) {
            state.spec = null
            return
        }
        val specId =
            h.createQuery(
                """
            SELECT COALESCE(MAX(spec_id), 0) + 1 FROM hog_partition_spec
            WHERE catalog_id = :catalogId AND table_id = :tableId
            """,
            )
                .bind("catalogId", catalogId)
                .bind("tableId", tableId)
                .mapTo(Long::class.javaObjectType)
                .one()
        h.createUpdate(
            """
            INSERT INTO hog_partition_spec (catalog_id, table_id, spec_id, begin_snapshot)
            VALUES (:catalogId, :tableId, :specId, :beginSnapshot)
            """,
        )
            .bind("catalogId", catalogId)
            .bind("tableId", tableId)
            .bind("specId", specId)
            .bind("beginSnapshot", snapshot)
            .execute()
        val batch =
            h.prepareBatch(
                """
            INSERT INTO hog_partition_field
                (catalog_id, table_id, spec_id, key_index, source_field_id, transform, transform_param)
            VALUES (:catalogId, :tableId, :specId, :keyIndex, :sourceFieldId, :transform, :transformParam)
            """,
            )
        op.fields.forEachIndexed { i, f ->
            batch
                .bind("catalogId", catalogId)
                .bind("tableId", tableId)
                .bind("specId", specId)
                .bind("keyIndex", i)
                .bind("sourceFieldId", f.sourceFieldId)
                .bind("transform", f.transform.wire)
                .bind("transformParam", f.transformParam)
                .add()
        }
        batch.execute()
        state.spec = PartitionSpec(specId, op.fields)
    }

    /**
     * SetPartitionSpec's twin for sort orders: validate every source
     * field against the post-ops live columns, retire the live spec,
     * mint sort_id = max + 1 with the new fields (empty = unsorted).
     * Advisory for writers, binding for compaction rewrites.
     */
    private fun setSortOrder(
        h: Handle,
        catalogId: Long,
        tableId: Long,
        snapshot: Long,
        state: TableState,
        op: AlterOp.SetSortOrder,
    ) {
        val seen = HashSet<Long>()
        for (f in op.fields) {
            if (state.cols.none { it.fieldId == f.sourceFieldId }) {
                throw HoglakeException.Validation(
                    "sort source field_id ${f.sourceFieldId} is not a live column",
                )
            }
            if (!seen.add(f.sourceFieldId)) {
                throw HoglakeException.Validation(
                    "duplicate sort source field_id ${f.sourceFieldId}",
                )
            }
        }
        endOrDeleteSortSpec(h, catalogId, tableId, snapshot)
        if (op.fields.isEmpty()) {
            state.sortSpec = null
            return
        }
        val sortId =
            h.createQuery(
                """
            SELECT COALESCE(MAX(sort_id), 0) + 1 FROM hog_sort_spec
            WHERE catalog_id = :catalogId AND table_id = :tableId
            """,
            )
                .bind("catalogId", catalogId)
                .bind("tableId", tableId)
                .mapTo(Long::class.javaObjectType)
                .one()
        h.createUpdate(
            """
            INSERT INTO hog_sort_spec (catalog_id, table_id, sort_id, begin_snapshot)
            VALUES (:catalogId, :tableId, :sortId, :beginSnapshot)
            """,
        )
            .bind("catalogId", catalogId)
            .bind("tableId", tableId)
            .bind("sortId", sortId)
            .bind("beginSnapshot", snapshot)
            .execute()
        val batch =
            h.prepareBatch(
                """
            INSERT INTO hog_sort_field
                (catalog_id, table_id, sort_id, key_index, source_field_id, direction, null_order)
            VALUES (:catalogId, :tableId, :sortId, :keyIndex, :sourceFieldId, :direction, :nullOrder)
            """,
            )
        op.fields.forEachIndexed { i, f ->
            batch
                .bind("catalogId", catalogId)
                .bind("tableId", tableId)
                .bind("sortId", sortId)
                .bind("keyIndex", i)
                .bind("sourceFieldId", f.sourceFieldId)
                .bind("direction", f.direction.wire)
                .bind("nullOrder", f.nullOrder.wire)
                .add()
        }
        batch.execute()
        state.sortSpec = SortSpec(sortId, op.fields)
    }

    // ---- row lifecycle helpers -------------------------------------------

    private fun requireColumn(
        state: TableState,
        name: String,
    ): Column =
        state.cols.find { it.def.name == name }
            ?: throw HoglakeException.Validation("column '$name' does not exist")

    /**
     * Retire a field's live column row at [snapshot]: delete it if this
     * request created it (begin_snapshot == snapshot — never visible),
     * else end-snapshot it.
     */
    private fun endOrDeleteColumnRow(
        h: Handle,
        catalogId: Long,
        tableId: Long,
        fieldId: Long,
        snapshot: Long,
    ) {
        val deleted =
            h.createUpdate(
                """
            DELETE FROM hog_column
            WHERE catalog_id = :catalogId AND table_id = :tableId
              AND field_id = :fieldId AND begin_snapshot = :snapshot
            """,
            )
                .bind("catalogId", catalogId)
                .bind("tableId", tableId)
                .bind("fieldId", fieldId)
                .bind("snapshot", snapshot)
                .execute()
        if (deleted == 0) {
            h.createUpdate(
                """
                UPDATE hog_column SET end_snapshot = :snapshot
                WHERE catalog_id = :catalogId AND table_id = :tableId
                  AND field_id = :fieldId AND end_snapshot IS NULL
                """,
            )
                .bind("catalogId", catalogId)
                .bind("tableId", tableId)
                .bind("fieldId", fieldId)
                .bind("snapshot", snapshot)
                .execute()
        }
    }

    /** Same delete-if-created-here-else-end rule for hog_table_version. */
    private fun endOrDeleteVersionRow(
        h: Handle,
        catalogId: Long,
        tableId: Long,
        snapshot: Long,
    ) {
        val deleted =
            h.createUpdate(
                """
            DELETE FROM hog_table_version
            WHERE catalog_id = :catalogId AND table_id = :tableId AND begin_snapshot = :snapshot
            """,
            )
                .bind("catalogId", catalogId)
                .bind("tableId", tableId)
                .bind("snapshot", snapshot)
                .execute()
        if (deleted == 0) {
            h.createUpdate(
                """
                UPDATE hog_table_version SET end_snapshot = :snapshot
                WHERE catalog_id = :catalogId AND table_id = :tableId AND end_snapshot IS NULL
                """,
            )
                .bind("catalogId", catalogId)
                .bind("tableId", tableId)
                .bind("snapshot", snapshot)
                .execute()
        }
    }

    /**
     * Retire the live partition spec (if any): delete it (fields cascade)
     * when this request created it, else end-snapshot it.
     */
    private fun endOrDeleteSpec(
        h: Handle,
        catalogId: Long,
        tableId: Long,
        snapshot: Long,
    ) {
        val deleted =
            h.createUpdate(
                """
            DELETE FROM hog_partition_spec
            WHERE catalog_id = :catalogId AND table_id = :tableId
              AND end_snapshot IS NULL AND begin_snapshot = :snapshot
            """,
            )
                .bind("catalogId", catalogId)
                .bind("tableId", tableId)
                .bind("snapshot", snapshot)
                .execute()
        if (deleted == 0) {
            h.createUpdate(
                """
                UPDATE hog_partition_spec SET end_snapshot = :snapshot
                WHERE catalog_id = :catalogId AND table_id = :tableId AND end_snapshot IS NULL
                """,
            )
                .bind("catalogId", catalogId)
                .bind("tableId", tableId)
                .bind("snapshot", snapshot)
                .execute()
        }
    }

    /** Same delete-if-created-here-else-end rule for the sort spec (fields cascade). */
    private fun endOrDeleteSortSpec(
        h: Handle,
        catalogId: Long,
        tableId: Long,
        snapshot: Long,
    ) {
        val deleted =
            h.createUpdate(
                """
            DELETE FROM hog_sort_spec
            WHERE catalog_id = :catalogId AND table_id = :tableId
              AND end_snapshot IS NULL AND begin_snapshot = :snapshot
            """,
            )
                .bind("catalogId", catalogId)
                .bind("tableId", tableId)
                .bind("snapshot", snapshot)
                .execute()
        if (deleted == 0) {
            h.createUpdate(
                """
                UPDATE hog_sort_spec SET end_snapshot = :snapshot
                WHERE catalog_id = :catalogId AND table_id = :tableId AND end_snapshot IS NULL
                """,
            )
                .bind("catalogId", catalogId)
                .bind("tableId", tableId)
                .bind("snapshot", snapshot)
                .execute()
        }
    }

    private companion object {
        val TEMPORAL_TRANSFORMS =
            setOf(Transform.YEAR, Transform.MONTH, Transform.DAY, Transform.HOUR)
        val TEMPORAL_TYPES =
            setOf(ColType.DATE, ColType.TIMESTAMP, ColType.TIMESTAMPTZ)
    }
}
