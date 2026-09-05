package com.posthog.hoglake.service

import com.posthog.hoglake.model.CatalogInfo
import com.posthog.hoglake.model.ChangeKind
import com.posthog.hoglake.model.Column
import com.posthog.hoglake.model.ColumnDef
import com.posthog.hoglake.model.CommitResult
import com.posthog.hoglake.model.ConsumerOffset
import com.posthog.hoglake.model.DataFile
import com.posthog.hoglake.model.HoglakeException
import com.posthog.hoglake.model.NamespaceInfo
import com.posthog.hoglake.model.Snapshot
import com.posthog.hoglake.model.TableInfo
import com.posthog.hoglake.persistence.CatalogRepo
import com.posthog.hoglake.persistence.FileRepo
import com.posthog.hoglake.persistence.Locks
import com.posthog.hoglake.persistence.NamespaceRepo
import com.posthog.hoglake.persistence.OffsetRepo
import com.posthog.hoglake.persistence.SnapshotRepo
import com.posthog.hoglake.persistence.TableRepo
import org.jdbi.v3.core.Handle
import org.jdbi.v3.core.Jdbi
import org.jdbi.v3.core.kotlin.inTransactionUnchecked
import org.jdbi.v3.core.kotlin.withHandleUnchecked

/**
 * Catalog DDL + read paths. Every DDL operation is one transaction that
 * (1) takes the per-catalog commit lock, (2) allocates the next
 * snapshot id / schema version off hog_catalog, (3) records the
 * snapshot + typed change row, then (4) writes the object rows. The
 * append-commit tail (commit/) follows the same shape.
 *
 * Reads honor the versioned-row rule: visible at S iff
 * begin_snapshot <= S AND (end_snapshot IS NULL OR S < end_snapshot).
 */
class CatalogService(private val jdbi: Jdbi) {

    // ---- catalogs --------------------------------------------------------

    fun createCatalog(name: String, dataPath: String): CatalogInfo {
        if (dataPath.isBlank()) throw HoglakeException.Validation("data_path must not be blank")
        return jdbi.inTransactionUnchecked { h ->
            val info = CatalogRepo.insert(h, name, dataPath)
            // Snapshot 0: the empty catalog at schema_version 0, no changes.
            SnapshotRepo.insert(h, info.catalogId, 0, 0)
            info
        }
    }

    fun getCatalog(name: String): CatalogInfo =
        jdbi.withHandleUnchecked { h -> requireCatalog(h, name) }

    fun listCatalogs(): List<CatalogInfo> =
        jdbi.withHandleUnchecked { h -> CatalogRepo.listAll(h) }

    // ---- namespaces ------------------------------------------------------

    fun createNamespace(catalog: String, name: String): NamespaceInfo {
        if (name.isBlank()) throw HoglakeException.Validation("namespace name must not be blank")
        return jdbi.inTransactionUnchecked { h ->
            val cat = requireCatalog(h, catalog)
            Locks.acquireCatalogCommitLock(h, cat.catalogId)
            if (NamespaceRepo.findLiveByName(h, cat.catalogId, name) != null) {
                throw HoglakeException.AlreadyExists("namespace '$name' already exists")
            }
            val alloc = CatalogRepo.allocateSnapshot(h, cat.catalogId)
            SnapshotRepo.insert(h, cat.catalogId, alloc.snapshotId, alloc.schemaVersion)
            val namespaceId = CatalogRepo.allocateNamespaceId(h, cat.catalogId)
            SnapshotRepo.insertChange(
                h, cat.catalogId, alloc.snapshotId, ChangeKind.NAMESPACE_CREATED, namespaceId,
            )
            NamespaceRepo.insert(h, cat.catalogId, namespaceId, name)
        }
    }

    fun listNamespaces(catalog: String): List<NamespaceInfo> =
        jdbi.withHandleUnchecked { h ->
            val cat = requireCatalog(h, catalog)
            NamespaceRepo.listLive(h, cat.catalogId)
        }

    // ---- tables ----------------------------------------------------------

    fun createTable(
        catalog: String,
        namespace: String,
        name: String,
        columns: List<ColumnDef>,
    ): TableInfo {
        if (name.isBlank()) throw HoglakeException.Validation("table name must not be blank")
        if (columns.isEmpty()) {
            throw HoglakeException.Validation("table '$name' must have at least one column")
        }
        val dupes = columns.groupingBy { it.name }.eachCount().filterValues { it > 1 }.keys
        if (dupes.isNotEmpty()) {
            throw HoglakeException.Validation("duplicate column names: ${dupes.sorted()}")
        }
        return jdbi.inTransactionUnchecked { h ->
            val cat = requireCatalog(h, catalog)
            Locks.acquireCatalogCommitLock(h, cat.catalogId)
            val ns = requireNamespace(h, cat, namespace)
            if (TableRepo.findLive(h, cat.catalogId, ns.namespaceId, name) != null) {
                throw HoglakeException.AlreadyExists(
                    "table '$name' already exists in namespace '$namespace'",
                )
            }
            val alloc = CatalogRepo.allocateSnapshot(h, cat.catalogId)
            SnapshotRepo.insert(h, cat.catalogId, alloc.snapshotId, alloc.schemaVersion)
            val tableId = CatalogRepo.allocateTableId(h, cat.catalogId)
            SnapshotRepo.insertChange(
                h, cat.catalogId, alloc.snapshotId, ChangeKind.TABLE_CREATED, tableId,
            )
            val tableUuid = TableRepo.insertTable(h, cat.catalogId, tableId, alloc.snapshotId)
            val firstFieldId = TableRepo.allocateFieldIds(h, cat.catalogId, tableId, columns.size)
            val cols = columns.mapIndexed { i, def ->
                Column(fieldId = firstFieldId + i, ordinal = i, def = def)
            }
            TableRepo.insertVersion(h, cat.catalogId, tableId, alloc.snapshotId, ns.namespaceId, name)
            TableRepo.insertColumns(h, cat.catalogId, tableId, alloc.snapshotId, cols)
            TableRepo.insertStatsRow(h, cat.catalogId, tableId)
            TableInfo(
                tableId = tableId,
                tableUuid = tableUuid,
                namespace = ns.name,
                name = name,
                columns = cols,
                recordCount = 0,
                fileCount = 0,
                fileSizeBytes = 0,
            )
        }
    }

    fun dropTable(catalog: String, namespace: String, table: String): CommitResult =
        jdbi.inTransactionUnchecked { h ->
            val cat = requireCatalog(h, catalog)
            Locks.acquireCatalogCommitLock(h, cat.catalogId)
            val ns = requireNamespace(h, cat, namespace)
            val t = TableRepo.findLive(h, cat.catalogId, ns.namespaceId, table)
                ?: throw HoglakeException.NotFound("table '$namespace.$table' in catalog '$catalog'")
            val alloc = CatalogRepo.allocateSnapshot(h, cat.catalogId)
            SnapshotRepo.insert(h, cat.catalogId, alloc.snapshotId, alloc.schemaVersion)
            SnapshotRepo.insertChange(
                h, cat.catalogId, alloc.snapshotId, ChangeKind.TABLE_DROPPED, t.tableId,
            )
            TableRepo.markDropped(h, cat.catalogId, t.tableId, alloc.snapshotId)
            FileRepo.endLiveFiles(h, cat.catalogId, t.tableId, alloc.snapshotId)
            CommitResult(snapshotId = alloc.snapshotId, schemaVersion = alloc.schemaVersion)
        }

    fun getTable(
        catalog: String,
        namespace: String,
        table: String,
        snapshot: Long? = null,
    ): TableInfo =
        jdbi.withHandleUnchecked { h ->
            val cat = requireCatalog(h, catalog)
            val at = resolveSnapshot(cat, snapshot)
            val ns = requireNamespace(h, cat, namespace)
            val t = TableRepo.findAt(h, cat.catalogId, ns.namespaceId, table, at)
                ?: throw HoglakeException.NotFound(
                    "table '$namespace.$table' in catalog '$catalog' at snapshot $at",
                )
            val stats = TableRepo.stats(h, cat.catalogId, t.tableId)
            TableInfo(
                tableId = t.tableId,
                tableUuid = t.tableUuid,
                namespace = ns.name,
                name = t.name,
                columns = TableRepo.columnsAt(h, cat.catalogId, t.tableId, at),
                recordCount = stats.recordCount,
                fileCount = FileRepo.countAt(h, cat.catalogId, t.tableId, at),
                fileSizeBytes = stats.fileSizeBytes,
            )
        }

    /** Live tables at head. Columns are NOT loaded here (empty lists). */
    fun listTables(catalog: String, namespace: String): List<TableInfo> =
        jdbi.withHandleUnchecked { h ->
            val cat = requireCatalog(h, catalog)
            val ns = requireNamespace(h, cat, namespace)
            TableRepo.listLive(h, cat.catalogId, ns.namespaceId).map { t ->
                val stats = TableRepo.stats(h, cat.catalogId, t.tableId)
                TableInfo(
                    tableId = t.tableId,
                    tableUuid = t.tableUuid,
                    namespace = ns.name,
                    name = t.name,
                    columns = emptyList(),
                    recordCount = stats.recordCount,
                    fileCount = FileRepo.countAt(h, cat.catalogId, t.tableId, cat.headSnapshotId),
                    fileSizeBytes = stats.fileSizeBytes,
                )
            }
        }

    // ---- files + changefeed ----------------------------------------------

    fun listFiles(
        catalog: String,
        namespace: String,
        table: String,
        snapshot: Long? = null,
    ): List<DataFile> =
        jdbi.withHandleUnchecked { h ->
            val cat = requireCatalog(h, catalog)
            val at = resolveSnapshot(cat, snapshot)
            val ns = requireNamespace(h, cat, namespace)
            val t = TableRepo.findAt(h, cat.catalogId, ns.namespaceId, table, at)
                ?: throw HoglakeException.NotFound(
                    "table '$namespace.$table' in catalog '$catalog' at snapshot $at",
                )
            FileRepo.listAt(h, cat.catalogId, t.tableId, at)
        }

    /**
     * Changefeed plan for (fromSnapshot, toSnapshot]: the table's
     * identity (uuid — incarnation changes are visible, not deduced),
     * the resolved range, and the files whose begin_snapshot falls in
     * the half-open range, in append order.
     */
    fun changes(
        catalog: String,
        namespace: String,
        table: String,
        fromSnapshot: Long,
        toSnapshot: Long? = null,
    ): Triple<java.util.UUID, LongRange, List<DataFile>> =
        jdbi.withHandleUnchecked { h ->
            val cat = requireCatalog(h, catalog)
            val to = resolveSnapshot(cat, toSnapshot)
            if (fromSnapshot < 0) {
                throw HoglakeException.Validation("from_snapshot must be >= 0, got $fromSnapshot")
            }
            if (fromSnapshot > to) {
                throw HoglakeException.Validation(
                    "from_snapshot $fromSnapshot is beyond to_snapshot $to",
                )
            }
            val ns = requireNamespace(h, cat, namespace)
            val t = TableRepo.findAt(h, cat.catalogId, ns.namespaceId, table, to)
                ?: throw HoglakeException.NotFound(
                    "table '$namespace.$table' in catalog '$catalog' at snapshot $to",
                )
            Triple(
                t.tableUuid,
                fromSnapshot..to,
                FileRepo.changedIn(h, cat.catalogId, t.tableId, fromSnapshot, to),
            )
        }

    // ---- snapshots -------------------------------------------------------

    /**
     * One page of snapshots with id > [after] (changes populated),
     * plus whether more pages exist.
     */
    fun listSnapshots(catalog: String, after: Long, limit: Int): Pair<List<Snapshot>, Boolean> {
        if (limit < 1) throw HoglakeException.Validation("limit must be >= 1, got $limit")
        return jdbi.withHandleUnchecked { h ->
            val cat = requireCatalog(h, catalog)
            val raw = SnapshotRepo.page(h, cat.catalogId, after, limit + 1)
            val hasMore = raw.size > limit
            val page = raw.take(limit)
            if (page.isEmpty()) return@withHandleUnchecked Pair(emptyList(), false)
            val changes = SnapshotRepo.changesFor(
                h, cat.catalogId, page.first().snapshotId, page.last().snapshotId,
            )
            Pair(
                page.map { it.copy(changes = changes[it.snapshotId] ?: emptyList()) },
                hasMore,
            )
        }
    }

    // ---- consumer offsets ------------------------------------------------

    fun commitOffset(
        catalog: String,
        consumerId: String,
        tableUuid: java.util.UUID,
        snapshotId: Long,
    ): ConsumerOffset =
        jdbi.inTransactionUnchecked { h ->
            val cat = requireCatalog(h, catalog)
            if (snapshotId < 0 || snapshotId > cat.headSnapshotId) {
                throw HoglakeException.Validation(
                    "snapshot $snapshotId out of range [0, ${cat.headSnapshotId}]",
                )
            }
            OffsetRepo.upsert(h, cat.catalogId, consumerId, tableUuid, snapshotId)
                ?: throw HoglakeException.OffsetRegression(
                    "consumer '$consumerId' already committed past snapshot $snapshotId " +
                        "for table $tableUuid",
                )
        }

    fun listOffsets(catalog: String, consumerId: String): List<ConsumerOffset> =
        jdbi.withHandleUnchecked { h ->
            val cat = requireCatalog(h, catalog)
            OffsetRepo.list(h, cat.catalogId, consumerId)
        }

    // ---- helpers ---------------------------------------------------------

    private fun requireCatalog(h: Handle, name: String): CatalogInfo =
        CatalogRepo.findByName(h, name)
            ?: throw HoglakeException.NotFound("catalog '$name'")

    private fun requireNamespace(h: Handle, cat: CatalogInfo, name: String): NamespaceInfo =
        NamespaceRepo.findLiveByName(h, cat.catalogId, name)
            ?: throw HoglakeException.NotFound("namespace '$name' in catalog '${cat.name}'")

    /** Time-travel target: default head; reject out-of-range ids. */
    private fun resolveSnapshot(cat: CatalogInfo, snapshot: Long?): Long {
        val s = snapshot ?: return cat.headSnapshotId
        if (s < 0 || s > cat.headSnapshotId) {
            throw HoglakeException.Validation(
                "snapshot $s out of range [0, ${cat.headSnapshotId}] for catalog '${cat.name}'",
            )
        }
        return s
    }
}
