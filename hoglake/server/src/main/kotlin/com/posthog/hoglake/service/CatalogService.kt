package com.posthog.hoglake.service

import com.posthog.hoglake.model.CatalogInfo
import com.posthog.hoglake.model.ChangeKind
import com.posthog.hoglake.model.ChangesPlan
import com.posthog.hoglake.model.Column
import com.posthog.hoglake.model.ColumnDef
import com.posthog.hoglake.model.CommitResult
import com.posthog.hoglake.model.ConsumerOffset
import com.posthog.hoglake.model.DataFile
import com.posthog.hoglake.model.HoglakeException
import com.posthog.hoglake.model.NamespaceInfo
import com.posthog.hoglake.model.Snapshot
import com.posthog.hoglake.model.TableInfo
import com.posthog.hoglake.observability.Audit
import com.posthog.hoglake.persistence.CatalogRepo
import com.posthog.hoglake.persistence.DeleteFileReadRepo
import com.posthog.hoglake.persistence.FileRepo
import com.posthog.hoglake.persistence.Locks
import com.posthog.hoglake.persistence.NamespaceRepo
import com.posthog.hoglake.persistence.OffsetRepo
import com.posthog.hoglake.persistence.SnapshotRepo
import com.posthog.hoglake.persistence.SortRepo
import com.posthog.hoglake.persistence.SpecRepo
import com.posthog.hoglake.persistence.TableRepo
import com.posthog.hoglake.persistence.TimeTravelRepo
import org.jdbi.v3.core.Handle
import org.jdbi.v3.core.Jdbi
import org.jdbi.v3.core.kotlin.inTransactionUnchecked
import org.jdbi.v3.core.kotlin.withHandleUnchecked
import java.time.Instant

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

    fun createCatalog(
        name: String,
        dataPath: String,
    ): CatalogInfo =
        Audit.audited("catalog_create", name, null, detail = { "data_path=$dataPath" }) {
            if (dataPath.isBlank()) throw HoglakeException.Validation("data_path must not be blank")
            jdbi.inTransactionUnchecked { h ->
                val info = CatalogRepo.insert(h, name, dataPath)
                // Snapshot 0: the empty catalog at schema_version 0, no changes.
                SnapshotRepo.insert(h, info.catalogId, 0, 0)
                info
            }
        }

    fun getCatalog(name: String): CatalogInfo = jdbi.withHandleUnchecked { h -> requireCatalog(h, name) }

    fun listCatalogs(): List<CatalogInfo> = jdbi.withHandleUnchecked { h -> CatalogRepo.listAll(h) }

    // ---- namespaces ------------------------------------------------------

    fun createNamespace(
        catalog: String,
        name: String,
    ): NamespaceInfo =
        Audit.audited(
            "namespace_create",
            catalog,
            name,
        ) {
            Identifiers.validate("namespace", name)
            jdbi.inTransactionUnchecked { h ->
                val cat = requireCatalog(h, catalog)
                Locks.acquireCatalogCommitLock(h, cat.catalogId)
                if (NamespaceRepo.findLiveByName(h, cat.catalogId, name) != null) {
                    throw HoglakeException.AlreadyExists("namespace '$name' already exists")
                }
                val alloc = CatalogRepo.allocateSnapshot(h, cat.catalogId)
                SnapshotRepo.insert(h, cat.catalogId, alloc.snapshotId, alloc.schemaVersion)
                val namespaceId = CatalogRepo.allocateNamespaceId(h, cat.catalogId)
                SnapshotRepo.insertChange(
                    h,
                    cat.catalogId,
                    alloc.snapshotId,
                    ChangeKind.NAMESPACE_CREATED,
                    namespaceId,
                )
                NamespaceRepo.insert(h, cat.catalogId, namespaceId, name)
            }
        }

    fun listNamespaces(catalog: String): List<NamespaceInfo> =
        jdbi.withHandleUnchecked { h ->
            val cat = requireCatalog(h, catalog)
            NamespaceRepo.listLive(h, cat.catalogId)
        }

    fun getNamespace(
        catalog: String,
        namespace: String,
    ): NamespaceInfo =
        jdbi.withHandleUnchecked { h ->
            val cat = requireCatalog(h, catalog)
            requireNamespace(h, cat, namespace)
        }

    // ---- tables ----------------------------------------------------------

    fun createTable(
        catalog: String,
        namespace: String,
        name: String,
        columns: List<ColumnDef>,
    ): TableInfo =
        Audit.audited(
            "table_create",
            catalog,
            "$namespace.$name",
            detail = { "columns=${columns.size}" },
        ) {
            Identifiers.validate("table", name)
            if (columns.isEmpty()) {
                throw HoglakeException.Validation("table '$name' must have at least one column")
            }
            columns.forEach { Identifiers.validate("column", it.name) }
            val dupes = columns.groupingBy { it.name }.eachCount().filterValues { it > 1 }.keys
            if (dupes.isNotEmpty()) {
                throw HoglakeException.Validation("duplicate column names: ${dupes.sorted()}")
            }
            jdbi.inTransactionUnchecked { h ->
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
                    h,
                    cat.catalogId,
                    alloc.snapshotId,
                    ChangeKind.TABLE_CREATED,
                    tableId,
                )
                val tableUuid = TableRepo.insertTable(h, cat.catalogId, tableId, alloc.snapshotId)
                val firstFieldId = TableRepo.allocateFieldIds(h, cat.catalogId, tableId, columns.size)
                val cols =
                    columns.mapIndexed { i, def ->
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

    fun dropTable(
        catalog: String,
        namespace: String,
        table: String,
    ): CommitResult =
        Audit.audited(
            "table_drop",
            catalog,
            "$namespace.$table",
            detail = { "snapshot=${it.snapshotId}" },
        ) {
            jdbi.inTransactionUnchecked { h ->
                val cat = requireCatalog(h, catalog)
                Locks.acquireCatalogCommitLock(h, cat.catalogId)
                val ns = requireNamespace(h, cat, namespace)
                val t =
                    TableRepo.findLive(h, cat.catalogId, ns.namespaceId, table)
                        ?: throw HoglakeException.NotFound("table '$namespace.$table' in catalog '$catalog'")
                val alloc = CatalogRepo.allocateSnapshot(h, cat.catalogId)
                SnapshotRepo.insert(h, cat.catalogId, alloc.snapshotId, alloc.schemaVersion)
                SnapshotRepo.insertChange(
                    h,
                    cat.catalogId,
                    alloc.snapshotId,
                    ChangeKind.TABLE_DROPPED,
                    t.tableId,
                )
                TableRepo.markDropped(h, cat.catalogId, t.tableId, alloc.snapshotId)
                // DVs FIRST, same end-snapshot pattern as the data files: a
                // live DV left open on a dropped table would be invisible
                // to expiry's range predicates (end_snapshot IS NULL never
                // sinks below the floor), leaking the row AND the object
                // forever. End-snapshotted here, the superseded-DV
                // lifecycle reclaims it: expiry queues the path once the
                // drop snapshot falls under the retention floor.
                FileRepo.endLiveDeleteFiles(h, cat.catalogId, t.tableId, alloc.snapshotId)
                FileRepo.endLiveFiles(h, cat.catalogId, t.tableId, alloc.snapshotId)
                CommitResult(snapshotId = alloc.snapshotId, schemaVersion = alloc.schemaVersion)
            }
        }

    fun getTable(
        catalog: String,
        namespace: String,
        table: String,
        snapshot: Long? = null,
        atTimestamp: Instant? = null,
    ): TableInfo =
        jdbi.withHandleUnchecked { h ->
            val cat = requireCatalog(h, catalog)
            val at = resolveReadSnapshot(h, cat, snapshot, atTimestamp)
            val ns = requireNamespace(h, cat, namespace)
            val t =
                TableRepo.findAt(h, cat.catalogId, ns.namespaceId, table, at)
                    ?: throw HoglakeException.NotFound(
                        "table '$namespace.$table' in catalog '$catalog' at snapshot $at",
                    )
            val agg = FileRepo.aggregateAt(h, cat.catalogId, t.tableId, at)
            TableInfo(
                tableId = t.tableId,
                tableUuid = t.tableUuid,
                namespace = ns.name,
                name = t.name,
                columns = TableRepo.columnsAt(h, cat.catalogId, t.tableId, at),
                recordCount = agg.recordCount,
                fileCount = agg.fileCount,
                fileSizeBytes = agg.fileSizeBytes,
                // The specs visible at the requested snapshot (null =
                // unpartitioned/unsorted there); listTables skips both.
                partitionSpec = SpecRepo.specAt(h, cat.catalogId, t.tableId, at),
                sortSpec = SortRepo.sortSpecAt(h, cat.catalogId, t.tableId, at),
            )
        }

    /** Live tables at head. Columns are NOT loaded here (empty lists). */
    fun listTables(
        catalog: String,
        namespace: String,
    ): List<TableInfo> =
        jdbi.withHandleUnchecked { h ->
            val cat = requireCatalog(h, catalog)
            val ns = requireNamespace(h, cat, namespace)
            TableRepo.listLive(h, cat.catalogId, ns.namespaceId).map { t ->
                val agg = FileRepo.aggregateAt(h, cat.catalogId, t.tableId, cat.headSnapshotId)
                TableInfo(
                    tableId = t.tableId,
                    tableUuid = t.tableUuid,
                    namespace = ns.name,
                    name = t.name,
                    columns = emptyList(),
                    recordCount = agg.recordCount,
                    fileCount = agg.fileCount,
                    fileSizeBytes = agg.fileSizeBytes,
                )
            }
        }

    // ---- files + changefeed ----------------------------------------------

    fun listFiles(
        catalog: String,
        namespace: String,
        table: String,
        snapshot: Long? = null,
        atTimestamp: Instant? = null,
    ): List<DataFile> =
        jdbi.withHandleUnchecked { h ->
            val cat = requireCatalog(h, catalog)
            val at = resolveReadSnapshot(h, cat, snapshot, atTimestamp)
            val ns = requireNamespace(h, cat, namespace)
            val t =
                TableRepo.findAt(h, cat.catalogId, ns.namespaceId, table, at)
                    ?: throw HoglakeException.NotFound(
                        "table '$namespace.$table' in catalog '$catalog' at snapshot $at",
                    )
            FileRepo.listAt(h, cat.catalogId, t.tableId, at)
        }

    /**
     * Changefeed plan for (fromSnapshot, toSnapshot]: the table's
     * identity (uuid — incarnation changes are visible, not deduced),
     * the resolved range, the files whose begin_snapshot falls in the
     * half-open range (append order), and the deletion vectors
     * registered in the same range (begin_snapshot, delete_file_id
     * order) — the deletions feed.
     *
     * Expiry guard: fromSnapshot is EXCLUSIVE, so the lowest snapshot
     * the plan draws from is fromSnapshot + 1. That must be retained:
     * fromSnapshot < earliest_snapshot_id - 1 means part of the range
     * has been expired -> [HoglakeException.Expired] (HTTP 410); the
     * consumer must reconcile from a full scan, never silently skip.
     */
    fun changes(
        catalog: String,
        namespace: String,
        table: String,
        fromSnapshot: Long,
        toSnapshot: Long? = null,
    ): ChangesPlan =
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
            val floor = TimeTravelRepo.expiryFloor(h, cat.catalogId)
            if (fromSnapshot < floor.earliestSnapshotId - 1) {
                throw HoglakeException.Expired(
                    "from_snapshot $fromSnapshot reaches below the expiry floor " +
                        "(earliest retained snapshot is ${floor.earliestSnapshotId}" +
                        "${floor.reachedAtSuffix()}): part of the range is " +
                        "gone; reconcile by re-reading from a full scan at a retained " +
                        "snapshot instead of consuming this feed",
                )
            }
            val ns = requireNamespace(h, cat, namespace)
            val t =
                TableRepo.findAt(h, cat.catalogId, ns.namespaceId, table, to)
                    ?: throw HoglakeException.NotFound(
                        "table '$namespace.$table' in catalog '$catalog' at snapshot $to",
                    )
            ChangesPlan(
                tableUuid = t.tableUuid,
                fromSnapshot = fromSnapshot,
                toSnapshot = to,
                files = FileRepo.changedIn(h, cat.catalogId, t.tableId, fromSnapshot, to),
                deleteFiles =
                    DeleteFileReadRepo.changedIn(
                        h,
                        cat.catalogId,
                        t.tableId,
                        fromSnapshot,
                        to,
                    ),
            )
        }

    // ---- snapshots -------------------------------------------------------

    /**
     * One page of snapshots (changes populated), plus whether more pages
     * exist. Two mutually exclusive cursors:
     *
     *  - [after] (default): ids > after, ascending — the original feed.
     *  - [before] non-null: ids < before, DESCENDING — a UI walks
     *    newest-first starting at head + 1 and pages down with the last
     *    id of each page. Supplying [before] alongside a non-zero
     *    [after] is a Validation (422).
     */
    fun listSnapshots(
        catalog: String,
        after: Long,
        limit: Int,
        before: Long? = null,
    ): Pair<List<Snapshot>, Boolean> {
        if (limit < 1) throw HoglakeException.Validation("limit must be >= 1, got $limit")
        if (before != null && after != 0L) {
            throw HoglakeException.Validation(
                "'before' and a non-zero 'after' are mutually exclusive; supply at most one cursor",
            )
        }
        return jdbi.withHandleUnchecked { h ->
            val cat = requireCatalog(h, catalog)
            val raw =
                if (before != null) {
                    SnapshotRepo.pageBefore(h, cat.catalogId, before, limit + 1)
                } else {
                    SnapshotRepo.page(h, cat.catalogId, after, limit + 1)
                }
            val hasMore = raw.size > limit
            val page = raw.take(limit)
            if (page.isEmpty()) return@withHandleUnchecked Pair(emptyList(), false)
            // The page is contiguous in either direction; changesFor takes
            // the range low-to-high.
            val changes =
                SnapshotRepo.changesFor(
                    h,
                    cat.catalogId,
                    minOf(page.first().snapshotId, page.last().snapshotId),
                    maxOf(page.first().snapshotId, page.last().snapshotId),
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
        Audit.audited(
            "offset_commit",
            catalog,
            "$consumerId/$tableUuid",
            detail = { "snapshot=$snapshotId" },
        ) {
            jdbi.inTransactionUnchecked { h ->
                val cat = requireCatalog(h, catalog)
                if (snapshotId < 0 || snapshotId > cat.headSnapshotId) {
                    throw HoglakeException.Validation(
                        "snapshot $snapshotId out of range [0, ${cat.headSnapshotId}]",
                    )
                }
                // The floor guard (mirrors every read path's 410 contract):
                // a committed offset below earliest_snapshot_id is a
                // position in expired history the consumer can never read
                // from — and on consumer_floor catalogs it would pin every
                // future expiry sweep to a zero-work return FOREVER (the
                // sweep's min-offset bound could never rise above the
                // floor, and no offset-delete API exists to unwedge it).
                if (snapshotId < cat.earliestSnapshotId) {
                    val floor = TimeTravelRepo.expiryFloor(h, cat.catalogId)
                    throw HoglakeException.Expired(
                        "cannot commit offset at snapshot $snapshotId: it is below the " +
                            "expiry floor (earliest retained snapshot is " +
                            "${floor.earliestSnapshotId}${floor.reachedAtSuffix()}); " +
                            "reconcile from a full scan at a retained snapshot",
                    )
                }
                // The uuid must name a table this catalog has EVER had — any
                // incarnation, dropped included (offsets deliberately survive
                // drops so consumers SEE incarnation changes). A garbage uuid
                // would otherwise pin the consumer_floor retention floor
                // forever.
                val known =
                    h.createQuery(
                        "SELECT 1 FROM hog_table WHERE catalog_id = :catalogId AND table_uuid = :tableUuid",
                    )
                        .bind("catalogId", cat.catalogId)
                        .bind("tableUuid", tableUuid)
                        .mapTo(Int::class.javaObjectType)
                        .findOne()
                        .isPresent
                if (!known) {
                    throw HoglakeException.Validation(
                        "unknown table_uuid $tableUuid in catalog '$catalog'",
                    )
                }
                OffsetRepo.upsert(h, cat.catalogId, consumerId, tableUuid, snapshotId)
                    ?: throw HoglakeException.OffsetRegression(
                        "consumer '$consumerId' already committed past snapshot $snapshotId " +
                            "for table $tableUuid",
                    )
            }
        }

    /** Every consumer's offsets, enriched with table names for listing. */
    fun listConsumers(catalog: String): List<com.posthog.hoglake.model.ConsumerTableOffset> =
        jdbi.withHandleUnchecked { h ->
            val cat = requireCatalog(h, catalog)
            OffsetRepo.listAll(h, cat.catalogId)
        }

    fun listOffsets(
        catalog: String,
        consumerId: String,
    ): List<ConsumerOffset> =
        jdbi.withHandleUnchecked { h ->
            val cat = requireCatalog(h, catalog)
            OffsetRepo.list(h, cat.catalogId, consumerId)
        }

    /** One (consumer, table_uuid) offset; no stored row -> NotFound (404). */
    fun getOffset(
        catalog: String,
        consumerId: String,
        tableUuid: java.util.UUID,
    ): ConsumerOffset =
        jdbi.withHandleUnchecked { h ->
            val cat = requireCatalog(h, catalog)
            OffsetRepo.find(h, cat.catalogId, consumerId, tableUuid)
                ?: throw HoglakeException.NotFound(
                    "no offset for consumer '$consumerId' on table $tableUuid " +
                        "in catalog '$catalog'",
                )
        }

    // ---- time travel -----------------------------------------------------

    /**
     * Resolve an ISO timestamp to a snapshot id: the largest retained
     * snapshot with snapshot_time <= [atTimestamp] (between two
     * snapshots -> the lower; after head's time -> head). A timestamp
     * before the earliest RETAINED snapshot's time ->
     * [HoglakeException.Expired].
     */
    fun resolveTimestamp(
        catalog: String,
        atTimestamp: Instant,
    ): Long =
        jdbi.withHandleUnchecked { h ->
            val cat = requireCatalog(h, catalog)
            TimeTravelRepo.resolveTimestamp(
                h,
                cat.catalogId,
                TimeTravelRepo.earliestSnapshotId(h, cat.catalogId),
                atTimestamp,
            )
        }

    // ---- helpers ---------------------------------------------------------

    private fun requireCatalog(
        h: Handle,
        name: String,
    ): CatalogInfo =
        CatalogRepo.findByName(h, name)
            ?: throw HoglakeException.NotFound("catalog '$name'")

    private fun requireNamespace(
        h: Handle,
        cat: CatalogInfo,
        name: String,
    ): NamespaceInfo =
        NamespaceRepo.findLiveByName(h, cat.catalogId, name)
            ?: throw HoglakeException.NotFound("namespace '$name' in catalog '${cat.name}'")

    /** Time-travel target: default head; reject out-of-range ids. */
    private fun resolveSnapshot(
        cat: CatalogInfo,
        snapshot: Long?,
    ): Long {
        val s = snapshot ?: return cat.headSnapshotId
        if (s < 0 || s > cat.headSnapshotId) {
            throw HoglakeException.Validation(
                "snapshot $s out of range [0, ${cat.headSnapshotId}] for catalog '${cat.name}'",
            )
        }
        return s
    }

    /**
     * Point-in-time read target for getTable/listFiles: at most one of
     * [snapshot] / [atTimestamp] (both -> Validation); a timestamp
     * resolves via [TimeTravelRepo.resolveTimestamp]; an explicit
     * snapshot id below the expiry floor -> [HoglakeException.Expired].
     * Head reads (neither given) never hit the floor.
     */
    private fun resolveReadSnapshot(
        h: Handle,
        cat: CatalogInfo,
        snapshot: Long?,
        atTimestamp: Instant?,
    ): Long {
        if (snapshot != null && atTimestamp != null) {
            throw HoglakeException.Validation(
                "snapshot and at_timestamp are mutually exclusive; supply at most one",
            )
        }
        if (atTimestamp != null) {
            return TimeTravelRepo.resolveTimestamp(
                h,
                cat.catalogId,
                TimeTravelRepo.earliestSnapshotId(h, cat.catalogId),
                atTimestamp,
            )
        }
        val at = resolveSnapshot(cat, snapshot)
        if (snapshot != null) {
            val floor = TimeTravelRepo.expiryFloor(h, cat.catalogId)
            if (at < floor.earliestSnapshotId) {
                throw HoglakeException.Expired(
                    "snapshot $at is below the expiry floor (earliest retained snapshot " +
                        "is ${floor.earliestSnapshotId}${floor.reachedAtSuffix()}) " +
                        "for catalog '${cat.name}'",
                )
            }
        }
        return at
    }
}
