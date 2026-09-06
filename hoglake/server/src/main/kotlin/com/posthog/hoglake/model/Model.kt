package com.posthog.hoglake.model

import java.time.Instant
import java.util.UUID

/**
 * Domain model. These are the shapes the persistence layer returns and
 * the API layer serializes; they mirror the OpenAPI schemas
 * (src/main/resources/openapi/hoglake.yaml) and the tables in
 * db/migration/V1__init.sql.
 */

enum class ColType {
    BOOLEAN,
    INT,
    LONG,
    FLOAT,
    DOUBLE,
    DECIMAL,
    DATE,
    TIME,
    TIMESTAMP,
    TIMESTAMPTZ,
    STRING,
    UUID_T,
    BINARY,
    ;

    /** Wire/DB name (lowercase; UUID_T stored as "uuid"). */
    val wire: String get() = if (this == UUID_T) "uuid" else name.lowercase()

    companion object {
        fun fromWire(s: String): ColType = if (s == "uuid") UUID_T else valueOf(s.uppercase())
    }
}

enum class StatsState {
    PROVIDED,
    PENDING,
    FAILED,
    ;

    val wire: String get() = name.lowercase()

    companion object {
        fun fromWire(s: String) = valueOf(s.uppercase())
    }
}

/** Typed conflict vocabulary — mirrors the CHECK constraint on hog_snapshot_change. */
enum class ChangeKind {
    NAMESPACE_CREATED,
    NAMESPACE_DROPPED,
    TABLE_CREATED,
    TABLE_DROPPED,
    TABLE_ALTERED,
    TABLE_INSERTED_INTO,
    TABLE_DELETED_FROM,
    TABLE_COMPACTED,
    VIEW_CREATED,
    VIEW_DROPPED,
    ;

    val wire: String get() = name.lowercase()

    companion object {
        fun fromWire(s: String) = valueOf(s.uppercase())
    }
}

/** Iceberg-semantics partition transforms (iceberg-federation.md §3). */
enum class Transform {
    IDENTITY,
    BUCKET,
    YEAR,
    MONTH,
    DAY,
    HOUR,
    ;

    val wire: String get() = name.lowercase()

    companion object {
        fun fromWire(s: String) = valueOf(s.uppercase())
    }
}

data class PartitionFieldDef(
    val sourceFieldId: Long,
    val transform: Transform,
    /** bucket(n): required for BUCKET, forbidden otherwise. */
    val transformParam: Int? = null,
)

data class PartitionSpec(
    val specId: Long,
    val fields: List<PartitionFieldDef>,
)

/** Sort direction for one sort-spec field (hog_sort_field.direction). */
enum class SortDirection {
    ASC,
    DESC,
    ;

    val wire: String get() = name.lowercase()

    companion object {
        fun fromWire(s: String) = valueOf(s.uppercase())
    }
}

/** Null placement for one sort-spec field (hog_sort_field.null_order). */
enum class NullOrder {
    NULLS_FIRST,
    NULLS_LAST,
    ;

    val wire: String get() = name.lowercase()

    companion object {
        fun fromWire(s: String) = valueOf(s.uppercase())
    }
}

data class SortFieldDef(
    val sourceFieldId: Long,
    val direction: SortDirection,
    val nullOrder: NullOrder,
)

/**
 * A table's versioned sort order. ADVISORY for writers (the server
 * never verifies file sortedness); BINDING for compaction rewrites.
 */
data class SortSpec(
    val sortId: Long,
    val fields: List<SortFieldDef>,
)

/** Widening promotions the ALTER path permits (Iceberg-compatible set). */
fun ColType.canPromoteTo(target: ColType): Boolean =
    when (this) {
        ColType.INT -> target == ColType.LONG
        ColType.FLOAT -> target == ColType.DOUBLE
        else -> false
    }

/** One typed ALTER TABLE operation. */
sealed class AlterOp {
    data class AddColumn(val def: ColumnDef) : AlterOp()

    data class DropColumn(val name: String) : AlterOp()

    data class RenameColumn(val from: String, val to: String) : AlterOp()

    data class PromoteColumn(val name: String, val to: ColType) : AlterOp()

    data class RenameTable(val newName: String) : AlterOp()

    /** Replace the partition spec (empty list = unpartitioned). */
    data class SetPartitionSpec(val fields: List<PartitionFieldDef>) : AlterOp()

    /** Replace the sort order (empty list = unsorted). */
    data class SetSortOrder(val fields: List<SortFieldDef>) : AlterOp()
}

data class CatalogInfo(
    val catalogId: Long,
    val name: String,
    val dataPath: String,
    val headSnapshotId: Long,
    val schemaVersion: Long,
    /**
     * snapshot_time of the expiry floor snapshot, captured when the
     * sweep advanced the floor; null until expiry first advances it.
     * The reconciliation anchor: 410s below the floor cite this.
     */
    val earliestSnapshotTime: Instant? = null,
    /**
     * The lifecycle slice starts here (options/expiry/cleanup read it):
     * one hog_catalog row mapping serves every reader — the
     * OptionsService.LifecycleCatalog duplicate that drifted behind this
     * mapper is gone. null = snapshot expiry disabled.
     */
    val snapshotRetentionSeconds: Long? = null,
    /** Expiry never passes the min consumer offset when true. */
    val consumerFloor: Boolean = true,
    val earliestSnapshotId: Long = 0,
)

data class NamespaceInfo(
    val namespaceId: Long,
    val name: String,
)

data class ColumnDef(
    val name: String,
    val type: ColType,
    val typeParams: Map<String, Any?>? = null,
    val nullable: Boolean = true,
)

data class Column(
    val fieldId: Long,
    val ordinal: Int,
    val def: ColumnDef,
)

data class TableInfo(
    val tableId: Long,
    val tableUuid: UUID,
    val namespace: String,
    val name: String,
    val columns: List<Column>,
    val recordCount: Long,
    val fileCount: Long,
    val fileSizeBytes: Long,
    /** Live partition spec; null = unpartitioned. */
    val partitionSpec: PartitionSpec? = null,
    /** Live sort order; null = unsorted. */
    val sortSpec: SortSpec? = null,
)

data class Snapshot(
    val snapshotId: Long,
    val snapshotTime: Instant,
    val schemaVersion: Long,
    val author: String?,
    val message: String?,
    val changes: List<SnapshotChange> = emptyList(),
)

data class SnapshotChange(
    val kind: ChangeKind,
    /** table_id, namespace_id, or view_id — every change kind names an object. */
    val objectId: Long,
)

data class DataFile(
    val dataFileId: Long,
    val tableId: Long,
    val path: String,
    val fileFormat: String,
    val recordCount: Long,
    val fileSizeBytes: Long,
    val footerSize: Long?,
    val rowIdStart: Long,
    val statsState: StatsState,
    val beginSnapshot: Long,
    val specId: Long? = null,
    /** Transformed partition values by key_index; null when unpartitioned. */
    val partitionValues: List<String?>? = null,
    /**
     * True for compaction outputs: row ids ride an explicit physical
     * `_hog_row_id` column (reserved parquet field id 2147483646) because
     * merged inputs need not be row-id-contiguous. When true,
     * row_id_start is min(input row ids) and has no positional meaning.
     */
    val explicitRowIds: Boolean = false,
)

/** A registered deletion-vector file (one live DV per data file). */
data class DeleteFile(
    val deleteFileId: Long,
    val dataFileId: Long,
    val path: String,
    val fileFormat: String,
    val deleteCount: Long,
    val fileSizeBytes: Long,
    val beginSnapshot: Long,
)

/** A data file paired with its live deletion vector, for read planning. */
data class ScanFile(
    val dataFile: DataFile,
    val deleteFile: DeleteFile?,
)

data class ColumnStats(
    val fieldId: Long,
    val valueCount: Long,
    val nullCount: Long,
    val nanCount: Long?,
    val sizeBytes: Long?,
    val lowerBound: ByteArray?,
    val upperBound: ByteArray?,
) {
    override fun equals(other: Any?): Boolean =
        other is ColumnStats &&
            fieldId == other.fieldId && valueCount == other.valueCount &&
            nullCount == other.nullCount && nanCount == other.nanCount &&
            sizeBytes == other.sizeBytes &&
            lowerBound.contentEquals(other.lowerBound) &&
            upperBound.contentEquals(other.upperBound)

    override fun hashCode(): Int = fieldId.hashCode()
}

/** One file offered to the commit endpoint. */
data class FileRegistration(
    val path: String,
    val recordCount: Long,
    val fileSizeBytes: Long,
    val footerSize: Long? = null,
    /** null = deferred stats: the file registers as PENDING for the hydrator. */
    val columnStats: List<ColumnStats>? = null,
    /**
     * Transformed partition values by key_index of the table's live spec.
     * Required (with matching arity) when the table is partitioned;
     * forbidden when it is not.
     */
    val partitionValues: List<String?>? = null,
)

data class TableAppend(
    val namespace: String,
    val table: String,
    val files: List<FileRegistration>,
    /**
     * Optional incarnation guard: when present, the commit fails with
     * CommitConflict if the live table resolved by name does not carry
     * this table_uuid — the atomic answer to the name-rebind race where
     * a table is dropped and recreated between a replicator's read and
     * its commit.
     */
    val expectedTableUuid: UUID? = null,
)

/** One deletion-vector registration: supersedes the file's live DV. */
data class DeleteFileRegistration(
    val dataFileId: Long,
    val path: String,
    val deleteCount: Long,
    val fileSizeBytes: Long,
)

data class TableDeletes(
    val namespace: String,
    val table: String,
    val files: List<DeleteFileRegistration>,
    /** Optional incarnation guard; see [TableAppend.expectedTableUuid]. */
    val expectedTableUuid: UUID? = null,
)

data class CommitRequest(
    val readSnapshot: Long? = null,
    val appends: List<TableAppend> = emptyList(),
    val deletes: List<TableDeletes> = emptyList(),
    val author: String? = null,
    val message: String? = null,
)

data class CommitResult(
    val snapshotId: Long,
    val schemaVersion: Long,
)

data class ConsumerOffset(
    val consumerId: String,
    val tableUuid: UUID,
    val committedSnapshot: Long,
    val updatedAt: Instant,
)

data class ViewInfo(
    val viewId: Long,
    val viewUuid: UUID,
    val namespace: String,
    val name: String,
    val dialect: String,
    val sql: String,
)

/** Catalog retention/behavior options (the options API surface). */
data class CatalogOptions(
    /** null = snapshot expiry disabled. */
    val snapshotRetentionSeconds: Long?,
    /** Expiry never passes the min consumer offset when true. */
    val consumerFloor: Boolean,
    val earliestSnapshotId: Long,
)

/** One expiry sweep's outcome. */
data class ExpiryResult(
    val snapshotsExpired: Long,
    val dataFilesQueued: Long,
    val deleteFilesQueued: Long,
    val newEarliestSnapshotId: Long,
    /** Non-null when the consumer floor capped the sweep (page-worthy). */
    val flooredByConsumer: String?,
)

/** One compaction run's outcome (POST /maintenance/compact + the loop). */
data class CompactionResult(
    val groupsCompacted: Long,
    val filesIn: Long,
    val filesOut: Long,
    val bytesIn: Long,
    val bytesOut: Long,
    /**
     * Groups planned but aborted at commit time because an input file
     * was no longer live (or the compactor's staged output claim was
     * reclaimed by a concurrent cleanup drain) — resolved by re-planning
     * on the next run, never by blocking foreground.
     */
    val skippedConflicts: Long,
    /**
     * Groups aborted at commit time because an input's deletion-vector
     * state changed since planning (a DV appeared, or the planned DV was
     * superseded by a grown one). The rewrite applied the PLANNED
     * vectors, so committing would resurrect rows deleted after the
     * plan's read — the group skips and re-plans instead. Never a lost
     * delete.
     */
    val dvSuperseded: Long = 0,
    /**
     * Groups skipped because some live column's type cannot be produced
     * from an input file's parquet type (anything outside identity or
     * the int->long / float->double promotions). Deterministic until the
     * schema or the file set changes; skip-with-reason, not a failure.
     */
    val unconvertibleSchema: Long = 0,
)

/**
 * One invariant check inside a verify run (POST /maintenance/verify).
 * [violations] is the TRUE count; [samples] is capped detail
 * (VerifyService.MAX_SAMPLES) so a badly broken catalog cannot produce
 * an unbounded response.
 */
data class VerifyCheck(
    val check: String,
    val status: String,
    val violations: Long,
    val samples: List<String>,
)

/** One verify run's report: per-check status + overall rollup. */
data class VerifyReport(
    val catalog: String,
    /** "pass" iff every check passed. */
    val status: String,
    val checks: List<VerifyCheck>,
)

/**
 * One rehydrate request's outcome (POST /maintenance/rehydrate):
 * how many 'failed' files were flipped back to 'pending' for the
 * hydrator to retry.
 */
data class RehydrateResult(
    val requeued: Long,
)

/** One cleanup drain's outcome. */
data class CleanupResult(
    val removed: Long,
    val missing: Long,
    /** Entries skipped because the path is still referenced — an
     *  invariant violation worth alerting on, never a deletion. */
    val stillReferenced: Long,
)

/** Changefeed plan for (from, to]: appended files + DVs registered in range. */
data class ChangesPlan(
    val tableUuid: UUID,
    val fromSnapshot: Long,
    val toSnapshot: Long,
    val files: List<DataFile>,
    val deleteFiles: List<DeleteFile>,
)

/** Service-level failures the API layer maps to status codes. */
sealed class HoglakeException(message: String) : RuntimeException(message) {
    class NotFound(what: String) : HoglakeException(what)

    class AlreadyExists(what: String) : HoglakeException(what)

    class CommitConflict(detail: String) : HoglakeException(detail)

    class Validation(detail: String) : HoglakeException(detail)

    class OffsetRegression(detail: String) : HoglakeException(detail)

    /** Requested range fell below the catalog's expiry floor -> HTTP 410. */
    class Expired(detail: String) : HoglakeException(detail)

    /**
     * The commit transaction's lock_timeout expired while queuing on the
     * per-catalog advisory commit lock (B2 admission control) -> HTTP
     * 503 `commit_queue_timeout` with Retry-After. Retryable
     * backpressure — the catalog is convoyed, not broken.
     */
    class CommitQueueTimeout(detail: String) : HoglakeException(detail)

    /**
     * A column rename was refused because live data files without
     * parquet field ids exist (`hog_data_file.missing_field_ids`):
     * id-less files bind columns by name, so the rename would silently
     * NULL their history in readers -> HTTP 409.
     */
    class IdlessFilesPresent(detail: String) : HoglakeException(detail)
}
