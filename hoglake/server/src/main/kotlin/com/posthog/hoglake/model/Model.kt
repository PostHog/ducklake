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
    BOOLEAN, INT, LONG, FLOAT, DOUBLE, DECIMAL, DATE, TIME,
    TIMESTAMP, TIMESTAMPTZ, STRING, UUID_T, BINARY;

    /** Wire/DB name (lowercase; UUID_T stored as "uuid"). */
    val wire: String get() = if (this == UUID_T) "uuid" else name.lowercase()

    companion object {
        fun fromWire(s: String): ColType =
            if (s == "uuid") UUID_T else valueOf(s.uppercase())
    }
}

enum class StatsState { PROVIDED, PENDING, FAILED;
    val wire: String get() = name.lowercase()
    companion object { fun fromWire(s: String) = valueOf(s.uppercase()) }
}

/** Typed conflict vocabulary — mirrors the CHECK constraint on hog_snapshot_change. */
enum class ChangeKind {
    NAMESPACE_CREATED, NAMESPACE_DROPPED,
    TABLE_CREATED, TABLE_DROPPED, TABLE_ALTERED, TABLE_INSERTED_INTO;

    val wire: String get() = name.lowercase()
    companion object { fun fromWire(s: String) = valueOf(s.uppercase()) }
}

data class CatalogInfo(
    val catalogId: Long,
    val name: String,
    val dataPath: String,
    val headSnapshotId: Long,
    val schemaVersion: Long,
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
    val objectId: Long?,
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
    override fun equals(other: Any?): Boolean = other is ColumnStats &&
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
)

data class TableAppend(
    val namespace: String,
    val table: String,
    val files: List<FileRegistration>,
)

data class CommitRequest(
    val readSnapshot: Long? = null,
    val appends: List<TableAppend>,
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

/** Service-level failures the API layer maps to status codes. */
sealed class HoglakeException(message: String) : RuntimeException(message) {
    class NotFound(what: String) : HoglakeException(what)
    class AlreadyExists(what: String) : HoglakeException(what)
    class CommitConflict(detail: String) : HoglakeException(detail)
    class Validation(detail: String) : HoglakeException(detail)
    class OffsetRegression(detail: String) : HoglakeException(detail)
}
