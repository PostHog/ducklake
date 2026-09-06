package com.posthog.hoglake.api

import com.posthog.hoglake.model.CatalogInfo
import com.posthog.hoglake.model.ChangesPlan
import com.posthog.hoglake.model.ColType
import com.posthog.hoglake.model.Column
import com.posthog.hoglake.model.ColumnDef
import com.posthog.hoglake.model.ColumnStats
import com.posthog.hoglake.model.CommitRequest
import com.posthog.hoglake.model.CommitResult
import com.posthog.hoglake.model.ConsumerOffset
import com.posthog.hoglake.model.DataFile
import com.posthog.hoglake.model.DeleteFile
import com.posthog.hoglake.model.DeleteFileRegistration
import com.posthog.hoglake.model.FileRegistration
import com.posthog.hoglake.model.HoglakeException
import com.posthog.hoglake.model.NamespaceInfo
import com.posthog.hoglake.model.ScanFile
import com.posthog.hoglake.model.Snapshot
import com.posthog.hoglake.model.TableAppend
import com.posthog.hoglake.model.TableDeletes
import com.posthog.hoglake.model.TableInfo
import com.posthog.hoglake.model.ViewInfo
import java.time.Instant
import java.util.UUID

/*
 * Wire DTOs mirroring the OpenAPI schemas (openapi/hoglake.yaml).
 *
 * Naming: Kotlin camelCase properties; the ObjectMapper installed in
 * App.module uses PropertyNamingStrategies.SNAKE_CASE, so the wire is
 * snake_case exactly as the spec spells it (head_snapshot_id, ...).
 *
 * Enums travel as their wire strings (ColType.wire / StatsState.wire /
 * ChangeKind.wire), never as Kotlin enum names. lower_bound/upper_bound
 * are `format: byte` — Jackson's native ByteArray <-> base64 mapping.
 */

// ---- errors --------------------------------------------------------------

data class ApiErrorDto(val error: String, val detail: String? = null)

// ---- catalogs ------------------------------------------------------------

data class CatalogDto(
    val name: String,
    val dataPath: String,
    val headSnapshotId: Long,
    val schemaVersion: Long,
    /** Expiry-floor snapshot's time; NON_NULL omits it until expiry first advances the floor. */
    val earliestSnapshotTime: Instant? = null,
)

fun CatalogInfo.toDto() = CatalogDto(name, dataPath, headSnapshotId, schemaVersion, earliestSnapshotTime)

data class CreateCatalogRequestDto(val name: String, val dataPath: String)

// ---- namespaces ----------------------------------------------------------

data class NamespaceDto(val name: String)

fun NamespaceInfo.toDto() = NamespaceDto(name)

data class CreateNamespaceRequestDto(val name: String)

// ---- tables --------------------------------------------------------------

data class ColumnDefDto(
    val name: String,
    val type: String,
    val typeParams: Map<String, Any?>? = null,
    val nullable: Boolean = true,
) {
    fun toModel(): ColumnDef =
        ColumnDef(
            name = name,
            type =
                try {
                    ColType.fromWire(type)
                } catch (_: IllegalArgumentException) {
                    throw HoglakeException.Validation("unknown column type '$type' for column '$name'")
                },
            typeParams = typeParams,
            nullable = nullable,
        )
}

data class CreateTableRequestDto(val name: String, val columns: List<ColumnDefDto>)

data class ColumnDto(
    val name: String,
    val type: String,
    val typeParams: Map<String, Any?>?,
    val nullable: Boolean,
    val fieldId: Long,
    val ordinal: Int,
)

fun Column.toDto() =
    ColumnDto(
        name = def.name,
        type = def.type.wire,
        typeParams = def.typeParams,
        nullable = def.nullable,
        fieldId = fieldId,
        ordinal = ordinal,
    )

data class TableSummaryDto(val name: String, val tableUuid: UUID)

fun TableInfo.toSummaryDto() = TableSummaryDto(name, tableUuid)

data class TableDto(
    val name: String,
    val namespace: String,
    val tableUuid: UUID,
    val columns: List<ColumnDto>,
    val recordCount: Long,
    val fileCount: Long,
    val fileSizeBytes: Long,
    /**
     * The spec visible at the requested snapshot (AlterDto's shape);
     * NON_NULL omits it for an unpartitioned table.
     */
    val partitionSpec: AlterPartitionSpecDto? = null,
    /** Sort order at the requested snapshot; NON_NULL omits when unsorted. */
    val sortSpec: AlterSortSpecDto? = null,
)

fun TableInfo.toDto() =
    TableDto(
        name = name,
        namespace = namespace,
        tableUuid = tableUuid,
        columns = columns.map { it.toDto() },
        recordCount = recordCount,
        fileCount = fileCount,
        fileSizeBytes = fileSizeBytes,
        partitionSpec = partitionSpec?.toAlterDto(),
        sortSpec = sortSpec?.toAlterDto(),
    )

// ---- commits -------------------------------------------------------------

data class ColumnStatsDto(
    val fieldId: Long,
    val valueCount: Long,
    val nullCount: Long,
    val nanCount: Long? = null,
    val sizeBytes: Long? = null,
    val lowerBound: ByteArray? = null,
    val upperBound: ByteArray? = null,
) {
    fun toModel() =
        ColumnStats(
            fieldId = fieldId,
            valueCount = valueCount,
            nullCount = nullCount,
            nanCount = nanCount,
            sizeBytes = sizeBytes,
            lowerBound = lowerBound,
            upperBound = upperBound,
        )

    // ByteArray members: identity equals/hashCode are fine — DTOs are
    // one-way carriers, never compared.
}

data class FileRegistrationDto(
    val path: String,
    val recordCount: Long,
    val fileSizeBytes: Long,
    val footerSize: Long? = null,
    val columnStats: List<ColumnStatsDto>? = null,
    val partitionValues: List<String?>? = null,
) {
    fun toModel() =
        FileRegistration(
            path = path,
            recordCount = recordCount,
            fileSizeBytes = fileSizeBytes,
            footerSize = footerSize,
            columnStats = columnStats?.map { it.toModel() },
            partitionValues = partitionValues,
        )
}

data class TableAppendDto(
    val namespace: String,
    val table: String,
    val files: List<FileRegistrationDto>,
    val expectedTableUuid: UUID? = null,
) {
    fun toModel(): TableAppend {
        if (files.isEmpty()) {
            throw HoglakeException.Validation("append to $namespace.$table has no files")
        }
        return TableAppend(namespace, table, files.map { it.toModel() }, expectedTableUuid)
    }
}

data class DeleteFileRegistrationDto(
    val dataFileId: Long,
    val path: String,
    val deleteCount: Long,
    val fileSizeBytes: Long,
) {
    fun toModel() =
        DeleteFileRegistration(
            dataFileId = dataFileId,
            path = path,
            deleteCount = deleteCount,
            fileSizeBytes = fileSizeBytes,
        )
}

data class TableDeletesDto(
    val namespace: String,
    val table: String,
    val files: List<DeleteFileRegistrationDto>,
    val expectedTableUuid: UUID? = null,
) {
    fun toModel(): TableDeletes {
        if (files.isEmpty()) {
            throw HoglakeException.Validation("deletes for $namespace.$table have no files")
        }
        return TableDeletes(namespace, table, files.map { it.toModel() }, expectedTableUuid)
    }
}

data class CommitRequestDto(
    val readSnapshot: Long? = null,
    // Both default empty per the spec: at least one must be non-empty,
    // which CommitService enforces (422 validation, not a 400).
    val appends: List<TableAppendDto> = emptyList(),
    val deletes: List<TableDeletesDto> = emptyList(),
    val author: String? = null,
    val message: String? = null,
) {
    fun toModel() =
        CommitRequest(
            readSnapshot = readSnapshot,
            appends = appends.map { it.toModel() },
            deletes = deletes.map { it.toModel() },
            author = author,
            message = message,
        )
}

data class CommitResultDto(val snapshotId: Long, val schemaVersion: Long)

fun CommitResult.toDto() = CommitResultDto(snapshotId, schemaVersion)

// ---- files + changefeed --------------------------------------------------

data class DataFileDto(
    val dataFileId: Long,
    val path: String,
    val fileFormat: String,
    val recordCount: Long,
    val fileSizeBytes: Long,
    val footerSize: Long?,
    val rowIdStart: Long,
    val statsState: String,
    val beginSnapshot: Long,
    // Partitioning binding; NON_NULL inclusion drops both when absent
    // (unpartitioned file, or a read path that does not load them).
    val specId: Long? = null,
    val partitionValues: List<String?>? = null,
    /** True for compaction outputs: row ids ride the physical _hog_row_id column. */
    val explicitRowIds: Boolean = false,
)

fun DataFile.toDto() =
    DataFileDto(
        dataFileId = dataFileId,
        path = path,
        fileFormat = fileFormat,
        recordCount = recordCount,
        fileSizeBytes = fileSizeBytes,
        footerSize = footerSize,
        rowIdStart = rowIdStart,
        statsState = statsState.wire,
        beginSnapshot = beginSnapshot,
        specId = specId,
        partitionValues = partitionValues,
        explicitRowIds = explicitRowIds,
    )

// ---- scan planning -------------------------------------------------------

data class DeleteFileDto(
    val deleteFileId: Long,
    val dataFileId: Long,
    val path: String,
    val fileFormat: String,
    val deleteCount: Long,
    val fileSizeBytes: Long,
    val beginSnapshot: Long,
)

fun DeleteFile.toDto() =
    DeleteFileDto(
        deleteFileId = deleteFileId,
        dataFileId = dataFileId,
        path = path,
        fileFormat = fileFormat,
        deleteCount = deleteCount,
        fileSizeBytes = fileSizeBytes,
        beginSnapshot = beginSnapshot,
    )

data class ScanFileDto(
    val dataFile: DataFileDto,
    val deleteFile: DeleteFileDto? = null,
)

fun ScanFile.toDto() =
    ScanFileDto(
        dataFile = dataFile.toDto(),
        deleteFile = deleteFile?.toDto(),
    )

data class ChangePlanDto(
    val tableUuid: UUID,
    val fromSnapshot: Long,
    val toSnapshot: Long,
    val files: List<DataFileDto>,
    /** DVs registered in (from, to] — the deletions feed. */
    val deleteFiles: List<DeleteFileDto>,
)

fun ChangesPlan.toDto() =
    ChangePlanDto(
        tableUuid = tableUuid,
        fromSnapshot = fromSnapshot,
        toSnapshot = toSnapshot,
        files = files.map { it.toDto() },
        deleteFiles = deleteFiles.map { it.toDto() },
    )

// ---- views ---------------------------------------------------------------

data class ViewDto(
    val name: String,
    val namespace: String,
    val viewUuid: UUID,
    val dialect: String,
    val sql: String,
)

fun ViewInfo.toDto() =
    ViewDto(
        name = name,
        namespace = namespace,
        viewUuid = viewUuid,
        dialect = dialect,
        sql = sql,
    )

data class CreateViewRequestDto(
    val name: String,
    /** Stored verbatim — the server never parses view SQL. */
    val sql: String,
    val dialect: String = "trino",
)

// ---- snapshots -----------------------------------------------------------

data class SnapshotChangeDto(val kind: String, val objectId: Long)

data class SnapshotDto(
    val snapshotId: Long,
    val snapshotTime: Instant,
    val schemaVersion: Long,
    val author: String?,
    val message: String?,
    val changes: List<SnapshotChangeDto>,
)

fun Snapshot.toDto() =
    SnapshotDto(
        snapshotId = snapshotId,
        snapshotTime = snapshotTime,
        schemaVersion = schemaVersion,
        author = author,
        message = message,
        changes = changes.map { SnapshotChangeDto(it.kind.wire, it.objectId) },
    )

data class SnapshotPageDto(val snapshots: List<SnapshotDto>, val hasMore: Boolean)

// ---- consumer offsets ----------------------------------------------------

data class ConsumerOffsetDto(
    val consumerId: String,
    val tableUuid: UUID,
    val committedSnapshot: Long,
    val updatedAt: Instant,
)

fun ConsumerOffset.toDto() = ConsumerOffsetDto(consumerId, tableUuid, committedSnapshot, updatedAt)

data class CommitOffsetRequestDto(val snapshotId: Long)

/** GET /v1/info — instance identity for the webui. Name omitted when unset. */
data class InstanceInfoDto(val name: String?)

/** One row of the catalog-wide consumer listing (GET /consumers). */
data class ConsumerTableOffsetDto(
    val tableUuid: UUID,
    val committedSnapshot: Long,
    val updatedAt: Instant,
    val namespace: String?,
    val tableName: String?,
    val tableDropped: Boolean,
)

data class ConsumerSummaryDto(
    val consumerId: String,
    val offsets: List<ConsumerTableOffsetDto>,
)

data class ConsumerListDto(val consumers: List<ConsumerSummaryDto>)

/** Group flat repo rows by consumer, preserving the repo's ordering. */
fun List<com.posthog.hoglake.model.ConsumerTableOffset>.toConsumerListDto() =
    ConsumerListDto(
        groupBy { it.consumerId }
            .map { (consumer, rows) ->
                ConsumerSummaryDto(
                    consumerId = consumer,
                    offsets =
                        rows.map {
                            ConsumerTableOffsetDto(
                                tableUuid = it.tableUuid,
                                committedSnapshot = it.committedSnapshot,
                                updatedAt = it.updatedAt,
                                namespace = it.namespace,
                                tableName = it.tableName,
                                tableDropped = it.tableDropped,
                            )
                        },
                )
            },
    )
