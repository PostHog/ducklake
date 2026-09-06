package com.posthog.hoglake.api

import com.posthog.hoglake.model.AlterOp
import com.posthog.hoglake.model.ColType
import com.posthog.hoglake.model.HoglakeException
import com.posthog.hoglake.model.NullOrder
import com.posthog.hoglake.model.PartitionFieldDef
import com.posthog.hoglake.model.PartitionSpec
import com.posthog.hoglake.model.SortDirection
import com.posthog.hoglake.model.SortFieldDef
import com.posthog.hoglake.model.SortSpec
import com.posthog.hoglake.model.TableInfo
import com.posthog.hoglake.model.Transform
import io.ktor.server.plugins.BadRequestException
import java.util.UUID

/*
 * Wire DTOs for the /alter endpoint (openapi/hoglake.yaml: AlterOp,
 * PartitionField, PartitionSpec, Table-with-partition_spec).
 *
 * Parse-time failures — unknown `op` discriminator, a missing field the
 * chosen op requires, empty ops — are [BadRequestException] (-> 400);
 * everything semantic (unknown column, illegal promotion, bad spec)
 * flows out of AlterService as HoglakeException (-> 404/409/422 via
 * ErrorMapping). Unknown enum LITERALS inside otherwise well-formed
 * fields (column type, transform) follow the ColumnDefDto precedent:
 * Validation -> 422.
 *
 * Types here are alter-prefixed to stay clash-free with Dto.kt, which
 * this file reuses by reference only (ColumnDefDto, ColumnDto).
 */

// ---- partition spec ------------------------------------------------------

data class AlterPartitionFieldDto(
    val sourceFieldId: Long,
    val transform: String,
    val transformParam: Int? = null,
) {
    fun toModel() =
        PartitionFieldDef(
            sourceFieldId = sourceFieldId,
            transform =
                try {
                    Transform.fromWire(transform)
                } catch (_: IllegalArgumentException) {
                    throw HoglakeException.Validation("unknown partition transform '$transform'")
                },
            transformParam = transformParam,
        )
}

fun PartitionFieldDef.toAlterDto() = AlterPartitionFieldDto(sourceFieldId, transform.wire, transformParam)

data class AlterPartitionSpecDto(
    val specId: Long,
    val fields: List<AlterPartitionFieldDto>,
)

fun PartitionSpec.toAlterDto() = AlterPartitionSpecDto(specId, fields.map { it.toAlterDto() })

// ---- sort order ----------------------------------------------------------

data class AlterSortFieldDto(
    val sourceFieldId: Long,
    val direction: String,
    val nullOrder: String,
) {
    fun toModel() =
        SortFieldDef(
            sourceFieldId = sourceFieldId,
            direction =
                try {
                    SortDirection.fromWire(direction)
                } catch (_: IllegalArgumentException) {
                    throw HoglakeException.Validation("unknown sort direction '$direction'")
                },
            nullOrder =
                try {
                    NullOrder.fromWire(nullOrder)
                } catch (_: IllegalArgumentException) {
                    throw HoglakeException.Validation("unknown null order '$nullOrder'")
                },
        )
}

fun SortFieldDef.toAlterDto() = AlterSortFieldDto(sourceFieldId, direction.wire, nullOrder.wire)

data class AlterSortSpecDto(
    val sortId: Long,
    val fields: List<AlterSortFieldDto>,
)

fun SortSpec.toAlterDto() = AlterSortSpecDto(sortId, fields.map { it.toAlterDto() })

// ---- request -------------------------------------------------------------

/** One wire AlterOp, discriminated by `op`; unused fields stay null. */
data class AlterOpDto(
    val op: String,
    val column: ColumnDefDto? = null,
    val name: String? = null,
    val from: String? = null,
    val to: String? = null,
    val newName: String? = null,
    val fields: List<AlterPartitionFieldDto>? = null,
    val sortFields: List<AlterSortFieldDto>? = null,
) {
    fun toModel(): AlterOp =
        when (op) {
            "add_column" -> AlterOp.AddColumn(required(column, "column").toModel())
            "drop_column" -> AlterOp.DropColumn(required(name, "name"))
            "rename_column" -> AlterOp.RenameColumn(required(from, "from"), required(to, "to"))
            "promote_column" ->
                AlterOp.PromoteColumn(required(name, "name"), parseColType(required(to, "to")))
            "rename_table" -> AlterOp.RenameTable(required(newName, "new_name"))
            "set_partition_spec" ->
                AlterOp.SetPartitionSpec(required(fields, "fields").map { it.toModel() })
            "set_sort_order" ->
                AlterOp.SetSortOrder(required(sortFields, "sort_fields").map { it.toModel() })
            else -> throw BadRequestException("unknown alter op '$op'")
        }

    private fun <T : Any> required(
        value: T?,
        field: String,
    ): T = value ?: throw BadRequestException("op '$op' requires field '$field'")

    private fun parseColType(s: String): ColType =
        try {
            ColType.fromWire(s)
        } catch (_: IllegalArgumentException) {
            throw HoglakeException.Validation("unknown column type '$s'")
        }
}

data class AlterTableRequestDto(val ops: List<AlterOpDto> = emptyList())

// ---- response ------------------------------------------------------------

/** The spec's Table schema including partition_spec / sort_spec (null = unpartitioned/unsorted). */
data class AlteredTableDto(
    val name: String,
    val namespace: String,
    val tableUuid: UUID,
    val columns: List<ColumnDto>,
    val recordCount: Long,
    val fileCount: Long,
    val fileSizeBytes: Long,
    val partitionSpec: AlterPartitionSpecDto? = null,
    val sortSpec: AlterSortSpecDto? = null,
)

fun TableInfo.toAlteredDto() =
    AlteredTableDto(
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
