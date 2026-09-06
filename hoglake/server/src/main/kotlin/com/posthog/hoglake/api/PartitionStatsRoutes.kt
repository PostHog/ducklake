package com.posthog.hoglake.api

import com.fasterxml.jackson.annotation.JsonInclude
import com.posthog.hoglake.model.PartitionDebt
import com.posthog.hoglake.model.PartitionStatsReport
import com.posthog.hoglake.model.PartitionValue
import com.posthog.hoglake.service.PartitionStatsService
import io.ktor.server.application.Application
import io.ktor.server.application.ApplicationCall
import io.ktor.server.plugins.BadRequestException
import io.ktor.server.response.respond
import io.ktor.server.routing.get
import io.ktor.server.routing.routing
import java.util.UUID

/**
 * GET /v1/catalogs/{catalog}/stats/partitions (openapi getPartitionStats):
 * leaf partitions ranked by compaction debt, for the webui's partition
 * health page. Handler translates wire <-> model only (Routes.kt
 * division of labor); semantics live in [PartitionStatsService], whose
 * HoglakeExceptions are mapped by ErrorMapping (404 unknown names, 422
 * for table-without-namespace / non-positive limit).
 */
fun Application.installPartitionStatsRoutes(stats: PartitionStatsService) {
    routing {
        get("/v1/catalogs/{catalog}/stats/partitions") {
            call.respond(
                stats.partitionStats(
                    call.catalog(),
                    call.request.queryParameters["namespace"],
                    call.request.queryParameters["table"],
                    call.statsLimit(),
                ).toDto(),
            )
        }
    }
}

/** `limit` query parameter: default 50, cap 500 (service-side), non-integer -> 400. */
private fun ApplicationCall.statsLimit(): Int =
    request.queryParameters["limit"]?.let {
        it.toIntOrNull()
            ?: throw BadRequestException("query parameter 'limit' must be an integer, got '$it'")
    } ?: PartitionStatsService.DEFAULT_LIMIT

// ---- wire DTOs (openapi/hoglake.yaml: PartitionStatsReport) ---------------

data class PartitionValueDto(
    val field: String,
    /**
     * ALWAYS included: a null partition value must serialize as
     * `"value": null`, not vanish under the app-wide NON_NULL
     * inclusion — the pair shape is the contract.
     */
    @get:JsonInclude(JsonInclude.Include.ALWAYS)
    val value: String?,
)

data class PartitionDebtDto(
    val namespace: String,
    val table: String,
    val tableUuid: UUID,
    val partitionValues: List<PartitionValueDto>,
    /** Omitted (NON_NULL) for an unpartitioned vintage. */
    val specId: Long?,
    val fileCount: Long,
    val smallFileCount: Long,
    val totalBytes: Long,
    val smallFileBytes: Long,
    val avgFileBytes: Long,
    val dvCount: Long,
    val debtScore: Long,
)

data class PartitionStatsReportDto(
    val partitions: List<PartitionDebtDto>,
    val truncated: Boolean,
    val staleSpecGroups: Long,
)

fun PartitionValue.toDto() = PartitionValueDto(field = field, value = value)

fun PartitionDebt.toDto() =
    PartitionDebtDto(
        namespace = namespace,
        table = table,
        tableUuid = tableUuid,
        partitionValues = partitionValues.map { it.toDto() },
        specId = specId,
        fileCount = fileCount,
        smallFileCount = smallFileCount,
        totalBytes = totalBytes,
        smallFileBytes = smallFileBytes,
        avgFileBytes = avgFileBytes,
        dvCount = dvCount,
        debtScore = debtScore,
    )

fun PartitionStatsReport.toDto() =
    PartitionStatsReportDto(
        partitions = partitions.map { it.toDto() },
        truncated = truncated,
        staleSpecGroups = staleSpecGroups,
    )
