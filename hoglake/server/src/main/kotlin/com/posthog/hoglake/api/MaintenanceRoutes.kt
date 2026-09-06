package com.posthog.hoglake.api

import com.fasterxml.jackson.databind.JsonNode
import com.posthog.hoglake.compaction.CompactionService
import com.posthog.hoglake.hydrator.Hydrator
import com.posthog.hoglake.service.CleanupService
import com.posthog.hoglake.service.ExpiryService
import com.posthog.hoglake.service.OptionsService
import com.posthog.hoglake.service.VerifyService
import io.ktor.http.HttpStatusCode
import io.ktor.server.application.Application
import io.ktor.server.application.ApplicationCall
import io.ktor.server.plugins.BadRequestException
import io.ktor.server.request.receive
import io.ktor.server.response.respond
import io.ktor.server.routing.get
import io.ktor.server.routing.patch
import io.ktor.server.routing.post
import io.ktor.server.routing.route
import io.ktor.server.routing.routing

/**
 * The lifecycle routes (openapi/hoglake.yaml): catalog options and the
 * manual maintenance triggers — the same code paths the background
 * loops call, so an operator can force a sweep and see its result.
 * Handlers only translate wire <-> model (Routes.kt division of labor);
 * semantics live in the services, mapped by ErrorMapping.
 *
 * The maintenance endpoints take an optional `?batch` override; the
 * defaults mirror Config's expiry/cleanup batch knobs (routes don't see
 * Config).
 */
fun Application.installMaintenanceRoutes(
    options: OptionsService,
    expiry: ExpiryService,
    cleanup: CleanupService,
    compaction: CompactionService,
    verify: VerifyService,
    hydrator: Hydrator,
) {
    routing {
        route("/v1/catalogs/{catalog}") {
            route("/options") {
                get {
                    call.respond(options.get(call.maintenanceCatalog()).toDto())
                }
                patch {
                    val req = OptionsPatchRequest.from(call.receive<JsonNode>())
                    call.respond(
                        options.patch(
                            call.maintenanceCatalog(),
                            req.snapshotRetentionSeconds,
                            req.consumerFloor,
                        ).toDto(),
                    )
                }
            }
            post("/maintenance/expire") {
                call.respond(
                    expiry.runOnce(
                        call.maintenanceCatalog(),
                        call.batchQuery() ?: DEFAULT_EXPIRE_BATCH,
                    ).toDto(),
                )
            }
            post("/maintenance/cleanup") {
                call.respond(
                    cleanup.runOnce(
                        call.maintenanceCatalog(),
                        call.batchQuery() ?: DEFAULT_CLEANUP_BATCH,
                    ).toDto(),
                )
            }
            // `batch` = groups rewritten this run (default: the config knob,
            // 1 — compaction takes tiny bites by construction).
            post("/maintenance/compact") {
                call.respond(
                    compaction.runOnce(
                        call.maintenanceCatalog(),
                        call.batchQuery(),
                    ).toDto(),
                )
            }
            // Metadata-only invariant scan (gaps.md B3, absorbing B4's
            // density assertion). Read-only, MVCC snapshot, no locks.
            post("/maintenance/verify") {
                call.respond(verify.runOnce(call.maintenanceCatalog()).toDto())
            }
            // Operator requeue for structurally-failed hydrations: flips
            // 'failed' files back to 'pending' (whole catalog, or one
            // table via ?namespace=..&table=.. — both together or
            // neither).
            post("/maintenance/rehydrate") {
                call.respond(
                    hydrator.rehydrateFailed(
                        call.maintenanceCatalog(),
                        call.request.queryParameters["namespace"],
                        call.request.queryParameters["table"],
                    ).toDto(),
                )
            }
            // DR/export surface (gaps.md B5): specified in
            // openapi/hoglake.yaml, 501 until built — the publications
            // pattern, so clients get the documented contract instead of
            // a bare 404 fall-through.
            get("/export") {
                call.respond(
                    HttpStatusCode.NotImplemented,
                    ApiErrorDto(
                        error = "not_implemented",
                        detail = "catalog export is specified but not yet implemented",
                    ),
                )
            }
        }
    }
}

/** Default `?batch` for POST /maintenance/expire (Config's expiryBatchSize default). */
private const val DEFAULT_EXPIRE_BATCH = 10_000

/** Default `?batch` for POST /maintenance/cleanup (Config's cleanupBatchSize default). */
private const val DEFAULT_CLEANUP_BATCH = 2_000

private fun ApplicationCall.maintenanceCatalog(): String =
    parameters["catalog"] ?: throw BadRequestException("missing path parameter 'catalog'")

private fun ApplicationCall.batchQuery(): Int? =
    request.queryParameters["batch"]?.let {
        it.toIntOrNull()
            ?: throw BadRequestException("query parameter 'batch' must be an integer, got '$it'")
    }
