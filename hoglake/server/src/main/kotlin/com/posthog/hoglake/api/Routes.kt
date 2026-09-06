package com.posthog.hoglake.api

import com.posthog.hoglake.commit.CommitService
import com.posthog.hoglake.service.CatalogService
import com.posthog.hoglake.service.ScanService
import io.ktor.http.HttpStatusCode
import io.ktor.server.application.Application
import io.ktor.server.application.ApplicationCall
import io.ktor.server.plugins.BadRequestException
import io.ktor.server.request.receive
import io.ktor.server.response.respond
import io.ktor.server.routing.delete
import io.ktor.server.routing.get
import io.ktor.server.routing.post
import io.ktor.server.routing.put
import io.ktor.server.routing.route
import io.ktor.server.routing.routing
import java.time.Instant
import java.time.format.DateTimeParseException
import java.util.UUID

/**
 * The /v1 routes, one-to-one with openapi/hoglake.yaml. Handlers only
 * translate wire <-> model and pick status codes; behavior (locking,
 * validation, conflict detection) lives in CatalogService/CommitService,
 * whose HoglakeExceptions are mapped by ErrorMapping.
 */
fun Application.installApiRoutes(
    catalogs: CatalogService,
    commits: CommitService,
    instanceName: String = "",
) {
    routing {
        get("/v1/info") {
            call.respond(InstanceInfoDto(name = instanceName.ifBlank { null }))
        }
        route("/v1/catalogs") {
            get {
                call.respond(catalogs.listCatalogs().map { it.toDto() })
            }
            post {
                val req = call.receive<CreateCatalogRequestDto>()
                call.respond(
                    HttpStatusCode.Created,
                    catalogs.createCatalog(req.name, req.dataPath).toDto(),
                )
            }

            route("/{catalog}") {
                get {
                    call.respond(catalogs.getCatalog(call.catalog()).toDto())
                }

                route("/namespaces") {
                    get {
                        call.respond(catalogs.listNamespaces(call.catalog()).map { it.toDto() })
                    }
                    post {
                        val req = call.receive<CreateNamespaceRequestDto>()
                        call.respond(
                            HttpStatusCode.Created,
                            catalogs.createNamespace(call.catalog(), req.name).toDto(),
                        )
                    }
                    get("/{namespace}") {
                        call.respond(
                            catalogs.getNamespace(call.catalog(), call.namespace()).toDto(),
                        )
                    }

                    route("/{namespace}/tables") {
                        get {
                            call.respond(
                                catalogs.listTables(call.catalog(), call.namespace())
                                    .map { it.toSummaryDto() },
                            )
                        }
                        post {
                            val req = call.receive<CreateTableRequestDto>()
                            call.respond(
                                HttpStatusCode.Created,
                                catalogs.createTable(
                                    call.catalog(),
                                    call.namespace(),
                                    req.name,
                                    req.columns.map { it.toModel() },
                                ).toDto(),
                            )
                        }

                        route("/{table}") {
                            get {
                                call.respond(
                                    catalogs.getTable(
                                        call.catalog(),
                                        call.namespace(),
                                        call.table(),
                                        call.longQuery("snapshot"),
                                        call.instantQuery("at_timestamp"),
                                    ).toDto(),
                                )
                            }
                            delete {
                                call.respond(
                                    catalogs.dropTable(
                                        call.catalog(),
                                        call.namespace(),
                                        call.table(),
                                    ).toDto(),
                                )
                            }
                            get("/files") {
                                call.respond(
                                    catalogs.listFiles(
                                        call.catalog(),
                                        call.namespace(),
                                        call.table(),
                                        call.longQuery("snapshot"),
                                        call.instantQuery("at_timestamp"),
                                    ).map { it.toDto() },
                                )
                            }
                            get("/changes") {
                                val from =
                                    call.longQuery("from_snapshot")
                                        ?: throw BadRequestException(
                                            "missing required query parameter 'from_snapshot'",
                                        )
                                call.respond(
                                    catalogs.changes(
                                        call.catalog(),
                                        call.namespace(),
                                        call.table(),
                                        from,
                                        call.longQuery("to_snapshot"),
                                    ).toDto(),
                                )
                            }
                        }
                    }
                }

                get("/snapshots") {
                    val after = call.longQuery("after") ?: 0L
                    val before = call.longQuery("before")
                    val limit = (call.intQuery("limit") ?: 1000).coerceAtMost(10_000)
                    val (page, hasMore) =
                        catalogs.listSnapshots(call.catalog(), after, limit, before)
                    call.respond(SnapshotPageDto(page.map { it.toDto() }, hasMore))
                }

                post("/commit") {
                    val req = call.receive<CommitRequestDto>()
                    call.respond(commits.commit(call.catalog(), req.toModel()).toDto())
                }

                get("/consumers") {
                    call.respond(
                        catalogs.listConsumers(call.catalog()).toConsumerListDto(),
                    )
                }

                route("/consumers/{consumer}/offsets") {
                    get {
                        call.respond(
                            catalogs.listOffsets(call.catalog(), call.consumer())
                                .map { it.toDto() },
                        )
                    }
                    get("/{tableUuid}") {
                        call.respond(
                            catalogs.getOffset(
                                call.catalog(),
                                call.consumer(),
                                call.uuidPath("tableUuid"),
                            ).toDto(),
                        )
                    }
                    put("/{tableUuid}") {
                        val tableUuid = call.uuidPath("tableUuid")
                        val req = call.receive<CommitOffsetRequestDto>()
                        call.respond(
                            catalogs.commitOffset(
                                call.catalog(),
                                call.consumer(),
                                tableUuid,
                                req.snapshotId,
                            ).toDto(),
                        )
                    }
                }
            }
        }
    }
}

/**
 * GET .../tables/{table}/scan?snapshot= — read planning (openapi
 * planScan): data files paired with their visible deletion vectors.
 * Installed separately so App.kt wires it with its own ScanService.
 */
fun Application.installScanRoutes(scan: ScanService) {
    routing {
        get("/v1/catalogs/{catalog}/namespaces/{namespace}/tables/{table}/scan") {
            call.respond(
                scan.planScan(
                    call.catalog(),
                    call.namespace(),
                    call.table(),
                    call.longQuery("snapshot"),
                    call.instantQuery("at_timestamp"),
                ).map { it.toDto() },
            )
        }
    }
}

// ---- parameter helpers ---------------------------------------------------
// internal (not private): ViewRoutes.kt shares them.

internal fun ApplicationCall.pathParam(name: String): String =
    parameters[name] ?: throw BadRequestException("missing path parameter '$name'")

internal fun ApplicationCall.catalog() = pathParam("catalog")

internal fun ApplicationCall.namespace() = pathParam("namespace")

private fun ApplicationCall.table() = pathParam("table")

private fun ApplicationCall.consumer() = pathParam("consumer")

private fun ApplicationCall.uuidPath(name: String): UUID {
    val raw = pathParam(name)
    return try {
        UUID.fromString(raw)
    } catch (_: IllegalArgumentException) {
        throw BadRequestException("path parameter '$name' must be a UUID, got '$raw'")
    }
}

private fun ApplicationCall.longQuery(name: String): Long? =
    request.queryParameters[name]?.let {
        it.toLongOrNull()
            ?: throw BadRequestException("query parameter '$name' must be an integer, got '$it'")
    }

private fun ApplicationCall.intQuery(name: String): Int? =
    request.queryParameters[name]?.let {
        it.toIntOrNull()
            ?: throw BadRequestException("query parameter '$name' must be an integer, got '$it'")
    }

/** ISO-8601 instant query parameter (e.g. 2026-09-04T12:00:00Z); unparseable -> 400. */
private fun ApplicationCall.instantQuery(name: String): Instant? =
    request.queryParameters[name]?.let {
        try {
            Instant.parse(it)
        } catch (_: DateTimeParseException) {
            throw BadRequestException(
                "query parameter '$name' must be an ISO-8601 instant, got '$it'",
            )
        }
    }
