package com.posthog.hoglake.api

import com.posthog.hoglake.service.AlterService
import io.ktor.server.application.Application
import io.ktor.server.application.ApplicationCall
import io.ktor.server.plugins.BadRequestException
import io.ktor.server.request.receive
import io.ktor.server.response.respond
import io.ktor.server.routing.post
import io.ktor.server.routing.routing

/**
 * POST /v1/catalogs/{catalog}/namespaces/{namespace}/tables/{table}/alter
 * (openapi/hoglake.yaml operationId alterTable). Same division of labor
 * as Routes.kt: the handler translates wire <-> model and enforces the
 * request's structural contract (minItems 1, op discriminator shape ->
 * 400); semantics live in AlterService, mapped by ErrorMapping.
 */
fun Application.installAlterRoutes(alter: AlterService) {
    routing {
        post("/v1/catalogs/{catalog}/namespaces/{namespace}/tables/{table}/alter") {
            val req = call.receive<AlterTableRequestDto>()
            if (req.ops.isEmpty()) {
                throw BadRequestException("ops must contain at least one operation")
            }
            val ops = req.ops.map { it.toModel() }
            call.respond(
                alter.alterTable(
                    call.alterPathParam("catalog"),
                    call.alterPathParam("namespace"),
                    call.alterPathParam("table"),
                    ops,
                ).toAlteredDto(),
            )
        }
    }
}

private fun ApplicationCall.alterPathParam(name: String): String =
    parameters[name] ?: throw BadRequestException("missing path parameter '$name'")
