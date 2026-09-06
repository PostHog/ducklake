package com.posthog.hoglake.api

import com.posthog.hoglake.service.ViewService
import io.ktor.http.HttpStatusCode
import io.ktor.server.application.Application
import io.ktor.server.application.ApplicationCall
import io.ktor.server.request.receive
import io.ktor.server.response.respond
import io.ktor.server.routing.delete
import io.ktor.server.routing.get
import io.ktor.server.routing.post
import io.ktor.server.routing.route
import io.ktor.server.routing.routing

/**
 * View routes (openapi listViews/createView/getView/dropView). View SQL
 * travels and is stored verbatim — the server never parses it.
 * Installed separately so App.kt wires it with its own ViewService.
 */
fun Application.installViewRoutes(views: ViewService) {
    routing {
        route("/v1/catalogs/{catalog}/namespaces/{namespace}/views") {
            get {
                call.respond(
                    views.list(call.catalog(), call.namespace()).map { it.toDto() },
                )
            }
            post {
                val req = call.receive<CreateViewRequestDto>()
                call.respond(
                    HttpStatusCode.Created,
                    views.create(
                        call.catalog(),
                        call.namespace(),
                        req.name,
                        req.sql,
                        req.dialect,
                    ).toDto(),
                )
            }
            route("/{view}") {
                get {
                    call.respond(
                        views.get(call.catalog(), call.namespace(), call.view()).toDto(),
                    )
                }
                delete {
                    call.respond(
                        views.drop(call.catalog(), call.namespace(), call.view()).toDto(),
                    )
                }
            }
        }
    }
}

private fun ApplicationCall.view() = pathParam("view")
