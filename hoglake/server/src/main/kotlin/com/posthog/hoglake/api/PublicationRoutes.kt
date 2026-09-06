package com.posthog.hoglake.api

import io.ktor.http.HttpStatusCode
import io.ktor.server.application.Application
import io.ktor.server.application.ApplicationCall
import io.ktor.server.response.respond
import io.ktor.server.routing.delete
import io.ktor.server.routing.get
import io.ktor.server.routing.post
import io.ktor.server.routing.route
import io.ktor.server.routing.routing

/**
 * CDC publications (the WAL tap): fully specified in
 * openapi/hoglake.yaml, not yet implemented. Every specified path
 * answers 501 with the spec's NotImplemented ApiError so clients get the
 * documented contract instead of a bare 404 fall-through. Request bodies
 * are deliberately not parsed — the endpoints do nothing yet, so
 * rejecting a body shape the implementation may still change would
 * over-commit the contract.
 */
fun Application.installPublicationRoutes() {
    routing {
        route("/v1/catalogs/{catalog}/publications") {
            get { call.respondNotImplemented() }
            post { call.respondNotImplemented() }
            route("/{publication}") {
                get { call.respondNotImplemented() }
                delete { call.respondNotImplemented() }
            }
        }
    }
}

private suspend fun ApplicationCall.respondNotImplemented() {
    respond(
        HttpStatusCode.NotImplemented,
        ApiErrorDto(
            error = "not_implemented",
            detail = "CDC publications are specified but not yet implemented",
        ),
    )
}
