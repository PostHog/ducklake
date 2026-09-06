package com.posthog.hoglake.observability

import io.ktor.server.application.ApplicationCall
import io.ktor.server.application.createApplicationPlugin
import io.ktor.util.AttributeKey
import java.util.UUID

/**
 * Request-id plugin: accepts an incoming X-Request-Id (passthrough) or
 * generates a UUID, stores it as a call attribute, and echoes it on the
 * response. CallLogging's `mdc(Audit.REQUEST_ID_MDC) { it.requestId }`
 * (App.kt) then propagates it into the MDC for the whole call pipeline
 * — coroutine-safe, which a bare MDC.put here would not be — so audit
 * lines emitted anywhere under a request carry request_id.
 *
 * Hand-rolled rather than Ktor's CallId plugin: ktor-server-call-id is
 * not on the classpath and build.gradle.kts is frozen; this covers the
 * same generate + header-passthrough contract.
 */
private val RequestIdKey = AttributeKey<String>("HoglakeRequestId")

const val REQUEST_ID_HEADER = "X-Request-Id"

val RequestId =
    createApplicationPlugin("HoglakeRequestId") {
        onCall { call ->
            val incoming =
                call.request.headers[REQUEST_ID_HEADER]
                    ?.takeIf { it.isNotBlank() && it.length <= 128 }
            val id = incoming ?: UUID.randomUUID().toString()
            call.attributes.put(RequestIdKey, id)
            call.response.headers.append(REQUEST_ID_HEADER, id)
        }
    }

/** The call's request id, once [RequestId] has run. */
val ApplicationCall.requestId: String?
    get() = attributes.getOrNull(RequestIdKey)
