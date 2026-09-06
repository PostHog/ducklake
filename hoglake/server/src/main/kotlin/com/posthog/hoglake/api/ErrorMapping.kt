package com.posthog.hoglake.api

import com.posthog.hoglake.model.HoglakeException
import io.github.oshai.kotlinlogging.KotlinLogging
import io.ktor.http.HttpHeaders
import io.ktor.http.HttpStatusCode
import io.ktor.serialization.JsonConvertException
import io.ktor.server.plugins.BadRequestException
import io.ktor.server.plugins.statuspages.StatusPagesConfig
import io.ktor.server.request.httpMethod
import io.ktor.server.request.uri
import io.ktor.server.response.respond

private val log = KotlinLogging.logger("com.posthog.hoglake.api.ErrorMapping")

/**
 * HoglakeException -> HTTP status mapping, plus the malformed-input and
 * catch-all handlers. Every error body is an ApiError {error, detail}
 * (the spec's schema); Ktor's default HTML pages never escape.
 *
 * - NotFound            -> 404
 * - AlreadyExists       -> 409
 * - CommitConflict      -> 409
 * - OffsetRegression    -> 409
 * - IdlessFilesPresent  -> 409
 * - Validation          -> 422
 * - Expired             -> 410
 * - CommitQueueTimeout  -> 503 + Retry-After (retryable backpressure,
 *                          never a generic 500)
 * - malformed body / unparseable query or path params -> 400
 * - anything else     -> 500 (logged; generic body, no internals)
 */
fun StatusPagesConfig.installErrorMapping() {
    exception<HoglakeException> { call, cause ->
        val (status, code) =
            when (cause) {
                is HoglakeException.NotFound -> HttpStatusCode.NotFound to "not_found"
                is HoglakeException.AlreadyExists -> HttpStatusCode.Conflict to "already_exists"
                is HoglakeException.CommitConflict -> HttpStatusCode.Conflict to "commit_conflict"
                is HoglakeException.OffsetRegression -> HttpStatusCode.Conflict to "offset_regression"
                is HoglakeException.IdlessFilesPresent -> HttpStatusCode.Conflict to "idless_files_present"
                is HoglakeException.Validation -> HttpStatusCode.UnprocessableEntity to "validation"
                is HoglakeException.Expired -> HttpStatusCode.Gone to "expired"
                is HoglakeException.CommitQueueTimeout -> {
                    // Explicit backpressure: the commit queued too long on
                    // the catalog lock. Clients back off and retry.
                    call.response.headers.append(HttpHeaders.RetryAfter, RETRY_AFTER_SECONDS)
                    HttpStatusCode.ServiceUnavailable to "commit_queue_timeout"
                }
            }
        call.respond(status, ApiErrorDto(error = code, detail = cause.message))
    }
    // Ktor wraps request-body deserialization failures in BadRequestException;
    // route helpers throw it directly for unparseable query/path parameters.
    exception<BadRequestException> { call, cause ->
        call.respond(HttpStatusCode.BadRequest, ApiErrorDto("bad_request", rootMessage(cause)))
    }
    exception<JsonConvertException> { call, cause ->
        call.respond(HttpStatusCode.BadRequest, ApiErrorDto("bad_request", rootMessage(cause)))
    }
    exception<Throwable> { call, cause ->
        log.error(cause) {
            "unhandled exception for ${call.request.httpMethod.value} ${call.request.uri}"
        }
        call.respond(
            HttpStatusCode.InternalServerError,
            ApiErrorDto("internal_error", "internal server error"),
        )
    }
}

/** Retry-After for 503 commit_queue_timeout: back off a beat, then retry. */
private const val RETRY_AFTER_SECONDS = "1"

private fun rootMessage(t: Throwable): String =
    generateSequence(t) { it.cause }.mapNotNull { it.message }.lastOrNull() ?: "malformed request"
