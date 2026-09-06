package com.posthog.hoglake.observability

import com.posthog.hoglake.model.HoglakeException
import net.logstash.logback.argument.StructuredArgument
import net.logstash.logback.argument.StructuredArguments.kv
import org.slf4j.LoggerFactory
import org.slf4j.MDC

/**
 * The first-class audit log (../README.md "Audit log as a first-class
 * feature — and never rows in a database"): every consequential action
 * emits ONE structured line to the dedicated "hoglake.audit" logger,
 * which logback.xml routes to stdout as JSON (LogstashEncoder,
 * additivity off). Emission is fire-and-forget into the process's log
 * stream — never a database row, and callers emit AFTER their
 * transaction commits (or fails), never inside it: the duckgres
 * query_log lesson generalized.
 *
 * actor is fixed to "anonymous" until auth lands. request_id comes from
 * the caller or, when in a request context, from the MDC entry the
 * RequestId plugin + CallLogging maintain; background jobs carry none.
 */
object Audit {
    const val LOGGER_NAME = "hoglake.audit"

    /** MDC key the RequestId plugin populates (CallLogging mdc). */
    const val REQUEST_ID_MDC = "request_id"

    // slf4j directly (not a KLogger wrapper): StructuredArguments must
    // reach the LogstashEncoder as event arguments.
    private val log = LoggerFactory.getLogger(LOGGER_NAME)

    /**
     * Emit one audit event. [obj] is the acted-on object (namespace,
     * namespace.table, consumer/table pair, ...); null for catalog-level
     * actions. [requestId] defaults to the request MDC when present.
     */
    fun event(
        action: String,
        catalog: String?,
        obj: String?,
        outcome: String,
        detail: String? = null,
        requestId: String? = null,
    ) {
        val args =
            mutableListOf<StructuredArgument>(
                kv("action", action),
                kv("actor", "anonymous"),
                kv("outcome", outcome),
            )
        catalog?.let { args += kv("catalog", it) }
        obj?.let { args += kv("object", it) }
        detail?.let { args += kv("detail", it) }
        (requestId ?: MDC.get(REQUEST_ID_MDC))?.let { args += kv("request_id", it) }
        log.info("$action $outcome", *args.toTypedArray())
    }

    /**
     * Run [block] (typically one service transaction) and emit exactly
     * one audit event AFTER it returns or throws — the event always
     * describes a finished (committed or rolled-back) action, never one
     * in flight.
     */
    inline fun <T> audited(
        action: String,
        catalog: String?,
        obj: String?,
        successOutcome: (T) -> String = { "ok" },
        detail: (T) -> String? = { null },
        block: () -> T,
    ): T {
        val result =
            try {
                block()
            } catch (e: Throwable) {
                event(action, catalog, obj, failureOutcome(e), e.message)
                throw e
            }
        event(action, catalog, obj, successOutcome(result), detail(result))
        return result
    }

    /** Failure outcome vocabulary for audit lines (ErrorMapping's cousin). */
    fun failureOutcome(e: Throwable): String =
        when (e) {
            is HoglakeException.CommitConflict -> "conflict"
            is HoglakeException.AlreadyExists -> "conflict"
            is HoglakeException.OffsetRegression -> "regression"
            is HoglakeException.Validation -> "validation"
            is HoglakeException.NotFound -> "not_found"
            else -> "error"
        }
}
