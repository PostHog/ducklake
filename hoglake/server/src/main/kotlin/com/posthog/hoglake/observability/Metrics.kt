package com.posthog.hoglake.observability

import com.posthog.hoglake.model.HoglakeException
import io.micrometer.core.instrument.MeterRegistry

/**
 * Source-incremented counters (README.md §8): counted where the event
 * happens (commit tail, expiry sweep, cleanup drain, hydrator), never
 * sampled. The facade is a process-global hook so services need no
 * registry plumbing; with no registry bound every call is a no-op, so
 * tests that never touch observability keep running unchanged.
 */
object Metrics {
    @Volatile
    private var registry: MeterRegistry? = null

    /** Bind the process registry (App.build). Last bind wins. */
    fun bind(r: MeterRegistry) {
        registry = r
    }

    /** Unbind (tests). */
    fun clear() {
        registry = null
    }

    /** hoglake_commits_total{catalog, result=committed|conflict|validation} */
    fun commitRecorded(
        catalog: String,
        result: String,
    ) = increment("hoglake_commits_total", 1.0, "catalog", catalog, "result", result)

    /** hoglake_snapshots_expired_total{catalog} */
    fun snapshotsExpired(
        catalog: String,
        count: Long,
    ) {
        if (count > 0) increment("hoglake_snapshots_expired_total", count.toDouble(), "catalog", catalog)
    }

    /** hoglake_files_removed_total{catalog} — physical S3 deletes only. */
    fun filesRemoved(
        catalog: String,
        count: Long,
    ) {
        if (count > 0) increment("hoglake_files_removed_total", count.toDouble(), "catalog", catalog)
    }

    /** hoglake_stats_hydrated_total{result=provided|failed} */
    fun statsHydrated(result: String) = increment("hoglake_stats_hydrated_total", 1.0, "result", result)

    /** The commit counter's result tag for a failed commit. */
    fun commitFailureResult(e: HoglakeException): String? =
        when (e) {
            is HoglakeException.CommitConflict -> "conflict"
            is HoglakeException.Validation -> "validation"
            else -> null
        }

    private fun increment(
        name: String,
        amount: Double,
        vararg tags: String,
    ) {
        val r = registry ?: return
        r.counter(name, *tags).increment(amount)
    }
}
