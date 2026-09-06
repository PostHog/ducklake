package com.posthog.hoglake.observability

import com.posthog.hoglake.model.HoglakeException
import io.micrometer.prometheusmetrics.PrometheusConfig
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Test

/**
 * The Metrics facade: a real PrometheusMeterRegistry when bound, a
 * strict no-op when not (services must never require a registry).
 */
class MetricsFacadeTest {
    @AfterEach
    fun unbind() = Metrics.clear()

    @Test
    fun `every counter call is a no-op with no registry bound`() {
        Metrics.clear()
        // Must not throw, must not NPE — services call these unconditionally.
        Metrics.commitRecorded("lake", "committed")
        Metrics.snapshotsExpired("lake", 5)
        Metrics.filesRemoved("lake", 3)
        Metrics.statsHydrated("provided")
    }

    @Test
    fun `counters land in the bound registry with the right names and tags`() {
        val registry = PrometheusMeterRegistry(PrometheusConfig.DEFAULT)
        Metrics.bind(registry)

        Metrics.commitRecorded("lake", "committed")
        Metrics.commitRecorded("lake", "committed")
        Metrics.commitRecorded("lake", "conflict")
        Metrics.commitRecorded("pond", "validation")
        Metrics.snapshotsExpired("lake", 7)
        Metrics.filesRemoved("lake", 4)
        Metrics.statsHydrated("provided")
        Metrics.statsHydrated("failed")

        fun counter(
            name: String,
            vararg tags: String,
        ) = registry.get(name).tags(*tags).counter().count()

        assertThat(counter("hoglake_commits_total", "catalog", "lake", "result", "committed"))
            .isEqualTo(2.0)
        assertThat(counter("hoglake_commits_total", "catalog", "lake", "result", "conflict"))
            .isEqualTo(1.0)
        assertThat(counter("hoglake_commits_total", "catalog", "pond", "result", "validation"))
            .isEqualTo(1.0)
        assertThat(counter("hoglake_snapshots_expired_total", "catalog", "lake")).isEqualTo(7.0)
        assertThat(counter("hoglake_files_removed_total", "catalog", "lake")).isEqualTo(4.0)
        assertThat(counter("hoglake_stats_hydrated_total", "result", "provided")).isEqualTo(1.0)
        assertThat(counter("hoglake_stats_hydrated_total", "result", "failed")).isEqualTo(1.0)

        // The Prometheus rendering keeps the exact metric names.
        val scrape = registry.scrape()
        assertThat(scrape).contains("hoglake_commits_total{")
        assertThat(scrape).contains("hoglake_snapshots_expired_total{")
        assertThat(scrape).contains("hoglake_files_removed_total{")
        assertThat(scrape).contains("hoglake_stats_hydrated_total{")
    }

    @Test
    fun `zero-count expiry and removal increments create no series`() {
        val registry = PrometheusMeterRegistry(PrometheusConfig.DEFAULT)
        Metrics.bind(registry)
        Metrics.snapshotsExpired("lake", 0)
        Metrics.filesRemoved("lake", 0)
        assertThat(registry.meters).isEmpty()
    }

    @Test
    fun `commit failure results map to the counter vocabulary`() {
        assertThat(Metrics.commitFailureResult(HoglakeException.CommitConflict("x")))
            .isEqualTo("conflict")
        assertThat(Metrics.commitFailureResult(HoglakeException.Validation("x")))
            .isEqualTo("validation")
        // NotFound (unknown catalog) is not a commit result — no counter.
        assertThat(Metrics.commitFailureResult(HoglakeException.NotFound("x"))).isNull()
    }
}
