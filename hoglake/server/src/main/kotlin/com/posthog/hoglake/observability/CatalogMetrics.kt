package com.posthog.hoglake.observability

import io.github.oshai.kotlinlogging.KotlinLogging
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.MultiGauge
import io.micrometer.core.instrument.Tags
import org.jdbi.v3.core.Jdbi
import org.jdbi.v3.core.kotlin.withHandleUnchecked
import java.util.concurrent.atomic.AtomicBoolean
import kotlin.concurrent.thread

/**
 * Catalog-health gauges (README.md §8 — the catalog reports on itself,
 * retiring the metrics-cron layer). A lightweight periodic sampler
 * refreshes per-catalog MultiGauges; each sample is ONE batched query
 * over all catalogs (scalar subselects, no per-gauge round trips) plus
 * one grouped query for consumer offsets.
 *
 * Notes on semantics:
 *  - hoglake_snapshot_count is the head - earliest + 1 approximation
 *    (retained ids are dense between floor and head by construction;
 *    cheap, no count(*) over hog_snapshot).
 *  - hoglake_head_age_seconds is now() - head snapshot_time, both read
 *    on the Postgres side so app/db clock skew cannot bend it.
 *  - hoglake_consumer_lag_snapshots{consumer} is head - min committed
 *    snapshot across the consumer's tables (its worst table). To cap
 *    cardinality, per-consumer series are only emitted while a catalog
 *    has <= MAX_CONSUMER_SERIES consumers; beyond that the catalog
 *    emits a single hoglake_consumer_lag_max instead.
 */
class CatalogMetrics(private val jdbi: Jdbi, private val registry: MeterRegistry) {
    private val log = KotlinLogging.logger {}

    private fun multiGauge(
        name: String,
        description: String,
    ): MultiGauge = MultiGauge.builder(name).description(description).register(registry)

    private val headSnapshotId =
        multiGauge("hoglake_head_snapshot_id", "Catalog head snapshot id")
    private val earliestSnapshotId =
        multiGauge("hoglake_earliest_snapshot_id", "Earliest retained snapshot id (expiry floor)")
    private val snapshotCount =
        multiGauge("hoglake_snapshot_count", "Retained snapshots (head - earliest + 1)")
    private val headAgeSeconds =
        multiGauge("hoglake_head_age_seconds", "Seconds since the head snapshot was committed")
    private val removalQueueDepth =
        multiGauge("hoglake_removal_queue_depth", "hog_file_removal entries awaiting cleanup")
    private val statsPendingFiles =
        multiGauge("hoglake_stats_pending_files", "Data files with stats_state = 'pending'")
    private val tableCount =
        multiGauge("hoglake_table_count", "Live (non-dropped) tables")
    private val consumerLag =
        multiGauge("hoglake_consumer_lag_snapshots", "head - min committed snapshot per consumer")
    private val consumerLagMax =
        multiGauge(
            "hoglake_consumer_lag_max",
            "Worst consumer lag; emitted instead of per-consumer series past $MAX_CONSUMER_SERIES consumers",
        )

    private data class CatalogRow(
        val name: String,
        val head: Long,
        val earliest: Long,
        val headAgeSeconds: Double?,
        val removalDepth: Long,
        val statsPending: Long,
        val tables: Long,
    )

    /** One sample: refresh every gauge from the catalog. Safe to call concurrently with traffic. */
    fun sampleOnce() {
        val rows =
            jdbi.withHandleUnchecked { h ->
                h.createQuery(
                    """
                SELECT c.name,
                       c.last_snapshot_id,
                       c.earliest_snapshot_id,
                       (SELECT extract(epoch FROM (now() - s.snapshot_time))
                          FROM hog_snapshot s
                         WHERE s.catalog_id = c.catalog_id
                           AND s.snapshot_id = c.last_snapshot_id) AS head_age_seconds,
                       (SELECT count(*) FROM hog_file_removal r
                         WHERE r.catalog_id = c.catalog_id) AS removal_depth,
                       (SELECT count(*) FROM hog_data_file f
                         WHERE f.catalog_id = c.catalog_id
                           AND f.stats_state = 'pending') AS stats_pending,
                       (SELECT count(*) FROM hog_table t
                         WHERE t.catalog_id = c.catalog_id
                           AND t.dropped_snapshot IS NULL) AS table_count
                  FROM hog_catalog c
                """,
                )
                    .map { rs, _ ->
                        CatalogRow(
                            name = rs.getString("name"),
                            head = rs.getLong("last_snapshot_id"),
                            earliest = rs.getLong("earliest_snapshot_id"),
                            headAgeSeconds =
                                rs.getObject("head_age_seconds")?.let {
                                    rs.getDouble("head_age_seconds")
                                },
                            removalDepth = rs.getLong("removal_depth"),
                            statsPending = rs.getLong("stats_pending"),
                            tables = rs.getLong("table_count"),
                        )
                    }
                    .list()
            }
        // consumer -> worst (lowest) committed snapshot across its tables.
        val offsets =
            jdbi.withHandleUnchecked { h ->
                h.createQuery(
                    """
                SELECT c.name AS catalog, o.consumer_id,
                       min(o.committed_snapshot) AS committed
                  FROM hog_consumer_offset o
                  JOIN hog_catalog c ON c.catalog_id = o.catalog_id
                 GROUP BY c.name, o.consumer_id
                """,
                )
                    .map { rs, _ ->
                        Triple(rs.getString("catalog"), rs.getString("consumer_id"), rs.getLong("committed"))
                    }
                    .list()
            }

        fun rowsOf(value: (CatalogRow) -> Number) =
            rows.map { MultiGauge.Row.of(Tags.of("catalog", it.name), value(it)) }

        headSnapshotId.register(rowsOf { it.head }, true)
        earliestSnapshotId.register(rowsOf { it.earliest }, true)
        snapshotCount.register(rowsOf { it.head - it.earliest + 1 }, true)
        headAgeSeconds.register(
            rows.mapNotNull { r ->
                r.headAgeSeconds?.let { MultiGauge.Row.of(Tags.of("catalog", r.name), it) }
            },
            true,
        )
        removalQueueDepth.register(rowsOf { it.removalDepth }, true)
        statsPendingFiles.register(rowsOf { it.statsPending }, true)
        tableCount.register(rowsOf { it.tables }, true)

        val headByCatalog = rows.associate { it.name to it.head }
        val byCatalog = offsets.groupBy { it.first }
        val lagRows = mutableListOf<MultiGauge.Row<Number>>()
        val lagMaxRows = mutableListOf<MultiGauge.Row<Number>>()
        for ((catalog, entries) in byCatalog) {
            val head = headByCatalog[catalog] ?: continue
            if (entries.size <= MAX_CONSUMER_SERIES) {
                entries.mapTo(lagRows) { (_, consumer, committed) ->
                    MultiGauge.Row.of(Tags.of("catalog", catalog, "consumer", consumer), head - committed)
                }
            } else {
                val maxLag = entries.maxOf { head - it.third }
                lagMaxRows += MultiGauge.Row.of(Tags.of("catalog", catalog), maxLag)
            }
        }
        consumerLag.register(lagRows, true)
        consumerLagMax.register(lagMaxRows, true)
    }

    /**
     * Background sampler loop on a daemon thread (Hydrator.startLoop
     * pattern). [intervalMs] <= 0 returns a no-op handle (tests drive
     * [sampleOnce] directly).
     */
    fun startLoop(intervalMs: Long): AutoCloseable {
        if (intervalMs <= 0) return AutoCloseable { }
        val running = AtomicBoolean(true)
        val worker =
            thread(name = "hoglake-metrics", isDaemon = true) {
                while (running.get()) {
                    try {
                        sampleOnce()
                    } catch (e: Exception) {
                        log.error(e) { "catalog metrics sample failed" }
                    }
                    try {
                        Thread.sleep(intervalMs)
                    } catch (_: InterruptedException) {
                        Thread.currentThread().interrupt()
                        break
                    }
                }
            }
        return AutoCloseable {
            running.set(false)
            worker.interrupt()
            worker.join(5_000)
        }
    }

    companion object {
        /** Per-catalog cap on hoglake_consumer_lag_snapshots series. */
        const val MAX_CONSUMER_SERIES = 100
    }
}
