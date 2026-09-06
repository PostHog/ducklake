package com.posthog.hoglake.observability

import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.Gauge
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.MultiGauge
import io.micrometer.core.instrument.Tags
import org.jdbi.v3.core.Jdbi
import org.jdbi.v3.core.kotlin.inTransactionUnchecked
import java.util.concurrent.atomic.AtomicLong

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
 *  - hoglake_removal_queue_depth counts UNDRAINED entries only: drained
 *    rows are the soft-deleted cleanup ledger, not pending work.
 *  - hoglake_missing_field_id_files counts LIVE flagged files — the
 *    same population the rename guard refuses on.
 *  - hoglake_consumer_lag_snapshots{consumer} is head - min committed
 *    snapshot across the consumer's tables (its worst table). To cap
 *    cardinality, per-consumer series are only emitted while a catalog
 *    has <= MAX_CONSUMER_SERIES consumers; hoglake_consumer_lag_max is
 *    ALWAYS emitted per catalog with offsets, so the alerting family
 *    never flaps when a catalog crosses the cap. Offsets are joined to
 *    hog_table (ExpiryService's floor query, mirrored) so garbage or
 *    expired uuids can never distort the lag series, head + offsets are
 *    read in one transaction, and lag is clamped at >= 0.
 *  - hoglake_metrics_last_sample_epoch (global) is the wall-clock time
 *    of the last successful sample — staleness of every other gauge is
 *    visible; hoglake_metrics_sample_errors_total counts failed samples.
 */
class CatalogMetrics(private val jdbi: Jdbi, private val registry: MeterRegistry) {
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
    private val statsFailedFiles =
        multiGauge(
            "hoglake_stats_failed_files",
            "Data files with stats_state = 'failed' (hydration failed loudly; B1)",
        )
    private val missingFieldIdFiles =
        multiGauge(
            "hoglake_missing_field_id_files",
            "Live data files whose parquet schema lacks field ids (rename-blocking)",
        )
    private val tableCount =
        multiGauge("hoglake_table_count", "Live (non-dropped) tables")
    private val consumerLag =
        multiGauge("hoglake_consumer_lag_snapshots", "head - min committed snapshot per consumer")
    private val consumerLagMax =
        multiGauge(
            "hoglake_consumer_lag_max",
            "Worst consumer lag per catalog; always emitted (per-consumer series stop past " +
                "$MAX_CONSUMER_SERIES consumers, this never does)",
        )

    /** Epoch seconds of the last successful sample; 0 = never sampled. */
    private val lastSampleEpoch = AtomicLong(0)

    init {
        Gauge.builder("hoglake_metrics_last_sample_epoch", lastSampleEpoch) { it.get().toDouble() }
            .description("Epoch seconds of the last successful catalog-metrics sample (0 = never)")
            .register(registry)
    }

    private val sampleErrors: Counter =
        Counter.builder("hoglake_metrics_sample_errors_total")
            .description("Catalog-metrics samples that failed with an exception")
            .register(registry)

    private data class CatalogRow(
        val name: String,
        val head: Long,
        val earliest: Long,
        val headAgeSeconds: Double?,
        val removalDepth: Long,
        val statsPending: Long,
        val statsFailed: Long,
        val missingFieldIds: Long,
        val tables: Long,
    )

    /** One sample: refresh every gauge from the catalog. Safe to call concurrently with traffic. */
    fun sampleOnce() {
        val (rows, offsets) =
            try {
                readSample()
            } catch (t: Throwable) {
                sampleErrors.increment()
                throw t
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
        statsFailedFiles.register(rowsOf { it.statsFailed }, true)
        missingFieldIdFiles.register(rowsOf { it.missingFieldIds }, true)
        tableCount.register(rowsOf { it.tables }, true)

        val headByCatalog = rows.associate { it.name to it.head }
        val byCatalog = offsets.groupBy { it.first }
        val lagRows = mutableListOf<MultiGauge.Row<Number>>()
        val lagMaxRows = mutableListOf<MultiGauge.Row<Number>>()
        for ((catalog, entries) in byCatalog) {
            val head = headByCatalog[catalog] ?: continue

            // Head and offsets come from one transaction, so lag cannot go
            // negative from a racing commit; the clamp is belt-and-braces.
            fun lag(committed: Long) = (head - committed).coerceAtLeast(0)
            if (entries.size <= MAX_CONSUMER_SERIES) {
                entries.mapTo(lagRows) { (_, consumer, committed) ->
                    MultiGauge.Row.of(Tags.of("catalog", catalog, "consumer", consumer), lag(committed))
                }
            }
            // lag_max is emitted unconditionally so the series never flaps
            // as a catalog crosses the per-consumer cardinality cap.
            lagMaxRows += MultiGauge.Row.of(Tags.of("catalog", catalog), entries.maxOf { lag(it.third) })
        }
        consumerLag.register(lagRows, true)
        consumerLagMax.register(lagMaxRows, true)
        lastSampleEpoch.set(System.currentTimeMillis() / 1000)
    }

    /** The two sample queries, in ONE transaction: head and offsets are a consistent pair. */
    private fun readSample(): Pair<List<CatalogRow>, List<Triple<String, String, Long>>> =
        jdbi.inTransactionUnchecked { h ->
            val rows =
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
                         WHERE r.catalog_id = c.catalog_id
                           AND r.drained_at IS NULL) AS removal_depth,
                       (SELECT count(*) FROM hog_data_file f
                         WHERE f.catalog_id = c.catalog_id
                           AND f.stats_state = 'pending') AS stats_pending,
                       (SELECT count(*) FROM hog_data_file f
                         WHERE f.catalog_id = c.catalog_id
                           AND f.stats_state = 'failed') AS stats_failed,
                       (SELECT count(*) FROM hog_data_file f
                         WHERE f.catalog_id = c.catalog_id
                           AND f.missing_field_ids
                           AND f.end_snapshot IS NULL) AS missing_field_ids,
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
                            statsFailed = rs.getLong("stats_failed"),
                            missingFieldIds = rs.getLong("missing_field_ids"),
                            tables = rs.getLong("table_count"),
                        )
                    }
                    .list()
            // consumer -> worst (lowest) committed snapshot across its
            // tables. The hog_table join mirrors ExpiryService's floor
            // query: only offsets whose table identity exists (any
            // incarnation) count — a garbage/expired uuid cannot distort
            // the lag series any more than it can pin retention.
            val offsets =
                h.createQuery(
                    """
                SELECT c.name AS catalog, o.consumer_id,
                       min(o.committed_snapshot) AS committed
                  FROM hog_consumer_offset o
                  JOIN hog_catalog c ON c.catalog_id = o.catalog_id
                  JOIN hog_table t
                    ON t.catalog_id = o.catalog_id AND t.table_uuid = o.table_uuid
                 GROUP BY c.name, o.consumer_id
                """,
                )
                    .map { rs, _ ->
                        Triple(rs.getString("catalog"), rs.getString("consumer_id"), rs.getLong("committed"))
                    }
                    .list()
            rows to offsets
        }

    companion object {
        /** Per-catalog cap on hoglake_consumer_lag_snapshots series. */
        const val MAX_CONSUMER_SERIES = 100
    }
}
