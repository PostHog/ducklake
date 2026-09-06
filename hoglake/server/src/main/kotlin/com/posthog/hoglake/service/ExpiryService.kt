package com.posthog.hoglake.service

import com.posthog.hoglake.model.CatalogInfo
import com.posthog.hoglake.model.ExpiryResult
import com.posthog.hoglake.model.HoglakeException
import com.posthog.hoglake.observability.Audit
import com.posthog.hoglake.observability.Metrics
import com.posthog.hoglake.persistence.CatalogRepo
import com.posthog.hoglake.persistence.Locks
import io.github.oshai.kotlinlogging.KotlinLogging
import org.jdbi.v3.core.Handle
import org.jdbi.v3.core.Jdbi
import org.jdbi.v3.core.kotlin.inTransactionUnchecked
import org.jdbi.v3.core.kotlin.withHandleUnchecked

/**
 * Consumer-aware, incremental snapshot expiry (README.md §6). One sweep
 * is ONE transaction under the per-catalog commit lock, so expiry
 * serializes with commits/DDL and a sweep can never observe (or leave)
 * a half-advanced floor.
 *
 * The floor math: with `earliest` the current earliest_snapshot_id,
 * the sweep computes
 *
 *   newEarliest = min( firstFresh ?: +inf,   // oldest snapshot inside the
 *                                            // retention window survives
 *                      head,                 // head NEVER expires
 *                      minConsumerOffset,    // consumer must still read
 *                                            // from its committed offset
 *                                            // (consumer_floor = true only)
 *                      earliest + batchSize) // incremental: bounded work
 *
 * where firstFresh = min snapshot_id with snapshot_time >= now() -
 * retention (so every snapshot below newEarliest is strictly older than
 * the cutoff, even under non-monotone clocks). Snapshots in
 * [earliest, newEarliest) are range-deleted; newEarliest <= earliest is
 * a zero-work sweep. flooredByConsumer names the pinning consumer iff
 * the consumer constraint bound the sweep below what time/head/batch
 * would have allowed — that includes a zero-work sweep the consumer
 * caused (page-worthy: a lagging consumer is pinning retention).
 *
 * File GC rides the same transaction, all range predicates (never
 * IN-lists): a file row whose end_snapshot <= newEarliest is invisible
 * at every surviving snapshot, so its path is queued into
 * hog_file_removal (a *suggestion* for CleanupService, never an
 * authorization — README.md §8) and the row deleted (FK cascades take
 * stats and partition values). Delete-vector rows go FIRST, including
 * live DVs whose data file is expiring — deleting the data-file row
 * would cascade them away un-queued, orphaning the object.
 *
 * A fifth step applies the same reachability rule to the accumulating
 * versioned DDL tables (hog_table_version, hog_column,
 * hog_partition_spec, hog_sort_spec, hog_view): rows with
 * end_snapshot <= newEarliest are invisible at every retained snapshot
 * and are deleted — DDL churn no longer grows them without bound. The
 * floor advance also captures the new floor snapshot's snapshot_time
 * into hog_catalog.earliest_snapshot_time (the reconciliation anchor
 * that 410s cite once the snapshot rows below the floor are gone).
 */
class ExpiryService(private val jdbi: Jdbi) {
    private val log = KotlinLogging.logger {}

    private companion object {
        /**
         * The end-snapshotted-but-never-deleted versioned tables (DDL
         * churn grows them without bound); sweep step 5 deletes their
         * below-floor corpses. Table names are a fixed compile-time
         * vocabulary, never derived from input (invariant 9 intact).
         */
        val VERSIONED_RETENTION_TABLES =
            listOf(
                "hog_table_version",
                "hog_column",
                "hog_partition_spec",
                "hog_sort_spec",
                "hog_view",
            )
    }

    /**
     * One expiry sweep for [catalog], expiring at most [batchSize]
     * snapshots. The audit event and the expired-snapshots counter are
     * emitted here, AFTER the sweep transaction has committed — and a
     * ZERO-WORK sweep (nothing expired or queued, no consumer floor in
     * play) emits no audit event at all, only an app-log debug line:
     * every-minute background no-ops must not flood the audit stream.
     */
    fun runOnce(
        catalog: String,
        batchSize: Int,
    ): ExpiryResult {
        val result =
            try {
                if (batchSize <= 0) {
                    throw HoglakeException.Validation("batch size must be positive (got $batchSize)")
                }
                jdbi.inTransactionUnchecked { h ->
                    val pre = CatalogRepo.require(h, catalog)
                    Locks.acquireCatalogCommitLock(h, pre.catalogId)
                    // Re-read under the lock: head/options may have moved while
                    // we queued behind a committer.
                    val cat = CatalogRepo.require(h, catalog)
                    sweep(h, cat, batchSize)
                }
            } catch (e: Throwable) {
                Audit.event("expiry", catalog, null, Audit.failureOutcome(e), e.message)
                throw e
            }
        // The floored-by page-worthy warn lives HERE, outside the sweep
        // transaction (and outside the advisory lock): the data is already
        // in the result, and log I/O must never ride the commit tail.
        result.flooredByConsumer?.let { consumer ->
            log.warn {
                "expiry for catalog '$catalog' floored by consumer '$consumer' " +
                    "(earliest stays at ${result.newEarliestSnapshotId})"
            }
        }
        Metrics.snapshotsExpired(catalog, result.snapshotsExpired)
        val zeroWork =
            result.snapshotsExpired == 0L && result.dataFilesQueued == 0L &&
                result.deleteFilesQueued == 0L && result.flooredByConsumer == null
        if (zeroWork) {
            log.debug { "expiry sweep for catalog '$catalog': nothing to do" }
        } else {
            Audit.event(
                "expiry",
                catalog,
                null,
                outcome = "ok",
                detail =
                    "snapshots_expired=${result.snapshotsExpired} " +
                        "data_files_queued=${result.dataFilesQueued} " +
                        "delete_files_queued=${result.deleteFilesQueued} " +
                        "new_earliest=${result.newEarliestSnapshotId}" +
                        (result.flooredByConsumer?.let { " floored_by_consumer=$it" } ?: ""),
            )
        }
        return result
    }

    private fun sweep(
        h: Handle,
        cat: CatalogInfo,
        batchSize: Int,
    ): ExpiryResult {
        val retention =
            cat.snapshotRetentionSeconds
                ?: return ExpiryResult(0, 0, 0, cat.earliestSnapshotId, null)

        // Oldest snapshot still inside the retention window; everything
        // below it is expirable time-wise.
        val firstFresh: Long? =
            h.createQuery(
                """
            SELECT min(snapshot_id) FROM hog_snapshot
            WHERE catalog_id = :catalogId
              AND snapshot_id >= :earliest
              AND snapshot_time >= now() - make_interval(secs => :retention)
            """,
            )
                .bind("catalogId", cat.catalogId)
                .bind("earliest", cat.earliestSnapshotId)
                .bind("retention", retention)
                .mapTo(Long::class.javaObjectType)
                .one()

        // The join to hog_table scopes the floor to offsets whose table
        // identity still exists (any incarnation, dropped included —
        // offsets survive drops by design). An offset whose table row is
        // gone entirely (expired away) must not pin retention forever.
        val minOffset: Pair<String, Long>? =
            if (cat.consumerFloor) {
                h.createQuery(
                    """
                SELECT o.consumer_id, o.committed_snapshot
                FROM hog_consumer_offset o
                JOIN hog_table t
                  ON t.catalog_id = o.catalog_id AND t.table_uuid = o.table_uuid
                WHERE o.catalog_id = :catalogId
                ORDER BY o.committed_snapshot, o.consumer_id
                LIMIT 1
                """,
                )
                    .bind("catalogId", cat.catalogId)
                    .map { rs, _ -> rs.getString("consumer_id") to rs.getLong("committed_snapshot") }
                    .findOne()
                    .orElse(null)
            } else {
                null
            }

        val unfloored =
            minOf(
                firstFresh ?: Long.MAX_VALUE,
                cat.headSnapshotId,
                cat.earliestSnapshotId + batchSize,
            )
        val newEarliest = minOf(unfloored, minOffset?.second ?: Long.MAX_VALUE)
        val flooredBy =
            minOffset
                ?.takeIf { it.second < unfloored && unfloored > cat.earliestSnapshotId }
                ?.first
        // NOTE: no logging in here — this runs inside the sweep transaction
        // under the catalog commit lock; runOnce warns AFTER commit.
        if (newEarliest <= cat.earliestSnapshotId) {
            return ExpiryResult(0, 0, 0, cat.earliestSnapshotId, flooredBy)
        }

        // 1) Delete-vector rows first: superseded DVs (end_snapshot in
        // range) plus live DVs riding an expiring data file — the data-file
        // delete below would cascade those away without queueing them.
        val deleteFilesQueued =
            h.createUpdate(
                """
            WITH doomed AS (
                DELETE FROM hog_delete_file dv
                WHERE dv.catalog_id = :catalogId
                  AND ((dv.end_snapshot IS NOT NULL AND dv.end_snapshot <= :newEarliest)
                       OR EXISTS (
                              SELECT 1 FROM hog_data_file df
                              WHERE df.catalog_id = dv.catalog_id
                                AND df.data_file_id = dv.data_file_id
                                AND df.end_snapshot IS NOT NULL
                                AND df.end_snapshot <= :newEarliest))
                RETURNING path
            )
            INSERT INTO hog_file_removal (catalog_id, path, file_kind, reason)
            SELECT :catalogId, path, 'delete', 'snapshot_expiry' FROM doomed
            """,
            )
                .bind("catalogId", cat.catalogId)
                .bind("newEarliest", newEarliest)
                .execute()

        // 2) Unreachable data files (cascades stats + partition values).
        val dataFilesQueued =
            h.createUpdate(
                """
            WITH doomed AS (
                DELETE FROM hog_data_file
                WHERE catalog_id = :catalogId
                  AND end_snapshot IS NOT NULL AND end_snapshot <= :newEarliest
                RETURNING path
            )
            INSERT INTO hog_file_removal (catalog_id, path, file_kind, reason)
            SELECT :catalogId, path, 'data', 'snapshot_expiry' FROM doomed
            """,
            )
                .bind("catalogId", cat.catalogId)
                .bind("newEarliest", newEarliest)
                .execute()

        // 3) The snapshots themselves (cascades hog_snapshot_change).
        val snapshotsExpired =
            h.createUpdate(
                """
            DELETE FROM hog_snapshot
            WHERE catalog_id = :catalogId
              AND snapshot_id >= :earliest AND snapshot_id < :newEarliest
            """,
            )
                .bind("catalogId", cat.catalogId)
                .bind("earliest", cat.earliestSnapshotId)
                .bind("newEarliest", newEarliest)
                .execute()

        // 4) Advance the floor, capturing the new floor snapshot's time in
        // the SAME update: that snapshot survives this sweep (only ids
        // BELOW newEarliest were deleted) but a later sweep will kill it,
        // and 410 reconciliation needs "the floor was reached at T" after
        // the times below it are gone.
        h.createUpdate(
            """
            UPDATE hog_catalog
               SET earliest_snapshot_id = :newEarliest,
                   earliest_snapshot_time =
                       (SELECT snapshot_time FROM hog_snapshot
                         WHERE catalog_id = :catalogId AND snapshot_id = :newEarliest)
             WHERE catalog_id = :catalogId
            """,
        )
            .bind("newEarliest", newEarliest)
            .bind("catalogId", cat.catalogId)
            .execute()

        // 5) Versioned-row retention for the accumulating DDL tables.
        // Correctness: a versioned row with end_snapshot = E is visible at
        // S iff S < E (visibility rule: begin <= S AND (end IS NULL OR
        // S < end)); every retained snapshot satisfies S >= newEarliest;
        // so E <= newEarliest means NO retained snapshot can see the row —
        // it is unreachable by any valid read (head reads see only
        // end IS NULL rows) and can be deleted outright. Live rows
        // (end IS NULL) and rows ending above the floor are untouched, so
        // time travel at every S >= newEarliest is unchanged. Child tables
        // (hog_partition_field, hog_sort_field) follow their spec headers
        // via FK ON DELETE CASCADE. Incremental like the steps above: the
        // range is bounded by newEarliest, which batchSize caps per sweep.
        for (table in VERSIONED_RETENTION_TABLES) {
            h.createUpdate(
                """
                DELETE FROM $table
                WHERE catalog_id = :catalogId
                  AND end_snapshot IS NOT NULL AND end_snapshot <= :newEarliest
                """,
            )
                .bind("catalogId", cat.catalogId)
                .bind("newEarliest", newEarliest)
                .execute()
        }

        return ExpiryResult(
            snapshotsExpired = snapshotsExpired.toLong(),
            dataFilesQueued = dataFilesQueued.toLong(),
            deleteFilesQueued = deleteFilesQueued.toLong(),
            newEarliestSnapshotId = newEarliest,
            flooredByConsumer = flooredBy,
        )
    }

    /**
     * One sweep across every catalog, for the background loop
     * (BackgroundLoops in App.kt). Catalogs are isolated: one catalog's
     * failure is logged and the rest proceed.
     */
    fun runOnceAllCatalogs(batchSize: Int): List<Pair<String, ExpiryResult>> {
        val names = jdbi.withHandleUnchecked { h -> CatalogRepo.listAll(h) }.map { it.name }
        val results = mutableListOf<Pair<String, ExpiryResult>>()
        for (name in names) {
            try {
                results += name to runOnce(name, batchSize)
            } catch (e: Exception) {
                log.error(e) { "expiry sweep failed for catalog '$name'; continuing" }
            }
        }
        return results
    }
}
