package com.posthog.hoglake.service

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
import java.util.concurrent.atomic.AtomicBoolean
import kotlin.concurrent.thread

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
 */
class ExpiryService(private val jdbi: Jdbi) {
    private val log = KotlinLogging.logger {}

    /**
     * One expiry sweep for [catalog], expiring at most [batchSize]
     * snapshots. The audit event and the expired-snapshots counter are
     * emitted here, AFTER the sweep transaction has committed.
     */
    fun runOnce(
        catalog: String,
        batchSize: Int,
    ): ExpiryResult =
        Audit.audited(
            "expiry",
            catalog,
            null,
            detail = { r ->
                "snapshots_expired=${r.snapshotsExpired} data_files_queued=${r.dataFilesQueued} " +
                    "delete_files_queued=${r.deleteFilesQueued} " +
                    "new_earliest=${r.newEarliestSnapshotId}" +
                    (r.flooredByConsumer?.let { " floored_by_consumer=$it" } ?: "")
            },
        ) {
            if (batchSize <= 0) {
                throw HoglakeException.Validation("batch size must be positive (got $batchSize)")
            }
            val result =
                jdbi.inTransactionUnchecked { h ->
                    val pre = LifecycleCatalog.require(h, catalog)
                    Locks.acquireCatalogCommitLock(h, pre.catalogId)
                    // Re-read under the lock: head/options may have moved while
                    // we queued behind a committer.
                    val cat = LifecycleCatalog.require(h, catalog)
                    sweep(h, cat, batchSize)
                }
            Metrics.snapshotsExpired(catalog, result.snapshotsExpired)
            result
        }

    private fun sweep(
        h: Handle,
        cat: LifecycleCatalog,
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
        if (flooredBy != null) {
            log.warn {
                "expiry for catalog '${cat.name}' floored by consumer '$flooredBy' at " +
                    "snapshot ${minOffset!!.second} (time/head/batch would have allowed $unfloored)"
            }
        }
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

        // 4) Advance the floor.
        h.createUpdate(
            "UPDATE hog_catalog SET earliest_snapshot_id = :newEarliest WHERE catalog_id = :catalogId",
        )
            .bind("newEarliest", newEarliest)
            .bind("catalogId", cat.catalogId)
            .execute()

        return ExpiryResult(
            snapshotsExpired = snapshotsExpired.toLong(),
            dataFilesQueued = dataFilesQueued.toLong(),
            deleteFilesQueued = deleteFilesQueued.toLong(),
            newEarliestSnapshotId = newEarliest,
            flooredByConsumer = flooredBy,
        )
    }

    /**
     * One sweep across every catalog, for the background loop. Catalogs
     * are isolated: one catalog's failure is logged and the rest proceed.
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

    /**
     * Background sweep loop on a daemon thread (Hydrator.startLoop
     * pattern). [intervalMs] <= 0 returns a no-op handle.
     */
    fun startLoop(
        intervalMs: Long,
        batchSize: Int,
    ): AutoCloseable {
        if (intervalMs <= 0) return AutoCloseable { }
        val running = AtomicBoolean(true)
        val worker =
            thread(name = "hoglake-expiry", isDaemon = true) {
                while (running.get()) {
                    try {
                        runOnceAllCatalogs(batchSize)
                    } catch (e: Exception) {
                        log.error(e) { "expiry sweep failed" }
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
}
