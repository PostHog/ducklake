package com.posthog.hoglake.service

import com.posthog.hoglake.Config
import com.posthog.hoglake.hydrator.ObjectStore
import com.posthog.hoglake.model.CleanupResult
import com.posthog.hoglake.model.HoglakeException
import com.posthog.hoglake.observability.Audit
import com.posthog.hoglake.observability.Metrics
import com.posthog.hoglake.persistence.CatalogRepo
import com.posthog.hoglake.persistence.Locks
import io.github.oshai.kotlinlogging.KotlinLogging
import org.jdbi.v3.core.Handle
import org.jdbi.v3.core.Jdbi
import org.jdbi.v3.core.kotlin.useTransactionUnchecked
import org.jdbi.v3.core.kotlin.withHandleUnchecked
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.s3.S3Client
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest
import software.amazon.awssdk.services.s3.model.HeadObjectRequest
import software.amazon.awssdk.services.s3.model.NoSuchKeyException
import software.amazon.awssdk.services.s3.model.S3Exception
import java.net.URI

/**
 * Physical-delete side of the file-removal queue [ObjectStore] cannot
 * provide (it is read/put-only and off-limits to this layer): existence
 * probe + delete, speaking the same `s3://bucket/key` URIs via
 * [ObjectStore.parse]. Same construction surface as ObjectStore so
 * App.kt wires it identically (`RemovalStore(cfg)`).
 */
class RemovalStore(
    endpoint: String?,
    region: String,
    accessKey: String?,
    secretKey: String?,
    pathStyle: Boolean,
) : AutoCloseable {
    constructor(config: Config) : this(
        endpoint = config.s3Endpoint.ifBlank { null },
        region = config.s3Region,
        accessKey = config.s3AccessKey.ifBlank { null },
        secretKey = config.s3SecretKey.ifBlank { null },
        pathStyle = config.s3PathStyle,
    )

    private val s3: S3Client =
        S3Client.builder()
            .region(Region.of(region))
            .apply {
                if (endpoint != null) endpointOverride(URI.create(endpoint))
                if (accessKey != null && secretKey != null) {
                    credentialsProvider(
                        StaticCredentialsProvider.create(AwsBasicCredentials.create(accessKey, secretKey)),
                    )
                }
            }
            .forcePathStyle(pathStyle)
            .build()

    enum class Outcome { REMOVED, MISSING }

    fun exists(pathUri: String): Boolean {
        val loc = ObjectStore.parse(pathUri)
        return try {
            s3.headObject(HeadObjectRequest.builder().bucket(loc.bucket).key(loc.key).build())
            true
        } catch (_: NoSuchKeyException) {
            false
        } catch (e: S3Exception) {
            // Some S3 implementations surface HEAD misses as a bare 404
            // (no modeled NoSuchKey error on a body-less response).
            if (e.statusCode() == 404) false else throw e
        }
    }

    /**
     * Delete the object at [pathUri]; an already-absent object is
     * [Outcome.MISSING] (DeleteObject alone is silently idempotent, so
     * the probe is what distinguishes "removed" from "was never there").
     */
    fun deleteIfExists(pathUri: String): Outcome {
        if (!exists(pathUri)) return Outcome.MISSING
        val loc = ObjectStore.parse(pathUri)
        s3.deleteObject(DeleteObjectRequest.builder().bucket(loc.bucket).key(loc.key).build())
        return Outcome.REMOVED
    }

    override fun close() = s3.close()
}

/**
 * Drains hog_file_removal: the physical-deletion half of expiry/GC,
 * decoupled from metadata deletion (README.md §8).
 *
 * The non-negotiable invariant: a queue entry is a suggestion, never an
 * authorization. At drain time every path is re-checked against
 * hog_data_file AND hog_delete_file (any row, live or historical, in
 * the entry's catalog); a still-referenced path is counted as an
 * invariant violation (`still_referenced` — alert-worthy), skipped,
 * and its queue row LEFT UNDRAINED with attempts/last_attempt_at
 * bumped: the reference may legitimately go away later (v1 accepts
 * that a permanently-referenced entry is re-checked every run —
 * bounded by batch order, never a deletion).
 *
 * Draining SOFT-deletes (the forensics lesson: "we reconstructed
 * split-brain forensics from S3 delete markers"): a settled entry is
 * marked drained_at + drained_outcome ('deleted' for a physical
 * removal, 'absent' for verified-already-gone) instead of losing its
 * row, so what cleanup touched, when, and after how many attempts
 * stays queryable. The drain query reads only undrained rows (partial
 * index), and every sweep also PURGES drained rows older than
 * [ledgerRetentionSeconds] — the ledger must not itself become the
 * unbounded-accumulation problem it documents.
 *
 * Deletes run in sub-batches of [subBatchSize] (25 in production;
 * constructor-tunable for tests). Each sub-batch is ONE transaction
 * that (1) takes the per-catalog advisory commit lock (Locks.kt — the
 * SAME key as every commit/DDL tail), (2) re-checks references, (3)
 * physically deletes, (4) settles the ledger rows — so a later
 * sub-batch failure never rolls back completed ones. A missing object
 * (404) is success ("already gone") and drains its row as 'absent'; a
 * per-object delete failure bumps attempts and leaves the row queued
 * for the next run without wedging the rest of the batch.
 *
 * WHY the lock (the check-then-delete TOCTOU): without it, a commit
 * transaction could pass ITS removal-queue check, insert a hog_data_file
 * row for a queued path, and be mid-flight (uncommitted, invisible to
 * READ COMMITTED) exactly when this drain computes referencedPaths —
 * the drain would see the path unreferenced, delete the object, and the
 * commit would then land a live row pointing at a deleted object.
 * Holding the catalog commit lock across the check+delete pair
 * serializes the two: either the commit finished first (its rows are
 * visible to the check, path skipped as still-referenced... and its own
 * queue-collision check would have 409'd anyway while the entry was
 * undrained), or the drain finishes first and the commit's collision
 * check runs after the entry settles. No interleaving remains.
 *
 * LOCK-HOLD BOUND: one sub-batch = one reference-check query + at most
 * [subBatchSize] × (HEAD + DELETE) S3 calls + two ledger UPDATEs. At
 * the production sub-batch of 25 and ~50 ms per S3 round trip that is
 * ≈ 2.5 s worst case per sub-batch — well under the 30 s commit
 * admission timeout; commits queue behind a sub-batch, never a full
 * batch. Keep [subBatchSize] small; the lock hold scales linearly in it.
 */
class CleanupService(
    private val jdbi: Jdbi,
    private val store: RemovalStore,
    private val subBatchSize: Int = SUB_BATCH,
    private val ledgerRetentionSeconds: Long = LEDGER_RETENTION_SECONDS,
) {
    private val log = KotlinLogging.logger {}

    init {
        require(subBatchSize > 0) { "subBatchSize must be positive (got $subBatchSize)" }
        require(ledgerRetentionSeconds > 0) {
            "ledgerRetentionSeconds must be positive (got $ledgerRetentionSeconds)"
        }
    }

    private data class Entry(val removalId: Long, val path: String)

    /**
     * Drain up to [batchSize] queue entries for [catalog]. The summary
     * audit event and the files-removed counter are emitted at the end
     * of the run (each sub-batch's queue drain is its own transaction;
     * nothing is emitted inside one); per-path events (file_deleted /
     * cleanup_violation) are emitted as the drain progresses, bounded by
     * the batch size. still_referenced > 0 is an invariant violation and
     * flags the run's audit outcome accordingly. A ZERO-WORK drain
     * (nothing removed, missing, or violated) emits no audit event —
     * app-log debug only, so idle background loops stay out of the
     * audit stream.
     */
    fun runOnce(
        catalog: String,
        batchSize: Int,
    ): CleanupResult {
        val result =
            try {
                doRunOnce(catalog, batchSize)
            } catch (e: Throwable) {
                Audit.event("cleanup", catalog, null, Audit.failureOutcome(e), e.message)
                throw e
            }
        Metrics.filesRemoved(catalog, result.removed)
        if (result.removed == 0L && result.missing == 0L && result.stillReferenced == 0L) {
            log.debug { "cleanup drain for catalog '$catalog': nothing to do" }
        } else {
            Audit.event(
                "cleanup",
                catalog,
                null,
                outcome = if (result.stillReferenced > 0) "invariant_violation" else "ok",
                detail =
                    "removed=${result.removed} missing=${result.missing} " +
                        "still_referenced=${result.stillReferenced}",
            )
        }
        return result
    }

    private fun doRunOnce(
        catalog: String,
        batchSize: Int,
    ): CleanupResult {
        if (batchSize <= 0) {
            throw HoglakeException.Validation("batch size must be positive (got $batchSize)")
        }
        val catalogId =
            jdbi.withHandleUnchecked { h ->
                CatalogRepo.require(h, catalog).catalogId
            }
        val batch =
            jdbi.withHandleUnchecked { h ->
                h.createQuery(
                    """
                SELECT removal_id, path FROM hog_file_removal
                WHERE catalog_id = :catalogId AND drained_at IS NULL
                ORDER BY removal_id
                LIMIT :limit
                """,
                )
                    .bind("catalogId", catalogId)
                    .bind("limit", batchSize)
                    .map { rs, _ -> Entry(rs.getLong("removal_id"), rs.getString("path")) }
                    .list()
            }

        var removed = 0L
        var missing = 0L
        var stillReferenced = 0L

        // Per-path audit events are collected inside the sub-batch
        // transaction and emitted AFTER it commits (invariant 8: audit
        // never rides a transaction — and never rides the catalog lock).
        data class PathEvent(val action: String, val path: String, val outcome: String, val detail: String?)
        for (sub in batch.chunked(subBatchSize)) {
            val events = mutableListOf<PathEvent>()
            jdbi.useTransactionUnchecked { h ->
                // The check+delete pair is serialized against the commit
                // tail by the SAME per-catalog advisory lock every commit
                // takes (see class KDoc for the race and the hold bound):
                // the reference check below can never go stale against an
                // in-flight commit registering one of these paths.
                Locks.acquireCatalogCommitLock(h, catalogId)
                // Liveness is checked per sub-batch at drain time, under
                // the lock: the freshest answer possible before touching
                // the object.
                val referenced = referencedPaths(h, catalogId, sub.map { it.path })
                // Ledger outcomes for this sub-batch: settled entries by
                // outcome, plus the ones that stay queued (attempts bump).
                val drainedByOutcome = mapOf("deleted" to mutableListOf<Long>(), "absent" to mutableListOf())
                val attempted = mutableListOf<Long>()
                for (entry in sub) {
                    if (entry.path in referenced) {
                        log.error {
                            "cleanup: path '${entry.path}' (removal_id ${entry.removalId}) is " +
                                "still referenced by the catalog — invariant violation; skipping"
                        }
                        // Per-path audit trail for the alert-worthy case: WHICH
                        // path the queue wrongly suggested. Bounded by batch size.
                        events +=
                            PathEvent(
                                "cleanup_violation",
                                entry.path,
                                "invariant_violation",
                                "removal_id=${entry.removalId} still referenced; not deleted",
                            )
                        stillReferenced++
                        attempted += entry.removalId
                        continue
                    }
                    try {
                        when (store.deleteIfExists(entry.path)) {
                            RemovalStore.Outcome.REMOVED -> {
                                // Physical deletions are the audit log's whole
                                // point: one event per object actually removed.
                                events += PathEvent("file_deleted", entry.path, "ok", null)
                                removed++
                                drainedByOutcome.getValue("deleted") += entry.removalId
                            }
                            RemovalStore.Outcome.MISSING -> {
                                log.info { "cleanup: '${entry.path}' already gone; draining queue row" }
                                missing++
                                drainedByOutcome.getValue("absent") += entry.removalId
                            }
                        }
                    } catch (e: Exception) {
                        // Leave the row queued (attempts bumped); the next run
                        // retries it.
                        log.error(e) {
                            "cleanup: delete failed for '${entry.path}' " +
                                "(removal_id ${entry.removalId}); leaving queued"
                        }
                        attempted += entry.removalId
                    }
                }
                // Soft-delete the settled entries: the row survives as
                // the queryable ledger of what cleanup did and when.
                for ((outcome, ids) in drainedByOutcome) {
                    if (ids.isEmpty()) continue
                    h.createUpdate(
                        """
                        UPDATE hog_file_removal
                           SET drained_at = now(), drained_outcome = :outcome,
                               last_attempt_at = now()
                         WHERE catalog_id = :catalogId AND removal_id = ANY(:ids)
                        """,
                    )
                        .bind("outcome", outcome)
                        .bind("catalogId", catalogId)
                        .bindArray("ids", Long::class.javaObjectType, ids)
                        .execute()
                }
                // Undrained entries (still-referenced, transient S3
                // failure) record the attempt and stay queued.
                if (attempted.isNotEmpty()) {
                    h.createUpdate(
                        """
                        UPDATE hog_file_removal
                           SET attempts = attempts + 1, last_attempt_at = now()
                         WHERE catalog_id = :catalogId AND removal_id = ANY(:ids)
                        """,
                    )
                        .bind("catalogId", catalogId)
                        .bindArray("ids", Long::class.javaObjectType, attempted)
                        .execute()
                }
            }
            // Sub-batch committed (lock released): emit its audit events.
            for (e in events) {
                Audit.event(e.action, catalog, e.path, outcome = e.outcome, detail = e.detail)
            }
        }
        purgeDrainedLedger(catalog, catalogId)
        return CleanupResult(removed, missing, stillReferenced)
    }

    /**
     * Ledger retention: drained rows older than [ledgerRetentionSeconds]
     * are hard-deleted so the soft-delete ledger cannot itself
     * accumulate without bound (the A1 lesson, applied to the fix for
     * A3). Undrained rows are never touched here.
     */
    private fun purgeDrainedLedger(
        catalog: String,
        catalogId: Long,
    ) {
        val purged =
            jdbi.withHandleUnchecked { h ->
                h.createUpdate(
                    """
                    DELETE FROM hog_file_removal
                    WHERE catalog_id = :catalogId
                      AND drained_at < now() - make_interval(secs => :retention)
                    """,
                )
                    .bind("catalogId", catalogId)
                    .bind("retention", ledgerRetentionSeconds)
                    .execute()
            }
        if (purged > 0) {
            log.debug { "cleanup: purged $purged drained ledger rows for catalog '$catalog'" }
        }
    }

    /**
     * Paths from [paths] that any file row (live or not) still claims.
     * Runs on the sub-batch transaction's handle, under the catalog
     * commit lock, so the answer cannot go stale against a commit.
     */
    private fun referencedPaths(
        h: Handle,
        catalogId: Long,
        paths: List<String>,
    ): Set<String> =
        h.createQuery(
            """
            SELECT path FROM hog_data_file
            WHERE catalog_id = :catalogId AND path = ANY(:paths)
            UNION
            SELECT path FROM hog_delete_file
            WHERE catalog_id = :catalogId AND path = ANY(:paths)
            """,
        )
            .bind("catalogId", catalogId)
            .bindArray("paths", String::class.java, paths)
            .mapTo(String::class.java)
            .list()
            .toSet()

    /**
     * One drain across every catalog, for the background loop
     * (BackgroundLoops in App.kt). Catalogs are isolated: one catalog's
     * failure is logged and the rest proceed.
     */
    fun runOnceAllCatalogs(batchSize: Int): List<Pair<String, CleanupResult>> {
        val names = jdbi.withHandleUnchecked { h -> CatalogRepo.listAll(h) }.map { it.name }
        val results = mutableListOf<Pair<String, CleanupResult>>()
        for (name in names) {
            try {
                results += name to runOnce(name, batchSize)
            } catch (e: Exception) {
                log.error(e) { "cleanup drain failed for catalog '$name'; continuing" }
            }
        }
        return results
    }

    private companion object {
        /**
         * Production sub-batch size for physical deletes. Deliberately
         * small: each sub-batch holds the per-catalog commit lock across
         * its S3 calls (the check-then-delete serialization), so the
         * worst-case foreground commit stall is subBatchSize × one S3
         * HEAD+DELETE round trip (≈ 2.5 s at 25 × ~50 ms/op — see the
         * class KDoc's bound), not the 500-entry convoy the old size
         * would have produced.
         */
        const val SUB_BATCH = 25

        /** Default drained-ledger retention: 30 days (HOGLAKE_REMOVAL_LEDGER_RETENTION_SECONDS). */
        const val LEDGER_RETENTION_SECONDS = 30L * 24 * 60 * 60
    }
}
