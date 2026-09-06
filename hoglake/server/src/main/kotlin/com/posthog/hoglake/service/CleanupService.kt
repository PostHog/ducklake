package com.posthog.hoglake.service

import com.posthog.hoglake.Config
import com.posthog.hoglake.hydrator.ObjectStore
import com.posthog.hoglake.model.CleanupResult
import com.posthog.hoglake.model.HoglakeException
import com.posthog.hoglake.observability.Audit
import com.posthog.hoglake.observability.Metrics
import com.posthog.hoglake.persistence.CatalogRepo
import io.github.oshai.kotlinlogging.KotlinLogging
import org.jdbi.v3.core.Jdbi
import org.jdbi.v3.core.kotlin.useHandleUnchecked
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
import java.util.concurrent.atomic.AtomicBoolean
import kotlin.concurrent.thread

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
 * and its queue row LEFT IN PLACE: the reference may legitimately go
 * away later (v1 accepts that a permanently-referenced entry is
 * re-checked every run — bounded by batch order, never a deletion).
 *
 * Deletes run in sub-batches of [subBatchSize] (500 in production;
 * constructor-tunable for tests). Each sub-batch deletes its objects
 * and then drains their queue rows in one transaction of its own, so a
 * later sub-batch failure never rolls back completed ones. A
 * missing object (404) is success ("already gone") and drains its row;
 * a per-object delete failure is logged and leaves its row queued for
 * the next run without wedging the rest of the batch.
 */
class CleanupService(
    private val jdbi: Jdbi,
    private val store: RemovalStore,
    private val subBatchSize: Int = SUB_BATCH,
) {
    private val log = KotlinLogging.logger {}

    init {
        require(subBatchSize > 0) { "subBatchSize must be positive (got $subBatchSize)" }
    }

    private data class Entry(val removalId: Long, val path: String)

    /**
     * Drain up to [batchSize] queue entries for [catalog]. The audit
     * event and the files-removed counter are emitted at the end of the
     * run (each sub-batch's queue drain is its own transaction; nothing
     * is emitted inside one). still_referenced > 0 is an invariant
     * violation and flags the run's audit outcome accordingly.
     */
    fun runOnce(
        catalog: String,
        batchSize: Int,
    ): CleanupResult =
        Audit.audited(
            "cleanup",
            catalog,
            null,
            successOutcome = { r -> if (r.stillReferenced > 0) "invariant_violation" else "ok" },
            detail = { r ->
                "removed=${r.removed} missing=${r.missing} still_referenced=${r.stillReferenced}"
            },
        ) {
            val result = doRunOnce(catalog, batchSize)
            Metrics.filesRemoved(catalog, result.removed)
            result
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
                LifecycleCatalog.require(h, catalog).catalogId
            }
        val batch =
            jdbi.withHandleUnchecked { h ->
                h.createQuery(
                    """
                SELECT removal_id, path FROM hog_file_removal
                WHERE catalog_id = :catalogId
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
        for (sub in batch.chunked(subBatchSize)) {
            // Liveness is checked per sub-batch at drain time, not when
            // the batch was selected: the freshest answer we can get
            // before touching the object.
            val referenced = referencedPaths(catalogId, sub.map { it.path })
            val drained = mutableListOf<Long>()
            for (entry in sub) {
                if (entry.path in referenced) {
                    log.error {
                        "cleanup: path '${entry.path}' (removal_id ${entry.removalId}) is " +
                            "still referenced by the catalog — invariant violation; skipping"
                    }
                    stillReferenced++
                    continue
                }
                try {
                    when (store.deleteIfExists(entry.path)) {
                        RemovalStore.Outcome.REMOVED -> removed++
                        RemovalStore.Outcome.MISSING -> {
                            log.info { "cleanup: '${entry.path}' already gone; draining queue row" }
                            missing++
                        }
                    }
                    drained += entry.removalId
                } catch (e: Exception) {
                    // Leave the row queued; the next run retries it.
                    log.error(e) {
                        "cleanup: delete failed for '${entry.path}' " +
                            "(removal_id ${entry.removalId}); leaving queued"
                    }
                }
            }
            if (drained.isNotEmpty()) {
                jdbi.useHandleUnchecked { h ->
                    h.createUpdate(
                        """
                        DELETE FROM hog_file_removal
                        WHERE catalog_id = :catalogId AND removal_id = ANY(:ids)
                        """,
                    )
                        .bind("catalogId", catalogId)
                        .bindArray("ids", Long::class.javaObjectType, drained)
                        .execute()
                }
            }
        }
        return CleanupResult(removed, missing, stillReferenced)
    }

    /** Paths from [paths] that any file row (live or not) still claims. */
    private fun referencedPaths(
        catalogId: Long,
        paths: List<String>,
    ): Set<String> =
        jdbi.withHandleUnchecked { h ->
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
        }

    /**
     * One drain across every catalog, for the background loop. Catalogs
     * are isolated: one catalog's failure is logged and the rest proceed.
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

    /**
     * Background drain loop on a daemon thread (Hydrator.startLoop
     * pattern), draining all catalogs. [intervalMs] <= 0 is a no-op.
     */
    fun startLoop(
        intervalMs: Long,
        batchSize: Int,
    ): AutoCloseable {
        if (intervalMs <= 0) return AutoCloseable { }
        val running = AtomicBoolean(true)
        val worker =
            thread(name = "hoglake-cleanup", isDaemon = true) {
                while (running.get()) {
                    try {
                        runOnceAllCatalogs(batchSize)
                    } catch (e: Exception) {
                        log.error(e) { "cleanup drain failed" }
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

    private companion object {
        /** Production sub-batch size for physical deletes. */
        const val SUB_BATCH = 500
    }
}
