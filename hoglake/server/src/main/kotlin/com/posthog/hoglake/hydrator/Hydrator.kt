package com.posthog.hoglake.hydrator

import com.fasterxml.jackson.databind.ObjectMapper
import com.posthog.hoglake.model.ColType
import com.posthog.hoglake.model.HoglakeException
import com.posthog.hoglake.model.RehydrateResult
import com.posthog.hoglake.observability.Audit
import com.posthog.hoglake.observability.Metrics
import com.posthog.hoglake.persistence.CatalogRepo
import com.posthog.hoglake.persistence.NamespaceRepo
import com.posthog.hoglake.persistence.TableRepo
import io.github.oshai.kotlinlogging.KotlinLogging
import org.apache.parquet.hadoop.ParquetFileReader
import org.apache.parquet.hadoop.metadata.ParquetMetadata
import org.apache.parquet.io.InputFile
import org.apache.parquet.io.SeekableInputStream
import org.jdbi.v3.core.Handle
import org.jdbi.v3.core.Jdbi
import org.jdbi.v3.core.kotlin.inTransactionUnchecked
import org.jdbi.v3.core.statement.Update
import software.amazon.awssdk.services.s3.model.NoSuchKeyException
import software.amazon.awssdk.services.s3.model.S3Exception
import java.io.EOFException
import java.io.IOException
import java.nio.ByteBuffer
import java.sql.Types

/**
 * Async stats hydration for deferred-stats registrations (the
 * footer-shipping decision in ../README.md): files committed with
 * `stats_state = 'pending'` get their parquet footers read from the object
 * store (parquet-java — the project's one parquet library), per-column
 * statistics aggregated across row groups, bounds encoded in Iceberg
 * single-value form, and the row flipped to `'provided'` — or `'failed'`
 * when the object is missing/unparseable or the registered record_count
 * does not match the footer.
 *
 * The footer read doubles as the field-id contract check: any primitive
 * leaf without a `PARQUET:field_id` flags the row
 * (`hog_data_file.missing_field_ids`) — such files bind columns by name,
 * so AlterService refuses column renames while one is live, and
 * CatalogMetrics gauges the flagged population
 * (`hoglake_missing_field_id_files`).
 *
 * Footer-only: nothing here decodes data pages. When the registration
 * carried `footer_size`, only the object's tail is fetched (ranged GET);
 * otherwise (or if the tail turns out not to contain everything the footer
 * parse needs) the whole object is fetched — capped at
 * [maxWholeObjectBytes] (HOGLAKE_HYDRATOR_MAX_WHOLE_OBJECT_BYTES): a
 * larger file without a usable footer_size fails structurally instead of
 * buffering unbounded bytes on the heap.
 *
 * Failure taxonomy: TRANSIENT object-store failures (throttle/5xx,
 * connection, timeout — anything but a definitive 404/NoSuchKey) leave
 * the file 'pending' (logged + `hoglake_hydrator_transient_errors_total`)
 * for the next sweep; STRUCTURAL ones (object missing, unparseable
 * footer, registration mismatch, over-cap) mark it 'failed'. 'failed' is
 * terminal to the sweep but operator-recoverable: [rehydrateFailed]
 * (POST /maintenance/rehydrate) flips failed rows back to pending.
 *
 * Work-claiming: one sweep is ONE transaction whose claim query uses
 * FOR UPDATE SKIP LOCKED, so concurrent replicas hydrate DISJOINT sets
 * instead of racing the same pending head through S3 (the guarded flip
 * kept that correct but paid N× the GETs). The row locks are held across
 * the footer fetches — acceptable for a background sweep bounded by
 * [runOnce]'s limit; foreground paths never lock pending rows.
 */
class Hydrator(
    private val jdbi: Jdbi,
    private val store: ObjectStore,
    /** Whole-object fallback cap; see class KDoc. */
    private val maxWholeObjectBytes: Long = DEFAULT_MAX_WHOLE_OBJECT_BYTES,
) {
    private val log = KotlinLogging.logger {}
    private val json = ObjectMapper()

    internal data class PendingFile(
        val catalogId: Long,
        val dataFileId: Long,
        val tableId: Long,
        val path: String,
        val recordCount: Long,
        val fileSizeBytes: Long,
        val footerSize: Long?,
        val beginSnapshot: Long,
    )

    /** A retryable object-store failure: the file must STAY pending. */
    private class TransientFetchException(message: String, cause: Throwable) : RuntimeException(message, cause)

    /**
     * The sweep's claim query: pending rows in (catalog_id, data_file_id)
     * order, locked FOR UPDATE with SKIP LOCKED so a concurrent replica's
     * sweep claims a disjoint set. Must run inside the sweep transaction
     * (the locks ARE the claim).
     */
    internal fun claimPending(
        h: Handle,
        limit: Int,
    ): List<PendingFile> =
        h.createQuery(
            """
            SELECT catalog_id, data_file_id, table_id, path, record_count,
                   file_size_bytes, footer_size, begin_snapshot
            FROM hog_data_file
            WHERE stats_state = 'pending'
            ORDER BY catalog_id, data_file_id
            LIMIT :limit
            FOR UPDATE SKIP LOCKED
            """,
        )
            .bind("limit", limit)
            .map { rs, _ ->
                PendingFile(
                    catalogId = rs.getLong("catalog_id"),
                    dataFileId = rs.getLong("data_file_id"),
                    tableId = rs.getLong("table_id"),
                    path = rs.getString("path"),
                    recordCount = rs.getLong("record_count"),
                    fileSizeBytes = rs.getLong("file_size_bytes"),
                    footerSize = rs.getObject("footer_size", java.lang.Long::class.java)?.toLong(),
                    beginSnapshot = rs.getLong("begin_snapshot"),
                )
            }
            .list()

    /**
     * One sweep: hydrate up to [limit] pending files. Returns files
     * processed (transient failures included — they were claimed and
     * attempted). Each file's writes ride a savepoint so one file's DB
     * failure never poisons the sweep transaction for the rest.
     */
    fun runOnce(limit: Int = 100): Int =
        jdbi.inTransaction<Int, Exception> { h ->
            val pending = claimPending(h, limit)
            for (file in pending) {
                h.savepoint(FILE_SAVEPOINT)
                try {
                    hydrate(h, file)
                } catch (e: TransientFetchException) {
                    h.rollbackToSavepoint(FILE_SAVEPOINT)
                    // Transient: the file STAYS pending; the next sweep
                    // retries it. The counter is the throttle-storm trace.
                    Metrics.hydratorTransientError()
                    log.warn(e) {
                        "transient object-store failure hydrating file ${file.dataFileId} " +
                            "(${file.path}); leaving pending for the next sweep"
                    }
                } catch (e: Exception) {
                    h.rollbackToSavepoint(FILE_SAVEPOINT)
                    // Structural: one bad file must not wedge the sweep.
                    log.error(e) {
                        "hydration failed for file ${file.dataFileId} (${file.path}); marking failed"
                    }
                    markFailed(h, file)
                }
            }
            pending.size
        }

    /**
     * Operator requeue (POST /v1/catalogs/{c}/maintenance/rehydrate):
     * flip the catalog's 'failed' files back to 'pending' — optionally
     * scoped to one table — so the sweep retries them. The recovery path
     * for structural failures whose cause was fixed (object re-uploaded,
     * registration corrected, cap raised).
     */
    fun rehydrateFailed(
        catalog: String,
        namespace: String? = null,
        table: String? = null,
    ): RehydrateResult =
        Audit.audited(
            "rehydrate",
            catalog,
            table?.let { "$namespace.$it" },
            detail = { "requeued=${it.requeued}" },
        ) {
            if ((namespace == null) != (table == null)) {
                throw HoglakeException.Validation(
                    "namespace and table must be supplied together (or neither, for the whole catalog)",
                )
            }
            jdbi.inTransactionUnchecked { h ->
                val cat = CatalogRepo.require(h, catalog)
                val tableId =
                    if (namespace != null && table != null) {
                        val ns =
                            NamespaceRepo.findLiveByName(h, cat.catalogId, namespace)
                                ?: throw HoglakeException.NotFound(
                                    "namespace '$namespace' in catalog '$catalog'",
                                )
                        val t =
                            TableRepo.findLive(h, cat.catalogId, ns.namespaceId, table)
                                ?: throw HoglakeException.NotFound(
                                    "table '$namespace.$table' in catalog '$catalog'",
                                )
                        t.tableId
                    } else {
                        null
                    }
                val requeued =
                    h.createUpdate(
                        """
                        UPDATE hog_data_file
                           SET stats_state = 'pending'
                        WHERE catalog_id = :catalogId AND stats_state = 'failed'
                          AND (:tableId::bigint IS NULL OR table_id = :tableId)
                        """,
                    )
                        .bind("catalogId", cat.catalogId)
                        .apply {
                            if (tableId == null) bindNull("tableId", Types.BIGINT) else bind("tableId", tableId)
                        }
                        .execute()
                RehydrateResult(requeued.toLong())
            }
        }

    private fun hydrate(
        h: Handle,
        file: PendingFile,
    ) {
        val footer = readFooter(file)
        // The field-id contract check rides the footer we already hold.
        val missingFieldIds = FooterStats.missingFieldIds(footer.fileMetaData.schema)
        val footerRows = footer.blocks.sumOf { it.rowCount }
        if (footerRows != file.recordCount) {
            log.error {
                "REGISTRATION MISMATCH for file ${file.dataFileId} (${file.path}): " +
                    "parquet footer has $footerRows rows but hog_data_file.record_count " +
                    "is ${file.recordCount}; marking failed, writing no stats"
            }
            markFailed(h, file, missingFieldIds)
            return
        }
        // Column binding: id-bearing files map by field id, a stable
        // identity — the LIVE column set is correct at any time. Id-less
        // files bind by NAME, so the names must resolve against the schema
        // the file was committed under (visible at its begin_snapshot),
        // never live-at-hydration: a drop+add-same-name between commit and
        // this sweep would otherwise land the old incarnation's stats
        // under the NEW field id, poisoning pruning on the new column.
        val columns =
            if (FooterStats.usesFieldIds(footer.fileMetaData.schema)) {
                catalogColumns(h, file, at = null)
            } else {
                catalogColumns(h, file, at = file.beginSnapshot)
            }
        val aggs = FooterStats.aggregate(footer, columns, file.path)
        for (agg in aggs) upsertStats(h, file, agg)
        val flipped =
            h.createUpdate(
                """
                UPDATE hog_data_file
                   SET stats_state = 'provided', missing_field_ids = :missingFieldIds
                WHERE catalog_id = :catalogId AND data_file_id = :dataFileId
                  AND stats_state = 'pending'
                """,
            )
                .bind("missingFieldIds", missingFieldIds)
                .bind("catalogId", file.catalogId)
                .bind("dataFileId", file.dataFileId)
                .execute()
        if (flipped == 0) {
            log.warn {
                "file ${file.dataFileId} left 'pending' concurrently; stats upserted anyway"
            }
        }
        Metrics.statsHydrated("provided")
        if (missingFieldIds) {
            log.warn {
                "file ${file.dataFileId} (${file.path}) has leaves without parquet field ids; " +
                    "flagged missing_field_ids (column renames on its table are blocked while it is live)"
            }
        }
        log.debug { "hydrated file ${file.dataFileId} (${file.path}): ${aggs.size} column stats" }
    }

    private fun upsertStats(
        h: Handle,
        file: PendingFile,
        agg: FooterStats.ColumnAgg,
    ) {
        h.createUpdate(
            """
            INSERT INTO hog_file_column_stats
                (catalog_id, data_file_id, field_id, value_count, null_count,
                 nan_count, size_bytes, lower_bound, upper_bound)
            VALUES (:catalogId, :dataFileId, :fieldId, :valueCount, :nullCount,
                    :nanCount, :sizeBytes, :lowerBound, :upperBound)
            ON CONFLICT (catalog_id, data_file_id, field_id) DO UPDATE SET
                value_count = EXCLUDED.value_count,
                null_count  = EXCLUDED.null_count,
                nan_count   = EXCLUDED.nan_count,
                size_bytes  = EXCLUDED.size_bytes,
                lower_bound = EXCLUDED.lower_bound,
                upper_bound = EXCLUDED.upper_bound
            """,
        )
            .bind("catalogId", file.catalogId)
            .bind("dataFileId", file.dataFileId)
            .bind("fieldId", agg.fieldId)
            .bind("valueCount", agg.valueCount)
            .bind("nullCount", agg.nullCount)
            .bindNullableLong("nanCount", agg.nanCount)
            .bindNullableLong("sizeBytes", agg.sizeBytes)
            .bindNullableBytes("lowerBound", agg.lowerBound)
            .bindNullableBytes("upperBound", agg.upperBound)
            .execute()
    }

    /**
     * Flip to 'failed'; when the footer WAS parsed (record-count
     * mismatch), [missingFieldIds] still records the contract check.
     * Rides the sweep transaction's handle (after a rollback to the
     * per-file savepoint the transaction is healthy again).
     */
    private fun markFailed(
        h: Handle,
        file: PendingFile,
        missingFieldIds: Boolean? = null,
    ) {
        Metrics.statsHydrated("failed")
        try {
            h.createUpdate(
                """
                UPDATE hog_data_file
                   SET stats_state = 'failed',
                       missing_field_ids = COALESCE(:missingFieldIds, missing_field_ids)
                WHERE catalog_id = :catalogId AND data_file_id = :dataFileId
                  AND stats_state = 'pending'
                """,
            )
                .apply {
                    if (missingFieldIds == null) {
                        bindNull("missingFieldIds", Types.BOOLEAN)
                    } else {
                        bind("missingFieldIds", missingFieldIds)
                    }
                }
                .bind("catalogId", file.catalogId)
                .bind("dataFileId", file.dataFileId)
                .execute()
        } catch (e: Exception) {
            log.error(e) { "could not mark file ${file.dataFileId} failed" }
        }
    }

    /**
     * The table's catalog columns for stats binding: [at] null = the LIVE
     * set (end_snapshot IS NULL — field-id binding); [at] non-null = the
     * set visible at that snapshot (the versioned-row rule — name-fallback
     * binding at the file's begin_snapshot).
     */
    private fun catalogColumns(
        h: Handle,
        file: PendingFile,
        at: Long?,
    ): List<CatalogColumn> =
        h.createQuery(
            if (at == null) {
                """
                SELECT field_id, name, col_type, type_params::text AS type_params
                FROM hog_column
                WHERE catalog_id = :catalogId AND table_id = :tableId
                  AND end_snapshot IS NULL
                ORDER BY ordinal
                """
            } else {
                """
                SELECT field_id, name, col_type, type_params::text AS type_params
                FROM hog_column
                WHERE catalog_id = :catalogId AND table_id = :tableId
                  AND begin_snapshot <= :at
                  AND (end_snapshot IS NULL OR :at < end_snapshot)
                ORDER BY ordinal
                """
            },
        )
            .bind("catalogId", file.catalogId)
            .bind("tableId", file.tableId)
            .apply { if (at != null) bind("at", at) }
            .map { rs, _ ->
                CatalogColumn(
                    fieldId = rs.getLong("field_id"),
                    name = rs.getString("name"),
                    type = ColType.fromWire(rs.getString("col_type")),
                    decimalScale =
                        rs.getString("type_params")?.let { params ->
                            json.readTree(params).get("scale")?.takeIf { it.isInt }?.asInt()
                        },
                )
            }
            .list()

    // ---- footer fetch ------------------------------------------------------

    private fun readFooter(file: PendingFile): ParquetMetadata {
        val footerSize = file.footerSize
        if (footerSize != null && footerSize > 0 && footerSize + FOOTER_SUFFIX < file.fileSizeBytes) {
            try {
                val tailStart = file.fileSizeBytes - footerSize - FOOTER_SUFFIX
                // Fetch failures are classified HERE, not swallowed into the
                // whole-object fallback: a throttled tail GET must surface as
                // transient (stay pending), and a 404 as structural — falling
                // back would turn an S3 blip into a whole-object fetch that
                // can trip the size cap and wrongly fail a good registration.
                val tail =
                    try {
                        store.getTail(file.path, tailStart)
                    } catch (e: Exception) {
                        throw classifyFetchFailure(e, file.path)
                    }
                return parseFooter(
                    RegionInputFile(file.fileSizeBytes, tailStart, tail, file.path),
                )
            } catch (e: TransientFetchException) {
                throw e
            } catch (e: Exception) {
                if (isMissingObject(e)) throw e
                // The tail PARSED wrong (registered footer_size too small,
                // region misses) — the whole-object fallback is for exactly
                // this case.
                log.debug(e) {
                    "tail read of ${file.path} (footer_size=$footerSize) insufficient; " +
                        "falling back to whole-object GET"
                }
            }
        }
        // The whole-object fallback buffers the object on the heap; the cap
        // is the OOM guard. Over-cap without a usable footer_size is
        // STRUCTURAL: retrying cannot shrink the file. file_size_bytes is
        // the registered size — the only pre-GET signal we have.
        if (file.fileSizeBytes > maxWholeObjectBytes) {
            throw IllegalStateException(
                "cannot hydrate ${file.path}: no usable footer_size and file_size_bytes " +
                    "${file.fileSizeBytes} exceeds the whole-object fallback cap " +
                    "$maxWholeObjectBytes bytes (HOGLAKE_HYDRATOR_MAX_WHOLE_OBJECT_BYTES); " +
                    "re-register with a correct footer_size or raise the cap, then rehydrate",
            )
        }
        val bytes =
            try {
                store.get(file.path)
            } catch (e: Exception) {
                throw classifyFetchFailure(e, file.path)
            }
        return parseFooter(RegionInputFile(bytes.size.toLong(), 0, bytes, file.path))
    }

    /**
     * Structural iff the object is definitively absent (NoSuchKey / bare
     * 404); everything else the store can throw — throttle, 5xx, auth
     * hiccups, connection resets, timeouts — is transient: retrying is
     * free (the file just stays pending) and correct once the condition
     * clears, whereas a terminal 'failed' would permanently de-stat every
     * file swept through one throttle storm.
     */
    private fun classifyFetchFailure(
        e: Exception,
        path: String,
    ): Exception =
        if (isMissingObject(e)) {
            e
        } else {
            TransientFetchException("transient object-store failure fetching $path", e)
        }

    private fun isMissingObject(e: Throwable): Boolean =
        generateSequence(e) { it.cause }.any {
            it is NoSuchKeyException || (it is S3Exception && it.statusCode() == 404)
        }

    private fun parseFooter(input: InputFile): ParquetMetadata = ParquetFileReader.open(input).use { it.footer }

    /**
     * A parquet-java [InputFile] over one cached byte region of the
     * object (the footer tail, or the whole object): serves reads that
     * fall entirely inside the region and refuses everything else with
     * [IOException] so the caller can fall back to a whole-object read.
     * The footer parse only touches the tail (footer length + magic,
     * then the thrift footer), so a correct footer_size never leaves
     * the region.
     */
    private class RegionInputFile(
        private val totalLength: Long,
        private val regionStart: Long,
        private val region: ByteArray,
        private val uri: String,
    ) : InputFile {
        override fun getLength(): Long = totalLength

        override fun newStream(): SeekableInputStream = RegionStream()

        private inner class RegionStream : SeekableInputStream() {
            private var pos = 0L

            private fun regionOffset(
                offset: Long,
                len: Int,
            ): Int {
                if (offset < regionStart || offset + len > regionStart + region.size) {
                    throw IOException(
                        "range [$offset, +$len) outside cached region " +
                            "[$regionStart, ${regionStart + region.size}) of $uri",
                    )
                }
                return Math.toIntExact(offset - regionStart)
            }

            override fun getPos(): Long = pos

            override fun seek(newPos: Long) {
                pos = newPos
            }

            override fun read(): Int {
                if (pos >= totalLength) return -1
                val idx = regionOffset(pos, 1)
                pos += 1
                return region[idx].toInt() and 0xFF
            }

            override fun read(
                b: ByteArray,
                off: Int,
                len: Int,
            ): Int {
                if (len == 0) return 0
                if (pos >= totalLength) return -1
                val n = Math.toIntExact(minOf(len.toLong(), totalLength - pos))
                val idx = regionOffset(pos, n)
                System.arraycopy(region, idx, b, off, n)
                pos += n
                return n
            }

            override fun readFully(bytes: ByteArray) = readFully(bytes, 0, bytes.size)

            override fun readFully(
                bytes: ByteArray,
                start: Int,
                len: Int,
            ) {
                if (pos + len > totalLength) throw EOFException("read past end of $uri")
                val idx = regionOffset(pos, len)
                System.arraycopy(region, idx, bytes, start, len)
                pos += len
            }

            override fun read(buf: ByteBuffer): Int {
                val len = buf.remaining()
                if (len == 0) return 0
                if (pos >= totalLength) return -1
                val n = Math.toIntExact(minOf(len.toLong(), totalLength - pos))
                val idx = regionOffset(pos, n)
                buf.put(region, idx, n)
                pos += n
                return n
            }

            override fun readFully(buf: ByteBuffer) {
                val len = buf.remaining()
                if (pos + len > totalLength) throw EOFException("read past end of $uri")
                val idx = regionOffset(pos, len)
                buf.put(region, idx, len)
                pos += len
            }
        }
    }

    companion object {
        /** 4-byte footer length + 4-byte "PAR1" magic at the end of the file. */
        private const val FOOTER_SUFFIX = 8L

        /** Default whole-object fallback cap: 256 MiB. */
        const val DEFAULT_MAX_WHOLE_OBJECT_BYTES: Long = 256L * 1024 * 1024

        /** Per-file savepoint name inside the sweep transaction. */
        private const val FILE_SAVEPOINT = "hoglake_hydrate_file"
    }
}

private fun Update.bindNullableLong(
    name: String,
    value: Long?,
): Update = if (value == null) bindNull(name, Types.BIGINT) else bind(name, value)

private fun Update.bindNullableBytes(
    name: String,
    value: ByteArray?,
): Update = if (value == null) bindNull(name, Types.BINARY) else bind(name, value)
