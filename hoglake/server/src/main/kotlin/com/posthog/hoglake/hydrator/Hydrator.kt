package com.posthog.hoglake.hydrator

import com.fasterxml.jackson.databind.ObjectMapper
import com.posthog.hoglake.model.ColType
import com.posthog.hoglake.observability.Metrics
import dev.hardwood.InputFile
import dev.hardwood.metadata.FileMetaData
import dev.hardwood.reader.ParquetFileReader
import io.github.oshai.kotlinlogging.KotlinLogging
import org.jdbi.v3.core.Handle
import org.jdbi.v3.core.Jdbi
import org.jdbi.v3.core.statement.Update
import java.io.IOException
import java.nio.ByteBuffer
import java.sql.Types
import java.util.concurrent.atomic.AtomicBoolean
import kotlin.concurrent.thread

/**
 * Async stats hydration for deferred-stats registrations (the
 * footer-shipping decision in ../README.md): files committed with
 * `stats_state = 'pending'` get their parquet footers read from the object
 * store, per-column statistics aggregated across row groups, bounds
 * encoded in Iceberg single-value form, and the row flipped to
 * `'provided'` — or `'failed'` when the object is missing/unparseable or
 * the registered record_count does not match the footer.
 *
 * Footer-only: nothing here decodes data pages. When the registration
 * carried `footer_size`, only the object's tail is fetched (ranged GET);
 * otherwise (or if the tail turns out not to contain everything the footer
 * parse needs) the whole object is fetched.
 */
class Hydrator(private val jdbi: Jdbi, private val store: ObjectStore) {
    private val log = KotlinLogging.logger {}
    private val json = ObjectMapper()

    private data class PendingFile(
        val catalogId: Long,
        val dataFileId: Long,
        val tableId: Long,
        val path: String,
        val recordCount: Long,
        val fileSizeBytes: Long,
        val footerSize: Long?,
    )

    /** One sweep: hydrate up to [limit] pending files. Returns files processed. */
    fun runOnce(limit: Int = 100): Int {
        val pending =
            jdbi.withHandle<List<PendingFile>, Exception> { h ->
                h.createQuery(
                    """
                SELECT catalog_id, data_file_id, table_id, path, record_count,
                       file_size_bytes, footer_size
                FROM hog_data_file
                WHERE stats_state = 'pending'
                ORDER BY catalog_id, data_file_id
                LIMIT :limit
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
                        )
                    }
                    .list()
            }
        for (file in pending) {
            try {
                hydrate(file)
            } catch (e: Exception) {
                // One bad file must not wedge the sweep.
                log.error(e) {
                    "hydration failed for file ${file.dataFileId} (${file.path}); marking failed"
                }
                markFailed(file)
            }
        }
        return pending.size
    }

    /**
     * Background sweep loop on a daemon thread. [intervalMs] <= 0 returns a
     * no-op handle (tests drive [runOnce] directly).
     */
    fun startLoop(intervalMs: Long): AutoCloseable {
        if (intervalMs <= 0) return AutoCloseable { }
        val running = AtomicBoolean(true)
        val worker =
            thread(name = "hoglake-hydrator", isDaemon = true) {
                while (running.get()) {
                    try {
                        runOnce()
                    } catch (e: Exception) {
                        log.error(e) { "hydrator sweep failed" }
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

    private fun hydrate(file: PendingFile) {
        val columns = liveColumns(file)
        val meta = readFooter(file)
        if (meta.numRows() != file.recordCount) {
            log.error {
                "REGISTRATION MISMATCH for file ${file.dataFileId} (${file.path}): " +
                    "parquet footer has ${meta.numRows()} rows but hog_data_file.record_count " +
                    "is ${file.recordCount}; marking failed, writing no stats"
            }
            markFailed(file)
            return
        }
        val aggs = FooterStats.aggregate(meta, columns, file.path)
        jdbi.useTransaction<Exception> { h ->
            for (agg in aggs) upsertStats(h, file, agg)
            val flipped =
                h.createUpdate(
                    """
                UPDATE hog_data_file SET stats_state = 'provided'
                WHERE catalog_id = :catalogId AND data_file_id = :dataFileId
                  AND stats_state = 'pending'
                """,
                )
                    .bind("catalogId", file.catalogId)
                    .bind("dataFileId", file.dataFileId)
                    .execute()
            if (flipped == 0) {
                log.warn {
                    "file ${file.dataFileId} left 'pending' concurrently; stats upserted anyway"
                }
            }
        }
        Metrics.statsHydrated("provided")
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

    private fun markFailed(file: PendingFile) {
        Metrics.statsHydrated("failed")
        try {
            jdbi.useHandle<Exception> { h ->
                h.createUpdate(
                    """
                    UPDATE hog_data_file SET stats_state = 'failed'
                    WHERE catalog_id = :catalogId AND data_file_id = :dataFileId
                      AND stats_state = 'pending'
                    """,
                )
                    .bind("catalogId", file.catalogId)
                    .bind("dataFileId", file.dataFileId)
                    .execute()
            }
        } catch (e: Exception) {
            log.error(e) { "could not mark file ${file.dataFileId} failed" }
        }
    }

    private fun liveColumns(file: PendingFile): List<CatalogColumn> =
        jdbi.withHandle<List<CatalogColumn>, Exception> { h ->
            h.createQuery(
                """
                SELECT field_id, name, col_type, type_params::text AS type_params
                FROM hog_column
                WHERE catalog_id = :catalogId AND table_id = :tableId
                  AND end_snapshot IS NULL
                ORDER BY ordinal
                """,
            )
                .bind("catalogId", file.catalogId)
                .bind("tableId", file.tableId)
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
        }

    // ---- footer fetch ------------------------------------------------------

    private fun readFooter(file: PendingFile): FileMetaData {
        val footerSize = file.footerSize
        if (footerSize != null && footerSize > 0 && footerSize + FOOTER_SUFFIX < file.fileSizeBytes) {
            try {
                return parseFooter(tailInput(file, footerSize))
            } catch (e: Exception) {
                log.debug(e) {
                    "tail read of ${file.path} (footer_size=$footerSize) insufficient; " +
                        "falling back to whole-object GET"
                }
            }
        }
        val bytes = store.get(file.path)
        return parseFooter(InputFile.of(ByteBuffer.wrap(bytes)))
    }

    private fun parseFooter(input: InputFile): FileMetaData = ParquetFileReader.open(input).use { it.fileMetaData }

    private fun tailInput(
        file: PendingFile,
        footerSize: Long,
    ): InputFile {
        val tailStart = file.fileSizeBytes - footerSize - FOOTER_SUFFIX
        val tail = store.getTail(file.path, tailStart)
        // Hardwood validates the leading "PAR1" magic at open, so fetch the
        // real prefix too (a second tiny ranged GET, never synthesized).
        val prefix = store.getPrefix(file.path, MAGIC_LENGTH)
        return TailInputFile(file.fileSizeBytes, prefix, tailStart, tail, file.path)
    }

    /**
     * An [InputFile] over a cached object prefix + tail: serves
     * absolute-offset range reads that fall entirely inside either cached
     * region, and refuses everything else so the caller can fall back to a
     * whole-object read.
     */
    private class TailInputFile(
        private val totalLength: Long,
        private val prefix: ByteArray,
        private val tailStart: Long,
        private val tail: ByteArray,
        private val uri: String,
    ) : InputFile {
        override fun open() {}

        override fun length(): Long = totalLength

        override fun name(): String = uri

        override fun close() {}

        override fun readRange(
            offset: Long,
            length: Int,
        ): ByteBuffer {
            if (offset >= 0 && offset + length <= prefix.size) {
                return ByteBuffer.wrap(prefix, Math.toIntExact(offset), length).slice()
            }
            if (offset >= tailStart && offset + length <= tailStart + tail.size) {
                return ByteBuffer.wrap(tail, Math.toIntExact(offset - tailStart), length).slice()
            }
            throw IOException(
                "range [$offset, +$length) outside cached prefix [0, ${prefix.size}) " +
                    "and tail [$tailStart, ${tailStart + tail.size}) of $uri",
            )
        }
    }

    private companion object {
        /** 4-byte footer length + 4-byte "PAR1" magic at the end of the file. */
        const val FOOTER_SUFFIX = 8L

        /** Leading "PAR1" magic. */
        const val MAGIC_LENGTH = 4
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
