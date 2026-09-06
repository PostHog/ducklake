package com.posthog.hoglake.hydrator

import com.fasterxml.jackson.databind.ObjectMapper
import com.posthog.hoglake.model.ColType
import com.posthog.hoglake.observability.Metrics
import io.github.oshai.kotlinlogging.KotlinLogging
import org.apache.parquet.hadoop.ParquetFileReader
import org.apache.parquet.hadoop.metadata.ParquetMetadata
import org.apache.parquet.io.InputFile
import org.apache.parquet.io.SeekableInputStream
import org.jdbi.v3.core.Handle
import org.jdbi.v3.core.Jdbi
import org.jdbi.v3.core.statement.Update
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

    private fun hydrate(file: PendingFile) {
        val columns = liveColumns(file)
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
            markFailed(file, missingFieldIds)
            return
        }
        val aggs = FooterStats.aggregate(footer, columns, file.path)
        jdbi.useTransaction<Exception> { h ->
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
     */
    private fun markFailed(
        file: PendingFile,
        missingFieldIds: Boolean? = null,
    ) {
        Metrics.statsHydrated("failed")
        try {
            jdbi.useHandle<Exception> { h ->
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

    private fun readFooter(file: PendingFile): ParquetMetadata {
        val footerSize = file.footerSize
        if (footerSize != null && footerSize > 0 && footerSize + FOOTER_SUFFIX < file.fileSizeBytes) {
            try {
                val tailStart = file.fileSizeBytes - footerSize - FOOTER_SUFFIX
                val tail = store.getTail(file.path, tailStart)
                return parseFooter(
                    RegionInputFile(file.fileSizeBytes, tailStart, tail, file.path),
                )
            } catch (e: Exception) {
                log.debug(e) {
                    "tail read of ${file.path} (footer_size=$footerSize) insufficient; " +
                        "falling back to whole-object GET"
                }
            }
        }
        val bytes = store.get(file.path)
        return parseFooter(RegionInputFile(bytes.size.toLong(), 0, bytes, file.path))
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

    private companion object {
        /** 4-byte footer length + 4-byte "PAR1" magic at the end of the file. */
        const val FOOTER_SUFFIX = 8L
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
