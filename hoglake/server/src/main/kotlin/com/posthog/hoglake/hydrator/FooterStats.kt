package com.posthog.hoglake.hydrator

import com.posthog.hoglake.model.ColType
import com.posthog.hoglake.stats.IcebergSingleValue
import dev.hardwood.metadata.ConvertedType
import dev.hardwood.metadata.FileMetaData
import dev.hardwood.metadata.LogicalType
import dev.hardwood.metadata.PhysicalType
import dev.hardwood.metadata.SchemaElement
import dev.hardwood.metadata.Statistics
import io.github.oshai.kotlinlogging.KotlinLogging
import java.math.BigInteger
import java.nio.ByteBuffer
import java.nio.ByteOrder

/** A live catalog column (hog_column, end_snapshot IS NULL) as the hydrator sees it. */
data class CatalogColumn(
    val fieldId: Long,
    val name: String,
    val type: ColType,
    /** From type_params for decimal columns; null when absent. */
    val decimalScale: Int?,
)

/**
 * Pure footer-to-stats aggregation: takes a parquet [FileMetaData] (footer
 * only — no data pages) and the table's live catalog columns, and produces
 * per-field aggregates in Iceberg single-value bound encoding.
 *
 * Column mapping prefers the parquet schema's field ids
 * (`PARQUET:field_id`) when the file carries any; otherwise it falls back
 * to name matching (logged as a warning — files written without field ids
 * lose rename-safety).
 *
 * Bounds are only produced when every column chunk contributes reliable
 * statistics (present, not the deprecated pre-TYPE_DEFINED_ORDER min/max,
 * decodable under the catalog type, not NaN). Anything else leaves the
 * bounds NULL — never guessed.
 */
object FooterStats {
    private val log = KotlinLogging.logger {}

    data class ColumnAgg(
        val fieldId: Long,
        val valueCount: Long,
        val nullCount: Long,
        val nanCount: Long?,
        val sizeBytes: Long?,
        val lowerBound: ByteArray?,
        val upperBound: ByteArray?,
    )

    /** A top-level primitive leaf of the parquet schema. */
    private data class Leaf(
        val name: String,
        val fieldId: Int?,
        val physical: PhysicalType?,
        val logical: LogicalType?,
        val converted: ConvertedType?,
        val scale: Int?,
    )

    fun aggregate(
        meta: FileMetaData,
        columns: List<CatalogColumn>,
        filePath: String,
    ): List<ColumnAgg> {
        val leaves = topLevelLeaves(meta.schema())
        val byName = leaves.associateBy { it.name }
        val useFieldIds = leaves.any { it.fieldId != null }
        val byFieldId = leaves.filter { it.fieldId != null }.associateBy { it.fieldId!! }
        if (!useFieldIds && columns.isNotEmpty()) {
            log.warn {
                "parquet schema of $filePath carries no field ids; " +
                    "falling back to column-name matching"
            }
        }

        val out = ArrayList<ColumnAgg>(columns.size)
        for (col in columns) {
            val leaf =
                if (useFieldIds) {
                    byFieldId[Math.toIntExact(col.fieldId)]
                } else {
                    byName[col.name]
                }
            if (leaf == null) {
                log.debug {
                    "column ${col.name} (field ${col.fieldId}) not present in $filePath; no stats"
                }
                continue
            }
            aggregateColumn(meta, col, leaf, filePath)?.let(out::add)
        }
        return out
    }

    private fun aggregateColumn(
        meta: FileMetaData,
        col: CatalogColumn,
        leaf: Leaf,
        filePath: String,
    ): ColumnAgg? {
        var valueCount = 0L
        var nullCount = 0L
        var nullCountKnown = true
        var nanCount = 0L
        var nanCountKnown = true
        var sizeBytes = 0L
        var boundsOk = true
        var min: Any? = null
        var max: Any? = null
        var chunks = 0

        for (rg in meta.rowGroups()) {
            for (chunk in rg.columns()) {
                val md = chunk.metaData() ?: continue
                val path = md.pathInSchema()
                if (path == null || path.elements().size != 1 || path.leafName() != leaf.name) continue
                chunks++
                valueCount += md.numValues()
                sizeBytes += md.totalCompressedSize()
                val st: Statistics? = md.statistics()
                val chunkNulls = st?.nullCount()
                if (chunkNulls == null) nullCountKnown = false else nullCount += chunkNulls
                val chunkNans = st?.nanCount()
                if (chunkNans == null) nanCountKnown = false else nanCount += chunkNans
                if (boundsOk) {
                    val bounds = chunkBounds(col, leaf, st)
                    if (bounds == null) {
                        boundsOk = false
                    } else {
                        val (lo, hi) = bounds
                        if (min == null || compare(col.type, lo, min!!) < 0) min = lo
                        if (max == null || compare(col.type, hi, max!!) > 0) max = hi
                    }
                }
            }
        }

        if (chunks == 0) {
            log.debug { "column ${col.name} has no chunks in $filePath; no stats" }
            return null
        }
        if (!nullCountKnown) {
            // null_count is NOT NULL in hog_file_column_stats; without it we
            // cannot write an honest row for this column.
            log.warn {
                "column ${col.name} in $filePath is missing null counts; skipping its stats row"
            }
            return null
        }
        return ColumnAgg(
            fieldId = col.fieldId,
            valueCount = valueCount,
            nullCount = nullCount,
            nanCount = if (nanCountKnown) nanCount else null,
            sizeBytes = sizeBytes,
            lowerBound = if (boundsOk && min != null) encodeBound(col.type, min!!) else null,
            upperBound = if (boundsOk && max != null) encodeBound(col.type, max!!) else null,
        )
    }

    /** Decoded (lower, upper) for one chunk, or null when unreliable. */
    private fun chunkBounds(
        col: CatalogColumn,
        leaf: Leaf,
        st: Statistics?,
    ): Pair<Any, Any>? {
        if (st == null || st.isMinMaxDeprecated) return null
        val rawMin = st.minValue() ?: return null
        val rawMax = st.maxValue() ?: return null
        val lo = decode(col, leaf, rawMin, upper = false) ?: return null
        val hi = decode(col, leaf, rawMax, upper = true) ?: return null
        if (isNan(lo) || isNan(hi)) return null
        return lo to hi
    }

    private fun isNan(v: Any): Boolean = (v is Float && v.isNaN()) || (v is Double && v.isNaN())

    /**
     * Decode one parquet statistics value into the typed representation for
     * the catalog column type, or null when the physical/logical shape does
     * not decode safely under that type.
     */
    private fun decode(
        col: CatalogColumn,
        leaf: Leaf,
        raw: ByteArray,
        upper: Boolean,
    ): Any? =
        when (col.type) {
            ColType.BOOLEAN ->
                if (leaf.physical == PhysicalType.BOOLEAN && raw.size == 1) raw[0] != 0.toByte() else null
            ColType.INT ->
                if (leaf.physical == PhysicalType.INT32) readIntLE(raw) else null
            ColType.LONG ->
                when (leaf.physical) {
                    PhysicalType.INT64 -> readLongLE(raw)
                    PhysicalType.INT32 -> readIntLE(raw)?.toLong()
                    else -> null
                }
            ColType.FLOAT ->
                if (leaf.physical == PhysicalType.FLOAT) readIntLE(raw)?.let { Float.fromBits(it) } else null
            ColType.DOUBLE ->
                when (leaf.physical) {
                    PhysicalType.DOUBLE -> readLongLE(raw)?.let { Double.fromBits(it) }
                    PhysicalType.FLOAT -> readIntLE(raw)?.let { Float.fromBits(it).toDouble() }
                    else -> null
                }
            ColType.DATE ->
                if (leaf.physical == PhysicalType.INT32) readIntLE(raw) else null
            ColType.TIME -> decodeTime(leaf, raw)
            ColType.TIMESTAMP, ColType.TIMESTAMPTZ -> decodeTimestamp(leaf, raw, upper)
            ColType.STRING ->
                if (leaf.physical == PhysicalType.BYTE_ARRAY) raw else null
            ColType.UUID_T ->
                if (leaf.physical == PhysicalType.FIXED_LEN_BYTE_ARRAY && raw.size == 16) raw else null
            ColType.BINARY ->
                when (leaf.physical) {
                    PhysicalType.BYTE_ARRAY, PhysicalType.FIXED_LEN_BYTE_ARRAY -> raw
                    else -> null
                }
            ColType.DECIMAL -> decodeDecimal(col, leaf, raw)
        }

    private fun decodeTime(
        leaf: Leaf,
        raw: ByteArray,
    ): Long? {
        val unit =
            (leaf.logical as? LogicalType.TimeType)?.unit()
                ?: when (leaf.converted) {
                    ConvertedType.TIME_MICROS -> LogicalType.TimeUnit.MICROS
                    ConvertedType.TIME_MILLIS -> LogicalType.TimeUnit.MILLIS
                    else -> null
                } ?: return null
        return when (unit) {
            LogicalType.TimeUnit.MICROS ->
                if (leaf.physical == PhysicalType.INT64) readLongLE(raw) else null
            LogicalType.TimeUnit.MILLIS ->
                if (leaf.physical == PhysicalType.INT32) readIntLE(raw)?.let { it * 1_000L } else null
            LogicalType.TimeUnit.NANOS -> null // sub-micro truncation of a time bound: skip
        }
    }

    private fun decodeTimestamp(
        leaf: Leaf,
        raw: ByteArray,
        upper: Boolean,
    ): Long? {
        if (leaf.physical != PhysicalType.INT64) return null
        val unit =
            (leaf.logical as? LogicalType.TimestampType)?.unit()
                ?: when (leaf.converted) {
                    ConvertedType.TIMESTAMP_MICROS -> LogicalType.TimeUnit.MICROS
                    ConvertedType.TIMESTAMP_MILLIS -> LogicalType.TimeUnit.MILLIS
                    else -> null
                } ?: return null
        val v = readLongLE(raw) ?: return null
        return when (unit) {
            LogicalType.TimeUnit.MICROS -> v
            LogicalType.TimeUnit.MILLIS -> Math.multiplyExact(v, 1_000L)
            // Nanos truncate: floor for the lower bound, ceil for the upper,
            // so the bound stays valid for the true values.
            LogicalType.TimeUnit.NANOS ->
                if (upper) Math.floorDiv(Math.addExact(v, 999L), 1_000L) else Math.floorDiv(v, 1_000L)
        }
    }

    private fun decodeDecimal(
        col: CatalogColumn,
        leaf: Leaf,
        raw: ByteArray,
    ): BigInteger? {
        val parquetScale = (leaf.logical as? LogicalType.DecimalType)?.scale() ?: leaf.scale ?: return null
        val catalogScale = col.decimalScale ?: return null
        if (parquetScale != catalogScale) {
            log.warn {
                "decimal scale mismatch for ${col.name}: parquet=$parquetScale catalog=$catalogScale; skipping bounds"
            }
            return null
        }
        return when (leaf.physical) {
            PhysicalType.INT32 -> readIntLE(raw)?.let { BigInteger.valueOf(it.toLong()) }
            PhysicalType.INT64 -> readLongLE(raw)?.let { BigInteger.valueOf(it) }
            PhysicalType.BYTE_ARRAY, PhysicalType.FIXED_LEN_BYTE_ARRAY ->
                if (raw.isEmpty()) null else BigInteger(raw)
            else -> null
        }
    }

    private fun encodeBound(
        type: ColType,
        v: Any,
    ): ByteArray =
        when (type) {
            ColType.STRING, ColType.UUID_T, ColType.BINARY -> (v as ByteArray).copyOf()
            else -> IcebergSingleValue.encode(type, v)
        }

    @Suppress("UNCHECKED_CAST")
    private fun compare(
        type: ColType,
        a: Any,
        b: Any,
    ): Int =
        when (type) {
            ColType.STRING, ColType.UUID_T, ColType.BINARY ->
                java.util.Arrays.compareUnsigned(a as ByteArray, b as ByteArray)
            else -> (a as Comparable<Any>).compareTo(b)
        }

    private fun readIntLE(raw: ByteArray): Int? =
        if (raw.size == 4) ByteBuffer.wrap(raw).order(ByteOrder.LITTLE_ENDIAN).int else null

    private fun readLongLE(raw: ByteArray): Long? =
        if (raw.size == 8) ByteBuffer.wrap(raw).order(ByteOrder.LITTLE_ENDIAN).long else null

    /**
     * Walk the flattened SchemaElement list and return the root's direct
     * primitive children. Nested structures are descended past (their leaf
     * chunks have multi-element paths and are ignored by [aggregateColumn]).
     */
    private fun topLevelLeaves(schema: List<SchemaElement>): List<Leaf> {
        if (schema.isEmpty()) return emptyList()
        val leaves = ArrayList<Leaf>()
        // Stack of remaining-children counters; size == current depth.
        val remaining = ArrayDeque<Int>()
        remaining.addLast(schema[0].numChildren() ?: 0)
        var i = 1
        while (i < schema.size && remaining.isNotEmpty()) {
            val el = schema[i]
            val depth = remaining.size
            remaining.addLast(remaining.removeLast() - 1)
            val children = el.numChildren() ?: 0
            if (depth == 1 && children == 0) {
                leaves.add(
                    Leaf(
                        name = el.name(),
                        fieldId = el.fieldId(),
                        physical = el.type(),
                        logical = el.logicalType(),
                        converted = el.convertedType(),
                        scale = el.scale(),
                    ),
                )
            }
            if (children > 0) remaining.addLast(children)
            while (remaining.isNotEmpty() && remaining.last() == 0) remaining.removeLast()
            i++
        }
        return leaves
    }
}
