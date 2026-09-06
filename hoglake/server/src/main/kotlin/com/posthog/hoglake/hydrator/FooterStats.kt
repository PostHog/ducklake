package com.posthog.hoglake.hydrator

import com.posthog.hoglake.model.ColType
import com.posthog.hoglake.stats.IcebergSingleValue
import io.github.oshai.kotlinlogging.KotlinLogging
import org.apache.parquet.column.statistics.Statistics
import org.apache.parquet.hadoop.metadata.BlockMetaData
import org.apache.parquet.hadoop.metadata.ParquetMetadata
import org.apache.parquet.schema.LogicalTypeAnnotation
import org.apache.parquet.schema.MessageType
import org.apache.parquet.schema.PrimitiveType
import org.apache.parquet.schema.Type
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
 * Pure footer-to-stats aggregation: takes a parquet footer
 * ([ParquetMetadata], parquet-java — the project's one parquet library)
 * and the table's live catalog columns, and produces per-field
 * aggregates in Iceberg single-value bound encoding. Footer only — no
 * data pages.
 *
 * Column mapping prefers the parquet schema's field ids
 * (`PARQUET:field_id`) when the file carries any; otherwise it falls
 * back to name matching (logged as a warning — files written without
 * field ids lose rename-safety, and [missingFieldIds] flags them for
 * the rename guard).
 *
 * Bounds are only produced when every column chunk contributes reliable
 * statistics (present with a decodable min/max under the catalog type,
 * not NaN; parquet-java itself refuses unreliable pre-TYPE_DEFINED_ORDER
 * deprecated min/max at footer decode). Anything else leaves the bounds
 * NULL — never guessed.
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
        val primitive: PrimitiveType,
    )

    /**
     * The field-id contract check: true when ANY primitive leaf of the
     * schema lacks a `PARQUET:field_id`. Such files bind columns by
     * name, so a later column rename would silently NULL their history
     * in readers — hog_data_file.missing_field_ids records the hazard
     * and AlterService refuses renames while a flagged file is live.
     * The reserved `_hog_row_id` id 2147483646 on compacted files is an
     * id like any other and never trips this.
     */
    fun missingFieldIds(schema: MessageType): Boolean = anyLeafWithoutId(schema.fields)

    /**
     * Whether [aggregate] will map columns by field id for this schema:
     * true when any top-level primitive leaf carries a `PARQUET:field_id`
     * (the same signal aggregate keys off). False = the name-fallback
     * path, whose column set the hydrator must resolve at the FILE's
     * begin_snapshot, not live-at-hydration.
     */
    fun usesFieldIds(schema: MessageType): Boolean = topLevelLeaves(schema).any { it.fieldId != null }

    private fun anyLeafWithoutId(fields: List<Type>): Boolean =
        fields.any { field ->
            if (field.isPrimitive) {
                field.id == null
            } else {
                anyLeafWithoutId(field.asGroupType().fields)
            }
        }

    fun aggregate(
        footer: ParquetMetadata,
        columns: List<CatalogColumn>,
        filePath: String,
    ): List<ColumnAgg> {
        val leaves = topLevelLeaves(footer.fileMetaData.schema)
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
            aggregateColumn(footer.blocks, col, leaf, filePath)?.let(out::add)
        }
        return out
    }

    private fun aggregateColumn(
        blocks: List<BlockMetaData>,
        col: CatalogColumn,
        leaf: Leaf,
        filePath: String,
    ): ColumnAgg? {
        var valueCount = 0L
        var nullCount = 0L
        var nullCountKnown = true
        var sizeBytes = 0L
        var boundsOk = true
        var min: Any? = null
        var max: Any? = null
        var chunks = 0

        for (block in blocks) {
            for (chunk in block.columns) {
                val path = chunk.path.toArray()
                if (path.size != 1 || path[0] != leaf.name) continue
                chunks++
                valueCount += chunk.valueCount
                sizeBytes += chunk.totalSize
                val st: Statistics<*>? = chunk.statistics
                if (st == null || !st.isNumNullsSet) nullCountKnown = false else nullCount += st.numNulls
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
            // Parquet footers carry no NaN counts; honest null over a guess.
            nanCount = null,
            sizeBytes = sizeBytes,
            lowerBound = if (boundsOk && min != null) encodeBound(col.type, min!!) else null,
            upperBound = if (boundsOk && max != null) encodeBound(col.type, max!!) else null,
        )
    }

    /** Decoded (lower, upper) for one chunk, or null when unreliable. */
    private fun chunkBounds(
        col: CatalogColumn,
        leaf: Leaf,
        st: Statistics<*>?,
    ): Pair<Any, Any>? {
        // hasNonNullValue is false when the footer carried no reliable
        // min/max (parquet-java already dropped deprecated min/max whose
        // sort order is untrustworthy) or the chunk was all-null.
        if (st == null || !st.hasNonNullValue()) return null
        val rawMin = st.minBytes ?: return null
        val rawMax = st.maxBytes ?: return null
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
    ): Any? {
        val physical = leaf.primitive.primitiveTypeName
        return when (col.type) {
            ColType.BOOLEAN ->
                if (physical == PrimitiveType.PrimitiveTypeName.BOOLEAN && raw.size == 1) {
                    raw[0] != 0.toByte()
                } else {
                    null
                }
            ColType.INT ->
                if (physical == PrimitiveType.PrimitiveTypeName.INT32) readIntLE(raw) else null
            ColType.LONG ->
                when (physical) {
                    PrimitiveType.PrimitiveTypeName.INT64 -> readLongLE(raw)
                    PrimitiveType.PrimitiveTypeName.INT32 -> readIntLE(raw)?.toLong()
                    else -> null
                }
            ColType.FLOAT ->
                if (physical == PrimitiveType.PrimitiveTypeName.FLOAT) {
                    readIntLE(raw)?.let { Float.fromBits(it) }
                } else {
                    null
                }
            ColType.DOUBLE ->
                when (physical) {
                    PrimitiveType.PrimitiveTypeName.DOUBLE -> readLongLE(raw)?.let { Double.fromBits(it) }
                    PrimitiveType.PrimitiveTypeName.FLOAT ->
                        readIntLE(raw)?.let { Float.fromBits(it).toDouble() }
                    else -> null
                }
            ColType.DATE ->
                if (physical == PrimitiveType.PrimitiveTypeName.INT32) readIntLE(raw) else null
            ColType.TIME -> decodeTime(leaf, raw)
            ColType.TIMESTAMP, ColType.TIMESTAMPTZ -> decodeTimestamp(leaf, raw, upper)
            ColType.STRING ->
                if (physical == PrimitiveType.PrimitiveTypeName.BINARY) raw else null
            ColType.UUID_T ->
                if (physical == PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY && raw.size == 16) {
                    raw
                } else {
                    null
                }
            ColType.BINARY ->
                when (physical) {
                    PrimitiveType.PrimitiveTypeName.BINARY,
                    PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY,
                    -> raw
                    else -> null
                }
            ColType.DECIMAL -> decodeDecimal(col, leaf, raw)
        }
    }

    private fun decodeTime(
        leaf: Leaf,
        raw: ByteArray,
    ): Long? {
        val unit =
            (leaf.primitive.logicalTypeAnnotation as? LogicalTypeAnnotation.TimeLogicalTypeAnnotation)
                ?.unit ?: return null
        return when (unit) {
            LogicalTypeAnnotation.TimeUnit.MICROS ->
                if (leaf.primitive.primitiveTypeName == PrimitiveType.PrimitiveTypeName.INT64) {
                    readLongLE(raw)
                } else {
                    null
                }
            LogicalTypeAnnotation.TimeUnit.MILLIS ->
                if (leaf.primitive.primitiveTypeName == PrimitiveType.PrimitiveTypeName.INT32) {
                    readIntLE(raw)?.let { it * 1_000L }
                } else {
                    null
                }
            LogicalTypeAnnotation.TimeUnit.NANOS -> null // sub-micro truncation of a time bound: skip
        }
    }

    private fun decodeTimestamp(
        leaf: Leaf,
        raw: ByteArray,
        upper: Boolean,
    ): Long? {
        if (leaf.primitive.primitiveTypeName != PrimitiveType.PrimitiveTypeName.INT64) return null
        val unit =
            (leaf.primitive.logicalTypeAnnotation as? LogicalTypeAnnotation.TimestampLogicalTypeAnnotation)
                ?.unit ?: return null
        val v = readLongLE(raw) ?: return null
        return try {
            when (unit) {
                LogicalTypeAnnotation.TimeUnit.MICROS -> v
                LogicalTypeAnnotation.TimeUnit.MILLIS -> Math.multiplyExact(v, 1_000L)
                // Nanos truncate: floor for the lower bound, ceil for the upper,
                // so the bound stays valid for the true values.
                LogicalTypeAnnotation.TimeUnit.NANOS ->
                    if (upper) Math.floorDiv(Math.addExact(v, 999L), 1_000L) else Math.floorDiv(v, 1_000L)
            }
        } catch (_: ArithmeticException) {
            // The unit conversion overflows int64 micros (a millis bound
            // near Long.MAX_VALUE — hostile-writer craftable). The decode
            // contract is "bounds NULL, never guessed" — an overflow must
            // degrade to a null bound, never escape and fail the file.
            null
        }
    }

    private fun decodeDecimal(
        col: CatalogColumn,
        leaf: Leaf,
        raw: ByteArray,
    ): BigInteger? {
        val parquetScale =
            (leaf.primitive.logicalTypeAnnotation as? LogicalTypeAnnotation.DecimalLogicalTypeAnnotation)
                ?.scale ?: return null
        val catalogScale = col.decimalScale ?: return null
        if (parquetScale != catalogScale) {
            log.warn {
                "decimal scale mismatch for ${col.name}: parquet=$parquetScale catalog=$catalogScale; skipping bounds"
            }
            return null
        }
        return when (leaf.primitive.primitiveTypeName) {
            PrimitiveType.PrimitiveTypeName.INT32 -> readIntLE(raw)?.let { BigInteger.valueOf(it.toLong()) }
            PrimitiveType.PrimitiveTypeName.INT64 -> readLongLE(raw)?.let { BigInteger.valueOf(it) }
            PrimitiveType.PrimitiveTypeName.BINARY,
            PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY,
            ->
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
     * The root's direct primitive children. Nested structures are
     * skipped (their leaf chunks have multi-element paths and are
     * ignored by [aggregateColumn]).
     */
    private fun topLevelLeaves(schema: MessageType): List<Leaf> =
        schema.fields
            .filter { it.isPrimitive }
            .map { field ->
                Leaf(
                    name = field.name,
                    fieldId = field.id?.intValue(),
                    primitive = field.asPrimitiveType(),
                )
            }
}
