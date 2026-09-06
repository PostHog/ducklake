package com.posthog.hoglake.compaction

import com.posthog.hoglake.model.ColType
import com.posthog.hoglake.model.Column
import com.posthog.hoglake.model.SortDirection
import com.posthog.hoglake.model.SortFieldDef
import org.apache.parquet.example.data.Group
import org.apache.parquet.example.data.simple.SimpleGroupFactory
import org.apache.parquet.example.data.simple.convert.GroupRecordConverter
import org.apache.parquet.hadoop.ParquetFileReader
import org.apache.parquet.hadoop.ParquetFileWriter
import org.apache.parquet.hadoop.example.ExampleParquetWriter
import org.apache.parquet.hadoop.metadata.CompressionCodecName
import org.apache.parquet.io.ColumnIOFactory
import org.apache.parquet.io.LocalInputFile
import org.apache.parquet.io.LocalOutputFile
import org.apache.parquet.schema.LogicalTypeAnnotation
import org.apache.parquet.schema.MessageType
import org.apache.parquet.schema.PrimitiveType
import org.apache.parquet.schema.Type
import org.apache.parquet.schema.Types
import java.math.BigInteger
import java.nio.file.Path

/**
 * A compaction group whose inputs cannot be rewritten under the live
 * schema: some live column's type cannot be produced from an input
 * file's parquet type (anything outside identity or the int->long /
 * float->double promotions). The sweep treats this as skip-with-reason
 * (CompactionResult.unconvertibleSchema), never a failure.
 */
class UnconvertibleSchemaException(message: String) : IllegalArgumentException(message)

/**
 * The compaction rewrite writer, on parquet-java — the project's one
 * parquet library (decision 2026-09-05: Hardwood is out entirely).
 * Compaction outputs MUST carry field ids on every column (files bind
 * to catalog columns by id, never by name; field ids are a registration
 * contract, enforced at hydration via
 * hog_data_file.missing_field_ids + the AlterService rename guard).
 *
 * What a rewrite does: read every input file's rows, APPLY each input's
 * live deletion vector (skip the deleted physical ordinals), re-shape
 * the survivors under the table's LIVE schema, concatenate them in
 * row-id order, and write one output file in which each row's hoglake
 * row id rides an explicit physical int64 column [ROW_ID_COLUMN]
 * (reserved field id [ROW_ID_FIELD_ID]). Explicit ids are the point:
 * merged inputs need not be row-id-contiguous — and DV application
 * punches holes inside a file's range — so the output can never rely on
 * positional ids (row_id_start + offset), and, because every row
 * carries its id, reordering the merged rows by the table's sort order
 * is SAFE. This kills the predecessor's sorted-compaction hazard
 * (DuckLake's sorted merge_adjacent_files silently REMAPPED rowids,
 * breaking CDC identity downstream; hoglake row ids survive any
 * ordering because they are data, not position).
 *
 * Heterogeneous inputs (files written across ALTERs) map to the live
 * schema by FIELD ID: a live column absent from an input null-fills; an
 * input column whose field id the live schema no longer knows (dropped
 * column) drops its data; promoted columns up-cast (int32->int64,
 * float->double). Id-less input columns (pre-field-id writers) fall
 * back to live-name matching. The one refusal is a live column whose
 * type cannot be produced from the input's physical type —
 * [UnconvertibleSchemaException], the group stays uncompacted. Because
 * the output must hold every live column (null-filled ones included),
 * all data columns are written OPTIONAL; catalog nullability is
 * metadata, not parquet repetition.
 */
object ParquetRewriter {
    /** The explicit row-id column compaction outputs carry. */
    const val ROW_ID_COLUMN = "_hog_row_id"

    /**
     * Reserved parquet field id for [ROW_ID_COLUMN] (documented in
     * V1__init.sql on hog_data_file.explicit_row_ids and in AGENT.md):
     * Int.MAX_VALUE - 1, far outside hog_table.next_field_id's reach.
     */
    const val ROW_ID_FIELD_ID = 2147483646

    /**
     * One input file staged to local disk: its row-id range start and
     * the decoded live DV to apply (null = no live DV).
     */
    data class Input(
        val localPath: Path,
        val rowIdStart: Long,
        val deletes: DeletionVector? = null,
    )

    /**
     * [rowsWritten] survivors; [minRowId] their smallest row id (the
     * output's row_id_start), null when every input row was deleted.
     */
    data class RewriteResult(val rowsWritten: Long, val minRowId: Long?)

    private class Row(val group: Group, val rowId: Long)

    /** How one matched input column lands in the output. */
    private enum class CopyMode { IDENTITY, INT_TO_LONG, FLOAT_TO_DOUBLE }

    /**
     * Merge [inputs] (caller orders them by rowIdStart) into [output]
     * under the live schema [liveColumns] (ordinal order). [sortFields]
     * non-empty sorts the merged survivors by the table's sort order
     * (nulls per spec); empty keeps row-id order.
     */
    fun rewrite(
        inputs: List<Input>,
        liveColumns: List<Column>,
        sortFields: List<SortFieldDef>,
        output: Path,
    ): RewriteResult {
        require(inputs.isNotEmpty()) { "rewrite needs at least one input" }
        val outputSchema = outputSchema(liveColumns)
        val dataFields = outputSchema.fields.dropLast(1) // all but _hog_row_id
        val rowIdIndex = outputSchema.fieldCount - 1

        val factory = SimpleGroupFactory(outputSchema)
        val rows = ArrayList<Row>()
        var minRowId: Long? = null
        for (input in inputs) {
            val schema = readSchema(input.localPath)
            // A previously-compacted input carries its ids in its own
            // row-id column; positional ids would be wrong for it.
            val srcRowIdIndex =
                schema.fields.indexOfFirst { it.name == ROW_ID_COLUMN }.takeIf { it >= 0 }
            val plan = columnPlan(schema, liveColumns, input.localPath)
            var applied = 0L
            readRows(input.localPath, schema) { src, ordinal ->
                if (input.deletes?.contains(ordinal) == true) {
                    applied++
                    return@readRows
                }
                val dst = factory.newGroup()
                for ((outIdx, step) in plan.withIndex()) {
                    if (step != null && src.getFieldRepetitionCount(step.srcIndex) > 0) {
                        copyValue(src, step, dst, outIdx, dataFields[outIdx].asPrimitiveType())
                    }
                }
                val rowId =
                    if (srcRowIdIndex != null) {
                        src.getLong(srcRowIdIndex, 0)
                    } else {
                        input.rowIdStart + ordinal
                    }
                dst.add(rowIdIndex, rowId)
                rows.add(Row(dst, rowId))
                minRowId = minOf(minRowId ?: rowId, rowId)
            }
            val expected = input.deletes?.cardinality ?: 0L
            check(applied == expected) {
                "deletion vector for ${input.localPath} claims $expected positions but only " +
                    "$applied fell inside the file — refusing a lossy compaction"
            }
        }

        // Sorting is safe ONLY because ids are explicit (above); sortedWith
        // is stable, so ties keep row-id order.
        val ordered =
            if (sortFields.isEmpty()) rows else rows.sortedWith(comparator(outputSchema, sortFields))

        ExampleParquetWriter.builder(LocalOutputFile(output))
            .withType(outputSchema)
            .withCompressionCodec(CompressionCodecName.UNCOMPRESSED)
            .withWriteMode(ParquetFileWriter.Mode.OVERWRITE)
            .build()
            .use { writer -> for (row in ordered) writer.write(row.group) }
        return RewriteResult(ordered.size.toLong(), minRowId)
    }

    // ---- schema ----------------------------------------------------------

    private fun readSchema(path: Path): MessageType =
        ParquetFileReader.open(LocalInputFile(path)).use { it.footer.fileMetaData.schema }

    /**
     * The output schema is the LIVE schema: every live column in
     * ordinal order (optional, field-id-stamped, type synthesized from
     * the catalog type), plus the required [ROW_ID_COLUMN].
     */
    private fun outputSchema(liveColumns: List<Column>): MessageType {
        require(liveColumns.isNotEmpty()) { "table has no live columns" }
        val dataFields = liveColumns.map { parquetTypeFor(it) }
        val rowIdField: Type =
            Types.required(PrimitiveType.PrimitiveTypeName.INT64)
                .id(ROW_ID_FIELD_ID)
                .named(ROW_ID_COLUMN)
        return MessageType("hoglake_compacted", dataFields + rowIdField)
    }

    /** Catalog type -> parquet type (pyhoglake's writer conventions: micros times, fixed(16) uuid). */
    private fun parquetTypeFor(column: Column): Type {
        val id = Math.toIntExact(column.fieldId)
        val name = column.def.name
        return when (column.def.type) {
            ColType.BOOLEAN ->
                Types.optional(PrimitiveType.PrimitiveTypeName.BOOLEAN).id(id).named(name)
            ColType.INT ->
                Types.optional(PrimitiveType.PrimitiveTypeName.INT32).id(id).named(name)
            ColType.LONG ->
                Types.optional(PrimitiveType.PrimitiveTypeName.INT64).id(id).named(name)
            ColType.FLOAT ->
                Types.optional(PrimitiveType.PrimitiveTypeName.FLOAT).id(id).named(name)
            ColType.DOUBLE ->
                Types.optional(PrimitiveType.PrimitiveTypeName.DOUBLE).id(id).named(name)
            ColType.DECIMAL ->
                Types.optional(PrimitiveType.PrimitiveTypeName.BINARY)
                    .`as`(LogicalTypeAnnotation.decimalType(decimalScale(column) ?: 0, decimalPrecision(column)))
                    .id(id).named(name)
            ColType.DATE ->
                Types.optional(PrimitiveType.PrimitiveTypeName.INT32)
                    .`as`(LogicalTypeAnnotation.dateType()).id(id).named(name)
            ColType.TIME ->
                Types.optional(PrimitiveType.PrimitiveTypeName.INT64)
                    .`as`(LogicalTypeAnnotation.timeType(false, LogicalTypeAnnotation.TimeUnit.MICROS))
                    .id(id).named(name)
            ColType.TIMESTAMP ->
                Types.optional(PrimitiveType.PrimitiveTypeName.INT64)
                    .`as`(LogicalTypeAnnotation.timestampType(false, LogicalTypeAnnotation.TimeUnit.MICROS))
                    .id(id).named(name)
            ColType.TIMESTAMPTZ ->
                Types.optional(PrimitiveType.PrimitiveTypeName.INT64)
                    .`as`(LogicalTypeAnnotation.timestampType(true, LogicalTypeAnnotation.TimeUnit.MICROS))
                    .id(id).named(name)
            ColType.STRING ->
                Types.optional(PrimitiveType.PrimitiveTypeName.BINARY)
                    .`as`(LogicalTypeAnnotation.stringType()).id(id).named(name)
            ColType.UUID_T ->
                Types.optional(PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY).length(16)
                    .`as`(LogicalTypeAnnotation.uuidType()).id(id).named(name)
            ColType.BINARY ->
                Types.optional(PrimitiveType.PrimitiveTypeName.BINARY).id(id).named(name)
        }
    }

    private fun decimalScale(column: Column): Int? = (column.def.typeParams?.get("scale") as? Number)?.toInt()

    private fun decimalPrecision(column: Column): Int =
        (column.def.typeParams?.get("precision") as? Number)?.toInt() ?: 38

    private class CopyStep(val srcIndex: Int, val mode: CopyMode)

    /**
     * Per live column (output order): where its value comes from in
     * [schema] and how it is up-cast, or null when the input predates
     * the column (null-fill). Matching is by parquet field id, with a
     * live-NAME fallback for id-less input columns. Unmatched input
     * columns (dropped field ids, unknown id-less names) simply do not
     * appear in any plan — their data drops with the rewrite, exactly
     * like every reader already treats them.
     */
    private fun columnPlan(
        schema: MessageType,
        liveColumns: List<Column>,
        inputPath: Path,
    ): List<CopyStep?> =
        liveColumns.map { column ->
            val srcIndex =
                schema.fields.indexOfFirst { it.id?.intValue()?.toLong() == column.fieldId }
                    .takeIf { it >= 0 }
                    ?: schema.fields.indexOfFirst { it.id == null && it.name == column.def.name }
                        .takeIf { it >= 0 }
                    ?: return@map null
            val src = schema.fields[srcIndex]
            if (!src.isPrimitive) {
                throw UnconvertibleSchemaException(
                    "column '${column.def.name}' is nested in $inputPath; compaction supports flat schemas only",
                )
            }
            CopyStep(srcIndex, copyMode(src.asPrimitiveType(), column, inputPath))
        }

    /**
     * How the live column is produced from the input's physical type:
     * identity, int32->int64, or float->double. Anything else — a
     * narrowing, a physical mismatch, a decimal scale change, a
     * non-micros time(stamp) unit — is [UnconvertibleSchemaException].
     */
    private fun copyMode(
        src: PrimitiveType,
        column: Column,
        inputPath: Path,
    ): CopyMode {
        val srcName = src.primitiveTypeName
        val live = column.def.type

        fun refuse(): Nothing =
            throw UnconvertibleSchemaException(
                "column '${column.def.name}' (live type ${live.wire}) cannot be produced from " +
                    "$srcName${src.logicalTypeAnnotation?.let { " ($it)" } ?: ""} in $inputPath",
            )
        return when (live) {
            ColType.BOOLEAN ->
                if (srcName == PrimitiveType.PrimitiveTypeName.BOOLEAN) CopyMode.IDENTITY else refuse()
            ColType.INT ->
                if (srcName == PrimitiveType.PrimitiveTypeName.INT32) CopyMode.IDENTITY else refuse()
            ColType.LONG ->
                when (srcName) {
                    PrimitiveType.PrimitiveTypeName.INT64 -> CopyMode.IDENTITY
                    PrimitiveType.PrimitiveTypeName.INT32 -> CopyMode.INT_TO_LONG
                    else -> refuse()
                }
            ColType.FLOAT ->
                if (srcName == PrimitiveType.PrimitiveTypeName.FLOAT) CopyMode.IDENTITY else refuse()
            ColType.DOUBLE ->
                when (srcName) {
                    PrimitiveType.PrimitiveTypeName.DOUBLE -> CopyMode.IDENTITY
                    PrimitiveType.PrimitiveTypeName.FLOAT -> CopyMode.FLOAT_TO_DOUBLE
                    else -> refuse()
                }
            ColType.DATE ->
                if (srcName == PrimitiveType.PrimitiveTypeName.INT32) CopyMode.IDENTITY else refuse()
            ColType.TIME -> {
                val unit = (src.logicalTypeAnnotation as? LogicalTypeAnnotation.TimeLogicalTypeAnnotation)?.unit
                if (srcName == PrimitiveType.PrimitiveTypeName.INT64 &&
                    (unit == null || unit == LogicalTypeAnnotation.TimeUnit.MICROS)
                ) {
                    CopyMode.IDENTITY
                } else {
                    refuse()
                }
            }
            ColType.TIMESTAMP, ColType.TIMESTAMPTZ -> {
                val unit =
                    (src.logicalTypeAnnotation as? LogicalTypeAnnotation.TimestampLogicalTypeAnnotation)?.unit
                if (srcName == PrimitiveType.PrimitiveTypeName.INT64 &&
                    (unit == null || unit == LogicalTypeAnnotation.TimeUnit.MICROS)
                ) {
                    CopyMode.IDENTITY
                } else {
                    refuse()
                }
            }
            ColType.STRING ->
                if (srcName == PrimitiveType.PrimitiveTypeName.BINARY) CopyMode.IDENTITY else refuse()
            ColType.BINARY ->
                if (srcName == PrimitiveType.PrimitiveTypeName.BINARY) CopyMode.IDENTITY else refuse()
            ColType.UUID_T ->
                if (srcName == PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY && src.typeLength == 16) {
                    CopyMode.IDENTITY
                } else {
                    refuse()
                }
            ColType.DECIMAL -> {
                val annotation =
                    src.logicalTypeAnnotation as? LogicalTypeAnnotation.DecimalLogicalTypeAnnotation
                        ?: refuse()
                val liveScale = decimalScale(column)
                val binaryish =
                    srcName == PrimitiveType.PrimitiveTypeName.BINARY ||
                        srcName == PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY
                if (binaryish && (liveScale == null || annotation.scale == liveScale)) {
                    CopyMode.IDENTITY
                } else {
                    refuse()
                }
            }
        }
    }

    // ---- row IO ----------------------------------------------------------

    private fun readRows(
        path: Path,
        schema: MessageType,
        consume: (Group, Long) -> Unit,
    ) {
        ParquetFileReader.open(LocalInputFile(path)).use { reader ->
            val columnIO = ColumnIOFactory().getColumnIO(schema)
            var ordinal = 0L
            var pages = reader.readNextRowGroup()
            while (pages != null) {
                val recordReader = columnIO.getRecordReader(pages, GroupRecordConverter(schema))
                repeat(Math.toIntExact(pages.rowCount)) {
                    consume(recordReader.read(), ordinal++)
                }
                pages = reader.readNextRowGroup()
            }
        }
    }

    private fun copyValue(
        src: Group,
        step: CopyStep,
        dst: Group,
        dstIdx: Int,
        primitive: PrimitiveType,
    ) {
        val srcIdx = step.srcIndex
        when (step.mode) {
            CopyMode.INT_TO_LONG -> dst.add(dstIdx, src.getInteger(srcIdx, 0).toLong())
            CopyMode.FLOAT_TO_DOUBLE -> dst.add(dstIdx, src.getFloat(srcIdx, 0).toDouble())
            CopyMode.IDENTITY ->
                when (primitive.primitiveTypeName) {
                    PrimitiveType.PrimitiveTypeName.BOOLEAN -> dst.add(dstIdx, src.getBoolean(srcIdx, 0))
                    PrimitiveType.PrimitiveTypeName.INT32 -> dst.add(dstIdx, src.getInteger(srcIdx, 0))
                    PrimitiveType.PrimitiveTypeName.INT64 -> dst.add(dstIdx, src.getLong(srcIdx, 0))
                    PrimitiveType.PrimitiveTypeName.FLOAT -> dst.add(dstIdx, src.getFloat(srcIdx, 0))
                    PrimitiveType.PrimitiveTypeName.DOUBLE -> dst.add(dstIdx, src.getDouble(srcIdx, 0))
                    PrimitiveType.PrimitiveTypeName.BINARY,
                    PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY,
                    -> dst.add(dstIdx, src.getBinary(srcIdx, 0))
                    PrimitiveType.PrimitiveTypeName.INT96, null ->
                        throw IllegalArgumentException(
                            "unsupported physical type ${primitive.primitiveTypeName}",
                        )
                }
        }
    }

    // ---- sorting ---------------------------------------------------------

    private fun comparator(
        schema: MessageType,
        sortFields: List<SortFieldDef>,
    ): Comparator<Row> {
        val keys =
            sortFields.map { f ->
                val idx =
                    schema.fields.indexOfFirst { it.id?.intValue()?.toLong() == f.sourceFieldId }
                require(idx >= 0) {
                    "sort source field_id ${f.sourceFieldId} not present in the output schema"
                }
                Triple(idx, schema.getType(idx).asPrimitiveType(), f)
            }
        return Comparator { a, b ->
            for ((idx, primitive, f) in keys) {
                val aNull = a.group.getFieldRepetitionCount(idx) == 0
                val bNull = b.group.getFieldRepetitionCount(idx) == 0
                if (aNull || bNull) {
                    if (aNull && bNull) continue
                    val nullsFirst = f.nullOrder == com.posthog.hoglake.model.NullOrder.NULLS_FIRST
                    return@Comparator if (aNull == nullsFirst) -1 else 1
                }
                var c = compareNonNull(primitive, a.group, b.group, idx)
                if (f.direction == SortDirection.DESC) c = -c
                if (c != 0) return@Comparator c
            }
            0
        }
    }

    /**
     * Typed comparison per physical type. Decimal-annotated binary
     * compares as the signed two's-complement BigInteger (an unsigned
     * byte compare mis-sorts negatives); other binary/fixed (string,
     * uuid, raw bytes) compare unsigned lexicographic, which for UTF-8
     * strings is code-point order. Float/Double order NaN greatest
     * (Kotlin's natural compareTo).
     */
    private fun compareNonNull(
        primitive: PrimitiveType,
        a: Group,
        b: Group,
        idx: Int,
    ): Int =
        when (primitive.primitiveTypeName) {
            PrimitiveType.PrimitiveTypeName.BOOLEAN ->
                a.getBoolean(idx, 0).compareTo(b.getBoolean(idx, 0))
            PrimitiveType.PrimitiveTypeName.INT32 ->
                a.getInteger(idx, 0).compareTo(b.getInteger(idx, 0))
            PrimitiveType.PrimitiveTypeName.INT64 ->
                a.getLong(idx, 0).compareTo(b.getLong(idx, 0))
            PrimitiveType.PrimitiveTypeName.FLOAT ->
                a.getFloat(idx, 0).compareTo(b.getFloat(idx, 0))
            PrimitiveType.PrimitiveTypeName.DOUBLE ->
                a.getDouble(idx, 0).compareTo(b.getDouble(idx, 0))
            PrimitiveType.PrimitiveTypeName.BINARY,
            PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY,
            -> {
                val ab = a.getBinary(idx, 0).bytes
                val bb = b.getBinary(idx, 0).bytes
                if (primitive.logicalTypeAnnotation is LogicalTypeAnnotation.DecimalLogicalTypeAnnotation) {
                    BigInteger(ab).compareTo(BigInteger(bb))
                } else {
                    java.util.Arrays.compareUnsigned(ab, bb)
                }
            }
            PrimitiveType.PrimitiveTypeName.INT96, null ->
                throw IllegalArgumentException("unsupported sort key type ${primitive.primitiveTypeName}")
        }
}
