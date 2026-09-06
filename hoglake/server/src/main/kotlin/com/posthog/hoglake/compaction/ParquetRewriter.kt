package com.posthog.hoglake.compaction

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
 * The compaction rewrite writer, on parquet-java — the project's one
 * parquet library (decision 2026-09-05: Hardwood is out entirely).
 * Compaction outputs MUST carry field ids on every column (files bind
 * to catalog columns by id, never by name; field ids are a registration
 * contract, enforced at hydration via
 * hog_data_file.missing_field_ids + the AlterService rename guard).
 *
 * What a rewrite does: read every input file's rows, concatenate them
 * in row-id order, and write one output file in which each row's
 * hoglake row id rides an explicit physical int64 column
 * [ROW_ID_COLUMN] (reserved field id [ROW_ID_FIELD_ID]). Explicit ids
 * are the point: merged inputs need not be row-id-contiguous, so the
 * output cannot rely on positional ids (row_id_start + offset), and —
 * because every row carries its id — reordering the merged rows by the
 * table's sort order is SAFE. This kills the predecessor's
 * sorted-compaction hazard (DuckLake's sorted merge_adjacent_files
 * silently REMAPPED rowids, breaking CDC identity downstream; hoglake
 * row ids survive any ordering because they are data, not position).
 *
 * v1 scope: all inputs of a group must share an identical top-level
 * primitive schema (ignoring field ids and a pre-existing row-id
 * column). Groups mixing schema vintages (files written across an
 * ALTER) are rejected with [IllegalArgumentException] and simply stay
 * uncompacted — heterogeneous-schema merge is future work.
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

    /** One input file staged to local disk, with its row-id range start. */
    data class Input(val localPath: Path, val rowIdStart: Long)

    private class Row(val group: Group, val rowId: Long)

    /**
     * Merge [inputs] (caller orders them by rowIdStart) into [output].
     * [fallbackFieldIds] maps live catalog column names to field ids for
     * inputs whose parquet schema carries no ids (pre-field-id writers);
     * a column resolvable neither way fails the rewrite. [sortFields]
     * non-empty sorts the merged rows by the table's sort order (nulls
     * per spec); empty keeps row-id order. Returns rows written.
     */
    fun rewrite(
        inputs: List<Input>,
        fallbackFieldIds: Map<String, Long>,
        sortFields: List<SortFieldDef>,
        output: Path,
    ): Long {
        require(inputs.isNotEmpty()) { "rewrite needs at least one input" }
        val baseSchema = readSchema(inputs.first().localPath)
        val outputSchema = outputSchema(baseSchema, fallbackFieldIds)
        val dataFields = outputSchema.fields.dropLast(1) // all but _hog_row_id
        val rowIdIndex = outputSchema.fieldCount - 1

        // Read + re-index every row against the output schema, in input
        // (row-id) order.
        val factory = SimpleGroupFactory(outputSchema)
        val rows = ArrayList<Row>()
        for (input in inputs) {
            val schema = readSchema(input.localPath)
            requireSameShape(baseSchema, schema, input.localPath)
            // A previously-compacted input carries its ids in its own
            // row-id column; positional ids would be wrong for it.
            val srcRowIdIndex =
                schema.fields.indexOfFirst { it.name == ROW_ID_COLUMN }.takeIf { it >= 0 }
            val srcIndexByOutIndex =
                dataFields.map { schema.getFieldIndex(it.name) }
            readRows(input.localPath, schema) { src, ordinal ->
                val dst = factory.newGroup()
                for ((outIdx, srcIdx) in srcIndexByOutIndex.withIndex()) {
                    if (src.getFieldRepetitionCount(srcIdx) > 0) {
                        copyValue(src, srcIdx, dst, outIdx, dataFields[outIdx].asPrimitiveType())
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
        return ordered.size.toLong()
    }

    // ---- schema ----------------------------------------------------------

    private fun readSchema(path: Path): MessageType =
        ParquetFileReader.open(LocalInputFile(path)).use { it.footer.fileMetaData.schema }

    /**
     * The output schema: the base input's data columns, each stamped
     * with its field id (kept from the input, else resolved from the
     * live catalog columns by name), plus the required [ROW_ID_COLUMN].
     */
    private fun outputSchema(
        base: MessageType,
        fallbackFieldIds: Map<String, Long>,
    ): MessageType {
        val dataFields =
            base.fields
                .filter { it.name != ROW_ID_COLUMN }
                .map { field ->
                    require(field.isPrimitive) {
                        "column '${field.name}' is nested; compaction supports flat schemas only"
                    }
                    require(field.asPrimitiveType().primitiveTypeName != PrimitiveType.PrimitiveTypeName.INT96) {
                        "column '${field.name}' is INT96; unsupported"
                    }
                    val id =
                        field.id?.intValue()
                            ?: fallbackFieldIds[field.name]?.let { Math.toIntExact(it) }
                            ?: throw IllegalArgumentException(
                                "column '${field.name}' has no parquet field id and no live " +
                                    "catalog column of that name to borrow one from",
                            )
                    field.withId(id)
                }
        val rowIdField: Type =
            Types.required(PrimitiveType.PrimitiveTypeName.INT64)
                .id(ROW_ID_FIELD_ID)
                .named(ROW_ID_COLUMN)
        return MessageType(base.name, dataFields + rowIdField)
    }

    /** Same top-level shape (name/repetition/physical/logical), ignoring ids + the row-id column. */
    private fun requireSameShape(
        base: MessageType,
        other: MessageType,
        path: Path,
    ) {
        require(shapeKey(base) == shapeKey(other)) {
            "input $path has a different schema than the group's first input; " +
                "compaction groups must share one schema vintage"
        }
    }

    private fun shapeKey(schema: MessageType): List<String> =
        schema.fields
            .filter { it.name != ROW_ID_COLUMN }
            .map { f ->
                val p = f.asPrimitiveType()
                "${f.name}|${f.repetition}|${p.primitiveTypeName}|${p.typeLength}|${p.logicalTypeAnnotation}"
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
        srcIdx: Int,
        dst: Group,
        dstIdx: Int,
        primitive: PrimitiveType,
    ) {
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
                throw IllegalArgumentException("unsupported physical type ${primitive.primitiveTypeName}")
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
