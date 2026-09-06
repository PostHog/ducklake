package com.posthog.hoglake.hydrator

import com.posthog.hoglake.model.ColType
import org.apache.parquet.column.Encoding
import org.apache.parquet.column.statistics.Statistics
import org.apache.parquet.hadoop.metadata.BlockMetaData
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData
import org.apache.parquet.hadoop.metadata.ColumnPath
import org.apache.parquet.hadoop.metadata.CompressionCodecName
import org.apache.parquet.hadoop.metadata.FileMetaData
import org.apache.parquet.hadoop.metadata.ParquetMetadata
import org.apache.parquet.schema.LogicalTypeAnnotation
import org.apache.parquet.schema.MessageType
import org.apache.parquet.schema.PrimitiveType
import org.apache.parquet.schema.Type
import org.apache.parquet.schema.Types
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import java.nio.ByteBuffer
import java.nio.ByteOrder

/**
 * Unit tests for footer aggregation against hand-built [ParquetMetadata]
 * (parquet-java's metadata classes are constructible records). This is
 * also where the field-id mapping and [FooterStats.missingFieldIds]
 * contract check are exercised without touching an object store.
 */
class FooterStatsTest {
    // ---- fixture helpers ---------------------------------------------------

    private fun leaf(
        name: String,
        physical: PrimitiveType.PrimitiveTypeName,
        fieldId: Int? = null,
        logical: LogicalTypeAnnotation? = null,
        typeLength: Int? = null,
    ): PrimitiveType {
        var b = Types.optional(physical)
        if (typeLength != null) b = b.length(typeLength)
        if (logical != null) b = b.`as`(logical)
        if (fieldId != null) b = b.id(fieldId)
        return b.named(name)
    }

    private fun schema(vararg fields: Type): MessageType = MessageType("root", fields.toList())

    /** Statistics with the given raw min/max bytes and null count (null = unknown). */
    private fun stats(
        type: PrimitiveType,
        min: ByteArray?,
        max: ByteArray?,
        nulls: Long? = 0L,
    ): Statistics<*> {
        val b = Statistics.getBuilderForReading(type)
        if (min != null && max != null) {
            b.withMin(min)
            b.withMax(max)
        }
        if (nulls != null) b.withNumNulls(nulls)
        return b.build()
    }

    private fun chunk(
        leaf: PrimitiveType,
        numValues: Long,
        st: Statistics<*>,
        compressedSize: Long = 100L,
    ): ColumnChunkMetaData =
        ColumnChunkMetaData.get(
            ColumnPath.get(leaf.name),
            leaf,
            CompressionCodecName.UNCOMPRESSED,
            null,
            setOf(Encoding.PLAIN),
            st,
            4L,
            0L,
            numValues,
            compressedSize,
            compressedSize * 2,
        )

    /** A nested chunk (multi-element path) that top-level aggregation must ignore. */
    private fun nestedChunk(
        leaf: PrimitiveType,
        parent: String,
        numValues: Long,
        st: Statistics<*>,
    ): ColumnChunkMetaData =
        ColumnChunkMetaData.get(
            ColumnPath.get(parent, leaf.name),
            leaf,
            CompressionCodecName.UNCOMPRESSED,
            null,
            setOf(Encoding.PLAIN),
            st,
            4L,
            0L,
            numValues,
            10L,
            20L,
        )

    private fun meta(
        schema: MessageType,
        numRows: Long,
        vararg groups: List<ColumnChunkMetaData>,
    ): ParquetMetadata {
        val blocks =
            groups.map { chunks ->
                BlockMetaData().apply {
                    rowCount = numRows
                    totalByteSize = chunks.sumOf { it.totalSize }
                    chunks.forEach { addColumn(it) }
                }
            }
        return ParquetMetadata(FileMetaData(schema, emptyMap(), "test"), blocks)
    }

    private fun le(v: Int): ByteArray = ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN).putInt(v).array()

    private fun le(v: Long): ByteArray = ByteBuffer.allocate(8).order(ByteOrder.LITTLE_ENDIAN).putLong(v).array()

    private fun le(v: Double): ByteArray = ByteBuffer.allocate(8).order(ByteOrder.LITTLE_ENDIAN).putDouble(v).array()

    private fun agg(
        meta: ParquetMetadata,
        vararg cols: CatalogColumn,
    ): Map<Long, FooterStats.ColumnAgg> =
        FooterStats.aggregate(meta, cols.toList(), "s3://t/f.parquet").associateBy { it.fieldId }

    // ---- mapping -----------------------------------------------------------

    @Test
    fun `parquet field ids beat name matching`() {
        // Parquet column is named "a_old"; the catalog renamed it to "a".
        // Field id 7 must carry the mapping.
        val a = leaf("a_old", PrimitiveType.PrimitiveTypeName.INT64, fieldId = 7)
        val b = leaf("b", PrimitiveType.PrimitiveTypeName.INT64, fieldId = 8)
        val m =
            meta(
                schema(a, b),
                10,
                listOf(
                    chunk(a, 10, stats(a, le(5L), le(9L))),
                    chunk(b, 10, stats(b, le(1L), le(2L))),
                ),
            )
        // Catalog: field 7 named "a" (renamed), and field 9 named "b" —
        // the name collision with parquet "b" (field 8) must NOT match.
        val out =
            agg(
                m,
                CatalogColumn(7, "a", ColType.LONG, null),
                CatalogColumn(9, "b", ColType.LONG, null),
            )
        assertThat(out).containsOnlyKeys(7L)
        assertThat(out[7L]!!.lowerBound).isEqualTo(le(5L))
        assertThat(out[7L]!!.upperBound).isEqualTo(le(9L))
    }

    @Test
    fun `falls back to names when the file has no field ids`() {
        val a = leaf("a", PrimitiveType.PrimitiveTypeName.INT64)
        val m = meta(schema(a), 10, listOf(chunk(a, 10, stats(a, le(-2L), le(4L), nulls = 3))))
        val out = agg(m, CatalogColumn(1, "a", ColType.LONG, null))
        assertThat(out).containsOnlyKeys(1L)
        with(out[1L]!!) {
            assertThat(valueCount).isEqualTo(10)
            assertThat(nullCount).isEqualTo(3)
            assertThat(lowerBound).isEqualTo(le(-2L))
            assertThat(upperBound).isEqualTo(le(4L))
        }
    }

    @Test
    fun `catalog columns absent from the file get no stats row`() {
        val a = leaf("a", PrimitiveType.PrimitiveTypeName.INT64)
        val m = meta(schema(a), 5, listOf(chunk(a, 5, stats(a, le(0L), le(1L)))))
        val out =
            agg(
                m,
                CatalogColumn(1, "a", ColType.LONG, null),
                CatalogColumn(2, "added_later", ColType.STRING, null),
            )
        assertThat(out).containsOnlyKeys(1L)
    }

    // ---- the field-id contract check ---------------------------------------

    @Test
    fun `missingFieldIds is false when every leaf has an id and true when any lacks one`() {
        val withIds =
            schema(
                leaf("a", PrimitiveType.PrimitiveTypeName.INT64, fieldId = 1),
                leaf("b", PrimitiveType.PrimitiveTypeName.INT64, fieldId = 2),
            )
        assertThat(FooterStats.missingFieldIds(withIds)).isFalse()

        val oneMissing =
            schema(
                leaf("a", PrimitiveType.PrimitiveTypeName.INT64, fieldId = 1),
                leaf("b", PrimitiveType.PrimitiveTypeName.INT64),
            )
        assertThat(FooterStats.missingFieldIds(oneMissing)).isTrue()
    }

    @Test
    fun `the reserved _hog_row_id id counts as an id like any other`() {
        val compacted =
            schema(
                leaf("a", PrimitiveType.PrimitiveTypeName.INT64, fieldId = 1),
                Types.required(PrimitiveType.PrimitiveTypeName.INT64)
                    .id(2147483646)
                    .named("_hog_row_id"),
            )
        assertThat(FooterStats.missingFieldIds(compacted)).isFalse()
    }

    @Test
    fun `missingFieldIds inspects nested leaves too`() {
        val nested =
            schema(
                leaf("a", PrimitiveType.PrimitiveTypeName.INT64, fieldId = 1),
                Types.optionalGroup()
                    .addField(leaf("x", PrimitiveType.PrimitiveTypeName.INT64))
                    .id(2)
                    .named("g"),
            )
        assertThat(FooterStats.missingFieldIds(nested)).isTrue()
    }

    // ---- multi row-group aggregation ---------------------------------------

    @Test
    fun `merges counts and bounds across row groups`() {
        val n = leaf("n", PrimitiveType.PrimitiveTypeName.INT64)
        val s =
            leaf(
                "s",
                PrimitiveType.PrimitiveTypeName.BINARY,
                logical = LogicalTypeAnnotation.stringType(),
            )
        val m =
            meta(
                schema(n, s),
                25,
                listOf(
                    chunk(n, 10, stats(n, le(5L), le(10L), nulls = 1), compressedSize = 40),
                    chunk(s, 10, stats(s, "banana".toByteArray(), "cherry".toByteArray())),
                ),
                listOf(
                    chunk(n, 15, stats(n, le(-3L), le(7L), nulls = 2), compressedSize = 60),
                    chunk(s, 15, stats(s, "apple".toByteArray(), "candy".toByteArray())),
                ),
            )
        val out =
            agg(
                m,
                CatalogColumn(1, "n", ColType.LONG, null),
                CatalogColumn(2, "s", ColType.STRING, null),
            )
        with(out[1L]!!) {
            assertThat(valueCount).isEqualTo(25)
            assertThat(nullCount).isEqualTo(3)
            assertThat(sizeBytes).isEqualTo(100)
            assertThat(lowerBound).isEqualTo(le(-3L))
            assertThat(upperBound).isEqualTo(le(10L))
        }
        with(out[2L]!!) {
            assertThat(lowerBound).isEqualTo("apple".toByteArray())
            assertThat(upperBound).isEqualTo("cherry".toByteArray())
        }
    }

    // ---- reliability gates -------------------------------------------------

    @Test
    fun `stats without min-max keep counts but drop bounds`() {
        // The shape parquet-java hands back for footers whose deprecated
        // min/max it refused (pre-TYPE_DEFINED_ORDER unreliable order):
        // null count present, no bounds.
        val a = leaf("a", PrimitiveType.PrimitiveTypeName.INT64)
        val m = meta(schema(a), 10, listOf(chunk(a, 10, stats(a, null, null, nulls = 4))))
        val out = agg(m, CatalogColumn(1, "a", ColType.LONG, null))
        with(out[1L]!!) {
            assertThat(valueCount).isEqualTo(10)
            assertThat(nullCount).isEqualTo(4)
            assertThat(lowerBound).isNull()
            assertThat(upperBound).isNull()
        }
    }

    @Test
    fun `one chunk without min-max poisons bounds for the whole file`() {
        val a = leaf("a", PrimitiveType.PrimitiveTypeName.INT64)
        val m =
            meta(
                schema(a),
                20,
                listOf(chunk(a, 10, stats(a, le(1L), le(2L), nulls = 0))),
                listOf(chunk(a, 10, stats(a, null, null, nulls = 0))),
            )
        val out = agg(m, CatalogColumn(1, "a", ColType.LONG, null))
        with(out[1L]!!) {
            assertThat(valueCount).isEqualTo(20)
            assertThat(lowerBound).isNull()
            assertThat(upperBound).isNull()
        }
    }

    @Test
    fun `missing null count drops the whole stats row`() {
        val a = leaf("a", PrimitiveType.PrimitiveTypeName.INT64)
        val m = meta(schema(a), 10, listOf(chunk(a, 10, stats(a, le(1L), le(2L), nulls = null))))
        assertThat(agg(m, CatalogColumn(1, "a", ColType.LONG, null))).isEmpty()
    }

    @Test
    fun `NaN bounds are never written`() {
        val d = leaf("d", PrimitiveType.PrimitiveTypeName.DOUBLE)
        val m = meta(schema(d), 10, listOf(chunk(d, 10, stats(d, le(Double.NaN), le(5.0)))))
        val out = agg(m, CatalogColumn(1, "d", ColType.DOUBLE, null))
        with(out[1L]!!) {
            assertThat(lowerBound).isNull()
            assertThat(upperBound).isNull()
        }
    }

    @Test
    fun `physical type mismatch keeps counts but drops bounds`() {
        val a = leaf("a", PrimitiveType.PrimitiveTypeName.BINARY)
        val m = meta(schema(a), 10, listOf(chunk(a, 10, stats(a, le(1L), le(2L)))))
        val out = agg(m, CatalogColumn(1, "a", ColType.LONG, null))
        with(out[1L]!!) {
            assertThat(valueCount).isEqualTo(10)
            assertThat(lowerBound).isNull()
            assertThat(upperBound).isNull()
        }
    }

    // ---- typed decoding ----------------------------------------------------

    @Test
    fun `int32 widens to catalog long`() {
        val a = leaf("a", PrimitiveType.PrimitiveTypeName.INT32)
        val m = meta(schema(a), 10, listOf(chunk(a, 10, stats(a, le(-7), le(9)))))
        val out = agg(m, CatalogColumn(1, "a", ColType.LONG, null))
        assertThat(out[1L]!!.lowerBound).isEqualTo(le(-7L))
        assertThat(out[1L]!!.upperBound).isEqualTo(le(9L))
    }

    @Test
    fun `timestamp millis convert exactly to micros`() {
        val ts =
            leaf(
                "ts",
                PrimitiveType.PrimitiveTypeName.INT64,
                logical = LogicalTypeAnnotation.timestampType(true, LogicalTypeAnnotation.TimeUnit.MILLIS),
            )
        val m = meta(schema(ts), 10, listOf(chunk(ts, 10, stats(ts, le(1_000L), le(2_500L)))))
        val out = agg(m, CatalogColumn(1, "ts", ColType.TIMESTAMPTZ, null))
        assertThat(out[1L]!!.lowerBound).isEqualTo(le(1_000_000L))
        assertThat(out[1L]!!.upperBound).isEqualTo(le(2_500_000L))
    }

    @Test
    fun `timestamp nanos floor the lower bound and ceil the upper`() {
        val ts =
            leaf(
                "ts",
                PrimitiveType.PrimitiveTypeName.INT64,
                logical = LogicalTypeAnnotation.timestampType(true, LogicalTypeAnnotation.TimeUnit.NANOS),
            )
        val m = meta(schema(ts), 10, listOf(chunk(ts, 10, stats(ts, le(1_500L), le(2_500L)))))
        val out = agg(m, CatalogColumn(1, "ts", ColType.TIMESTAMPTZ, null))
        assertThat(out[1L]!!.lowerBound).isEqualTo(le(1L))
        assertThat(out[1L]!!.upperBound).isEqualTo(le(3L))
    }

    @Test
    fun `timestamp millis near Long MAX overflows to null bounds, never an exception`() {
        // Pinned regression (bug hunt #10): millis -> micros uses
        // multiplyExact; a bound near Long.MAX_VALUE (hostile-writer
        // craftable) used to throw ArithmeticException out of decode and
        // fail the WHOLE file. The "bounds NULL, never guessed" contract
        // demands a null bound and an otherwise-honest stats row.
        val ts =
            leaf(
                "ts",
                PrimitiveType.PrimitiveTypeName.INT64,
                logical = LogicalTypeAnnotation.timestampType(true, LogicalTypeAnnotation.TimeUnit.MILLIS),
            )
        val m = meta(schema(ts), 10, listOf(chunk(ts, 10, stats(ts, le(Long.MAX_VALUE - 1), le(Long.MAX_VALUE)))))
        val out = agg(m, CatalogColumn(1, "ts", ColType.TIMESTAMPTZ, null))
        assertThat(out).containsOnlyKeys(1L) // the row survives
        assertThat(out[1L]!!.valueCount).isEqualTo(10)
        assertThat(out[1L]!!.lowerBound).isNull()
        assertThat(out[1L]!!.upperBound).isNull()
    }

    @Test
    fun `timestamp nanos near Long MAX overflows the upper-bound ceil to null bounds`() {
        // The NANOS upper bound ceils via addExact(v, 999): craftable
        // overflow on the upper side specifically.
        val ts =
            leaf(
                "ts",
                PrimitiveType.PrimitiveTypeName.INT64,
                logical = LogicalTypeAnnotation.timestampType(true, LogicalTypeAnnotation.TimeUnit.NANOS),
            )
        val m = meta(schema(ts), 10, listOf(chunk(ts, 10, stats(ts, le(0L), le(Long.MAX_VALUE)))))
        val out = agg(m, CatalogColumn(1, "ts", ColType.TIMESTAMPTZ, null))
        assertThat(out).containsOnlyKeys(1L)
        assertThat(out[1L]!!.lowerBound).isNull()
        assertThat(out[1L]!!.upperBound).isNull()
    }

    @Test
    fun `timestamp with no logical annotation drops bounds`() {
        val ts = leaf("ts", PrimitiveType.PrimitiveTypeName.INT64)
        val m = meta(schema(ts), 10, listOf(chunk(ts, 10, stats(ts, le(1L), le(2L)))))
        val out = agg(m, CatalogColumn(1, "ts", ColType.TIMESTAMPTZ, null))
        assertThat(out[1L]!!.lowerBound).isNull()
    }

    @Test
    fun `decimal byte-array bounds pass through at matching scale`() {
        val d =
            leaf(
                "d",
                PrimitiveType.PrimitiveTypeName.BINARY,
                logical = LogicalTypeAnnotation.decimalType(2, 10),
            )
        val m =
            meta(
                schema(d),
                10,
                // -0.01 .. 14.20
                listOf(chunk(d, 10, stats(d, byteArrayOf(-1), byteArrayOf(0x05, 0x8C.toByte())))),
            )
        val out = agg(m, CatalogColumn(1, "d", ColType.DECIMAL, decimalScale = 2))
        assertThat(out[1L]!!.lowerBound).isEqualTo(byteArrayOf(-1))
        assertThat(out[1L]!!.upperBound).isEqualTo(byteArrayOf(0x05, 0x8C.toByte()))
    }

    @Test
    fun `decimal scale mismatch drops bounds`() {
        val d =
            leaf(
                "d",
                PrimitiveType.PrimitiveTypeName.BINARY,
                logical = LogicalTypeAnnotation.decimalType(2, 10),
            )
        val m = meta(schema(d), 10, listOf(chunk(d, 10, stats(d, byteArrayOf(1), byteArrayOf(2)))))
        val out = agg(m, CatalogColumn(1, "d", ColType.DECIMAL, decimalScale = 3))
        assertThat(out[1L]!!.lowerBound).isNull()
        assertThat(out[1L]!!.upperBound).isNull()
    }

    @Test
    fun `uuid fixed16 bounds pass through`() {
        val lo = ByteArray(16) { 0x00 }
        val hi = ByteArray(16) { 0xAB.toByte() }
        val u =
            leaf(
                "u",
                PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY,
                logical = LogicalTypeAnnotation.uuidType(),
                typeLength = 16,
            )
        val m = meta(schema(u), 10, listOf(chunk(u, 10, stats(u, lo, hi))))
        val out = agg(m, CatalogColumn(1, "u", ColType.UUID_T, null))
        assertThat(out[1L]!!.lowerBound).isEqualTo(lo)
        assertThat(out[1L]!!.upperBound).isEqualTo(hi)
    }

    @Test
    fun `string bounds compare as unsigned bytes`() {
        // 0xC2 0xB5 (µ) must sort above ASCII despite the negative signed byte.
        val s =
            leaf(
                "s",
                PrimitiveType.PrimitiveTypeName.BINARY,
                logical = LogicalTypeAnnotation.stringType(),
            )
        val m =
            meta(
                schema(s),
                10,
                listOf(chunk(s, 5, stats(s, "a".toByteArray(), "µ".toByteArray()))),
                listOf(chunk(s, 5, stats(s, "b".toByteArray(), "z".toByteArray()))),
            )
        val out = agg(m, CatalogColumn(1, "s", ColType.STRING, null))
        assertThat(out[1L]!!.lowerBound).isEqualTo("a".toByteArray())
        assertThat(out[1L]!!.upperBound).isEqualTo("µ".toByteArray())
    }

    @Test
    fun `nested leaves are ignored, top-level ones still map`() {
        // root { a: int64, g: group { x: int64 } }
        val a = leaf("a", PrimitiveType.PrimitiveTypeName.INT64)
        val x = leaf("x", PrimitiveType.PrimitiveTypeName.INT64)
        val g = Types.optionalGroup().addField(x).named("g")
        val m =
            meta(
                schema(a, g),
                10,
                listOf(
                    chunk(a, 10, stats(a, le(1L), le(2L))),
                    nestedChunk(x, "g", 10, stats(x, le(9L), le(9L))),
                ),
            )
        val out =
            agg(
                m,
                CatalogColumn(1, "a", ColType.LONG, null),
                CatalogColumn(2, "x", ColType.LONG, null),
            )
        assertThat(out).containsOnlyKeys(1L)
        assertThat(out[1L]!!.upperBound).isEqualTo(le(2L))
    }
}
