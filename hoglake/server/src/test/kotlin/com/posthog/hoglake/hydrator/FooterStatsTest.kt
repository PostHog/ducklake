package com.posthog.hoglake.hydrator

import com.posthog.hoglake.model.ColType
import dev.hardwood.metadata.ColumnChunk
import dev.hardwood.metadata.ColumnMetaData
import dev.hardwood.metadata.CompressionCodec
import dev.hardwood.metadata.Encoding
import dev.hardwood.metadata.FieldPath
import dev.hardwood.metadata.FileMetaData
import dev.hardwood.metadata.LogicalType
import dev.hardwood.metadata.PhysicalType
import dev.hardwood.metadata.RepetitionType
import dev.hardwood.metadata.RowGroup
import dev.hardwood.metadata.SchemaElement
import dev.hardwood.metadata.Statistics
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import java.nio.ByteBuffer
import java.nio.ByteOrder

/**
 * Unit tests for footer aggregation against hand-built [FileMetaData]
 * (Hardwood's metadata classes are plain records). This is also where the
 * field-id mapping path is exercised: Hardwood 1.1.0.Beta1 cannot WRITE
 * field ids, so files from the integration test only cover name fallback.
 */
class FooterStatsTest {
    // ---- fixture helpers ---------------------------------------------------

    private fun root(children: Int): SchemaElement =
        SchemaElement("root", null, null, null, children, null, null, null, null, null)

    private fun leaf(
        name: String,
        physical: PhysicalType,
        fieldId: Int? = null,
        logical: LogicalType? = null,
        scale: Int? = null,
        typeLength: Int? = null,
    ): SchemaElement =
        SchemaElement(
            name, physical, typeLength, RepetitionType.OPTIONAL, null,
            null, scale, null, fieldId, logical,
        )

    private fun stats(
        min: ByteArray?,
        max: ByteArray?,
        nulls: Long? = 0L,
        deprecated: Boolean = false,
        nan: Long? = null,
    ): Statistics = Statistics(min, max, nulls, null, deprecated, true, true, nan)

    private fun chunk(
        name: String,
        physical: PhysicalType,
        numValues: Long,
        st: Statistics?,
        compressedSize: Long = 100L,
    ): ColumnChunk =
        ColumnChunk(
            ColumnMetaData(
                physical, listOf(Encoding.PLAIN), FieldPath.of(name),
                CompressionCodec.UNCOMPRESSED, numValues, compressedSize * 2,
                compressedSize, emptyMap(), 4L, null, st, null, null, null, null, null,
            ),
            null,
            null,
            null,
            null,
            null,
        )

    private fun meta(
        schema: List<SchemaElement>,
        numRows: Long,
        vararg groups: List<ColumnChunk>,
    ): FileMetaData =
        FileMetaData(
            2,
            schema,
            numRows,
            groups.map { RowGroup(it, it.sumOf { c -> c.metaData().totalCompressedSize() }, numRows) },
            emptyMap(),
            "test",
            null,
        )

    private fun le(v: Int): ByteArray = ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN).putInt(v).array()

    private fun le(v: Long): ByteArray = ByteBuffer.allocate(8).order(ByteOrder.LITTLE_ENDIAN).putLong(v).array()

    private fun le(v: Double): ByteArray = ByteBuffer.allocate(8).order(ByteOrder.LITTLE_ENDIAN).putDouble(v).array()

    private fun agg(
        meta: FileMetaData,
        vararg cols: CatalogColumn,
    ): Map<Long, FooterStats.ColumnAgg> =
        FooterStats.aggregate(meta, cols.toList(), "s3://t/f.parquet").associateBy { it.fieldId }

    // ---- mapping -----------------------------------------------------------

    @Test
    fun `parquet field ids beat name matching`() {
        // Parquet column is named "a_old"; the catalog renamed it to "a".
        // Field id 7 must carry the mapping.
        val m =
            meta(
                listOf(
                    root(2),
                    leaf("a_old", PhysicalType.INT64, fieldId = 7),
                    leaf("b", PhysicalType.INT64, fieldId = 8),
                ),
                10,
                listOf(
                    chunk("a_old", PhysicalType.INT64, 10, stats(le(5L), le(9L))),
                    chunk("b", PhysicalType.INT64, 10, stats(le(1L), le(2L))),
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
        val m =
            meta(
                listOf(root(1), leaf("a", PhysicalType.INT64)),
                10,
                listOf(chunk("a", PhysicalType.INT64, 10, stats(le(-2L), le(4L), nulls = 3))),
            )
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
        val m =
            meta(
                listOf(root(1), leaf("a", PhysicalType.INT64)),
                5,
                listOf(chunk("a", PhysicalType.INT64, 5, stats(le(0L), le(1L)))),
            )
        val out =
            agg(
                m,
                CatalogColumn(1, "a", ColType.LONG, null),
                CatalogColumn(2, "added_later", ColType.STRING, null),
            )
        assertThat(out).containsOnlyKeys(1L)
    }

    // ---- multi row-group aggregation ---------------------------------------

    @Test
    fun `merges counts and bounds across row groups`() {
        val m =
            meta(
                listOf(
                    root(2),
                    leaf("n", PhysicalType.INT64),
                    leaf("s", PhysicalType.BYTE_ARRAY, logical = LogicalType.StringType()),
                ),
                25,
                listOf(
                    chunk("n", PhysicalType.INT64, 10, stats(le(5L), le(10L), nulls = 1), compressedSize = 40),
                    chunk("s", PhysicalType.BYTE_ARRAY, 10, stats("banana".toByteArray(), "cherry".toByteArray())),
                ),
                listOf(
                    chunk("n", PhysicalType.INT64, 15, stats(le(-3L), le(7L), nulls = 2), compressedSize = 60),
                    chunk("s", PhysicalType.BYTE_ARRAY, 15, stats("apple".toByteArray(), "candy".toByteArray())),
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
    fun `deprecated min-max keeps counts but drops bounds`() {
        val m =
            meta(
                listOf(root(1), leaf("a", PhysicalType.INT64)),
                10,
                listOf(chunk("a", PhysicalType.INT64, 10, stats(le(1L), le(2L), nulls = 4, deprecated = true))),
            )
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
        val m =
            meta(
                listOf(root(1), leaf("a", PhysicalType.INT64)),
                20,
                listOf(chunk("a", PhysicalType.INT64, 10, stats(le(1L), le(2L), nulls = 0))),
                listOf(chunk("a", PhysicalType.INT64, 10, stats(null, null, nulls = 0))),
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
        val m =
            meta(
                listOf(root(1), leaf("a", PhysicalType.INT64)),
                10,
                listOf(chunk("a", PhysicalType.INT64, 10, stats(le(1L), le(2L), nulls = null))),
            )
        assertThat(agg(m, CatalogColumn(1, "a", ColType.LONG, null))).isEmpty()
    }

    @Test
    fun `NaN bounds are never written`() {
        val m =
            meta(
                listOf(root(1), leaf("d", PhysicalType.DOUBLE)),
                10,
                listOf(chunk("d", PhysicalType.DOUBLE, 10, stats(le(Double.NaN), le(5.0)))),
            )
        val out = agg(m, CatalogColumn(1, "d", ColType.DOUBLE, null))
        with(out[1L]!!) {
            assertThat(lowerBound).isNull()
            assertThat(upperBound).isNull()
        }
    }

    @Test
    fun `physical type mismatch keeps counts but drops bounds`() {
        val m =
            meta(
                listOf(root(1), leaf("a", PhysicalType.BYTE_ARRAY)),
                10,
                listOf(chunk("a", PhysicalType.BYTE_ARRAY, 10, stats(le(1L), le(2L)))),
            )
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
        val m =
            meta(
                listOf(root(1), leaf("a", PhysicalType.INT32)),
                10,
                listOf(chunk("a", PhysicalType.INT32, 10, stats(le(-7), le(9)))),
            )
        val out = agg(m, CatalogColumn(1, "a", ColType.LONG, null))
        assertThat(out[1L]!!.lowerBound).isEqualTo(le(-7L))
        assertThat(out[1L]!!.upperBound).isEqualTo(le(9L))
    }

    @Test
    fun `timestamp millis convert exactly to micros`() {
        val m =
            meta(
                listOf(
                    root(1),
                    leaf(
                        "ts",
                        PhysicalType.INT64,
                        logical = LogicalType.TimestampType(true, LogicalType.TimeUnit.MILLIS),
                    ),
                ),
                10,
                listOf(chunk("ts", PhysicalType.INT64, 10, stats(le(1_000L), le(2_500L)))),
            )
        val out = agg(m, CatalogColumn(1, "ts", ColType.TIMESTAMPTZ, null))
        assertThat(out[1L]!!.lowerBound).isEqualTo(le(1_000_000L))
        assertThat(out[1L]!!.upperBound).isEqualTo(le(2_500_000L))
    }

    @Test
    fun `timestamp nanos floor the lower bound and ceil the upper`() {
        val m =
            meta(
                listOf(
                    root(1),
                    leaf(
                        "ts",
                        PhysicalType.INT64,
                        logical = LogicalType.TimestampType(true, LogicalType.TimeUnit.NANOS),
                    ),
                ),
                10,
                listOf(chunk("ts", PhysicalType.INT64, 10, stats(le(1_500L), le(2_500L)))),
            )
        val out = agg(m, CatalogColumn(1, "ts", ColType.TIMESTAMPTZ, null))
        assertThat(out[1L]!!.lowerBound).isEqualTo(le(1L))
        assertThat(out[1L]!!.upperBound).isEqualTo(le(3L))
    }

    @Test
    fun `timestamp with unknown unit drops bounds`() {
        val m =
            meta(
                listOf(root(1), leaf("ts", PhysicalType.INT64)),
                10,
                listOf(chunk("ts", PhysicalType.INT64, 10, stats(le(1L), le(2L)))),
            )
        val out = agg(m, CatalogColumn(1, "ts", ColType.TIMESTAMPTZ, null))
        assertThat(out[1L]!!.lowerBound).isNull()
    }

    @Test
    fun `decimal byte-array bounds pass through at matching scale`() {
        val m =
            meta(
                listOf(
                    root(1),
                    leaf("d", PhysicalType.BYTE_ARRAY, logical = LogicalType.DecimalType(2, 10)),
                ),
                10,
                listOf(
                    chunk(
                        "d",
                        PhysicalType.BYTE_ARRAY,
                        10,
                        // -0.01 .. 14.20
                        stats(byteArrayOf(-1), byteArrayOf(0x05, 0x8C.toByte())),
                    ),
                ),
            )
        val out = agg(m, CatalogColumn(1, "d", ColType.DECIMAL, decimalScale = 2))
        assertThat(out[1L]!!.lowerBound).isEqualTo(byteArrayOf(-1))
        assertThat(out[1L]!!.upperBound).isEqualTo(byteArrayOf(0x05, 0x8C.toByte()))
    }

    @Test
    fun `decimal scale mismatch drops bounds`() {
        val m =
            meta(
                listOf(
                    root(1),
                    leaf("d", PhysicalType.BYTE_ARRAY, logical = LogicalType.DecimalType(2, 10)),
                ),
                10,
                listOf(chunk("d", PhysicalType.BYTE_ARRAY, 10, stats(byteArrayOf(1), byteArrayOf(2)))),
            )
        val out = agg(m, CatalogColumn(1, "d", ColType.DECIMAL, decimalScale = 3))
        assertThat(out[1L]!!.lowerBound).isNull()
        assertThat(out[1L]!!.upperBound).isNull()
    }

    @Test
    fun `uuid fixed16 bounds pass through`() {
        val lo = ByteArray(16) { 0x00 }
        val hi = ByteArray(16) { 0xAB.toByte() }
        val m =
            meta(
                listOf(
                    root(1),
                    leaf("u", PhysicalType.FIXED_LEN_BYTE_ARRAY, logical = LogicalType.UuidType(), typeLength = 16),
                ),
                10,
                listOf(chunk("u", PhysicalType.FIXED_LEN_BYTE_ARRAY, 10, stats(lo, hi))),
            )
        val out = agg(m, CatalogColumn(1, "u", ColType.UUID_T, null))
        assertThat(out[1L]!!.lowerBound).isEqualTo(lo)
        assertThat(out[1L]!!.upperBound).isEqualTo(hi)
    }

    @Test
    fun `string bounds compare as unsigned bytes`() {
        // 0xC2 0xB5 (µ) must sort above ASCII despite the negative signed byte.
        val m =
            meta(
                listOf(root(1), leaf("s", PhysicalType.BYTE_ARRAY, logical = LogicalType.StringType())),
                10,
                listOf(chunk("s", PhysicalType.BYTE_ARRAY, 5, stats("a".toByteArray(), "µ".toByteArray()))),
                listOf(chunk("s", PhysicalType.BYTE_ARRAY, 5, stats("b".toByteArray(), "z".toByteArray()))),
            )
        val out = agg(m, CatalogColumn(1, "s", ColType.STRING, null))
        assertThat(out[1L]!!.lowerBound).isEqualTo("a".toByteArray())
        assertThat(out[1L]!!.upperBound).isEqualTo("µ".toByteArray())
    }

    @Test
    fun `nested leaves are ignored, top-level ones still map`() {
        // root { a: int64, g: group { x: int64 } }
        val schema =
            listOf(
                root(2),
                leaf("a", PhysicalType.INT64),
                SchemaElement("g", null, null, RepetitionType.OPTIONAL, 1, null, null, null, null, null),
                leaf("x", PhysicalType.INT64),
            )
        val m =
            meta(
                schema,
                10,
                listOf(
                    chunk("a", PhysicalType.INT64, 10, stats(le(1L), le(2L))),
                    ColumnChunk(
                        ColumnMetaData(
                            PhysicalType.INT64, listOf(Encoding.PLAIN), FieldPath.of("g", "x"),
                            CompressionCodec.UNCOMPRESSED, 10, 10, 10, emptyMap(), 4L,
                            null, stats(le(9L), le(9L)), null, null, null, null, null,
                        ),
                        null,
                        null,
                        null,
                        null,
                        null,
                    ),
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
