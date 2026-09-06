package com.posthog.hoglake.compaction

import com.posthog.hoglake.model.NullOrder
import com.posthog.hoglake.model.SortDirection
import com.posthog.hoglake.model.SortFieldDef
import org.apache.parquet.example.data.Group
import org.apache.parquet.example.data.simple.SimpleGroupFactory
import org.apache.parquet.example.data.simple.convert.GroupRecordConverter
import org.apache.parquet.hadoop.ParquetFileReader
import org.apache.parquet.hadoop.example.ExampleParquetWriter
import org.apache.parquet.hadoop.metadata.CompressionCodecName
import org.apache.parquet.io.ColumnIOFactory
import org.apache.parquet.io.LocalInputFile
import org.apache.parquet.io.LocalOutputFile
import org.apache.parquet.schema.LogicalTypeAnnotation
import org.apache.parquet.schema.MessageType
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName
import org.apache.parquet.schema.Types
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import java.nio.file.Files
import java.nio.file.Path

/**
 * ParquetRewriter unit coverage against real local parquet files (no
 * containers): explicit row-id materialization across NON-ADJACENT
 * inputs, sort-order application with null placement, field ids on
 * every output column (footer re-read), the id fallback for inputs
 * without embedded ids, and the re-compaction path (an input that
 * already carries _hog_row_id keeps its ids).
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class ParquetRewriterTest {
    private val tmp: Path = Files.createTempDirectory("rewriter-test")

    @AfterAll
    fun tearDown() {
        tmp.toFile().deleteRecursively()
    }

    // id long (field 1), name string optional (2), score double optional (3)
    private fun schema(withIds: Boolean): MessageType {
        fun <T : Types.Builder<T, out org.apache.parquet.schema.Type>> T.maybeId(id: Int): T =
            if (withIds) this.id(id) else this
        return Types.buildMessage()
            .addField(Types.required(PrimitiveTypeName.INT64).maybeId(1).named("id"))
            .addField(
                Types.optional(PrimitiveTypeName.BINARY)
                    .`as`(LogicalTypeAnnotation.stringType()).maybeId(2).named("name"),
            )
            .addField(Types.optional(PrimitiveTypeName.DOUBLE).maybeId(3).named("score"))
            .named("t")
    }

    private data class TestRow(val id: Long, val name: String?, val score: Double?)

    private fun writeInput(
        fileName: String,
        rows: List<TestRow>,
        withIds: Boolean = true,
    ): Path {
        val path = tmp.resolve(fileName)
        val schema = schema(withIds)
        val factory = SimpleGroupFactory(schema)
        ExampleParquetWriter.builder(LocalOutputFile(path))
            .withType(schema)
            .withCompressionCodec(CompressionCodecName.UNCOMPRESSED)
            .build()
            .use { w ->
                for (r in rows) {
                    val g = factory.newGroup()
                    g.add("id", r.id)
                    r.name?.let { g.add("name", it) }
                    r.score?.let { g.add("score", it) }
                    w.write(g)
                }
            }
        return path
    }

    private data class OutRow(val id: Long, val name: String?, val score: Double?, val rowId: Long)

    private fun readOutput(path: Path): Pair<MessageType, List<OutRow>> {
        val out = mutableListOf<OutRow>()
        lateinit var schema: MessageType
        ParquetFileReader.open(LocalInputFile(path)).use { reader ->
            schema = reader.footer.fileMetaData.schema
            val columnIO = ColumnIOFactory().getColumnIO(schema)
            var pages = reader.readNextRowGroup()
            while (pages != null) {
                val rr = columnIO.getRecordReader(pages, GroupRecordConverter(schema))
                repeat(Math.toIntExact(pages.rowCount)) {
                    val g: Group = rr.read()
                    out +=
                        OutRow(
                            id = g.getLong(schema.getFieldIndex("id"), 0),
                            name =
                                if (g.getFieldRepetitionCount(schema.getFieldIndex("name")) == 0) {
                                    null
                                } else {
                                    g.getString(schema.getFieldIndex("name"), 0)
                                },
                            score =
                                if (g.getFieldRepetitionCount(schema.getFieldIndex("score")) == 0) {
                                    null
                                } else {
                                    g.getDouble(schema.getFieldIndex("score"), 0)
                                },
                            rowId = g.getLong(schema.getFieldIndex(ParquetRewriter.ROW_ID_COLUMN), 0),
                        )
                }
                pages = reader.readNextRowGroup()
            }
        }
        return schema to out
    }

    private val fallback = mapOf("id" to 1L, "name" to 2L, "score" to 3L)

    @Test
    fun `merges non-adjacent inputs preserving input row ids in the explicit column`() {
        // Row-id ranges 0..2 and 10..12 — deliberately NOT adjacent.
        val a =
            writeInput("a.parquet", listOf(TestRow(100, "x", 1.0), TestRow(101, null, 2.0), TestRow(102, "z", null)))
        val b = writeInput("b.parquet", listOf(TestRow(200, "m", 3.0), TestRow(201, "n", 4.0), TestRow(202, "o", 5.0)))
        val out = tmp.resolve("out1.parquet")
        val written =
            ParquetRewriter.rewrite(
                listOf(ParquetRewriter.Input(a, 0), ParquetRewriter.Input(b, 10)),
                fallback,
                emptyList(),
                out,
            )
        assertThat(written).isEqualTo(6)
        val (schema, rows) = readOutput(out)
        assertThat(rows.map { it.rowId }).containsExactly(0L, 1L, 2L, 10L, 11L, 12L)
        assertThat(rows.map { it.id }).containsExactly(100L, 101L, 102L, 200L, 201L, 202L)
        assertThat(rows[1].name).isNull()
        assertThat(rows[2].score).isNull()
        // Every column carries its field id; the row-id column the reserved one.
        assertThat(schema.getType("id").id.intValue()).isEqualTo(1)
        assertThat(schema.getType("name").id.intValue()).isEqualTo(2)
        assertThat(schema.getType("score").id.intValue()).isEqualTo(3)
        assertThat(schema.getType(ParquetRewriter.ROW_ID_COLUMN).id.intValue())
            .isEqualTo(ParquetRewriter.ROW_ID_FIELD_ID)
    }

    @Test
    fun `sorts merged rows by the sort spec with null placement, ids ride along`() {
        val a = writeInput("s-a.parquet", listOf(TestRow(1, "e", 5.0), TestRow(2, "d", null), TestRow(3, "c", 1.0)))
        val b = writeInput("s-b.parquet", listOf(TestRow(4, "b", 4.0), TestRow(5, "a", null), TestRow(6, "f", 0.5)))
        val out = tmp.resolve("out2.parquet")
        ParquetRewriter.rewrite(
            listOf(ParquetRewriter.Input(a, 0), ParquetRewriter.Input(b, 100)),
            fallback,
            listOf(SortFieldDef(3, SortDirection.ASC, NullOrder.NULLS_LAST)),
            out,
        )
        val (_, rows) = readOutput(out)
        assertThat(rows.map { it.score }).containsExactly(0.5, 1.0, 4.0, 5.0, null, null)
        // Nulls keep row-id order among themselves (stable sort): 1 then 101.
        assertThat(rows.map { it.rowId }).containsExactly(102L, 2L, 100L, 0L, 1L, 101L)
    }

    @Test
    fun `descending with nulls first`() {
        val a = writeInput("d-a.parquet", listOf(TestRow(1, "a", 1.0), TestRow(2, "b", null), TestRow(3, "c", 9.0)))
        val out = tmp.resolve("out3.parquet")
        ParquetRewriter.rewrite(
            listOf(ParquetRewriter.Input(a, 0)),
            fallback,
            listOf(SortFieldDef(3, SortDirection.DESC, NullOrder.NULLS_FIRST)),
            out,
        )
        val (_, rows) = readOutput(out)
        assertThat(rows.map { it.score }).containsExactly(null, 9.0, 1.0)
    }

    @Test
    fun `inputs without embedded field ids borrow them from the catalog by name`() {
        val a = writeInput("no-ids.parquet", listOf(TestRow(1, "a", 1.0)), withIds = false)
        val out = tmp.resolve("out4.parquet")
        ParquetRewriter.rewrite(listOf(ParquetRewriter.Input(a, 0)), fallback, emptyList(), out)
        val (schema, _) = readOutput(out)
        assertThat(schema.getType("id").id.intValue()).isEqualTo(1)
        assertThat(schema.getType("score").id.intValue()).isEqualTo(3)
    }

    @Test
    fun `a column resolvable neither by id nor by catalog name fails the rewrite`() {
        val a = writeInput("orphan.parquet", listOf(TestRow(1, "a", 1.0)), withIds = false)
        assertThatThrownBy {
            ParquetRewriter.rewrite(
                listOf(ParquetRewriter.Input(a, 0)),
                // name/score deliberately unresolvable:
                mapOf("id" to 1L),
                emptyList(),
                tmp.resolve("out5.parquet"),
            )
        }.isInstanceOf(IllegalArgumentException::class.java)
            .hasMessageContaining("no parquet field id")
    }

    @Test
    fun `re-compacting an explicit-row-id input keeps its ids, never positional`() {
        val a = writeInput("r-a.parquet", listOf(TestRow(1, "a", 1.0), TestRow(2, "b", 2.0)))
        val out1 = tmp.resolve("first.parquet")
        ParquetRewriter.rewrite(listOf(ParquetRewriter.Input(a, 40)), fallback, emptyList(), out1)
        // Second pass: rowIdStart deliberately WRONG (0) — the ids must come
        // from the file's own _hog_row_id column, not position.
        val out2 = tmp.resolve("second.parquet")
        ParquetRewriter.rewrite(listOf(ParquetRewriter.Input(out1, 0)), fallback, emptyList(), out2)
        val (schema, rows) = readOutput(out2)
        assertThat(rows.map { it.rowId }).containsExactly(40L, 41L)
        // And the schema still has exactly one row-id column.
        assertThat(schema.fields.count { it.name == ParquetRewriter.ROW_ID_COLUMN }).isEqualTo(1)
    }

    @Test
    fun `mixed schema vintages are rejected`() {
        val a = writeInput("mix-a.parquet", listOf(TestRow(1, "a", 1.0)))
        val extra =
            Types.buildMessage()
                .addField(Types.required(PrimitiveTypeName.INT64).id(1).named("id"))
                .named("t")
        val b = tmp.resolve("mix-b.parquet")
        ExampleParquetWriter.builder(LocalOutputFile(b))
            .withType(extra)
            .withCompressionCodec(CompressionCodecName.UNCOMPRESSED)
            .build()
            .use { w ->
                val g = SimpleGroupFactory(extra).newGroup()
                g.add("id", 9L)
                w.write(g)
            }
        assertThatThrownBy {
            ParquetRewriter.rewrite(
                listOf(ParquetRewriter.Input(a, 0), ParquetRewriter.Input(b, 10)),
                fallback,
                emptyList(),
                tmp.resolve("out6.parquet"),
            )
        }.isInstanceOf(IllegalArgumentException::class.java)
            .hasMessageContaining("different schema")
    }
}
