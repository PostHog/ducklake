package com.posthog.hoglake.hydrator

import com.posthog.hoglake.stats.IcebergSingleValue
import com.posthog.hoglake.testing.PgTestSupport
import org.apache.parquet.example.data.simple.SimpleGroupFactory
import org.apache.parquet.hadoop.example.ExampleParquetWriter
import org.apache.parquet.hadoop.metadata.CompressionCodecName
import org.apache.parquet.io.LocalOutputFile
import org.apache.parquet.schema.LogicalTypeAnnotation
import org.apache.parquet.schema.MessageType
import org.apache.parquet.schema.PrimitiveType
import org.apache.parquet.schema.Types
import org.assertj.core.api.Assertions.assertThat
import org.awaitility.Awaitility.await
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.testcontainers.containers.MinIOContainer
import java.nio.ByteBuffer
import java.nio.ByteOrder
import java.nio.file.Files
import java.time.Duration

/**
 * End-to-end hydrator test: real parquet files written with parquet-java
 * (field ids included — the registration contract), uploaded to MinIO,
 * registered as 'pending' hog_data_file rows, hydrated, and verified
 * against the known column statistics. Also the field-id contract: a
 * file whose schema lacks ids hydrates via name fallback but is flagged
 * missing_field_ids; the reserved `_hog_row_id` id never trips it.
 */
@Tag("integration")
class HydratorIntegrationTest {
    private val db = PgTestSupport.freshDatabase()
    private val jdbi get() = db.jdbi
    private val hydrator by lazy { Hydrator(jdbi, store) }

    // ---- known file content ------------------------------------------------
    //
    // 25 rows:
    //   id    (long, required, field id 1): 0..24
    //   score (double, id 2)              : null when i % 5 == 0 (5 nulls), else i * 1.5
    //   name  (string, id 3)              : null when i == 13 (1 null), else "row-%02d"
    //   ts    (timestamptz, id 4)         : epoch second 1_700_000_000 + i, micros

    private companion object {
        const val ROWS = 25
        const val BUCKET = "hoglake-test"
        const val EPOCH0 = 1_700_000_000L
        const val ROW_ID_FIELD_ID = 2147483646

        val minio: MinIOContainer by lazy {
            MinIOContainer("minio/minio:RELEASE.2023-09-04T19-57-37Z").also { it.start() }
        }

        val store: ObjectStore by lazy {
            ObjectStore(
                endpoint = minio.s3URL,
                region = "us-east-1",
                accessKey = minio.userName,
                secretKey = minio.password,
                pathStyle = true,
            ).also { it.createBucket(BUCKET) }
        }

        /** The sample schema; [withIds] false drops every field id. */
        fun sampleSchema(withIds: Boolean): MessageType {
            fun Types.PrimitiveBuilder<PrimitiveType>.maybeId(id: Int) = if (withIds) id(id) else this

            return MessageType(
                "hoglake_test",
                Types.required(PrimitiveType.PrimitiveTypeName.INT64).maybeId(1).named("id"),
                Types.optional(PrimitiveType.PrimitiveTypeName.DOUBLE).maybeId(2).named("score"),
                Types.optional(PrimitiveType.PrimitiveTypeName.BINARY)
                    .`as`(LogicalTypeAnnotation.stringType()).maybeId(3).named("name"),
                Types.optional(PrimitiveType.PrimitiveTypeName.INT64)
                    .`as`(LogicalTypeAnnotation.timestampType(true, LogicalTypeAnnotation.TimeUnit.MICROS))
                    .maybeId(4)
                    .named("ts"),
            )
        }

        fun writeSampleParquet(schema: MessageType): ByteArray {
            val tmp = Files.createTempFile("hoglake-hydrator", ".parquet")
            try {
                Files.deleteIfExists(tmp)
                val factory = SimpleGroupFactory(schema)
                ExampleParquetWriter.builder(LocalOutputFile(tmp))
                    .withType(schema)
                    .withCompressionCodec(CompressionCodecName.UNCOMPRESSED)
                    .build()
                    .use { writer ->
                        for (i in 0 until ROWS) {
                            val g = factory.newGroup()
                            g.add("id", i.toLong())
                            if (i % 5 != 0) g.add("score", i * 1.5)
                            if (i != 13) g.add("name", "row-%02d".format(i))
                            g.add("ts", (EPOCH0 + i) * 1_000_000L)
                            writer.write(g)
                        }
                    }
                return Files.readAllBytes(tmp)
            } finally {
                Files.deleteIfExists(tmp)
            }
        }

        val parquetBytes: ByteArray by lazy { writeSampleParquet(sampleSchema(withIds = true)) }

        /** Thrift footer length, from the 4 LE bytes before the trailing magic. */
        fun footerSizeOf(bytes: ByteArray): Long =
            ByteBuffer.wrap(bytes, bytes.size - 8, 4)
                .order(ByteOrder.LITTLE_ENDIAN).int.toLong()

        val footerSize: Long by lazy { footerSizeOf(parquetBytes) }
    }

    // ---- catalog seeding ---------------------------------------------------

    private var catalogSeq = 0

    private fun seedCatalogAndTable(): Long {
        val name = "cat${catalogSeq++}"
        val catalogId =
            jdbi.withHandle<Long, Exception> { h ->
                h.createQuery(
                    "INSERT INTO hog_catalog (name, data_path) VALUES (:n, 's3://$BUCKET/') RETURNING catalog_id",
                ).bind("n", name).mapTo(Long::class.java).one()
            }
        jdbi.useHandle<Exception> { h ->
            h.execute(
                "INSERT INTO hog_namespace (catalog_id, namespace_id, name) VALUES (?, 1, 'ns')",
                catalogId,
            )
            h.execute(
                "INSERT INTO hog_table (catalog_id, table_id, created_snapshot) VALUES (?, 1, 1)",
                catalogId,
            )
            var ordinal = 0
            for ((fieldId, spec) in listOf(
                1L to ("id" to "long"),
                2L to ("score" to "double"),
                3L to ("name" to "string"),
                4L to ("ts" to "timestamptz"),
            )) {
                h.execute(
                    """
                    INSERT INTO hog_column
                        (catalog_id, table_id, field_id, begin_snapshot, name, col_type, ordinal)
                    VALUES (?, 1, ?, 1, ?, ?, ?)
                    """,
                    catalogId,
                    fieldId,
                    spec.first,
                    spec.second,
                    ordinal++,
                )
            }
        }
        return catalogId
    }

    private fun seedDataFile(
        catalogId: Long,
        dataFileId: Long,
        path: String,
        recordCount: Long,
        fileSizeBytes: Long,
        footerSize: Long?,
    ) {
        jdbi.useHandle<Exception> { h ->
            h.execute(
                """
                INSERT INTO hog_data_file
                    (catalog_id, data_file_id, table_id, begin_snapshot, path,
                     record_count, file_size_bytes, footer_size, row_id_start, stats_state)
                VALUES (?, ?, 1, 1, ?, ?, ?, ?, 0, 'pending')
                """,
                catalogId,
                dataFileId,
                path,
                recordCount,
                fileSizeBytes,
                footerSize,
            )
        }
    }

    private fun statsState(
        catalogId: Long,
        dataFileId: Long,
    ): String =
        jdbi.withHandle<String, Exception> { h ->
            h.createQuery(
                "SELECT stats_state FROM hog_data_file WHERE catalog_id = ? AND data_file_id = ?",
            ).bind(0, catalogId).bind(1, dataFileId).mapTo(String::class.java).one()
        }

    private fun missingFieldIds(
        catalogId: Long,
        dataFileId: Long,
    ): Boolean =
        jdbi.withHandle<Boolean, Exception> { h ->
            h.createQuery(
                "SELECT missing_field_ids FROM hog_data_file WHERE catalog_id = ? AND data_file_id = ?",
            ).bind(0, catalogId).bind(1, dataFileId).mapTo(Boolean::class.java).one()
        }

    private data class StatsRow(
        val valueCount: Long,
        val nullCount: Long,
        val nanCount: Long?,
        val sizeBytes: Long?,
        val lower: ByteArray?,
        val upper: ByteArray?,
    )

    private fun statsRows(
        catalogId: Long,
        dataFileId: Long,
    ): Map<Long, StatsRow> =
        jdbi.withHandle<Map<Long, StatsRow>, Exception> { h ->
            h.createQuery(
                """
                SELECT field_id, value_count, null_count, nan_count, size_bytes,
                       lower_bound, upper_bound
                FROM hog_file_column_stats
                WHERE catalog_id = ? AND data_file_id = ?
                """,
            ).bind(0, catalogId).bind(1, dataFileId)
                .map { rs, _ ->
                    rs.getLong("field_id") to
                        StatsRow(
                            valueCount = rs.getLong("value_count"),
                            nullCount = rs.getLong("null_count"),
                            nanCount = rs.getObject("nan_count", java.lang.Long::class.java)?.toLong(),
                            sizeBytes = rs.getObject("size_bytes", java.lang.Long::class.java)?.toLong(),
                            lower = rs.getBytes("lower_bound"),
                            upper = rs.getBytes("upper_bound"),
                        )
                }
                .list().toMap()
        }

    private fun assertSampleStats(
        catalogId: Long,
        dataFileId: Long,
    ) {
        val stats = statsRows(catalogId, dataFileId)
        assertThat(stats).containsOnlyKeys(1L, 2L, 3L, 4L)

        with(stats[1L]!!) { // id: 0..24, no nulls
            assertThat(valueCount).isEqualTo(25)
            assertThat(nullCount).isEqualTo(0)
            assertThat(sizeBytes).isGreaterThan(0)
            assertThat(lower).isEqualTo(IcebergSingleValue.encodeLong(0))
            assertThat(upper).isEqualTo(IcebergSingleValue.encodeLong(24))
        }
        with(stats[2L]!!) { // score: 5 nulls, min 1.5, max 36.0
            assertThat(valueCount).isEqualTo(25)
            assertThat(nullCount).isEqualTo(5)
            assertThat(lower).isEqualTo(IcebergSingleValue.encodeDouble(1.5))
            assertThat(upper).isEqualTo(IcebergSingleValue.encodeDouble(36.0))
        }
        with(stats[3L]!!) { // name: 1 null, "row-00".."row-24"
            assertThat(valueCount).isEqualTo(25)
            assertThat(nullCount).isEqualTo(1)
            assertThat(lower).isEqualTo(IcebergSingleValue.encodeString("row-00"))
            assertThat(upper).isEqualTo(IcebergSingleValue.encodeString("row-24"))
        }
        with(stats[4L]!!) { // ts: micros since epoch
            assertThat(valueCount).isEqualTo(25)
            assertThat(nullCount).isEqualTo(0)
            assertThat(lower).isEqualTo(IcebergSingleValue.encodeTimestampMicros(EPOCH0 * 1_000_000L))
            assertThat(upper).isEqualTo(IcebergSingleValue.encodeTimestampMicros((EPOCH0 + 24) * 1_000_000L))
        }
    }

    // ---- tests -------------------------------------------------------------

    @Test
    fun `hydrates a pending file end to end - field ids present so the flag stays false`() {
        val catalogId = seedCatalogAndTable()
        val path = "s3://$BUCKET/t1/good.parquet"
        store.put(path, parquetBytes)
        // footer_size seeded -> exercises the ranged tail read.
        seedDataFile(catalogId, 1, path, ROWS.toLong(), parquetBytes.size.toLong(), footerSize)

        assertThat(hydrator.runOnce()).isEqualTo(1)
        assertThat(statsState(catalogId, 1)).isEqualTo("provided")
        assertThat(missingFieldIds(catalogId, 1)).isFalse()
        assertSampleStats(catalogId, 1)

        // Nothing left pending.
        assertThat(hydrator.runOnce()).isEqualTo(0)
    }

    @Test
    fun `a file without field ids hydrates by name fallback but is flagged`() {
        val catalogId = seedCatalogAndTable()
        val idless = writeSampleParquet(sampleSchema(withIds = false))
        val path = "s3://$BUCKET/t1/idless.parquet"
        store.put(path, idless)
        seedDataFile(catalogId, 1, path, ROWS.toLong(), idless.size.toLong(), footerSizeOf(idless))

        assertThat(hydrator.runOnce()).isEqualTo(1)
        assertThat(statsState(catalogId, 1)).isEqualTo("provided")
        assertThat(missingFieldIds(catalogId, 1)).isTrue()
        // Name fallback still produced honest stats.
        assertSampleStats(catalogId, 1)
    }

    @Test
    fun `the reserved _hog_row_id field id does not trip the flag`() {
        val catalogId = seedCatalogAndTable()
        // A compacted-style file: the sample schema (with ids) plus the
        // explicit row-id column under the reserved id.
        val schema =
            MessageType(
                "hoglake_test",
                sampleSchema(withIds = true).fields +
                    Types.required(PrimitiveType.PrimitiveTypeName.INT64)
                        .id(ROW_ID_FIELD_ID)
                        .named("_hog_row_id"),
            )
        val factory = SimpleGroupFactory(schema)
        val tmp = Files.createTempFile("hoglake-rowid", ".parquet")
        Files.deleteIfExists(tmp)
        ExampleParquetWriter.builder(LocalOutputFile(tmp))
            .withType(schema)
            .withCompressionCodec(CompressionCodecName.UNCOMPRESSED)
            .build()
            .use { writer ->
                for (i in 0 until ROWS) {
                    val g = factory.newGroup()
                    g.add("id", i.toLong())
                    if (i % 5 != 0) g.add("score", i * 1.5)
                    if (i != 13) g.add("name", "row-%02d".format(i))
                    g.add("ts", (EPOCH0 + i) * 1_000_000L)
                    g.add("_hog_row_id", 1000L + i)
                    writer.write(g)
                }
            }
        val bytes = Files.readAllBytes(tmp)
        Files.deleteIfExists(tmp)

        val path = "s3://$BUCKET/t1/compacted.parquet"
        store.put(path, bytes)
        seedDataFile(catalogId, 1, path, ROWS.toLong(), bytes.size.toLong(), footerSizeOf(bytes))

        assertThat(hydrator.runOnce()).isEqualTo(1)
        assertThat(statsState(catalogId, 1)).isEqualTo("provided")
        assertThat(missingFieldIds(catalogId, 1)).isFalse()
    }

    @Test
    fun `record count mismatch fails the file and writes no stats`() {
        val catalogId = seedCatalogAndTable()
        val path = "s3://$BUCKET/t1/mismatch.parquet"
        store.put(path, parquetBytes)
        // Registration lied: claims 26 rows, footer says 25.
        seedDataFile(catalogId, 1, path, ROWS + 1L, parquetBytes.size.toLong(), null)

        assertThat(hydrator.runOnce()).isEqualTo(1)
        assertThat(statsState(catalogId, 1)).isEqualTo("failed")
        assertThat(statsRows(catalogId, 1)).isEmpty()
    }

    @Test
    fun `a missing object fails without wedging the next file`() {
        val catalogId = seedCatalogAndTable()
        val goodPath = "s3://$BUCKET/t1/after-missing.parquet"
        store.put(goodPath, parquetBytes)
        // data_file_id 1 (processed first) points at nothing.
        seedDataFile(catalogId, 1, "s3://$BUCKET/t1/nope.parquet", ROWS.toLong(), parquetBytes.size.toLong(), null)
        seedDataFile(catalogId, 2, goodPath, ROWS.toLong(), parquetBytes.size.toLong(), null)

        assertThat(hydrator.runOnce()).isEqualTo(2)
        assertThat(statsState(catalogId, 1)).isEqualTo("failed")
        assertThat(statsRows(catalogId, 1)).isEmpty()
        assertThat(statsState(catalogId, 2)).isEqualTo("provided")
        assertThat(statsRows(catalogId, 2)).containsOnlyKeys(1L, 2L, 3L, 4L)
    }

    @Test
    fun `background loop hydrates and a non-positive interval is a no-op`() {
        // interval <= 0: nothing registers, close is safe.
        com.posthog.hoglake.BackgroundLoops().use { it.register("hydrator", 0) { hydrator.runOnce() } }
        com.posthog.hoglake.BackgroundLoops().use { it.register("hydrator", -1) { hydrator.runOnce() } }

        val catalogId = seedCatalogAndTable()
        val path = "s3://$BUCKET/t1/loop.parquet"
        store.put(path, parquetBytes)
        seedDataFile(catalogId, 1, path, ROWS.toLong(), parquetBytes.size.toLong(), footerSize)

        com.posthog.hoglake.BackgroundLoops().use { loops ->
            loops.register("hydrator", 50) { hydrator.runOnce() }
            await().atMost(Duration.ofSeconds(30)).untilAsserted {
                assertThat(statsState(catalogId, 1)).isEqualTo("provided")
            }
        }
    }
}
