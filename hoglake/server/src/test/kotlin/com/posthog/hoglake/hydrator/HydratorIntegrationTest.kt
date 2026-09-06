package com.posthog.hoglake.hydrator

import com.posthog.hoglake.stats.IcebergSingleValue
import com.posthog.hoglake.testing.PgTestSupport
import dev.hardwood.OutputFile
import dev.hardwood.metadata.LogicalType
import dev.hardwood.metadata.PhysicalType
import dev.hardwood.metadata.RepetitionType
import dev.hardwood.schema.FileSchema
import dev.hardwood.writer.ParquetFileWriter
import dev.hardwood.writer.WriterConfig
import org.assertj.core.api.Assertions.assertThat
import org.awaitility.Awaitility.await
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.testcontainers.containers.MinIOContainer
import java.nio.ByteBuffer
import java.nio.ByteOrder
import java.nio.file.Files
import java.time.Duration
import java.time.Instant

/**
 * End-to-end hydrator test: a real parquet file written with Hardwood,
 * uploaded to MinIO, registered as a 'pending' hog_data_file, hydrated,
 * and verified against the known column statistics.
 *
 * Hardwood 1.1.0.Beta1 does not write PARQUET:field_id, so these files
 * exercise the name-matching fallback; the field-id path is covered by
 * [FooterStatsTest] against hand-built footers.
 */
@Tag("integration")
class HydratorIntegrationTest {
    private val db = PgTestSupport.freshDatabase()
    private val jdbi get() = db.jdbi
    private val hydrator by lazy { Hydrator(jdbi, store) }

    // ---- known file content ------------------------------------------------
    //
    // 25 rows, row groups of <= 10 rows (so >= 3 groups):
    //   id    (long, required): 0..24
    //   score (double)        : null when i % 5 == 0 (5 nulls), else i * 1.5
    //   name  (string)        : null when i == 13 (1 null), else "row-%02d"
    //   ts    (timestamptz)   : epoch second 1_700_000_000 + i, micros precision

    private companion object {
        const val ROWS = 25
        const val BUCKET = "hoglake-test"
        const val EPOCH0 = 1_700_000_000L

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

        val parquetBytes: ByteArray by lazy { writeSampleParquet() }

        /** Thrift footer length, from the 4 LE bytes before the trailing magic. */
        val footerSize: Long by lazy {
            ByteBuffer.wrap(parquetBytes, parquetBytes.size - 8, 4)
                .order(ByteOrder.LITTLE_ENDIAN).int.toLong()
        }

        fun writeSampleParquet(): ByteArray {
            val schema =
                FileSchema.builder("hoglake_test")
                    .addColumn("id", PhysicalType.INT64, RepetitionType.REQUIRED)
                    .addColumn("score", PhysicalType.DOUBLE, RepetitionType.OPTIONAL)
                    .addColumn("name", PhysicalType.BYTE_ARRAY, RepetitionType.OPTIONAL, LogicalType.StringType())
                    .addColumn(
                        "ts",
                        PhysicalType.INT64,
                        RepetitionType.OPTIONAL,
                        LogicalType.TimestampType(true, LogicalType.TimeUnit.MICROS),
                    )
                    .build()
            val tmp = Files.createTempFile("hoglake-hydrator", ".parquet")
            try {
                ParquetFileWriter.create(
                    OutputFile.of(tmp),
                    schema,
                    WriterConfig.builder().rowGroupTargetRows(10).build(),
                ).use { writer ->
                    val rows = writer.rowWriter()
                    for (i in 0 until ROWS) {
                        rows.writeRow { r ->
                            r.setLong("id", i.toLong())
                            if (i % 5 == 0) r.setNull("score") else r.setDouble("score", i * 1.5)
                            if (i == 13) r.setNull("name") else r.setString("name", "row-%02d".format(i))
                            r.setTimestamp("ts", Instant.ofEpochSecond(EPOCH0 + i))
                        }
                    }
                }
                return Files.readAllBytes(tmp)
            } finally {
                Files.deleteIfExists(tmp)
            }
        }
    }

    // ---- catalog seeding ---------------------------------------------------

    private fun seedCatalogAndTable(): Long {
        val catalogId =
            jdbi.withHandle<Long, Exception> { h ->
                h.createQuery(
                    "INSERT INTO hog_catalog (name, data_path) VALUES ('cat', 's3://$BUCKET/') RETURNING catalog_id",
                ).mapTo(Long::class.java).one()
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

    // ---- tests -------------------------------------------------------------

    @Test
    fun `hydrates a pending file end to end`() {
        val catalogId = seedCatalogAndTable()
        val path = "s3://$BUCKET/t1/good.parquet"
        store.put(path, parquetBytes)
        // footer_size seeded -> exercises the ranged tail read.
        seedDataFile(catalogId, 1, path, ROWS.toLong(), parquetBytes.size.toLong(), footerSize)

        assertThat(hydrator.runOnce()).isEqualTo(1)
        assertThat(statsState(catalogId, 1)).isEqualTo("provided")

        val stats = statsRows(catalogId, 1)
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

        // Nothing left pending.
        assertThat(hydrator.runOnce()).isEqualTo(0)
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
        // interval <= 0: nothing starts, close is safe.
        hydrator.startLoop(0).close()
        hydrator.startLoop(-1).close()

        val catalogId = seedCatalogAndTable()
        val path = "s3://$BUCKET/t1/loop.parquet"
        store.put(path, parquetBytes)
        seedDataFile(catalogId, 1, path, ROWS.toLong(), parquetBytes.size.toLong(), footerSize)

        hydrator.startLoop(50).use {
            await().atMost(Duration.ofSeconds(30)).untilAsserted {
                assertThat(statsState(catalogId, 1)).isEqualTo("provided")
            }
        }
    }
}
