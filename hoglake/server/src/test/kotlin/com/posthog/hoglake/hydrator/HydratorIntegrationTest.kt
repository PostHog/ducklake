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

    // Per-method lifecycle mints a fresh database (and pool) per test;
    // closing it is what keeps the shared PG container under
    // max_connections across the whole suite.
    @org.junit.jupiter.api.AfterEach
    fun tearDown() = db.close()

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

    private fun catalogName(catalogId: Long): String =
        jdbi.withHandle<String, Exception> { h ->
            h.createQuery("SELECT name FROM hog_catalog WHERE catalog_id = ?")
                .bind(0, catalogId).mapTo(String::class.java).one()
        }

    @Test
    fun `a transient store failure leaves the file pending and the next sweep hydrates it`() {
        // Pinned regression (bug hunt #8): S3 throttle/connection failures
        // used to flip files to terminal 'failed'. They must stay pending.
        val catalogId = seedCatalogAndTable()
        val path = "s3://$BUCKET/t1/transient.parquet"
        store.put(path, parquetBytes)
        seedDataFile(catalogId, 1, path, ROWS.toLong(), parquetBytes.size.toLong(), footerSize)

        // A store pointing at a closed port: every fetch fails with a
        // connection error — transient by classification.
        ObjectStore(
            endpoint = "http://127.0.0.1:9",
            region = "us-east-1",
            accessKey = "unused",
            secretKey = "unused",
            pathStyle = true,
        ).use { broken ->
            assertThat(Hydrator(jdbi, broken).runOnce()).isEqualTo(1)
        }
        assertThat(statsState(catalogId, 1)).isEqualTo("pending")
        assertThat(statsRows(catalogId, 1)).isEmpty()

        // The condition "clears" (the real store): same row hydrates.
        assertThat(hydrator.runOnce()).isEqualTo(1)
        assertThat(statsState(catalogId, 1)).isEqualTo("provided")
        assertSampleStats(catalogId, 1)
    }

    @Test
    fun `rehydrate flips failed back to pending and the sweep retries them`() {
        val catalogId = seedCatalogAndTable()
        val path = "s3://$BUCKET/t1/rehydrate.parquet"
        store.put(path, parquetBytes)
        // Structural failure: the registration lies about record_count.
        seedDataFile(catalogId, 1, path, ROWS + 1L, parquetBytes.size.toLong(), null)
        assertThat(hydrator.runOnce()).isEqualTo(1)
        assertThat(statsState(catalogId, 1)).isEqualTo("failed")

        // Operator fixes the registration, then requeues the catalog.
        jdbi.useHandle<Exception> { h ->
            h.execute(
                "UPDATE hog_data_file SET record_count = ? WHERE catalog_id = ? AND data_file_id = 1",
                ROWS.toLong(),
                catalogId,
            )
        }
        val result = hydrator.rehydrateFailed(catalogName(catalogId))
        assertThat(result.requeued).isEqualTo(1)
        assertThat(statsState(catalogId, 1)).isEqualTo("pending")
        assertThat(hydrator.runOnce()).isEqualTo(1)
        assertThat(statsState(catalogId, 1)).isEqualTo("provided")
    }

    @Test
    fun `rehydrate table scope hits only that table and half a scope is a validation`() {
        val catalogId = seedCatalogAndTable()
        // A second table with its own failed file, plus a live version row
        // so the name resolves.
        jdbi.useHandle<Exception> { h ->
            h.execute(
                "INSERT INTO hog_table (catalog_id, table_id, created_snapshot) VALUES (?, 2, 1)",
                catalogId,
            )
            h.execute(
                """
                INSERT INTO hog_table_version (catalog_id, table_id, begin_snapshot, namespace_id, name)
                VALUES (?, 1, 1, 1, 't_one'), (?, 2, 1, 1, 't_two')
                """,
                catalogId,
                catalogId,
            )
            for ((fileId, tableId) in listOf(1L to 1L, 2L to 2L)) {
                h.execute(
                    """
                    INSERT INTO hog_data_file
                        (catalog_id, data_file_id, table_id, begin_snapshot, path,
                         record_count, file_size_bytes, row_id_start, stats_state)
                    VALUES (?, ?, ?, 1, 's3://$BUCKET/t$tableId/failed.parquet', 10, 100, 0, 'failed')
                    """,
                    catalogId,
                    fileId,
                    tableId,
                )
            }
        }
        val cat = catalogName(catalogId)
        val scoped = hydrator.rehydrateFailed(cat, "ns", "t_two")
        assertThat(scoped.requeued).isEqualTo(1)
        assertThat(statsState(catalogId, 1)).isEqualTo("failed") // other table untouched
        assertThat(statsState(catalogId, 2)).isEqualTo("pending")

        org.assertj.core.api.Assertions.assertThatThrownBy { hydrator.rehydrateFailed(cat, "ns", null) }
            .isInstanceOf(com.posthog.hoglake.model.HoglakeException.Validation::class.java)
        org.assertj.core.api.Assertions.assertThatThrownBy { hydrator.rehydrateFailed(cat, "ns", "nope") }
            .isInstanceOf(com.posthog.hoglake.model.HoglakeException.NotFound::class.java)
    }

    @Test
    fun `name fallback binds at the file's begin snapshot, not live-at-hydration`() {
        // Pinned regression (bug hunt #9): drop+add same-name between the
        // commit and the sweep. The id-less file's stats must land under
        // the ORIGINAL field id (the incarnation the file was written
        // against), never the new one.
        val catalogId = seedCatalogAndTable()
        val idless = writeSampleParquet(sampleSchema(withIds = false))
        val path = "s3://$BUCKET/t1/idless-rebind.parquet"
        store.put(path, idless)
        // The file lands at snapshot 2 (columns began at snapshot 1).
        jdbi.useHandle<Exception> { h ->
            h.execute(
                """
                INSERT INTO hog_data_file
                    (catalog_id, data_file_id, table_id, begin_snapshot, path,
                     record_count, file_size_bytes, footer_size, row_id_start, stats_state)
                VALUES (?, 1, 1, 2, ?, ?, ?, ?, 0, 'pending')
                """,
                catalogId,
                path,
                ROWS.toLong(),
                idless.size.toLong(),
                footerSizeOf(idless),
            )
            // Snapshot 3: drop 'score' (field 2), add a NEW 'score' (field 5).
            h.execute(
                "UPDATE hog_column SET end_snapshot = 3 WHERE catalog_id = ? AND field_id = 2",
                catalogId,
            )
            h.execute(
                """
                INSERT INTO hog_column
                    (catalog_id, table_id, field_id, begin_snapshot, name, col_type, ordinal)
                VALUES (?, 1, 5, 3, 'score', 'double', 4)
                """,
                catalogId,
            )
        }

        assertThat(hydrator.runOnce()).isEqualTo(1)
        assertThat(statsState(catalogId, 1)).isEqualTo("provided")
        val stats = statsRows(catalogId, 1)
        // Everything under the ORIGINAL ids — 'score' under field 2, and
        // NOTHING under the new incarnation's field 5.
        assertThat(stats).containsOnlyKeys(1L, 2L, 3L, 4L)
        assertSampleStats(catalogId, 1)
    }

    @Test
    fun `whole-object fallback is capped - at the cap hydrates, over it fails structurally`() {
        // Pinned regression (bug hunt #11): the whole-object fallback used
        // to buffer arbitrarily large objects on the heap. file_size_bytes
        // is fabricated around the cap; footer_size is absent so both rows
        // take the fallback path.
        val catalogId = seedCatalogAndTable()
        val cap = parquetBytes.size.toLong()
        val atCap = "s3://$BUCKET/t1/at-cap.parquet"
        val overCap = "s3://$BUCKET/t1/over-cap.parquet"
        store.put(atCap, parquetBytes)
        store.put(overCap, parquetBytes)
        seedDataFile(catalogId, 1, atCap, ROWS.toLong(), cap, null)
        seedDataFile(catalogId, 2, overCap, ROWS.toLong(), cap + 1, null)

        val capped = Hydrator(jdbi, store, maxWholeObjectBytes = cap)
        assertThat(capped.runOnce()).isEqualTo(2)
        assertThat(statsState(catalogId, 1)).isEqualTo("provided") // boundary: == cap is allowed
        assertThat(statsState(catalogId, 2)).isEqualTo("failed") // cap + 1: structural
        assertThat(statsRows(catalogId, 2)).isEmpty()
    }

    @Test
    fun `concurrent claims are disjoint - FOR UPDATE SKIP LOCKED`() {
        // Pinned regression (bug hunt #12): N replicas used to select the
        // same pending head and burn N x the S3 GETs on identical files.
        val catalogId = seedCatalogAndTable()
        seedDataFile(catalogId, 1, "s3://$BUCKET/t1/claim-a.parquet", ROWS.toLong(), 100, null)
        seedDataFile(catalogId, 2, "s3://$BUCKET/t1/claim-b.parquet", ROWS.toLong(), 100, null)

        val h1 = jdbi.open()
        val h2 = jdbi.open()
        try {
            h1.begin()
            h2.begin()
            val first = hydrator.claimPending(h1, 1)
            assertThat(first).hasSize(1)
            val second = hydrator.claimPending(h2, 10)
            // Disjoint sets: the second replica skips the first's locked row.
            assertThat(second.map { it.dataFileId }).doesNotContainAnyElementsOf(first.map { it.dataFileId })
            assertThat(first.map { it.dataFileId } + second.map { it.dataFileId })
                .containsExactlyInAnyOrder(1L, 2L)
        } finally {
            h1.rollback()
            h2.rollback()
            h1.close()
            h2.close()
        }
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
