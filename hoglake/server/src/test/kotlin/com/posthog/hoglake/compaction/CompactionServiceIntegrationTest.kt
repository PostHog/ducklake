package com.posthog.hoglake.compaction

import com.posthog.hoglake.commit.CommitService
import com.posthog.hoglake.hydrator.ObjectStore
import com.posthog.hoglake.model.AlterOp
import com.posthog.hoglake.model.ChangeKind
import com.posthog.hoglake.model.ColType
import com.posthog.hoglake.model.ColumnDef
import com.posthog.hoglake.model.ColumnStats
import com.posthog.hoglake.model.CommitRequest
import com.posthog.hoglake.model.DeleteFileRegistration
import com.posthog.hoglake.model.FileRegistration
import com.posthog.hoglake.model.NullOrder
import com.posthog.hoglake.model.SortDirection
import com.posthog.hoglake.model.SortFieldDef
import com.posthog.hoglake.model.TableAppend
import com.posthog.hoglake.model.TableDeletes
import com.posthog.hoglake.service.AlterService
import com.posthog.hoglake.service.CatalogService
import com.posthog.hoglake.service.ExpiryService
import com.posthog.hoglake.service.ScanService
import com.posthog.hoglake.stats.IcebergSingleValue
import com.posthog.hoglake.testing.PgTestSupport
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
import org.jdbi.v3.core.kotlin.useHandleUnchecked
import org.jdbi.v3.core.kotlin.withHandleUnchecked
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import org.testcontainers.containers.MinIOContainer
import java.nio.file.Files
import java.util.concurrent.atomic.AtomicInteger

/**
 * End-to-end compaction against real parquet in MinIO: rewrite
 * correctness (explicit row ids across non-adjacent inputs, sort-order
 * application, field ids), commit semantics (end-snapshotted inputs,
 * time travel, aggregates, the table_compacted change), typed stats
 * aggregation, the changefeed-exclusion contract, the plan-to-commit
 * DV race, and the expiry lifecycle of compacted-away inputs.
 */
@Tag("integration")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class CompactionServiceIntegrationTest {
    private val db = PgTestSupport.freshDatabase()
    private val catalogs = CatalogService(db.jdbi)
    private val commits = CommitService(db.jdbi)
    private val alter = AlterService(db.jdbi)
    private val scans = ScanService(db.jdbi)
    private val counter = AtomicInteger(0)

    private val cfg = CompactionConfig(targetBytes = 512L * 1024 * 1024, minInputFiles = 2, maxGroupsPerRun = 10)
    private val svc by lazy { CompactionService(db.jdbi, store, cfg) }

    private companion object {
        const val BUCKET = "hoglake-compaction-test"

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
    }

    @AfterAll
    fun tearDown() = db.close()

    // ---- parquet helpers ---------------------------------------------------

    // Table shape everywhere here: id long (field 1, required),
    // name string (2, optional), score double (3, optional).
    private val schema: MessageType =
        Types.buildMessage()
            .addField(Types.required(PrimitiveTypeName.INT64).id(1).named("id"))
            .addField(
                Types.optional(PrimitiveTypeName.BINARY)
                    .`as`(LogicalTypeAnnotation.stringType()).id(2).named("name"),
            )
            .addField(Types.optional(PrimitiveTypeName.DOUBLE).id(3).named("score"))
            .named("t")

    private data class TestRow(val id: Long, val name: String?, val score: Double?)

    private fun parquetBytes(rows: List<TestRow>): ByteArray {
        val tmp = Files.createTempFile("compact-e2e", ".parquet")
        try {
            Files.delete(tmp)
            val factory = SimpleGroupFactory(schema)
            ExampleParquetWriter.builder(LocalOutputFile(tmp))
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
            return Files.readAllBytes(tmp)
        } finally {
            Files.deleteIfExists(tmp)
        }
    }

    private data class OutRow(val id: Long, val name: String?, val score: Double?, val rowId: Long)

    private fun readParquet(bytes: ByteArray): Pair<MessageType, List<OutRow>> {
        val tmp = Files.createTempFile("compact-read", ".parquet")
        try {
            Files.write(tmp, bytes)
            val out = mutableListOf<OutRow>()
            lateinit var fileSchema: MessageType
            ParquetFileReader.open(LocalInputFile(tmp)).use { reader ->
                fileSchema = reader.footer.fileMetaData.schema
                val columnIO = ColumnIOFactory().getColumnIO(fileSchema)
                var pages = reader.readNextRowGroup()
                while (pages != null) {
                    val rr = columnIO.getRecordReader(pages, GroupRecordConverter(fileSchema))
                    repeat(Math.toIntExact(pages.rowCount)) {
                        val g: Group = rr.read()

                        fun has(name: String) = g.getFieldRepetitionCount(fileSchema.getFieldIndex(name)) > 0
                        out +=
                            OutRow(
                                id = g.getLong(fileSchema.getFieldIndex("id"), 0),
                                name = if (has("name")) g.getString(fileSchema.getFieldIndex("name"), 0) else null,
                                score = if (has("score")) g.getDouble(fileSchema.getFieldIndex("score"), 0) else null,
                                rowId = g.getLong(fileSchema.getFieldIndex(ParquetRewriter.ROW_ID_COLUMN), 0),
                            )
                    }
                    pages = reader.readNextRowGroup()
                }
            }
            return fileSchema to out
        } finally {
            Files.deleteIfExists(tmp)
        }
    }

    // ---- fixture -----------------------------------------------------------

    private class Fixture(
        val cat: String,
        val tableId: Long,
        val fileIds: List<Long>,
        val paths: List<String>,
        val appendSnapshot: Long,
        val dvSnapshot: Long,
    )

    /**
     * Catalog with table ns.t sorted by score ASC NULLS_LAST; three
     * 5-row files (row-id ranges 0..4 / 5..9 / 10..14) with shipped
     * typed stats; a DV on the MIDDLE file, so the compactable set
     * {f1, f3} is row-id NON-ADJACENT.
     */
    private fun fixture(): Fixture {
        val cat = "compact-e2e-${counter.incrementAndGet()}"
        catalogs.createCatalog(cat, "s3://$BUCKET/$cat")
        catalogs.createNamespace(cat, "ns")
        catalogs.createTable(
            cat,
            "ns",
            "t",
            listOf(
                ColumnDef("id", ColType.LONG, nullable = false),
                ColumnDef("name", ColType.STRING),
                ColumnDef("score", ColType.DOUBLE),
            ),
        )
        alter.alterTable(
            cat,
            "ns",
            "t",
            listOf(AlterOp.SetSortOrder(listOf(SortFieldDef(3, SortDirection.ASC, NullOrder.NULLS_LAST)))),
        )

        // id values deliberately include NEGATIVES: a raw binary min/max of
        // the little-endian encodings would get these bounds wrong.
        val f1 =
            listOf(
                TestRow(-10, "e", 5.0),
                TestRow(-8, "d", 3.0),
                TestRow(-6, null, null),
                TestRow(-4, "b", 8.0),
                TestRow(-2, "a", 1.0),
            )
        val f2 = (0L until 5L).map { TestRow(it, "mid-$it", it.toDouble()) }
        val f3 =
            listOf(
                TestRow(1, "j", 2.0),
                TestRow(2, "i", 9.0),
                TestRow(3, "h", 4.0),
                TestRow(4, null, null),
                TestRow(5, "f", 0.5),
            )

        fun register(
            name: String,
            rows: List<TestRow>,
        ): FileRegistration {
            val bytes = parquetBytes(rows)
            val path = "s3://$BUCKET/$cat/data/ns/t/$name.parquet"
            store.put(path, bytes)
            val nonNullScores = rows.mapNotNull { it.score }
            val nonNullNames = rows.mapNotNull { it.name }
            return FileRegistration(
                path = path,
                recordCount = rows.size.toLong(),
                fileSizeBytes = bytes.size.toLong(),
                columnStats =
                    listOf(
                        ColumnStats(
                            fieldId = 1,
                            valueCount = rows.size.toLong(),
                            nullCount = 0,
                            nanCount = null,
                            sizeBytes = 40,
                            lowerBound = IcebergSingleValue.encodeLong(rows.minOf { it.id }),
                            upperBound = IcebergSingleValue.encodeLong(rows.maxOf { it.id }),
                        ),
                        ColumnStats(
                            fieldId = 2,
                            valueCount = rows.size.toLong(),
                            nullCount = rows.count { it.name == null }.toLong(),
                            nanCount = null,
                            sizeBytes = 50,
                            lowerBound = IcebergSingleValue.encodeString(nonNullNames.min()),
                            upperBound = IcebergSingleValue.encodeString(nonNullNames.max()),
                        ),
                        ColumnStats(
                            fieldId = 3,
                            valueCount = rows.size.toLong(),
                            nullCount = rows.count { it.score == null }.toLong(),
                            nanCount = 0,
                            sizeBytes = 40,
                            lowerBound = IcebergSingleValue.encodeDouble(nonNullScores.min()),
                            upperBound = IcebergSingleValue.encodeDouble(nonNullScores.max()),
                        ),
                    ),
            )
        }

        val regs = listOf(register("f1", f1), register("f2", f2), register("f3", f3))
        val appendSnap =
            commits.commit(cat, CommitRequest(appends = listOf(TableAppend("ns", "t", regs)))).snapshotId
        val fileIds =
            db.jdbi.withHandleUnchecked { h ->
                h.createQuery(
                    """
                    SELECT f.data_file_id FROM hog_data_file f
                    JOIN hog_catalog c ON c.catalog_id = f.catalog_id
                    WHERE c.name = :cat ORDER BY f.row_id_start
                    """,
                )
                    .bind("cat", cat)
                    .mapTo(Long::class.java)
                    .list()
            }
        // DV on the middle file: excluded from compaction AND makes the
        // remaining candidates non-adjacent (0..4 and 10..14).
        val dvSnap =
            commits.commit(
                cat,
                CommitRequest(
                    readSnapshot = appendSnap,
                    deletes =
                        listOf(
                            TableDeletes(
                                "ns",
                                "t",
                                listOf(
                                    DeleteFileRegistration(fileIds[1], "s3://$BUCKET/$cat/dv/f2.dv", 2, 16),
                                ),
                            ),
                        ),
                ),
            ).snapshotId
        val tableId = catalogs.getTable(cat, "ns", "t").tableId
        return Fixture(cat, tableId, fileIds, regs.map { it.path }, appendSnap, dvSnap)
    }

    // ---- the big one -------------------------------------------------------

    @Test
    fun `compacts non-adjacent inputs end to end with sorted output, explicit ids, typed stats`() {
        val fx = fixture()
        val result = svc.runOnce(fx.cat, cfg)
        assertThat(result.groupsCompacted).isEqualTo(1)
        assertThat(result.filesIn).isEqualTo(2)
        assertThat(result.filesOut).isEqualTo(1)
        assertThat(result.skippedConflicts).isEqualTo(0)
        assertThat(result.bytesOut).isGreaterThan(0)

        val compactionSnap = catalogs.getCatalog(fx.cat).headSnapshotId

        // -- commit semantics: inputs end-snapshotted at S, visible at S-1.
        val filesAtHead = catalogs.listFiles(fx.cat, "ns", "t")
        assertThat(filesAtHead).hasSize(2) // f2 + the compacted output
        val output = filesAtHead.single { it.explicitRowIds }
        assertThat(output.path).contains("/data/ns/t/compacted-")
        assertThat(output.recordCount).isEqualTo(10)
        assertThat(output.rowIdStart).isEqualTo(0) // min of inputs; positional meaning void
        assertThat(output.beginSnapshot).isEqualTo(compactionSnap)
        assertThat(output.statsState.wire).isEqualTo("provided")
        val before = catalogs.listFiles(fx.cat, "ns", "t", snapshot = compactionSnap - 1)
        assertThat(before.map { it.path }).containsExactlyElementsOf(fx.paths)
        assertThat(before.none { it.explicitRowIds }).isTrue()

        // -- aggregates unchanged across the compaction snapshot.
        val aggBefore = catalogs.getTable(fx.cat, "ns", "t", snapshot = compactionSnap - 1)
        val aggAfter = catalogs.getTable(fx.cat, "ns", "t")
        assertThat(aggAfter.recordCount).isEqualTo(aggBefore.recordCount).isEqualTo(15)
        assertThat(aggAfter.fileCount).isEqualTo(2)

        // -- exactly one table_compacted change row on the snapshot.
        val (page, _) = catalogs.listSnapshots(fx.cat, compactionSnap - 1, 1)
        val changes = page.single().changes
        assertThat(changes).hasSize(1)
        assertThat(changes.single().kind).isEqualTo(ChangeKind.TABLE_COMPACTED)
        assertThat(changes.single().objectId).isEqualTo(fx.tableId)

        // -- scan exposes the flag and pairs f2 with its DV.
        val scan = scans.planScan(fx.cat, "ns", "t")
        assertThat(scan.single { it.dataFile.explicitRowIds }.deleteFile).isNull()
        assertThat(scan.single { !it.dataFile.explicitRowIds }.deleteFile).isNotNull

        // -- the physical output: sorted by score ASC NULLS_LAST, ids
        //    preserved across the NON-ADJACENT inputs (0..4 and 10..14).
        val (outSchema, rows) = readParquet(store.get(output.path))
        assertThat(outSchema.getType("id").id.intValue()).isEqualTo(1)
        assertThat(outSchema.getType("name").id.intValue()).isEqualTo(2)
        assertThat(outSchema.getType("score").id.intValue()).isEqualTo(3)
        assertThat(outSchema.getType(ParquetRewriter.ROW_ID_COLUMN).id.intValue())
            .isEqualTo(ParquetRewriter.ROW_ID_FIELD_ID)
        assertThat(rows.map { it.score })
            .containsExactly(0.5, 1.0, 2.0, 3.0, 4.0, 5.0, 8.0, 9.0, null, null)
        assertThat(rows.map { it.rowId })
            .containsExactly(14L, 4L, 10L, 1L, 12L, 0L, 3L, 11L, 2L, 13L)

        // -- typed stats aggregation: signed long bounds span the negative
        //    inputs; string/double bounds merge correctly; counts sum.
        val stats =
            db.jdbi.withHandleUnchecked { h ->
                h.createQuery(
                    """
                    SELECT field_id, value_count, null_count, lower_bound, upper_bound
                    FROM hog_file_column_stats s
                    JOIN hog_catalog c ON c.catalog_id = s.catalog_id
                    WHERE c.name = :cat AND s.data_file_id = :fileId
                    """,
                )
                    .bind("cat", fx.cat)
                    .bind("fileId", output.dataFileId)
                    .map { rs, _ ->
                        rs.getLong("field_id") to
                            Triple(
                                rs.getLong("value_count") to rs.getLong("null_count"),
                                rs.getBytes("lower_bound"),
                                rs.getBytes("upper_bound"),
                            )
                    }
                    .list()
                    .toMap()
            }
        assertThat(stats).containsOnlyKeys(1L, 2L, 3L)
        assertThat(stats[1L]!!.first).isEqualTo(10L to 0L)
        assertThat(stats[1L]!!.second).isEqualTo(IcebergSingleValue.encodeLong(-10))
        assertThat(stats[1L]!!.third).isEqualTo(IcebergSingleValue.encodeLong(5))
        assertThat(stats[2L]!!.first).isEqualTo(10L to 2L)
        assertThat(stats[2L]!!.second).isEqualTo(IcebergSingleValue.encodeString("a"))
        assertThat(stats[2L]!!.third).isEqualTo(IcebergSingleValue.encodeString("j"))
        assertThat(stats[3L]!!.first).isEqualTo(10L to 2L)
        assertThat(stats[3L]!!.second).isEqualTo(IcebergSingleValue.encodeDouble(0.5))
        assertThat(stats[3L]!!.third).isEqualTo(IcebergSingleValue.encodeDouble(9.0))

        // -- nothing entered the removal queue (inputs live on for time travel).
        val queued =
            db.jdbi.withHandleUnchecked { h ->
                h.createQuery(
                    """
                    SELECT count(*) FROM hog_file_removal r
                    JOIN hog_catalog c ON c.catalog_id = r.catalog_id
                    WHERE c.name = :cat
                    """,
                ).bind("cat", fx.cat).mapTo(Long::class.java).one()
            }
        assertThat(queued).isZero()

        // -- a second run finds nothing left to do.
        val again = svc.runOnce(fx.cat, cfg)
        assertThat(again.groupsCompacted).isZero()
        assertThat(again.skippedConflicts).isZero()
    }

    @Test
    fun `changefeed replays original files and never the compacted output`() {
        val fx = fixture()
        svc.runOnce(fx.cat, cfg)
        val head = catalogs.getCatalog(fx.cat).headSnapshotId

        // Full replay across the compaction: the ORIGINAL three files in
        // their original row-id ranges; the compacted file never appears.
        val plan = catalogs.changes(fx.cat, "ns", "t", fromSnapshot = 0)
        assertThat(plan.toSnapshot).isEqualTo(head)
        assertThat(plan.files.map { it.path }).containsExactlyElementsOf(fx.paths)
        assertThat(plan.files.map { it.rowIdStart }).containsExactly(0L, 5L, 10L)
        assertThat(plan.files.none { it.explicitRowIds }).isTrue()
        assertThat(plan.deleteFiles).hasSize(1) // the DV feed is unaffected

        // A consumer strictly spanning only the compaction snapshot sees
        // NO new files at all.
        val tail = catalogs.changes(fx.cat, "ns", "t", fromSnapshot = fx.dvSnapshot)
        assertThat(tail.files).isEmpty()
        assertThat(tail.deleteFiles).isEmpty()
    }

    @Test
    fun `a DV registered between plan and commit aborts the group`() {
        val fx = fixture()
        val plan = svc.planTable(fx.cat, "ns", "t", cfg)
        val group = plan.groups.single()
        assertThat(group.files.map { it.dataFileId })
            .containsExactly(fx.fileIds[0], fx.fileIds[2])

        // The race: after planning, a client registers a DV against f3.
        commits.commit(
            fx.cat,
            CommitRequest(
                readSnapshot = catalogs.getCatalog(fx.cat).headSnapshotId,
                deletes =
                    listOf(
                        TableDeletes(
                            "ns",
                            "t",
                            listOf(DeleteFileRegistration(fx.fileIds[2], "s3://$BUCKET/${fx.cat}/dv/f3.dv", 1, 8)),
                        ),
                    ),
            ),
        )
        val headBefore = catalogs.getCatalog(fx.cat).headSnapshotId

        // Commit-time re-verification aborts the group: no snapshot, no
        // metadata change, inputs untouched.
        assertThat(svc.compactPlannedGroup(fx.cat, "ns", "t", group)).isNull()
        assertThat(catalogs.getCatalog(fx.cat).headSnapshotId).isEqualTo(headBefore)
        assertThat(catalogs.listFiles(fx.cat, "ns", "t").map { it.path })
            .containsExactlyElementsOf(fx.paths)

        // A fresh run counts the shrunken candidate set: only f1 is left
        // DV-free, below min_input_files -> nothing to do, nothing skipped.
        val rerun = svc.runOnce(fx.cat, cfg)
        assertThat(rerun.groupsCompacted).isZero()
        assertThat(rerun.skippedConflicts).isZero()
    }

    @Test
    fun `compacted-away inputs become expiry-queueable once unreachable`() {
        val fx = fixture()
        svc.runOnce(fx.cat, cfg)

        // Age every snapshot far past a 60s retention and sweep: the floor
        // advances to head, the inputs' end_snapshot sinks below it, and
        // their paths enter the removal queue. The live output and f2 stay.
        db.jdbi.useHandleUnchecked { h ->
            h.createUpdate(
                """
                UPDATE hog_snapshot SET snapshot_time = now() - interval '1 hour'
                WHERE catalog_id = (SELECT catalog_id FROM hog_catalog WHERE name = :cat)
                """,
            ).bind("cat", fx.cat).execute()
            h.createUpdate(
                "UPDATE hog_catalog SET snapshot_retention_seconds = 60 WHERE name = :cat",
            ).bind("cat", fx.cat).execute()
        }
        val expiry = ExpiryService(db.jdbi).runOnce(fx.cat, batchSize = 1000)
        assertThat(expiry.dataFilesQueued).isEqualTo(2)

        val queuedPaths =
            db.jdbi.withHandleUnchecked { h ->
                h.createQuery(
                    """
                    SELECT r.path FROM hog_file_removal r
                    JOIN hog_catalog c ON c.catalog_id = r.catalog_id
                    WHERE c.name = :cat AND r.file_kind = 'data'
                    """,
                ).bind("cat", fx.cat).mapTo(String::class.java).list()
            }
        assertThat(queuedPaths).containsExactlyInAnyOrder(fx.paths[0], fx.paths[2])
        // The compacted output and the DV'd middle file survive.
        assertThat(catalogs.listFiles(fx.cat, "ns", "t").map { it.path })
            .contains(fx.paths[1])
            .anyMatch { it.contains("compacted-") }
    }

    @Test
    fun `inputs with pending stats produce a pending output for the hydrator`() {
        val cat = "compact-pending-${counter.incrementAndGet()}"
        catalogs.createCatalog(cat, "s3://$BUCKET/$cat")
        catalogs.createNamespace(cat, "ns")
        catalogs.createTable(cat, "ns", "t", listOf(ColumnDef("id", ColType.LONG, nullable = false)))
        val rows1 = listOf(TestRow(1, null, null), TestRow(2, null, null))
        val rows2 = listOf(TestRow(3, null, null))
        // The parquet shape carries embedded ids for all three physical
        // columns, so the rewrite needs no catalog-name fallback even
        // though only `id` is a catalog column here.
        val regs =
            listOf(rows1, rows2).mapIndexed { i, rows ->
                val bytes = parquetBytes(rows)
                val path = "s3://$BUCKET/$cat/data/ns/t/p$i.parquet"
                store.put(path, bytes)
                // No columnStats: registers as stats_state='pending'.
                FileRegistration(path, rows.size.toLong(), bytes.size.toLong())
            }
        commits.commit(cat, CommitRequest(appends = listOf(TableAppend("ns", "t", regs))))
        val result = svc.runOnce(cat, cfg)
        assertThat(result.groupsCompacted).isEqualTo(1)
        val output = catalogs.listFiles(cat, "ns", "t").single()
        assertThat(output.explicitRowIds).isTrue()
        assertThat(output.statsState.wire).isEqualTo("pending")
        assertThat(output.recordCount).isEqualTo(3)
    }
}
