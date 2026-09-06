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
import com.posthog.hoglake.service.CleanupService
import com.posthog.hoglake.service.ExpiryService
import com.posthog.hoglake.service.RemovalStore
import com.posthog.hoglake.service.ScanService
import com.posthog.hoglake.service.VerifyService
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
 * End-to-end compaction against real parquet + puffin in MinIO: rewrite
 * correctness (explicit row ids across non-adjacent inputs, DV
 * application, sort-order application, field ids, heterogeneous-schema
 * groups under the live schema), commit semantics (end-snapshotted
 * inputs AND their dead DVs, time travel, aggregates, the
 * table_compacted change, the staging-ticket lifecycle), typed stats
 * aggregation, the changefeed-exclusion contract, the plan-to-commit
 * races (input death, DV appearance, DV supersession), orphan
 * reclamation via the cleanup drain, verify composition, and the
 * expiry lifecycle of compacted-away inputs.
 */
@Tag("integration")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class CompactionServiceIntegrationTest {
    private val db = PgTestSupport.freshDatabase()
    private val catalogs = CatalogService(db.jdbi)
    private val commits = CommitService(db.jdbi)
    private val alter = AlterService(db.jdbi)
    private val scans = ScanService(db.jdbi)
    private val verify = VerifyService(db.jdbi)
    private val counter = AtomicInteger(0)

    private val cfg = CompactionConfig(targetBytes = 512L * 1024 * 1024, minInputFiles = 2, maxGroupsPerRun = 10)
    private val svc by lazy { CompactionService(db.jdbi, store, cfg) }
    private val cleanup by lazy { CleanupService(db.jdbi, removalStore) }

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

        val removalStore: RemovalStore by lazy {
            RemovalStore(
                endpoint = minio.s3URL,
                region = "us-east-1",
                accessKey = minio.userName,
                secretKey = minio.password,
                pathStyle = true,
            )
        }
    }

    @AfterAll
    fun tearDown() = db.close()

    // ---- parquet helpers ---------------------------------------------------

    // Table shape for the fixture: id long (field 1, required),
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

    private fun parquetBytes(rows: List<TestRow>): ByteArray =
        customParquetBytes(
            schema,
            rows.map { r ->
                { g: Group ->
                    g.add("id", r.id)
                    r.name?.let { g.add("name", it) }
                    r.score?.let { g.add("score", it) }
                }
            },
        )

    private fun customParquetBytes(
        fileSchema: MessageType,
        rows: List<(Group) -> Unit>,
    ): ByteArray {
        val tmp = Files.createTempFile("compact-e2e", ".parquet")
        try {
            Files.delete(tmp)
            val factory = SimpleGroupFactory(fileSchema)
            ExampleParquetWriter.builder(LocalOutputFile(tmp))
                .withType(fileSchema)
                .withCompressionCodec(CompressionCodecName.UNCOMPRESSED)
                .build()
                .use { w ->
                    for (fill in rows) {
                        val g = factory.newGroup()
                        fill(g)
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
        var fileSchema: MessageType? = null
        val out = mutableListOf<OutRow>()
        readGroups(bytes) { s, g ->
            fileSchema = s

            fun has(name: String) = g.getFieldRepetitionCount(s.getFieldIndex(name)) > 0
            out +=
                OutRow(
                    id = g.getLong(s.getFieldIndex("id"), 0),
                    name = if (has("name")) g.getString(s.getFieldIndex("name"), 0) else null,
                    score = if (has("score")) g.getDouble(s.getFieldIndex("score"), 0) else null,
                    rowId = g.getLong(s.getFieldIndex(ParquetRewriter.ROW_ID_COLUMN), 0),
                )
        }
        return fileSchema!! to out
    }

    private fun readGroups(
        bytes: ByteArray,
        consume: (MessageType, Group) -> Unit,
    ) {
        val tmp = Files.createTempFile("compact-read", ".parquet")
        try {
            Files.write(tmp, bytes)
            ParquetFileReader.open(LocalInputFile(tmp)).use { reader ->
                val fileSchema = reader.footer.fileMetaData.schema
                val columnIO = ColumnIOFactory().getColumnIO(fileSchema)
                var pages = reader.readNextRowGroup()
                while (pages != null) {
                    val rr = columnIO.getRecordReader(pages, GroupRecordConverter(fileSchema))
                    repeat(Math.toIntExact(pages.rowCount)) { consume(fileSchema, rr.read()) }
                    pages = reader.readNextRowGroup()
                }
            }
        } finally {
            Files.deleteIfExists(tmp)
        }
    }

    /** Every output row's _hog_row_id, in physical order. */
    private fun readRowIds(bytes: ByteArray): List<Long> {
        val ids = mutableListOf<Long>()
        readGroups(bytes) { s, g -> ids += g.getLong(s.getFieldIndex(ParquetRewriter.ROW_ID_COLUMN), 0) }
        return ids
    }

    /** Upload a real puffin DV and register it against [dataFileId]. */
    private fun registerDv(
        cat: String,
        dataFileId: Long,
        path: String,
        positions: List<Long>,
    ) {
        val bytes = PuffinTestFiles.deletionVector(positions)
        store.put(path, bytes)
        commits.commit(
            cat,
            CommitRequest(
                readSnapshot = catalogs.getCatalog(cat).headSnapshotId,
                deletes =
                    listOf(
                        TableDeletes(
                            "ns",
                            "t",
                            listOf(
                                DeleteFileRegistration(dataFileId, path, positions.size.toLong(), bytes.size.toLong()),
                            ),
                        ),
                    ),
            ),
        )
    }

    private data class RemovalRow(
        val path: String,
        val reason: String,
        val drainedOutcome: String?,
    )

    private fun removalRows(cat: String): List<RemovalRow> =
        db.jdbi.withHandleUnchecked { h ->
            h.createQuery(
                """
                SELECT r.path, r.reason, r.drained_outcome FROM hog_file_removal r
                JOIN hog_catalog c ON c.catalog_id = r.catalog_id
                WHERE c.name = :cat ORDER BY r.removal_id
                """,
            )
                .bind("cat", cat)
                .map { rs, _ ->
                    RemovalRow(rs.getString("path"), rs.getString("reason"), rs.getString("drained_outcome"))
                }
                .list()
        }

    private fun assertVerifyPasses(cat: String) {
        val report = verify.runOnce(cat)
        assertThat(report.status)
            .describedAs("verify checks: " + report.checks.joinToString { "${it.check}=${it.violations}" })
            .isEqualTo("pass")
    }

    // ---- fixture -----------------------------------------------------------

    private class Fixture(
        val cat: String,
        val tableId: Long,
        val fileIds: List<Long>,
        val paths: List<String>,
        val dvPath: String?,
        val appendSnapshot: Long,
        val dvSnapshot: Long,
    )

    /**
     * Catalog with table ns.t sorted by score ASC NULLS_LAST; three
     * 5-row files (row-id ranges 0..4 / 5..9 / 10..14) with shipped
     * typed stats. With [dvOnMiddle], a REAL puffin DV on the middle
     * file deleting positions 1 and 3 (row ids 6 and 8) — the group
     * mixes DV-bearing and DV-free inputs.
     */
    private fun fixture(dvOnMiddle: Boolean = true): Fixture {
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
        var dvPath: String? = null
        if (dvOnMiddle) {
            // REAL puffin DV on the middle file: positions 1 and 3 of f2,
            // i.e. row ids 6 and 8 die when the group compacts.
            dvPath = "s3://$BUCKET/$cat/dv/f2.puffin"
            registerDv(cat, fileIds[1], dvPath, listOf(1L, 3L))
        }
        val tableId = catalogs.getTable(cat, "ns", "t").tableId
        return Fixture(
            cat,
            tableId,
            fileIds,
            regs.map { it.path },
            dvPath,
            appendSnap,
            catalogs.getCatalog(cat).headSnapshotId,
        )
    }

    // ---- the big one -------------------------------------------------------

    @Test
    fun `compacts a DV-bearing group end to end - vectors applied, ids preserved, sorted output`() {
        val fx = fixture()
        val result = svc.runOnce(fx.cat, cfg)
        assertThat(result.groupsCompacted).isEqualTo(1)
        assertThat(result.filesIn).isEqualTo(3)
        assertThat(result.filesOut).isEqualTo(1)
        assertThat(result.skippedConflicts).isZero()
        assertThat(result.dvSuperseded).isZero()
        assertThat(result.unconvertibleSchema).isZero()
        assertThat(result.bytesOut).isGreaterThan(0)

        val compactionSnap = catalogs.getCatalog(fx.cat).headSnapshotId

        // -- commit semantics: ALL inputs (the DV-bearing one included)
        //    end-snapshotted at S, visible at S-1.
        val filesAtHead = catalogs.listFiles(fx.cat, "ns", "t")
        assertThat(filesAtHead).hasSize(1)
        val output = filesAtHead.single()
        assertThat(output.explicitRowIds).isTrue()
        assertThat(output.path).contains("/data/ns/t/compacted-")
        assertThat(output.recordCount).isEqualTo(13) // 15 gross - 2 deleted
        assertThat(output.rowIdStart).isEqualTo(0) // min surviving id; positional meaning void
        assertThat(output.beginSnapshot).isEqualTo(compactionSnap)
        // A DV'd input makes the inputs' registered counts wrong for the
        // survivor set: honest 'pending', hydrator fills from the footer.
        assertThat(output.statsState.wire).isEqualTo("pending")
        val before = catalogs.listFiles(fx.cat, "ns", "t", snapshot = compactionSnap - 1)
        assertThat(before.map { it.path }).containsExactlyElementsOf(fx.paths)
        assertThat(before.none { it.explicitRowIds }).isTrue()

        // -- aggregates: gross 15 before, 13 after (the deleted rows are
        //    physically gone from the visible file set).
        assertThat(catalogs.getTable(fx.cat, "ns", "t", snapshot = compactionSnap - 1).recordCount).isEqualTo(15)
        val aggAfter = catalogs.getTable(fx.cat, "ns", "t")
        assertThat(aggAfter.recordCount).isEqualTo(13)
        assertThat(aggAfter.fileCount).isEqualTo(1)

        // -- exactly one table_compacted change row on the snapshot.
        val (page, _) = catalogs.listSnapshots(fx.cat, compactionSnap - 1, 1)
        val changes = page.single().changes
        assertThat(changes).hasSize(1)
        assertThat(changes.single().kind).isEqualTo(ChangeKind.TABLE_COMPACTED)
        assertThat(changes.single().objectId).isEqualTo(fx.tableId)

        // -- the applied DV died with its file: the scan is one explicit
        //    file with NO delete file.
        val scan = scans.planScan(fx.cat, "ns", "t")
        assertThat(scan).hasSize(1)
        assertThat(scan.single().dataFile.explicitRowIds).isTrue()
        assertThat(scan.single().deleteFile).isNull()

        // -- the physical output: sorted by score ASC NULLS_LAST, ids
        //    preserved across the inputs, holes where the DV deleted
        //    (row ids 6 and 8 are gone FOREVER).
        val (outSchema, rows) = readParquet(store.get(output.path))
        assertThat(outSchema.getType("id").id.intValue()).isEqualTo(1)
        assertThat(outSchema.getType("name").id.intValue()).isEqualTo(2)
        assertThat(outSchema.getType("score").id.intValue()).isEqualTo(3)
        assertThat(outSchema.getType(ParquetRewriter.ROW_ID_COLUMN).id.intValue())
            .isEqualTo(ParquetRewriter.ROW_ID_FIELD_ID)
        assertThat(rows.map { it.score })
            .containsExactly(0.0, 0.5, 1.0, 2.0, 2.0, 3.0, 4.0, 4.0, 5.0, 8.0, 9.0, null, null)
        assertThat(rows.map { it.rowId })
            .containsExactly(5L, 14L, 4L, 7L, 10L, 1L, 9L, 12L, 0L, 3L, 11L, 2L, 13L)

        // -- the staging ticket settled as 'registered' in the group's
        //    commit; nothing is left undrained.
        val removals = removalRows(fx.cat)
        assertThat(removals).hasSize(1)
        assertThat(removals.single().path).isEqualTo(output.path)
        assertThat(removals.single().reason).isEqualTo("compaction_staging")
        assertThat(removals.single().drainedOutcome).isEqualTo("registered")

        // -- verify composes with explicit-row-id holes: all green.
        assertVerifyPasses(fx.cat)

        // -- a second run finds nothing left to do.
        val again = svc.runOnce(fx.cat, cfg)
        assertThat(again.groupsCompacted).isZero()
        assertThat(again.skippedConflicts).isZero()
    }

    @Test
    fun `a DV-free group aggregates typed stats from the inputs`() {
        val fx = fixture(dvOnMiddle = false)
        val result = svc.runOnce(fx.cat, cfg)
        assertThat(result.groupsCompacted).isEqualTo(1)
        assertThat(result.filesIn).isEqualTo(3)

        val output = catalogs.listFiles(fx.cat, "ns", "t").single()
        assertThat(output.explicitRowIds).isTrue()
        assertThat(output.recordCount).isEqualTo(15)
        assertThat(output.statsState.wire).isEqualTo("provided")

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
        assertThat(stats[1L]!!.first).isEqualTo(15L to 0L)
        assertThat(stats[1L]!!.second).isEqualTo(IcebergSingleValue.encodeLong(-10))
        assertThat(stats[1L]!!.third).isEqualTo(IcebergSingleValue.encodeLong(5))
        assertThat(stats[2L]!!.first).isEqualTo(15L to 2L)
        assertThat(stats[2L]!!.second).isEqualTo(IcebergSingleValue.encodeString("a"))
        assertThat(stats[2L]!!.third).isEqualTo(IcebergSingleValue.encodeString("mid-4"))
        assertThat(stats[3L]!!.first).isEqualTo(15L to 2L)
        assertThat(stats[3L]!!.second).isEqualTo(IcebergSingleValue.encodeDouble(0.0))
        assertThat(stats[3L]!!.third).isEqualTo(IcebergSingleValue.encodeDouble(9.0))
        assertVerifyPasses(fx.cat)
    }

    @Test
    fun `a wrong-width stats bound never wedges the sweep - the group compacts with that bound null`() {
        // Pinned regression (bug hunt #5, belt-and-braces half): a bound
        // that does not decode under the live type (a 4-byte residue from
        // a pre-fix promote, or a racing hydrator's stale-typed upsert)
        // used to throw out of mergeBound EVERY sweep — a poison group
        // wedged until its inputs expired. It must instead be treated as
        // absent: the group compacts, that column's merged bound is null.
        val fx = fixture(dvOnMiddle = false)
        db.jdbi.useHandleUnchecked { h ->
            h.execute(
                """
                UPDATE hog_file_column_stats SET lower_bound = ?
                WHERE catalog_id = (SELECT catalog_id FROM hog_catalog WHERE name = ?)
                  AND data_file_id = ? AND field_id = 1
                """,
                IcebergSingleValue.encodeInt(-10),
                fx.cat,
                fx.fileIds[0],
            )
        }

        val result = svc.runOnce(fx.cat, cfg)
        assertThat(result.groupsCompacted).isEqualTo(1) // not wedged

        val output = catalogs.listFiles(fx.cat, "ns", "t").single { it.explicitRowIds }
        val stats =
            db.jdbi.withHandleUnchecked { h ->
                h.createQuery(
                    """
                    SELECT field_id, lower_bound, upper_bound FROM hog_file_column_stats s
                    JOIN hog_catalog c ON c.catalog_id = s.catalog_id
                    WHERE c.name = :cat AND s.data_file_id = :fileId
                    """,
                )
                    .bind("cat", fx.cat)
                    .bind("fileId", output.dataFileId)
                    .map { rs, _ ->
                        rs.getLong("field_id") to Pair(rs.getBytes("lower_bound"), rs.getBytes("upper_bound"))
                    }
                    .list()
                    .toMap()
            }
        // The poisoned side is null (honest absence), the clean side merged.
        assertThat(stats[1L]!!.first).isNull()
        assertThat(stats[1L]!!.second).isEqualTo(IcebergSingleValue.encodeLong(5))
        // Untouched columns merged normally.
        assertThat(stats[2L]!!.first).isEqualTo(IcebergSingleValue.encodeString("a"))
        assertThat(stats[3L]!!.second).isEqualTo(IcebergSingleValue.encodeDouble(9.0))
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

    // ---- plan-to-commit races ----------------------------------------------

    @Test
    fun `a DV appearing after planning skips the group and cleanup reclaims the staged output`() {
        val fx = fixture()
        val plan = svc.planTable(fx.cat, "ns", "t", cfg)
        val group = plan.groups.single()
        assertThat(group.files.map { it.dataFileId }).containsExactlyElementsOf(fx.fileIds)

        // The race: after planning, a client registers a DV against f3
        // (planned DV-free). The rewrite applied the PLANNED vectors, so
        // committing would resurrect this delete.
        val f3DvPath = "s3://$BUCKET/${fx.cat}/dv/f3.puffin"
        registerDv(fx.cat, fx.fileIds[2], f3DvPath, listOf(0L))
        val headBefore = catalogs.getCatalog(fx.cat).headSnapshotId

        assertThat(svc.compactPlannedGroup(fx.cat, "ns", "t", group))
            .isEqualTo(CompactionService.GroupOutcome.SkippedDvSuperseded)

        // No snapshot, no metadata change, inputs untouched, the new DV live.
        assertThat(catalogs.getCatalog(fx.cat).headSnapshotId).isEqualTo(headBefore)
        assertThat(catalogs.listFiles(fx.cat, "ns", "t").map { it.path })
            .containsExactlyElementsOf(fx.paths)
        val f3Scan = scans.planScan(fx.cat, "ns", "t").single { it.dataFile.dataFileId == fx.fileIds[2] }
        assertThat(f3Scan.deleteFile!!.path).isEqualTo(f3DvPath)

        // The staged output is an undrained claim ticket whose object
        // exists — the NORMAL cleanup drain reclaims it (liveness check
        // passes: the path was never registered).
        val staged = removalRows(fx.cat).single { it.reason == "compaction_staging" }
        assertThat(staged.drainedOutcome).isNull()
        assertThat(removalStore.exists(staged.path)).isTrue()
        val drained = cleanup.runOnce(fx.cat, batchSize = 100)
        assertThat(drained.removed).isEqualTo(1)
        assertThat(drained.stillReferenced).isZero()
        assertThat(removalStore.exists(staged.path)).isFalse()
        assertThat(removalRows(fx.cat).single { it.reason == "compaction_staging" }.drainedOutcome)
            .isEqualTo("deleted")
        assertVerifyPasses(fx.cat)
    }

    @Test
    fun `a superseded DV after planning skips the group and the next run applies the grown vector`() {
        val fx = fixture()
        val plan = svc.planTable(fx.cat, "ns", "t", cfg)
        val group = plan.groups.single()

        // The race: f2's DV grows (supersession) after planning — the
        // grown vector also kills position 4 (row id 9).
        val grownPath = "s3://$BUCKET/${fx.cat}/dv/f2-grown.puffin"
        registerDv(fx.cat, fx.fileIds[1], grownPath, listOf(1L, 3L, 4L))
        val headBefore = catalogs.getCatalog(fx.cat).headSnapshotId

        assertThat(svc.compactPlannedGroup(fx.cat, "ns", "t", group))
            .isEqualTo(CompactionService.GroupOutcome.SkippedDvSuperseded)
        assertThat(catalogs.getCatalog(fx.cat).headSnapshotId).isEqualTo(headBefore)

        // The newer vector is preserved and live.
        val f2Scan = scans.planScan(fx.cat, "ns", "t").single { it.dataFile.dataFileId == fx.fileIds[1] }
        assertThat(f2Scan.deleteFile!!.path).isEqualTo(grownPath)
        assertThat(f2Scan.deleteFile!!.deleteCount).isEqualTo(3)

        // A fresh sweep re-plans against the grown vector and compacts:
        // row ids 6, 8 AND 9 are gone.
        val rerun = svc.runOnce(fx.cat, cfg)
        assertThat(rerun.groupsCompacted).isEqualTo(1)
        val output = catalogs.listFiles(fx.cat, "ns", "t").single()
        assertThat(output.recordCount).isEqualTo(12)
        assertThat(readRowIds(store.get(output.path)))
            .containsExactlyInAnyOrder(0L, 1L, 2L, 3L, 4L, 5L, 7L, 10L, 11L, 12L, 13L, 14L)
        assertVerifyPasses(fx.cat)
    }

    @Test
    fun `an input end-snapshotted after planning skips the group as a conflict`() {
        val fx = fixture()
        val plan = svc.planTable(fx.cat, "ns", "t", cfg)
        val group = plan.groups.single()

        // Simulate a concurrent compactor/expiry killing f1 after planning.
        db.jdbi.useHandleUnchecked { h ->
            h.execute(
                """
                UPDATE hog_data_file SET end_snapshot = begin_snapshot + 1
                WHERE catalog_id = (SELECT catalog_id FROM hog_catalog WHERE name = ?)
                  AND data_file_id = ?
                """,
                fx.cat,
                fx.fileIds[0],
            )
        }
        val headBefore = catalogs.getCatalog(fx.cat).headSnapshotId
        assertThat(svc.compactPlannedGroup(fx.cat, "ns", "t", group))
            .isEqualTo(CompactionService.GroupOutcome.SkippedConflict)
        assertThat(catalogs.getCatalog(fx.cat).headSnapshotId).isEqualTo(headBefore)
    }

    // ---- staging-ticket lifecycle ------------------------------------------

    @Test
    fun `a committed group's output survives the cleanup drain`() {
        val fx = fixture()
        svc.runOnce(fx.cat, cfg)
        val output = catalogs.listFiles(fx.cat, "ns", "t").single()

        val drained = cleanup.runOnce(fx.cat, batchSize = 100)
        assertThat(drained.removed).isZero()
        assertThat(drained.missing).isZero()
        assertThat(drained.stillReferenced).isZero()
        assertThat(removalStore.exists(output.path)).isTrue()
        assertThat(removalRows(fx.cat).single().drainedOutcome).isEqualTo("registered")
    }

    // ---- DV pathology ------------------------------------------------------

    @Test
    fun `an all-deleted group commits an empty output and verify stays green`() {
        val cat = "compact-empty-${counter.incrementAndGet()}"
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
        val regs =
            listOf(
                listOf(TestRow(1, "a", 1.0), TestRow(2, "b", 2.0)),
                listOf(TestRow(3, "c", 3.0)),
            ).mapIndexed { i, rows ->
                val bytes = parquetBytes(rows)
                val path = "s3://$BUCKET/$cat/data/ns/t/e$i.parquet"
                store.put(path, bytes)
                FileRegistration(path, rows.size.toLong(), bytes.size.toLong())
            }
        commits.commit(cat, CommitRequest(appends = listOf(TableAppend("ns", "t", regs))))
        val fileIds =
            catalogs.listFiles(cat, "ns", "t").sortedBy { it.rowIdStart }.map { it.dataFileId }
        // Every row of every input dies.
        registerDv(cat, fileIds[0], "s3://$BUCKET/$cat/dv/e0.puffin", listOf(0L, 1L))
        registerDv(cat, fileIds[1], "s3://$BUCKET/$cat/dv/e1.puffin", listOf(0L))

        val result = svc.runOnce(cat, cfg)
        assertThat(result.groupsCompacted).isEqualTo(1)
        val output = catalogs.listFiles(cat, "ns", "t").single()
        assertThat(output.explicitRowIds).isTrue()
        assertThat(output.recordCount).isZero()
        assertThat(output.rowIdStart).isZero() // min input start: diagnostics only
        assertThat(readRowIds(store.get(output.path))).isEmpty()
        assertThat(scans.planScan(cat, "ns", "t").single().deleteFile).isNull()
        assertVerifyPasses(cat)
    }

    @Test
    fun `recompacting an explicit-row-id output with a DV preserves surviving ids`() {
        val cat = "compact-recompact-${counter.incrementAndGet()}"
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

        fun appendRows(
            name: String,
            rows: List<TestRow>,
        ) {
            val bytes = parquetBytes(rows)
            val path = "s3://$BUCKET/$cat/data/ns/t/$name.parquet"
            store.put(path, bytes)
            commits.commit(
                cat,
                CommitRequest(
                    appends =
                        listOf(
                            TableAppend(
                                "ns",
                                "t",
                                listOf(FileRegistration(path, rows.size.toLong(), bytes.size.toLong())),
                            ),
                        ),
                ),
            )
        }
        appendRows("a", (0L..2L).map { TestRow(100 + it, "a$it", it.toDouble()) }) // row ids 0..2
        appendRows("b", (0L..1L).map { TestRow(200 + it, "b$it", it.toDouble()) }) // row ids 3..4

        // First compaction: unsorted table -> physical order = row-id order.
        assertThat(svc.runOnce(cat, cfg).groupsCompacted).isEqualTo(1)
        val first = catalogs.listFiles(cat, "ns", "t").single()
        assertThat(first.explicitRowIds).isTrue()
        assertThat(readRowIds(store.get(first.path))).containsExactly(0L, 1L, 2L, 3L, 4L)

        // A DV lands on the compacted output: physical positions 0 and 4,
        // i.e. row ids 0 and 4 die. Then more data arrives.
        registerDv(cat, first.dataFileId, "s3://$BUCKET/$cat/dv/first.puffin", listOf(0L, 4L))
        appendRows("c", (0L..1L).map { TestRow(300 + it, "c$it", it.toDouble()) }) // row ids 5..6

        // Second compaction: the explicit-id input's DV drops by POSITION,
        // survivors keep the ids their _hog_row_id column carries.
        val second = svc.runOnce(cat, cfg)
        assertThat(second.groupsCompacted).isEqualTo(1)
        val output = catalogs.listFiles(cat, "ns", "t").single()
        assertThat(output.recordCount).isEqualTo(5)
        assertThat(output.rowIdStart).isEqualTo(1) // 0 died; min survivor is 1
        assertThat(readRowIds(store.get(output.path))).containsExactly(1L, 2L, 3L, 5L, 6L)
        assertThat(scans.planScan(cat, "ns", "t").single().deleteFile).isNull()
        assertVerifyPasses(cat)
    }

    // ---- heterogeneous schemas ---------------------------------------------

    @Test
    fun `a heterogeneous group compacts under the live schema - add null-fills, promote up-casts, drop drops`() {
        val cat = "compact-hetero-${counter.incrementAndGet()}"
        catalogs.createCatalog(cat, "s3://$BUCKET/$cat")
        catalogs.createNamespace(cat, "ns")
        catalogs.createTable(
            cat,
            "ns",
            "t",
            listOf(ColumnDef("id", ColType.INT, nullable = false), ColumnDef("old", ColType.STRING)),
        )

        // Vintage 1: written under (id int [1], old string [2]).
        val v1Schema =
            Types.buildMessage()
                .addField(Types.required(PrimitiveTypeName.INT32).id(1).named("id"))
                .addField(
                    Types.optional(PrimitiveTypeName.BINARY)
                        .`as`(LogicalTypeAnnotation.stringType()).id(2).named("old"),
                )
                .named("t")
        val v1Bytes =
            customParquetBytes(
                v1Schema,
                listOf(
                    { g ->
                        g.add("id", 10)
                        g.add("old", "dead-data")
                    },
                    { g -> g.add("id", 20) },
                ),
            )
        val v1Path = "s3://$BUCKET/$cat/data/ns/t/v1.parquet"
        store.put(v1Path, v1Bytes)
        commits.commit(
            cat,
            CommitRequest(
                appends =
                    listOf(
                        TableAppend("ns", "t", listOf(FileRegistration(v1Path, 2, v1Bytes.size.toLong()))),
                    ),
            ),
        )

        // The ALTER: id promotes to long, old drops, score arrives.
        alter.alterTable(
            cat,
            "ns",
            "t",
            listOf(
                AlterOp.PromoteColumn("id", ColType.LONG),
                AlterOp.DropColumn("old"),
                AlterOp.AddColumn(ColumnDef("score", ColType.DOUBLE)),
            ),
        )
        val scoreFieldId =
            catalogs.getTable(cat, "ns", "t").columns.single { it.def.name == "score" }.fieldId

        // Vintage 2: written under the live schema.
        val v2Schema =
            Types.buildMessage()
                .addField(Types.required(PrimitiveTypeName.INT64).id(1).named("id"))
                .addField(
                    Types.optional(PrimitiveTypeName.DOUBLE).id(Math.toIntExact(scoreFieldId)).named("score"),
                )
                .named("t")
        val v2Bytes =
            customParquetBytes(
                v2Schema,
                listOf({ g ->
                    g.add("id", 30L)
                    g.add("score", 1.5)
                }),
            )
        val v2Path = "s3://$BUCKET/$cat/data/ns/t/v2.parquet"
        store.put(v2Path, v2Bytes)
        commits.commit(
            cat,
            CommitRequest(
                appends =
                    listOf(
                        TableAppend("ns", "t", listOf(FileRegistration(v2Path, 1, v2Bytes.size.toLong()))),
                    ),
            ),
        )

        val result = svc.runOnce(cat, cfg)
        assertThat(result.groupsCompacted).isEqualTo(1)
        assertThat(result.unconvertibleSchema).isZero()

        val output = catalogs.listFiles(cat, "ns", "t").single()
        assertThat(output.recordCount).isEqualTo(3)
        assertThat(output.explicitRowIds).isTrue()

        // Read back: the LIVE schema (dropped 'old' is gone; its data
        // dropped with it), int32 values up-cast to int64, the added
        // column null-filled for vintage-1 rows, field ids intact.
        data class HetRow(val id: Long, val score: Double?, val rowId: Long)

        var outSchema: MessageType? = null
        val rows = mutableListOf<HetRow>()
        readGroups(store.get(output.path)) { s, g ->
            outSchema = s
            val scoreIdx = s.getFieldIndex("score")
            rows +=
                HetRow(
                    id = g.getLong(s.getFieldIndex("id"), 0),
                    score = if (g.getFieldRepetitionCount(scoreIdx) > 0) g.getDouble(scoreIdx, 0) else null,
                    rowId = g.getLong(s.getFieldIndex(ParquetRewriter.ROW_ID_COLUMN), 0),
                )
        }
        assertThat(outSchema!!.fields.map { it.name })
            .containsExactly("id", "score", ParquetRewriter.ROW_ID_COLUMN)
        assertThat(outSchema!!.getType("id").asPrimitiveType().primitiveTypeName)
            .isEqualTo(PrimitiveTypeName.INT64)
        assertThat(outSchema!!.getType("id").id.intValue()).isEqualTo(1)
        assertThat(outSchema!!.getType("score").id.intValue().toLong()).isEqualTo(scoreFieldId)
        assertThat(rows).containsExactly(
            HetRow(10, null, 0),
            HetRow(20, null, 1),
            HetRow(30, 1.5, 2),
        )
        assertVerifyPasses(cat)
    }

    @Test
    fun `an unproducible live column skips the group as unconvertible_schema and touches nothing`() {
        val cat = "compact-unconv-${counter.incrementAndGet()}"
        catalogs.createCatalog(cat, "s3://$BUCKET/$cat")
        catalogs.createNamespace(cat, "ns")
        // Live 'id' is INT; the files (a hostile/buggy writer) store
        // field 1 as INT64 — narrowing, never converted.
        catalogs.createTable(cat, "ns", "t", listOf(ColumnDef("id", ColType.INT, nullable = false)))
        val badSchema =
            Types.buildMessage()
                .addField(Types.required(PrimitiveTypeName.INT64).id(1).named("id"))
                .named("t")
        val regs =
            (0..1).map { i ->
                val bytes = customParquetBytes(badSchema, listOf({ g -> g.add("id", i.toLong()) }))
                val path = "s3://$BUCKET/$cat/data/ns/t/bad$i.parquet"
                store.put(path, bytes)
                FileRegistration(path, 1, bytes.size.toLong())
            }
        commits.commit(cat, CommitRequest(appends = listOf(TableAppend("ns", "t", regs))))
        val headBefore = catalogs.getCatalog(cat).headSnapshotId

        val result = svc.runOnce(cat, cfg)
        assertThat(result.groupsCompacted).isZero()
        assertThat(result.unconvertibleSchema).isEqualTo(1)
        assertThat(result.skippedConflicts).isZero()
        assertThat(result.dvSuperseded).isZero()

        // Nothing moved: no snapshot, both files live, no staging ticket
        // (unconvertibility is detected before anything is staged or
        // uploaded).
        assertThat(catalogs.getCatalog(cat).headSnapshotId).isEqualTo(headBefore)
        assertThat(catalogs.listFiles(cat, "ns", "t")).hasSize(2)
        assertThat(removalRows(cat)).isEmpty()
    }

    // ---- lifecycle ---------------------------------------------------------

    @Test
    fun `compacted-away inputs and their dead DV become expiry-queueable once unreachable`() {
        val fx = fixture()
        svc.runOnce(fx.cat, cfg)

        // Age every snapshot far past a 60s retention and sweep: the floor
        // advances to head, the inputs' (and the applied DV's) end_snapshot
        // sinks below it, and their paths enter the removal queue. Only the
        // live output stays.
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
        assertThat(expiry.dataFilesQueued).isEqualTo(3)
        assertThat(expiry.deleteFilesQueued).isEqualTo(1)

        val queued =
            db.jdbi.withHandleUnchecked { h ->
                h.createQuery(
                    """
                    SELECT r.path, r.file_kind FROM hog_file_removal r
                    JOIN hog_catalog c ON c.catalog_id = r.catalog_id
                    WHERE c.name = :cat AND r.reason = 'snapshot_expiry'
                    """,
                ).bind("cat", fx.cat)
                    .map { rs, _ -> rs.getString("path") to rs.getString("file_kind") }
                    .list()
            }
        assertThat(queued.filter { it.second == "data" }.map { it.first })
            .containsExactlyInAnyOrderElementsOf(fx.paths)
        assertThat(queued.filter { it.second == "delete" }.map { it.first })
            .containsExactly(fx.dvPath)
        // Only the compacted output survives.
        assertThat(catalogs.listFiles(fx.cat, "ns", "t").map { it.path })
            .singleElement()
            .matches { it.contains("compacted-") }
    }

    @Test
    fun `inputs with pending stats produce a pending output for the hydrator`() {
        val cat = "compact-pending-${counter.incrementAndGet()}"
        catalogs.createCatalog(cat, "s3://$BUCKET/$cat")
        catalogs.createNamespace(cat, "ns")
        catalogs.createTable(cat, "ns", "t", listOf(ColumnDef("id", ColType.LONG, nullable = false)))
        val rows1 = listOf(TestRow(1, null, null), TestRow(2, null, null))
        val rows2 = listOf(TestRow(3, null, null))
        // The parquet shape carries name/score under field ids the live
        // schema does not know — the rewrite DROPS those columns (dropped
        // -column semantics), keeping only the live `id`.
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
        assertThat(readSchemaOnly(store.get(output.path)).fields.map { it.name })
            .containsExactly("id", ParquetRewriter.ROW_ID_COLUMN)
    }

    private fun readSchemaOnly(bytes: ByteArray): MessageType {
        val tmp = Files.createTempFile("compact-schema", ".parquet")
        try {
            Files.write(tmp, bytes)
            return ParquetFileReader.open(LocalInputFile(tmp)).use { it.footer.fileMetaData.schema }
        } finally {
            Files.deleteIfExists(tmp)
        }
    }
}
