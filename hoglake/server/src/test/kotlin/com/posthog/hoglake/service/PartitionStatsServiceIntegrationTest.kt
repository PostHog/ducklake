package com.posthog.hoglake.service

import com.posthog.hoglake.commit.CommitService
import com.posthog.hoglake.model.AlterOp
import com.posthog.hoglake.model.ColType
import com.posthog.hoglake.model.ColumnDef
import com.posthog.hoglake.model.CommitRequest
import com.posthog.hoglake.model.DeleteFileRegistration
import com.posthog.hoglake.model.FileRegistration
import com.posthog.hoglake.model.HoglakeException
import com.posthog.hoglake.model.PartitionFieldDef
import com.posthog.hoglake.model.PartitionValue
import com.posthog.hoglake.model.TableAppend
import com.posthog.hoglake.model.TableDeletes
import com.posthog.hoglake.model.Transform
import com.posthog.hoglake.testing.PgTestSupport
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import java.util.concurrent.atomic.AtomicInteger

/**
 * PartitionStatsService: leaf partitions ranked by compaction debt.
 * Metadata-only — no object store anywhere. The threshold here is a
 * test-sized 1000 bytes; production wires Config.compactionTargetBytes
 * (the same knob CompactionService plans with).
 */
@Tag("integration")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class PartitionStatsServiceIntegrationTest {
    private val db = PgTestSupport.freshDatabase()
    private val catalogs = CatalogService(db.jdbi)
    private val commits = CommitService(db.jdbi)
    private val alter = AlterService(db.jdbi)
    private val counter = AtomicInteger(0)

    /** Strict less-than 1000 = "small", mirroring the compaction planner. */
    private val svc = PartitionStatsService(db.jdbi, smallFileThresholdBytes = 1000)

    @AfterAll
    fun tearDown() = db.close()

    private fun fixture(
        columns: List<ColumnDef> = listOf(ColumnDef("id", ColType.LONG), ColumnDef("team", ColType.STRING)),
        table: String = "t",
    ): String {
        val cat = "pstats-cat-${counter.incrementAndGet()}"
        catalogs.createCatalog(cat, "s3://bucket/$cat")
        catalogs.createNamespace(cat, "ns")
        catalogs.createTable(cat, "ns", table, columns)
        return cat
    }

    private fun append(
        cat: String,
        table: String,
        vararg files: FileRegistration,
    ) = commits.commit(cat, CommitRequest(appends = listOf(TableAppend("ns", table, files.toList()))))

    private fun file(
        name: String,
        bytes: Long,
        records: Long = 10,
        values: List<String?>? = null,
    ) = FileRegistration(
        path = "s3://bucket/x/$name.parquet",
        recordCount = records,
        fileSizeBytes = bytes,
        partitionValues = values,
    )

    private fun partitionByTeam(
        cat: String,
        table: String = "t",
    ) = alter.alterTable(
        cat,
        "ns",
        table,
        listOf(AlterOp.SetPartitionSpec(listOf(PartitionFieldDef(2, Transform.IDENTITY)))),
    )

    // ---- ranking -------------------------------------------------------------

    @Test
    fun `unpartitioned vintage and partitions rank together by small-file debt`() {
        val cat = fixture()
        // Pre-spec vintage: three small files, spec-less.
        append(cat, "t", file("u1", 100), file("u2", 100), file("u3", 100))
        partitionByTeam(cat)
        append(
            cat,
            "t",
            file("p1a", 100, values = listOf("p1")),
            file("p1b", 100, values = listOf("p1")),
            file("p1c", 100, values = listOf("p1")),
            file("p1d", 100, values = listOf("p1")),
            file("p2a", 100, values = listOf("p2")),
            file("p2big", 1500, values = listOf("p2")),
        )

        val report = svc.partitionStats(cat, null, null, 50)
        assertThat(report.truncated).isFalse()
        assertThat(report.partitions).hasSize(3)

        val tableUuid = catalogs.getTable(cat, "ns", "t", null, null).tableUuid
        assertThat(report.partitions.map { it.tableUuid }).containsOnly(tableUuid)
        assertThat(report.partitions.map { it.namespace }).containsOnly("ns")
        assertThat(report.partitions.map { it.table }).containsOnly("t")

        val (p1, unpart, p2) = report.partitions
        assertThat(p1.partitionValues).containsExactly(PartitionValue("team", "p1"))
        assertThat(p1.specId).isEqualTo(1)
        assertThat(p1.fileCount).isEqualTo(4)
        assertThat(p1.smallFileCount).isEqualTo(4)
        assertThat(p1.debtScore).isEqualTo(4)
        assertThat(p1.totalBytes).isEqualTo(400)
        assertThat(p1.smallFileBytes).isEqualTo(400)
        assertThat(p1.avgFileBytes).isEqualTo(100)
        assertThat(p1.dvCount).isEqualTo(0)

        // The pre-spec vintage: one group, empty values, no spec.
        assertThat(unpart.partitionValues).isEmpty()
        assertThat(unpart.specId).isNull()
        assertThat(unpart.debtScore).isEqualTo(3)

        assertThat(p2.partitionValues).containsExactly(PartitionValue("team", "p2"))
        assertThat(p2.fileCount).isEqualTo(2)
        assertThat(p2.smallFileCount).isEqualTo(1)
        assertThat(p2.debtScore).isEqualTo(1)
        assertThat(p2.totalBytes).isEqualTo(1600)
        assertThat(p2.smallFileBytes).isEqualTo(100)
        assertThat(p2.avgFileBytes).isEqualTo(800)

        // The spec-less vintage is not the table's current spec vintage.
        assertThat(report.staleSpecGroups).isEqualTo(1)
    }

    @Test
    fun `worst-case seeded ordering is debt desc with small-byte tiebreak`() {
        val cat = fixture()
        // Pre-spec vintage: debt 2, small bytes 1200.
        append(cat, "t", file("u1", 600), file("u2", 600))
        partitionByTeam(cat)
        append(
            cat,
            "t",
            // a: debt 3, small bytes 900.
            file("a1", 300, values = listOf("a")),
            file("a2", 300, values = listOf("a")),
            file("a3", 300, values = listOf("a")),
            // b: debt 3, small bytes 300 — loses the tie to a.
            file("b1", 100, values = listOf("b")),
            file("b2", 100, values = listOf("b")),
            file("b3", 100, values = listOf("b")),
            // c: debt 4 — the winner.
            file("c1", 50, values = listOf("c")),
            file("c2", 50, values = listOf("c")),
            file("c3", 50, values = listOf("c")),
            file("c4", 50, values = listOf("c")),
            // d: debt 2, small bytes 800 — loses the tie to the vintage's 1200.
            file("d1", 400, values = listOf("d")),
            file("d2", 400, values = listOf("d")),
            file("dbig", 2000, values = listOf("d")),
            // e: no debt at all, still listed.
            file("ebig", 5000, values = listOf("e")),
        )

        val report = svc.partitionStats(cat, null, null, 50)
        assertThat(report.partitions.map { it.partitionValues.map(PartitionValue::value) })
            .containsExactly(
                listOf("c"),
                listOf("a"),
                listOf("b"),
                emptyList(),
                listOf("d"),
                listOf("e"),
            )
        assertThat(report.partitions.map { it.debtScore }).containsExactly(4L, 3L, 3L, 2L, 2L, 0L)
        assertThat(report.partitions).allSatisfy { assertThat(it.debtScore).isEqualTo(it.smallFileCount) }
    }

    // ---- partition field naming ------------------------------------------------

    @Test
    fun `field names are column names for identity and column_transform otherwise`() {
        val cat =
            fixture(
                columns =
                    listOf(
                        ColumnDef("id", ColType.LONG),
                        ColumnDef("team", ColType.STRING),
                        ColumnDef("ts", ColType.TIMESTAMP),
                    ),
            )
        alter.alterTable(
            cat,
            "ns",
            "t",
            listOf(
                AlterOp.SetPartitionSpec(
                    listOf(
                        PartitionFieldDef(2, Transform.IDENTITY),
                        PartitionFieldDef(3, Transform.MONTH),
                    ),
                ),
            ),
        )
        append(
            cat,
            "t",
            file("a", 100, values = listOf("42", "2026-09")),
            file("b", 100, values = listOf(null, "2026-10")),
        )

        val report = svc.partitionStats(cat, null, null, 50)
        assertThat(report.partitions).hasSize(2)
        val by = report.partitions.associateBy { it.partitionValues.map(PartitionValue::value) }
        assertThat(by[listOf("42", "2026-09")]!!.partitionValues)
            .containsExactly(PartitionValue("team", "42"), PartitionValue("ts_month", "2026-09"))
        // A null partition value keeps its field pairing.
        assertThat(by[listOf(null, "2026-10")]!!.partitionValues)
            .containsExactly(PartitionValue("team", null), PartitionValue("ts_month", "2026-10"))
        assertThat(report.staleSpecGroups).isEqualTo(0)
    }

    @Test
    fun `files under an older spec group under their own spec vintage`() {
        val cat =
            fixture(
                columns =
                    listOf(
                        ColumnDef("id", ColType.LONG),
                        ColumnDef("team", ColType.STRING),
                        ColumnDef("region", ColType.STRING),
                    ),
            )
        partitionByTeam(cat) // spec 1: identity(team)
        append(cat, "t", file("old1", 100, values = listOf("t1")), file("old2", 100, values = listOf("t1")))
        alter.alterTable(
            cat,
            "ns",
            "t",
            listOf(AlterOp.SetPartitionSpec(listOf(PartitionFieldDef(3, Transform.IDENTITY)))),
        ) // spec 2: identity(region)
        append(cat, "t", file("new1", 100, values = listOf("r1")))

        val report = svc.partitionStats(cat, null, null, 50)
        assertThat(report.partitions).hasSize(2)
        val bySpec = report.partitions.associateBy { it.specId }
        assertThat(bySpec[1L]!!.partitionValues).containsExactly(PartitionValue("team", "t1"))
        assertThat(bySpec[1L]!!.fileCount).isEqualTo(2)
        assertThat(bySpec[2L]!!.partitionValues).containsExactly(PartitionValue("region", "r1"))
        // The spec-1 group is a leftover vintage; spec 2 is current.
        assertThat(report.staleSpecGroups).isEqualTo(1)
    }

    // ---- filters ---------------------------------------------------------------

    @Test
    fun `namespace and table filters scope the ranking, and half a scope is refused`() {
        val cat = "pstats-cat-${counter.incrementAndGet()}"
        catalogs.createCatalog(cat, "s3://bucket/$cat")
        catalogs.createNamespace(cat, "ns1")
        catalogs.createNamespace(cat, "ns2")
        val cols = listOf(ColumnDef("id", ColType.LONG))
        catalogs.createTable(cat, "ns1", "t1", cols)
        catalogs.createTable(cat, "ns1", "t2", cols)
        catalogs.createTable(cat, "ns2", "t3", cols)
        for ((ns, t) in listOf("ns1" to "t1", "ns1" to "t2", "ns2" to "t3")) {
            commits.commit(
                cat,
                CommitRequest(appends = listOf(TableAppend(ns, t, listOf(file("$ns-$t", 100))))),
            )
        }

        assertThat(svc.partitionStats(cat, null, null, 50).partitions).hasSize(3)

        val ns1 = svc.partitionStats(cat, "ns1", null, 50)
        assertThat(ns1.partitions.map { it.table }).containsExactlyInAnyOrder("t1", "t2")

        val one = svc.partitionStats(cat, "ns1", "t1", 50)
        assertThat(one.partitions.map { it.table }).containsExactly("t1")

        assertThatThrownBy { svc.partitionStats(cat, null, "t1", 50) }
            .isInstanceOf(HoglakeException.Validation::class.java)
        assertThatThrownBy { svc.partitionStats(cat, "nope", null, 50) }
            .isInstanceOf(HoglakeException.NotFound::class.java)
        assertThatThrownBy { svc.partitionStats(cat, "ns1", "nope", 50) }
            .isInstanceOf(HoglakeException.NotFound::class.java)
        assertThatThrownBy { svc.partitionStats("no-such-catalog", null, null, 50) }
            .isInstanceOf(HoglakeException.NotFound::class.java)
    }

    // ---- limit / truncation ------------------------------------------------------

    @Test
    fun `limit truncates the ranking and reports it`() {
        val cat = fixture()
        partitionByTeam(cat)
        append(
            cat,
            "t",
            file("a1", 100, values = listOf("a")),
            file("a2", 100, values = listOf("a")),
            file("a3", 100, values = listOf("a")),
            file("b1", 100, values = listOf("b")),
            file("b2", 100, values = listOf("b")),
            file("c1", 100, values = listOf("c")),
        )

        val page = svc.partitionStats(cat, null, null, 2)
        assertThat(page.truncated).isTrue()
        assertThat(page.partitions.map { it.partitionValues.single().value })
            .containsExactly("a", "b")
        // The stale-spec rollup covers the WHOLE group set, not the page.
        assertThat(page.staleSpecGroups).isEqualTo(0)

        val all = svc.partitionStats(cat, null, null, 3)
        assertThat(all.truncated).isFalse()
        assertThat(all.partitions).hasSize(3)

        assertThatThrownBy { svc.partitionStats(cat, null, null, 0) }
            .isInstanceOf(HoglakeException.Validation::class.java)
    }

    // ---- small-file threshold boundary ---------------------------------------------

    @Test
    fun `threshold is strict less-than, matching the compaction planner`() {
        val cat = fixture(columns = listOf(ColumnDef("id", ColType.LONG)))
        append(cat, "t", file("under", 999), file("at", 1000), file("over", 1001))

        val entry = svc.partitionStats(cat, null, null, 50).partitions.single()
        assertThat(entry.fileCount).isEqualTo(3)
        assertThat(entry.smallFileCount).isEqualTo(1)
        assertThat(entry.smallFileBytes).isEqualTo(999)
        assertThat(entry.debtScore).isEqualTo(1)
        assertThat(entry.totalBytes).isEqualTo(3000)
        assertThat(entry.avgFileBytes).isEqualTo(1000)
    }

    // ---- dropped tables -------------------------------------------------------------

    @Test
    fun `dropped tables never appear`() {
        val cat = fixture(columns = listOf(ColumnDef("id", ColType.LONG)))
        catalogs.createTable(cat, "ns", "doomed", listOf(ColumnDef("id", ColType.LONG)))
        append(cat, "t", file("keep", 100))
        append(cat, "doomed", file("gone", 100))
        assertThat(svc.partitionStats(cat, null, null, 50).partitions.map { it.table })
            .containsExactlyInAnyOrder("t", "doomed")

        catalogs.dropTable(cat, "ns", "doomed")
        assertThat(svc.partitionStats(cat, null, null, 50).partitions.map { it.table })
            .containsExactly("t")
    }

    // ---- deletion vectors -------------------------------------------------------------

    @Test
    fun `dv_count counts live DVs, one per file, supersession not double-counted`() {
        val cat = fixture(columns = listOf(ColumnDef("id", ColType.LONG)))
        val snap = append(cat, "t", file("a", 100), file("b", 100), file("c", 100)).snapshotId
        val fileIds =
            db.jdbi.withHandle<List<Long>, Exception> { h ->
                h.createQuery(
                    """
                    SELECT f.data_file_id FROM hog_data_file f
                    JOIN hog_catalog c ON c.catalog_id = f.catalog_id
                    WHERE c.name = :cat ORDER BY f.data_file_id
                    """,
                )
                    .bind("cat", cat)
                    .mapTo(Long::class.java).list()
            }

        fun dv(
            readSnapshot: Long,
            dataFileId: Long,
            name: String,
            count: Long,
        ) = commits.commit(
            cat,
            CommitRequest(
                readSnapshot = readSnapshot,
                deletes =
                    listOf(
                        TableDeletes(
                            "ns",
                            "t",
                            listOf(
                                DeleteFileRegistration(
                                    dataFileId = dataFileId,
                                    path = "s3://bucket/x/$name.dv",
                                    deleteCount = count,
                                    fileSizeBytes = 10,
                                ),
                            ),
                        ),
                    ),
            ),
        ).snapshotId

        val s1 = dv(snap, fileIds[0], "a1", 1)
        // Supersede a's DV: still ONE live DV for that file.
        val s2 = dv(s1, fileIds[0], "a2", 2)
        dv(s2, fileIds[1], "b1", 1)

        val entry = svc.partitionStats(cat, null, null, 50).partitions.single()
        assertThat(entry.fileCount).isEqualTo(3) // DV'd files stay live
        assertThat(entry.dvCount).isEqualTo(2)
    }
}
