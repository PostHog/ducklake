package com.posthog.hoglake.compaction

import com.posthog.hoglake.commit.CommitService
import com.posthog.hoglake.hydrator.ObjectStore
import com.posthog.hoglake.model.AlterOp
import com.posthog.hoglake.model.ColType
import com.posthog.hoglake.model.ColumnDef
import com.posthog.hoglake.model.CommitRequest
import com.posthog.hoglake.model.DeleteFileRegistration
import com.posthog.hoglake.model.FileRegistration
import com.posthog.hoglake.model.PartitionFieldDef
import com.posthog.hoglake.model.TableAppend
import com.posthog.hoglake.model.TableDeletes
import com.posthog.hoglake.model.Transform
import com.posthog.hoglake.service.AlterService
import com.posthog.hoglake.service.CatalogService
import com.posthog.hoglake.testing.PgTestSupport
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import java.util.concurrent.atomic.AtomicInteger

/**
 * Compaction PLANNING is metadata-only: no object-store contact
 * happens here (the store below points at a dead endpoint). Covers
 * candidate selection (live, under target, DV-free), bucketing by
 * (spec_id, partition_values), greedy size-capped runs, and the
 * min-input-files threshold.
 */
@Tag("integration")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class CompactionPlanningIntegrationTest {
    private val db = PgTestSupport.freshDatabase()
    private val catalogs = CatalogService(db.jdbi)
    private val commits = CommitService(db.jdbi)
    private val alter = AlterService(db.jdbi)
    private val counter = AtomicInteger(0)

    private val deadStore =
        ObjectStore(
            endpoint = "http://127.0.0.1:9",
            region = "us-east-1",
            accessKey = "unused",
            secretKey = "unused",
            pathStyle = true,
        )

    private val cfg = CompactionConfig(targetBytes = 1000, minInputFiles = 2, maxGroupsPerRun = 10)
    private val svc = CompactionService(db.jdbi, deadStore, cfg)

    @AfterAll
    fun tearDown() {
        deadStore.close()
        db.close()
    }

    private fun fixture(columns: List<ColumnDef> = listOf(ColumnDef("id", ColType.LONG))): String {
        val cat = "plan-cat-${counter.incrementAndGet()}"
        catalogs.createCatalog(cat, "s3://bucket/$cat")
        catalogs.createNamespace(cat, "ns")
        catalogs.createTable(cat, "ns", "t", columns)
        return cat
    }

    private fun append(
        cat: String,
        vararg files: FileRegistration,
    ) = commits.commit(cat, CommitRequest(appends = listOf(TableAppend("ns", "t", files.toList()))))

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

    @Test
    fun `small live files group together and files at or over target are excluded`() {
        val cat = fixture()
        append(cat, file("a", 300), file("b", 300), file("big", 1000), file("c", 300))
        val plan = svc.planTable(cat, "ns", "t", cfg)
        assertThat(plan.groups).hasSize(1)
        assertThat(plan.groups.single().files.map { it.path })
            .containsExactly(
                "s3://bucket/x/a.parquet",
                "s3://bucket/x/b.parquet",
                "s3://bucket/x/c.parquet",
            )
        assertThat(plan.groups.single().totalBytes).isEqualTo(900)
    }

    @Test
    fun `greedy grouping splits runs at the byte target`() {
        val cat = fixture()
        append(cat, file("a", 400), file("b", 400), file("c", 400), file("d", 400))
        val plan = svc.planTable(cat, "ns", "t", cfg)
        // 400+400 <= 1000, adding the third would exceed: two runs of two.
        assertThat(plan.groups).hasSize(2)
        assertThat(plan.groups[0].files).hasSize(2)
        assertThat(plan.groups[1].files).hasSize(2)
    }

    @Test
    fun `runs below min_input_files are not worth rewriting`() {
        val cat = fixture()
        append(cat, file("a", 100), file("b", 100), file("c", 100))
        val plan =
            svc.planTable(cat, "ns", "t", cfg.copy(minInputFiles = 4))
        assertThat(plan.groups).isEmpty()
    }

    @Test
    fun `a file with a live deletion vector is a candidate carrying its planned vector`() {
        val cat = fixture()
        val snap = append(cat, file("a", 100), file("b", 100), file("c", 100)).snapshotId
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
        commits.commit(
            cat,
            CommitRequest(
                readSnapshot = snap,
                deletes =
                    listOf(
                        TableDeletes(
                            "ns",
                            "t",
                            listOf(
                                DeleteFileRegistration(
                                    dataFileId = fileIds[1],
                                    path = "s3://bucket/x/b.dv",
                                    deleteCount = 1,
                                    fileSizeBytes = 10,
                                ),
                            ),
                        ),
                    ),
            ),
        )
        val plan = svc.planTable(cat, "ns", "t", cfg)
        // DV-bearing files compact too: the plan captures the live vector's
        // identity so execution applies it and commit detects supersession.
        assertThat(plan.groups).hasSize(1)
        val group = plan.groups.single()
        assertThat(group.files.map { it.path })
            .containsExactly("s3://bucket/x/a.parquet", "s3://bucket/x/b.parquet", "s3://bucket/x/c.parquet")
        assertThat(group.files.map { it.dv?.path })
            .containsExactly(null, "s3://bucket/x/b.dv", null)
        val plannedDv = group.files[1].dv!!
        assertThat(plannedDv.deleteCount).isEqualTo(1)
        // Survivors = gross minus the planned deletes.
        assertThat(group.totalRecords).isEqualTo(30)
        assertThat(group.survivingRecords).isEqualTo(29)
    }

    @Test
    fun `groups never mix partition values or spec vintages`() {
        val cat = fixture(listOf(ColumnDef("id", ColType.LONG), ColumnDef("bucket", ColType.STRING)))
        // Two unpartitioned files, then a spec, then files across two partitions.
        append(cat, file("u1", 100), file("u2", 100))
        alter.alterTable(
            cat,
            "ns",
            "t",
            listOf(AlterOp.SetPartitionSpec(listOf(PartitionFieldDef(2, Transform.IDENTITY)))),
        )
        append(
            cat,
            file("p1a", 100, values = listOf("p1")),
            file("p1b", 100, values = listOf("p1")),
            file("p2a", 100, values = listOf("p2")),
            file("p2b", 100, values = listOf("p2")),
        )
        val plan = svc.planTable(cat, "ns", "t", cfg)
        assertThat(plan.groups).hasSize(3)
        val byValues = plan.groups.associateBy { it.partitionValues }
        assertThat(byValues[null]!!.files.map { it.path })
            .containsExactly("s3://bucket/x/u1.parquet", "s3://bucket/x/u2.parquet")
        assertThat(byValues[listOf<String?>("p1")]!!.files.map { it.path })
            .containsExactly("s3://bucket/x/p1a.parquet", "s3://bucket/x/p1b.parquet")
        assertThat(byValues[listOf<String?>("p2")]!!.files.map { it.path })
            .containsExactly("s3://bucket/x/p2a.parquet", "s3://bucket/x/p2b.parquet")
        assertThat(byValues[listOf<String?>("p1")]!!.specId).isEqualTo(1)
    }

    @Test
    fun `candidates are ordered by row_id_start within a group`() {
        val cat = fixture()
        append(cat, file("a", 100, records = 5), file("b", 100, records = 7), file("c", 100, records = 3))
        val plan = svc.planTable(cat, "ns", "t", cfg)
        assertThat(plan.groups.single().files.map { it.rowIdStart }).containsExactly(0L, 5L, 12L)
    }
}
