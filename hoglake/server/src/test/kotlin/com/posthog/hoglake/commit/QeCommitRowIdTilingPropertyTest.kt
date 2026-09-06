package com.posthog.hoglake.commit

import com.posthog.hoglake.model.ColType
import com.posthog.hoglake.model.ColumnDef
import com.posthog.hoglake.model.CommitRequest
import com.posthog.hoglake.model.FileRegistration
import com.posthog.hoglake.model.TableAppend
import com.posthog.hoglake.service.CatalogService
import com.posthog.hoglake.testing.PgTestSupport
import io.kotest.property.Arb
import io.kotest.property.arbitrary.arbitrary
import io.kotest.property.arbitrary.int
import io.kotest.property.arbitrary.of
import io.kotest.property.checkAll
import kotlinx.coroutines.runBlocking
import org.assertj.core.api.Assertions.assertThat
import org.jdbi.v3.core.kotlin.withHandleUnchecked
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import java.util.concurrent.atomic.AtomicInteger

/**
 * The lineage guarantee as a property (fuzzing.md layer 1): for
 * GENERATED sequences of append commits — random table subsets, file
 * counts, and record counts INCLUDING zero-record files — the per-table
 * row-id ranges must tile [0, totalRows) exactly, in commit order, with
 * no overlap and no gap; hog_table_stats.next_row_id must equal the sum
 * of every record_count ever appended; and snapshot ids must stay dense
 * 0..head.
 */
@Tag("integration")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class QeCommitRowIdTilingPropertyTest {
    private val db = PgTestSupport.freshDatabase()
    private val catalogs = CatalogService(db.jdbi)
    private val commits = CommitService(db.jdbi)
    private val caseCounter = AtomicInteger(0)

    @AfterAll
    fun tearDown() = db.close()

    /** One commit: per-table lists of file record counts (empty map impossible). */
    private data class Plan(val tableCount: Int, val commits: List<Map<Int, List<Long>>>)

    private val arbRecordCount: Arb<Long> =
        Arb.of(0L, 0L, 1L, 2L, 7L, 100L, 4_095L, 1_000_000L)

    private val arbPlan: Arb<Plan> =
        arbitrary {
            val tableCount = Arb.int(1..3).bind()
            val commitCount = Arb.int(1..6).bind()
            val commits =
                (0 until commitCount).map {
                    val touched =
                        (0 until tableCount).filter { Arb.int(0..1).bind() == 1 }
                            .ifEmpty { listOf(0) }
                    touched.associateWith {
                        val fileCount = Arb.int(1..4).bind()
                        (0 until fileCount).map { arbRecordCount.bind() }
                    }
                }
            Plan(tableCount, commits)
        }

    @Test
    fun `row-id ranges tile exactly under generated append sequences`(): Unit =
        runBlocking {
            checkAll(15, arbPlan) { plan ->
                val name = "qe-tile-${caseCounter.incrementAndGet()}"
                catalogs.createCatalog(name, "s3://qe-tile/$name")
                catalogs.createNamespace(name, "ns")
                val tables =
                    (0 until plan.tableCount).map { i ->
                        catalogs.createTable(
                            name,
                            "ns",
                            "t$i",
                            listOf(ColumnDef("id", ColType.LONG, nullable = false)),
                        )
                    }

                var fileSeq = 0
                // Expected per-table (row_id_start, record_count) in append order.
                val expected = Array(plan.tableCount) { mutableListOf<Pair<Long, Long>>() }
                val nextRowId = LongArray(plan.tableCount)

                for (commit in plan.commits) {
                    val appends =
                        commit.map { (tableIdx, counts) ->
                            TableAppend(
                                namespace = "ns",
                                table = "t$tableIdx",
                                files =
                                    counts.map { rc ->
                                        FileRegistration(
                                            path = "s3://qe-tile/$name/f${fileSeq++}.parquet",
                                            recordCount = rc,
                                            fileSizeBytes = rc * 8,
                                        )
                                    },
                            )
                        }
                    val result = commits.commit(name, CommitRequest(appends = appends))
                    assertThat(result.snapshotId).isGreaterThan(0)
                    for ((tableIdx, counts) in commit) {
                        for (rc in counts) {
                            expected[tableIdx] += nextRowId[tableIdx] to rc
                            nextRowId[tableIdx] += rc
                        }
                    }
                }

                db.jdbi.withHandleUnchecked { h ->
                    val catalogId =
                        h.createQuery(
                            "SELECT catalog_id FROM hog_catalog WHERE name = ?",
                        ).bind(0, name).mapTo(Long::class.java).one()

                    // Snapshot ids dense 0..head.
                    val (head, count, min) =
                        h.createQuery(
                            """
                    SELECT c.last_snapshot_id, count(s.snapshot_id), min(s.snapshot_id)
                      FROM hog_catalog c JOIN hog_snapshot s ON s.catalog_id = c.catalog_id
                     WHERE c.catalog_id = ?
                     GROUP BY c.last_snapshot_id
                    """,
                        ).bind(0, catalogId)
                            .map { rs, _ -> Triple(rs.getLong(1), rs.getLong(2), rs.getLong(3)) }
                            .one()
                    assertThat(min).isEqualTo(0)
                    assertThat(count).describedAs("snapshot ids dense").isEqualTo(head + 1)

                    for ((i, t) in tables.withIndex()) {
                        // Files in append order (data_file_id is allocation order).
                        val actual =
                            h.createQuery(
                                """
                        SELECT row_id_start, record_count FROM hog_data_file
                         WHERE catalog_id = :c AND table_id = :t
                         ORDER BY data_file_id
                        """,
                            )
                                .bind("c", catalogId)
                                .bind("t", t.tableId)
                                .map { rs, _ -> rs.getLong(1) to rs.getLong(2) }
                                .list()
                        assertThat(actual)
                            .describedAs("table t%d ranges tile [0, %d) in append order", i, nextRowId[i])
                            .isEqualTo(expected[i])

                        // Cross-check with an ORDER BY row_id_start view: no
                        // overlap means sorting by start yields the same prefix
                        // sums (zero-width ranges may tie on start; break ties
                        // by id which equals append order).
                        val sorted = actual.sortedWith(compareBy({ it.first }))
                        var cursor = 0L
                        for ((start, rc) in sorted) {
                            assertThat(start)
                                .describedAs("no gap/overlap at row %d of t%d", cursor, i)
                                .isEqualTo(cursor)
                            cursor += rc
                        }
                        assertThat(cursor).isEqualTo(nextRowId[i])

                        val (dbNext, dbRecords) =
                            h.createQuery(
                                """
                        SELECT next_row_id, record_count FROM hog_table_stats
                         WHERE catalog_id = :c AND table_id = :t
                        """,
                            )
                                .bind("c", catalogId)
                                .bind("t", t.tableId)
                                .map { rs, _ -> rs.getLong(1) to rs.getLong(2) }
                                .one()
                        assertThat(dbNext)
                            .describedAs("next_row_id == sum of record_counts ever appended")
                            .isEqualTo(nextRowId[i])
                        assertThat(dbRecords).isEqualTo(nextRowId[i])
                    }
                }
            }
        }
}
