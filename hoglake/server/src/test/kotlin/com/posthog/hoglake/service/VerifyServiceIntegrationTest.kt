package com.posthog.hoglake.service

import com.posthog.hoglake.commit.CommitService
import com.posthog.hoglake.model.ColType
import com.posthog.hoglake.model.ColumnDef
import com.posthog.hoglake.model.CommitRequest
import com.posthog.hoglake.model.DeleteFileRegistration
import com.posthog.hoglake.model.FileRegistration
import com.posthog.hoglake.model.TableAppend
import com.posthog.hoglake.model.TableDeletes
import com.posthog.hoglake.model.VerifyReport
import com.posthog.hoglake.testing.PgTestSupport
import org.assertj.core.api.Assertions.assertThat
import org.jdbi.v3.core.kotlin.useHandleUnchecked
import org.jdbi.v3.core.kotlin.withHandleUnchecked
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance

/**
 * B3 — the invariant scan: a healthy catalog passes every check, and
 * each violation class, seeded via raw SQL (the service layer refuses
 * to produce these states), trips exactly its own check.
 */
@Tag("integration")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class VerifyServiceIntegrationTest {
    private val db = PgTestSupport.freshDatabase()
    private val catalogs = CatalogService(db.jdbi)
    private val commits = CommitService(db.jdbi)
    private val expiry = ExpiryService(db.jdbi)
    private val verify = VerifyService(db.jdbi)

    @AfterAll
    fun tearDown() = db.close()

    private fun catalogId(name: String): Long =
        db.jdbi.withHandleUnchecked { h ->
            h.createQuery("SELECT catalog_id FROM hog_catalog WHERE name = ?")
                .bind(0, name).mapTo(Long::class.java).one()
        }

    /** catalog + ns + table "t", plus [files] appended row counts. */
    private fun seed(
        catalog: String,
        files: List<Long> = emptyList(),
    ): Long {
        catalogs.createCatalog(catalog, "s3://vfy/$catalog")
        catalogs.createNamespace(catalog, "ns")
        catalogs.createTable(catalog, "ns", "t", listOf(ColumnDef("id", ColType.LONG)))
        if (files.isNotEmpty()) {
            commits.commit(
                catalog,
                CommitRequest(
                    appends =
                        listOf(
                            TableAppend(
                                "ns",
                                "t",
                                files.mapIndexed { i, rc ->
                                    FileRegistration("s3://vfy/$catalog/f$i.parquet", rc, rc * 8)
                                },
                            ),
                        ),
                ),
            )
        }
        return catalogId(catalog)
    }

    private fun VerifyReport.check(name: String) = checks.single { it.check == name }

    @Test
    fun `a freshly created never-committed catalog passes every check`() {
        // Pinned for bug hunt #14 (a FALSE finding, verified here at the
        // service level): createCatalog seeds snapshot 0, so a
        // never-committed catalog has head=0, earliest=0, count(*)=1 —
        // exactly head - earliest + 1. Formula and seed agree.
        catalogs.createCatalog("vfy-fresh", "s3://vfy/vfy-fresh")
        val report = verify.runOnce("vfy-fresh")
        assertThat(report.status).isEqualTo("pass")
        assertThat(report.check("snapshot_density").violations).isZero()
    }

    private fun assertOnlyFails(
        report: VerifyReport,
        vararg failing: String,
    ) {
        assertThat(report.status).isEqualTo(if (failing.isEmpty()) "pass" else "fail")
        for (c in report.checks) {
            if (c.check in failing) {
                assertThat(c.status).describedAs("check %s should fail", c.check).isEqualTo("fail")
                assertThat(c.violations).isGreaterThan(0)
                assertThat(c.samples).isNotEmpty()
            } else {
                assertThat(c.status).describedAs("check %s should pass", c.check).isEqualTo("pass")
                assertThat(c.violations).isEqualTo(0)
            }
        }
    }

    @Test
    fun `a healthy catalog passes every check - commits, DVs, drop, and expiry included`() {
        val catalog = "vfy-healthy"
        seed(catalog, files = listOf(10L, 5L, 0L))
        // A DV, a supersession, a dropped table, and an expiry sweep: the
        // realistic states verify must NOT flag.
        val head =
            db.jdbi.withHandleUnchecked { h ->
                h.createQuery("SELECT last_snapshot_id FROM hog_catalog WHERE name = ?")
                    .bind(0, catalog).mapTo(Long::class.java).one()
            }
        commits.commit(
            catalog,
            CommitRequest(
                readSnapshot = head,
                deletes =
                    listOf(
                        TableDeletes(
                            "ns",
                            "t",
                            listOf(DeleteFileRegistration(1, "s3://vfy/$catalog/dv1.puffin", 2, 16)),
                        ),
                    ),
            ),
        )
        commits.commit(
            catalog,
            CommitRequest(
                readSnapshot = head + 1,
                deletes =
                    listOf(
                        TableDeletes(
                            "ns",
                            "t",
                            listOf(DeleteFileRegistration(1, "s3://vfy/$catalog/dv2.puffin", 4, 16)),
                        ),
                    ),
            ),
        )
        catalogs.createTable(catalog, "ns", "doomed", listOf(ColumnDef("id", ColType.LONG)))
        commits.commit(
            catalog,
            CommitRequest(
                appends =
                    listOf(
                        TableAppend(
                            "ns",
                            "doomed",
                            listOf(FileRegistration("s3://vfy/$catalog/doomed.parquet", 3, 24)),
                        ),
                    ),
            ),
        )
        catalogs.dropTable(catalog, "ns", "doomed")
        // Age + retention so an expiry sweep really moves the floor.
        db.jdbi.useHandleUnchecked { h ->
            h.createUpdate(
                """
                UPDATE hog_snapshot SET snapshot_time = now() - make_interval(secs => 3600)
                WHERE catalog_id = :c AND snapshot_id < 3
                """,
            ).bind("c", catalogId(catalog)).execute()
            h.createUpdate(
                "UPDATE hog_catalog SET snapshot_retention_seconds = 60, consumer_floor = false " +
                    "WHERE name = :n",
            ).bind("n", catalog).execute()
        }
        val swept = expiry.runOnce(catalog, batchSize = 1000)
        assertThat(swept.snapshotsExpired).isGreaterThan(0)

        val report = verify.runOnce(catalog)
        assertThat(report.catalog).isEqualTo(catalog)
        assertThat(report.checks.map { it.check }).containsExactly(
            "row_id_tiling",
            "delete_vectors",
            "orphans",
            "removal_queue",
            "snapshot_density",
            "next_row_id",
        )
        assertOnlyFails(report)
    }

    @Test
    fun `an overlapping positional row-id range trips row_id_tiling only`() {
        val cid = seed("vfy-tiling", files = listOf(10L, 10L)) // ranges [0,10) [10,20)
        db.jdbi.useHandleUnchecked { h ->
            h.createUpdate(
                """
                INSERT INTO hog_data_file (catalog_id, data_file_id, table_id, begin_snapshot,
                                           path, record_count, file_size_bytes, row_id_start)
                SELECT :c, 999, table_id, 3, 's3://vfy/vfy-tiling/overlap.parquet', 10, 80, 5
                FROM hog_table_stats WHERE catalog_id = :c
                """,
            ).bind("c", cid).execute()
        }
        assertOnlyFails(verify.runOnce("vfy-tiling"), "row_id_tiling")
        val sample = verify.runOnce("vfy-tiling").check("row_id_tiling").samples.first()
        assertThat(sample).contains("overlaps")
    }

    @Test
    fun `a compaction-style explicit_row_ids file sharing input ids is NOT a tiling violation`() {
        val cid = seed("vfy-explicit", files = listOf(10L, 10L))
        // A compacted output covering both inputs' ids: row_id_start =
        // min(input starts), record_count = sum — positionally it would
        // "overlap" everything, which is exactly why the sweep exempts it.
        db.jdbi.useHandleUnchecked { h ->
            h.createUpdate(
                """
                INSERT INTO hog_data_file (catalog_id, data_file_id, table_id, begin_snapshot,
                                           path, record_count, file_size_bytes, row_id_start,
                                           explicit_row_ids)
                SELECT :c, 998, table_id, 3, 's3://vfy/vfy-explicit/compacted.parquet', 20, 160, 0, true
                FROM hog_table_stats WHERE catalog_id = :c
                """,
            ).bind("c", cid).execute()
        }
        assertOnlyFails(verify.runOnce("vfy-explicit"))
    }

    @Test
    fun `a shrinking deletion-vector chain trips delete_vectors only`() {
        val catalog = "vfy-dv"
        val cid = seed(catalog, files = listOf(10L))
        commits.commit(
            catalog,
            CommitRequest(
                readSnapshot = 3,
                deletes =
                    listOf(
                        TableDeletes(
                            "ns",
                            "t",
                            listOf(DeleteFileRegistration(1, "s3://vfy/$catalog/dv.puffin", 2, 16)),
                        ),
                    ),
            ),
        )
        // A fabricated HISTORICAL DV with a HIGHER count than the live
        // one that superseded it: the chain shrank — invariant 3 broken.
        db.jdbi.useHandleUnchecked { h ->
            h.createUpdate(
                """
                INSERT INTO hog_delete_file (catalog_id, delete_file_id, table_id, data_file_id,
                                             begin_snapshot, end_snapshot, path, delete_count,
                                             file_size_bytes)
                SELECT :c, 997, table_id, 1, 1, 4, 's3://vfy/vfy-dv/old.puffin', 5, 16
                FROM hog_table_stats WHERE catalog_id = :c
                """,
            ).bind("c", cid).execute()
        }
        assertOnlyFails(verify.runOnce(catalog), "delete_vectors")
        assertThat(verify.runOnce(catalog).check("delete_vectors").samples.first())
            .contains("shrank")
    }

    @Test
    fun `a live file row on a dropped table trips orphans only`() {
        val catalog = "vfy-orphan"
        val cid = seed(catalog) // no files: nothing for tiling/next_row_id to see
        catalogs.dropTable(catalog, "ns", "t")
        db.jdbi.useHandleUnchecked { h ->
            h.createUpdate(
                """
                INSERT INTO hog_data_file (catalog_id, data_file_id, table_id, begin_snapshot,
                                           path, record_count, file_size_bytes, row_id_start)
                SELECT :c, 996, table_id, 1, 's3://vfy/vfy-orphan/live-on-dropped.parquet', 0, 0, 0
                FROM hog_table_stats WHERE catalog_id = :c
                """,
            ).bind("c", cid).execute()
        }
        assertOnlyFails(verify.runOnce(catalog), "orphans")
    }

    @Test
    fun `an undrained removal-queue row whose path is still referenced trips removal_queue only`() {
        val catalog = "vfy-queue"
        val cid = seed(catalog, files = listOf(5L))
        db.jdbi.useHandleUnchecked { h ->
            h.createUpdate(
                """
                INSERT INTO hog_file_removal (catalog_id, path, file_kind, reason)
                VALUES (:c, 's3://vfy/$catalog/f0.parquet', 'data', 'snapshot_expiry')
                """,
            ).bind("c", cid).execute()
        }
        assertOnlyFails(verify.runOnce(catalog), "removal_queue")
        // A DRAINED ledger row with the same path is history, not a violation.
        db.jdbi.useHandleUnchecked { h ->
            h.createUpdate(
                "UPDATE hog_file_removal SET drained_at = now(), drained_outcome = 'absent' " +
                    "WHERE catalog_id = :c",
            ).bind("c", cid).execute()
        }
        assertOnlyFails(verify.runOnce(catalog))
    }

    @Test
    fun `a hole in the snapshot sequence trips snapshot_density only`() {
        val cid = seed("vfy-density", files = listOf(2L)) // snapshots 0..3
        db.jdbi.useHandleUnchecked { h ->
            h.createUpdate("DELETE FROM hog_snapshot WHERE catalog_id = :c AND snapshot_id = 1")
                .bind("c", cid).execute()
        }
        assertOnlyFails(verify.runOnce("vfy-density"), "snapshot_density")
        assertThat(verify.runOnce("vfy-density").check("snapshot_density").samples.first())
            .contains("count(*)")
    }

    @Test
    fun `an allocator behind its handed-out ranges trips next_row_id only`() {
        val cid = seed("vfy-alloc", files = listOf(10L)) // range [0,10), next_row_id = 10
        db.jdbi.useHandleUnchecked { h ->
            h.createUpdate("UPDATE hog_table_stats SET next_row_id = 5 WHERE catalog_id = :c")
                .bind("c", cid).execute()
        }
        assertOnlyFails(verify.runOnce("vfy-alloc"), "next_row_id")
    }
}
