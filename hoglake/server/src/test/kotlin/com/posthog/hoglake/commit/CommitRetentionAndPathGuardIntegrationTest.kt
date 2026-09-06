package com.posthog.hoglake.commit

import com.posthog.hoglake.model.ColType
import com.posthog.hoglake.model.ColumnDef
import com.posthog.hoglake.model.CommitRequest
import com.posthog.hoglake.model.DeleteFileRegistration
import com.posthog.hoglake.model.FileRegistration
import com.posthog.hoglake.model.HoglakeException
import com.posthog.hoglake.model.TableAppend
import com.posthog.hoglake.model.TableDeletes
import com.posthog.hoglake.persistence.SnapshotRepo
import com.posthog.hoglake.service.AlterService
import com.posthog.hoglake.service.CatalogService
import com.posthog.hoglake.service.ExpiryService
import com.posthog.hoglake.testing.PgTestSupport
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.jdbi.v3.core.kotlin.useHandleUnchecked
import org.jdbi.v3.core.kotlin.withHandleUnchecked
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import java.util.concurrent.atomic.AtomicInteger

/**
 * Pinned regressions for the commit tail's retention/path guards:
 *
 *  - bug hunt #1: a readSnapshot below the expiry floor must 410 —
 *    expiry cascade-deleted the change rows the conflict check needs,
 *    so the OCC window is silently truncated below the floor;
 *  - bug hunt #2 (narrow fix): a registered path with an UNDRAINED
 *    hog_file_removal row is a typed 409 (the cleanup drain would
 *    delete the object out from under the new row); after the drain
 *    settles, the path is registrable again — duplicate paths against
 *    file rows stay legal (pinned contract, no path uniqueness);
 *  - bug hunt #17: the per-table byte rollup is overflow-checked;
 *  - bug hunt #15: snapshot_time is statement time (clock_timestamp),
 *    not transaction-start time, so a commit that queued on the catalog
 *    lock cannot record a time older than an earlier-committed
 *    snapshot's.
 */
@Tag("integration")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class CommitRetentionAndPathGuardIntegrationTest {
    private val db = PgTestSupport.freshDatabase()
    private val jdbi get() = db.jdbi
    private val catalogs = CatalogService(jdbi)
    private val alter = AlterService(jdbi)
    private val commits = CommitService(jdbi)
    private val expiry = ExpiryService(jdbi)
    private val counter = AtomicInteger(0)

    @AfterAll
    fun tearDown() = db.close()

    /** Catalog + ns.t(id long, name string) via the real DDL services. */
    private fun fixture(): String {
        val cat = "cg-${counter.incrementAndGet()}"
        catalogs.createCatalog(cat, "s3://bucket/$cat")
        catalogs.createNamespace(cat, "ns")
        catalogs.createTable(
            cat,
            "ns",
            "t",
            listOf(ColumnDef("id", ColType.LONG, nullable = false), ColumnDef("name", ColType.STRING)),
        )
        return cat
    }

    private fun catalogId(cat: String): Long = catalogs.getCatalog(cat).catalogId

    private fun append(
        cat: String,
        path: String,
        readSnapshot: Long? = null,
    ) = commits.commit(
        cat,
        CommitRequest(
            readSnapshot = readSnapshot,
            appends = listOf(TableAppend("ns", "t", listOf(FileRegistration(path, 10, 100)))),
        ),
    )

    /** Age every snapshot far past [retention] and enable expiry. */
    private fun ageAndEnableRetention(
        cat: String,
        retentionSeconds: Long = 3600,
    ) = jdbi.useHandleUnchecked { h ->
        val id = catalogId(cat)
        h.execute(
            "UPDATE hog_snapshot SET snapshot_time = now() - interval '2 days' WHERE catalog_id = ?",
            id,
        )
        h.execute(
            "UPDATE hog_catalog SET snapshot_retention_seconds = ? WHERE catalog_id = ?",
            retentionSeconds,
            id,
        )
    }

    private fun dataFileCount(cat: String): Long =
        jdbi.withHandleUnchecked { h ->
            h.createQuery("SELECT count(*) FROM hog_data_file WHERE catalog_id = ?")
                .bind(0, catalogId(cat)).mapTo(Long::class.java).one()
        }

    // ---- #1: readSnapshot below the expiry floor -------------------------

    @Test
    fun `readSnapshot below the floor is 410 with zero writes, at the floor commits`() {
        val cat = fixture()
        append(cat, "s3://bucket/$cat/f1.parquet")
        // The change row the conflict check would need: table_altered.
        alter.alterTable(
            cat,
            "ns",
            "t",
            listOf(com.posthog.hoglake.model.AlterOp.AddColumn(ColumnDef("extra", ColType.STRING))),
        )
        val alteredSnap = catalogs.getCatalog(cat).headSnapshotId
        append(cat, "s3://bucket/$cat/f2.parquet")

        // Expire everything below head: the table_altered change row is gone.
        ageAndEnableRetention(cat)
        val sweep = expiry.runOnce(cat, batchSize = 1000)
        val floor = sweep.newEarliestSnapshotId
        val head = catalogs.getCatalog(cat).headSnapshotId
        assertThat(floor).isEqualTo(head)
        assertThat(floor).isGreaterThan(alteredSnap)
        val alteredChangeRows =
            jdbi.withHandleUnchecked { h ->
                h.createQuery(
                    "SELECT count(*) FROM hog_snapshot_change WHERE catalog_id = ? AND snapshot_id = ?",
                ).bind(0, catalogId(cat)).bind(1, alteredSnap).mapTo(Long::class.java).one()
            }
        assertThat(alteredChangeRows).isZero() // the OCC evidence really expired

        val headBefore = catalogs.getCatalog(cat).headSnapshotId
        val filesBefore = dataFileCount(cat)
        assertThatThrownBy { append(cat, "s3://bucket/$cat/f3.parquet", readSnapshot = floor - 1) }
            .isInstanceOf(HoglakeException.Expired::class.java)
            .hasMessageContaining("below the expiry floor")
        // Zero writes: no snapshot minted, no file row landed.
        assertThat(catalogs.getCatalog(cat).headSnapshotId).isEqualTo(headBefore)
        assertThat(dataFileCount(cat)).isEqualTo(filesBefore)

        // readSnapshot == earliest still has its whole conflict window retained.
        val ok = append(cat, "s3://bucket/$cat/f3.parquet", readSnapshot = floor)
        assertThat(ok.snapshotId).isEqualTo(headBefore + 1)
    }

    // ---- #2 narrow fix: undrained removal-queue paths --------------------

    @Test
    fun `a data path with an undrained removal row is a 409 until the drain settles`() {
        val cat = fixture()
        val id = catalogId(cat)
        val path = "s3://bucket/$cat/reused.parquet"
        val removalId =
            jdbi.withHandleUnchecked { h ->
                h.createQuery(
                    """
                    INSERT INTO hog_file_removal (catalog_id, path, file_kind, reason)
                    VALUES (?, ?, 'data', 'snapshot_expiry') RETURNING removal_id
                    """,
                ).bind(0, id).bind(1, path).mapTo(Long::class.java).one()
            }

        assertThatThrownBy { append(cat, path) }
            .isInstanceOf(HoglakeException.CommitConflict::class.java)
            .hasMessageContaining("scheduled for deletion")
            .hasMessageContaining(path)

        // Drain settles the entry (soft-delete): the path is registrable again.
        jdbi.useHandleUnchecked { h ->
            h.execute(
                "UPDATE hog_file_removal SET drained_at = now(), drained_outcome = 'deleted' WHERE removal_id = ?",
                removalId,
            )
        }
        append(cat, path)
        assertThat(dataFileCount(cat)).isEqualTo(1)
    }

    @Test
    fun `a DV path with an undrained removal row is a 409 too`() {
        val cat = fixture()
        val id = catalogId(cat)
        val committed = append(cat, "s3://bucket/$cat/base.parquet")
        val dataFileId =
            jdbi.withHandleUnchecked { h ->
                h.createQuery("SELECT data_file_id FROM hog_data_file WHERE catalog_id = ?")
                    .bind(0, id).mapTo(Long::class.java).one()
            }
        val dvPath = "s3://bucket/$cat/reused.dv"
        jdbi.useHandleUnchecked { h ->
            h.execute(
                "INSERT INTO hog_file_removal (catalog_id, path, file_kind, reason) " +
                    "VALUES (?, ?, 'delete', 'snapshot_expiry')",
                id,
                dvPath,
            )
        }
        assertThatThrownBy {
            commits.commit(
                cat,
                CommitRequest(
                    readSnapshot = committed.snapshotId,
                    deletes =
                        listOf(
                            TableDeletes("ns", "t", listOf(DeleteFileRegistration(dataFileId, dvPath, 1, 16))),
                        ),
                ),
            )
        }
            .isInstanceOf(HoglakeException.CommitConflict::class.java)
            .hasMessageContaining("scheduled for deletion")
    }

    @Test
    fun `duplicate paths against existing file rows stay legal - the pinned contract`() {
        val cat = fixture()
        val path = "s3://bucket/$cat/dup.parquet"
        append(cat, path)
        append(cat, path) // same path, no removal-queue entry: still legal
        assertThat(dataFileCount(cat)).isEqualTo(2)
    }

    // ---- #17: byte rollup overflow ----------------------------------------

    @Test
    fun `file_size_bytes sum overflow is a typed validation, not silent wrap`() {
        val cat = fixture()
        assertThatThrownBy {
            commits.commit(
                cat,
                CommitRequest(
                    appends =
                        listOf(
                            TableAppend(
                                "ns",
                                "t",
                                listOf(
                                    FileRegistration("s3://bucket/$cat/big1.parquet", 1, Long.MAX_VALUE - 5),
                                    FileRegistration("s3://bucket/$cat/big2.parquet", 1, 10),
                                ),
                            ),
                        ),
                ),
            )
        }
            .isInstanceOf(HoglakeException.Validation::class.java)
            .hasMessageContaining("file_size_bytes sum overflows")
        assertThat(dataFileCount(cat)).isZero()
    }

    // ---- #15: snapshot_time is statement time, under the lock -------------

    @Test
    fun `snapshot_time is stamped at statement time, not transaction start`() {
        val cat = fixture()
        val id = catalogId(cat)
        jdbi.useHandleUnchecked { h ->
            h.begin()
            try {
                val txnStart =
                    h.createQuery("SELECT now()")
                        .mapTo(java.time.OffsetDateTime::class.java).one()
                Thread.sleep(80) // the "queued on the advisory lock" window
                SnapshotRepo.insert(h, id, 99, 0)
                val recorded =
                    h.createQuery(
                        "SELECT snapshot_time FROM hog_snapshot WHERE catalog_id = ? AND snapshot_id = 99",
                    ).bind(0, id).mapTo(java.time.OffsetDateTime::class.java).one()
                // now() (txn start) would make these equal; clock_timestamp()
                // must be strictly later than the transaction's start.
                assertThat(recorded).isAfter(txnStart)
            } finally {
                h.rollback()
            }
        }
    }
}
