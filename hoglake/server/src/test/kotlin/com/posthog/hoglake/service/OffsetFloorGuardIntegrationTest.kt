package com.posthog.hoglake.service

import com.posthog.hoglake.commit.CommitService
import com.posthog.hoglake.model.ColType
import com.posthog.hoglake.model.ColumnDef
import com.posthog.hoglake.model.CommitRequest
import com.posthog.hoglake.model.FileRegistration
import com.posthog.hoglake.model.HoglakeException
import com.posthog.hoglake.model.TableAppend
import com.posthog.hoglake.testing.PgTestSupport
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.jdbi.v3.core.kotlin.useHandleUnchecked
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance

/**
 * Pinned regression for bug hunt #3: commitOffset must reject snapshots
 * below the expiry floor with the read paths' 410 contract. An accepted
 * below-floor offset row on a consumer_floor catalog would bound every
 * future sweep's newEarliest at or below the current floor — a
 * PERMANENT zero-work wedge with no offset-delete API to undo it.
 */
@Tag("integration")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class OffsetFloorGuardIntegrationTest {
    private val db = PgTestSupport.freshDatabase()
    private val jdbi get() = db.jdbi
    private val catalogs = CatalogService(jdbi)
    private val commits = CommitService(jdbi)
    private val expiry = ExpiryService(jdbi)

    @AfterAll
    fun tearDown() = db.close()

    private fun ageSnapshots(catalogId: Long) =
        jdbi.useHandleUnchecked { h ->
            h.execute(
                "UPDATE hog_snapshot SET snapshot_time = now() - interval '2 days' WHERE catalog_id = ?",
                catalogId,
            )
        }

    @Test
    fun `below-floor offsets are refused, at-floor accepted, and the sweep keeps advancing`() {
        val cat = "offset-floor"
        catalogs.createCatalog(cat, "s3://bucket/$cat")
        catalogs.createNamespace(cat, "ns")
        catalogs.createTable(cat, "ns", "t", listOf(ColumnDef("id", ColType.LONG, nullable = false)))
        val info = catalogs.getTable(cat, "ns", "t")
        val catalogId = catalogs.getCatalog(cat).catalogId
        jdbi.useHandleUnchecked { h ->
            h.execute("UPDATE hog_catalog SET snapshot_retention_seconds = 3600 WHERE catalog_id = ?", catalogId)
        }

        fun append(name: String) =
            commits.commit(
                cat,
                CommitRequest(
                    appends =
                        listOf(TableAppend("ns", "t", listOf(FileRegistration("s3://bucket/$cat/$name", 5, 50)))),
                ),
            )
        append("f1.parquet")
        append("f2.parquet")

        // Advance the floor to head.
        ageSnapshots(catalogId)
        val floor = expiry.runOnce(cat, batchSize = 1000).newEarliestSnapshotId
        assertThat(floor).isEqualTo(catalogs.getCatalog(cat).headSnapshotId)

        // Below the floor: refused, typed 410-style — the wedge-maker.
        assertThatThrownBy { catalogs.commitOffset(cat, "c1", info.tableUuid, floor - 1) }
            .isInstanceOf(HoglakeException.Expired::class.java)
            .hasMessageContaining("below the expiry floor")
        // At the floor: a legitimate position, accepted.
        assertThat(catalogs.commitOffset(cat, "c1", info.tableUuid, floor).committedSnapshot)
            .isEqualTo(floor)

        // Wedge-impossibility regression: the refused pin left nothing
        // behind, so once the consumer catches up the next sweep still
        // advances past the old floor.
        append("f3.parquet")
        append("f4.parquet")
        val newHead = catalogs.getCatalog(cat).headSnapshotId
        catalogs.commitOffset(cat, "c1", info.tableUuid, newHead)
        ageSnapshots(catalogId)
        val next = expiry.runOnce(cat, batchSize = 1000)
        assertThat(next.newEarliestSnapshotId).isEqualTo(newHead)
        assertThat(next.newEarliestSnapshotId).isGreaterThan(floor)
        assertThat(next.flooredByConsumer).isNull()
    }
}
