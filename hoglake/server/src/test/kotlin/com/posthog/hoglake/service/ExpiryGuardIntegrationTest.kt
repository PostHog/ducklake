package com.posthog.hoglake.service

import com.posthog.hoglake.model.ColType
import com.posthog.hoglake.model.ColumnDef
import com.posthog.hoglake.model.HoglakeException
import com.posthog.hoglake.testing.PgTestSupport
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.jdbi.v3.core.kotlin.withHandleUnchecked
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import java.time.Instant
import java.time.ZoneOffset

/**
 * Expiry-floor guards (HoglakeException.Expired -> HTTP 410) on the
 * changefeed and the point-in-time read paths. earliest_snapshot_id is
 * set via direct SQL — the expiry service that advances it for real is
 * out of scope here.
 *
 * Timeline: S0 catalog, S1 namespace, S2 table t, S3-S5 padding DDL,
 * file f1@S3; then earliest_snapshot_id := 4 and deterministic
 * snapshot times base+S minutes.
 */
@Tag("integration")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class ExpiryGuardIntegrationTest {
    private val db = PgTestSupport.freshDatabase()
    private val svc = CatalogService(db.jdbi)
    private val scan = ScanService(db.jdbi)
    private val idCol = ColumnDef("id", ColType.LONG, nullable = false)

    private val cat = "floor-cat"
    private var catalogId = 0L
    private val base = Instant.parse("2026-01-01T00:00:00Z")

    private fun timeOf(snapshot: Long): Instant = base.plusSeconds(snapshot * 60)

    @BeforeAll
    fun setUp() {
        catalogId = svc.createCatalog(cat, "s3://bucket/floor").catalogId // S0
        svc.createNamespace(cat, "ns") // S1
        val tableId = svc.createTable(cat, "ns", "t", listOf(idCol)).tableId // S2
        svc.createTable(cat, "ns", "pad3", listOf(idCol))
        svc.createTable(cat, "ns", "pad4", listOf(idCol))
        svc.createTable(cat, "ns", "pad5", listOf(idCol))
        check(svc.getCatalog(cat).headSnapshotId == 5L)

        db.jdbi.withHandleUnchecked { h ->
            h.createUpdate(
                """
                INSERT INTO hog_data_file
                    (catalog_id, data_file_id, table_id, begin_snapshot, path,
                     record_count, file_size_bytes, row_id_start)
                VALUES (:cid, 1, :tid, 3, 's3://bucket/floor/f1.parquet', 50, 4096, 0)
                """,
            ).bind("cid", catalogId).bind("tid", tableId).execute()
            // Deterministic snapshot times, then raise the floor to 4.
            for (s in 0L..5L) {
                h.createUpdate(
                    """
                    UPDATE hog_snapshot SET snapshot_time = :t
                    WHERE catalog_id = :cid AND snapshot_id = :sid
                    """,
                )
                    .bind("t", timeOf(s).atOffset(ZoneOffset.UTC))
                    .bind("cid", catalogId)
                    .bind("sid", s)
                    .execute()
            }
            h.createUpdate(
                """
                UPDATE hog_catalog
                   SET earliest_snapshot_id = 4, earliest_snapshot_time = :t
                 WHERE catalog_id = :cid
                """,
            )
                .bind("t", timeOf(4).atOffset(ZoneOffset.UTC))
                .bind("cid", catalogId)
                .execute()
        }
    }

    @AfterAll
    fun tearDown() = db.close()

    @Test
    fun `changes below the floor is Expired and names the remedy`() {
        // from is exclusive: from = earliest - 1 = 3 only draws from S4+.
        assertThat(svc.changes(cat, "ns", "t", fromSnapshot = 3).files).isEmpty()

        assertThatThrownBy { svc.changes(cat, "ns", "t", fromSnapshot = 2) }
            .isInstanceOf(HoglakeException.Expired::class.java)
            .hasMessageContaining("from_snapshot 2")
            .hasMessageContaining("earliest retained snapshot is 4")
            .hasMessageContaining("reached at ${timeOf(4)}")
            .hasMessageContaining("full scan")

        assertThatThrownBy { svc.changes(cat, "ns", "t", fromSnapshot = 0) }
            .isInstanceOf(HoglakeException.Expired::class.java)

        // Range validation still wins for nonsense input.
        assertThatThrownBy { svc.changes(cat, "ns", "t", fromSnapshot = -1) }
            .isInstanceOf(HoglakeException.Validation::class.java)
    }

    @Test
    fun `point-in-time reads below the floor are Expired but head reads are not`() {
        // Explicit snapshot below the floor: all three read paths.
        assertThatThrownBy { svc.getTable(cat, "ns", "t", snapshot = 3) }
            .isInstanceOf(HoglakeException.Expired::class.java)
            .hasMessageContaining("snapshot 3")
            .hasMessageContaining("earliest retained snapshot is 4")
            .hasMessageContaining("reached at ${timeOf(4)}")
        assertThatThrownBy { svc.listFiles(cat, "ns", "t", snapshot = 3) }
            .isInstanceOf(HoglakeException.Expired::class.java)
        assertThatThrownBy { scan.planScan(cat, "ns", "t", snapshot = 3) }
            .isInstanceOf(HoglakeException.Expired::class.java)
            .hasMessageContaining("reached at ${timeOf(4)}")

        // At the floor and above: fine.
        assertThat(svc.getTable(cat, "ns", "t", snapshot = 4).name).isEqualTo("t")
        assertThat(svc.listFiles(cat, "ns", "t", snapshot = 4)).hasSize(1)
        assertThat(scan.planScan(cat, "ns", "t", snapshot = 4)).hasSize(1)

        // Head reads (no explicit snapshot) never hit the floor.
        assertThat(svc.getTable(cat, "ns", "t").name).isEqualTo("t")
        assertThat(svc.listFiles(cat, "ns", "t")).hasSize(1)
        assertThat(scan.planScan(cat, "ns", "t")).hasSize(1)

        // Out-of-range ids are still Validation, not Expired.
        assertThatThrownBy { svc.getTable(cat, "ns", "t", snapshot = -1) }
            .isInstanceOf(HoglakeException.Validation::class.java)
        assertThatThrownBy { svc.getTable(cat, "ns", "t", snapshot = 99) }
            .isInstanceOf(HoglakeException.Validation::class.java)
    }

    @Test
    fun `catalog info carries the floor time - and null before expiry ever ran`() {
        assertThat(svc.getCatalog(cat).earliestSnapshotTime).isEqualTo(timeOf(4))
        val fresh = svc.createCatalog("floor-fresh", "s3://bucket/fresh")
        assertThat(fresh.earliestSnapshotTime).isNull()
        assertThat(svc.getCatalog("floor-fresh").earliestSnapshotTime).isNull()
    }

    @Test
    fun `at_timestamp below the earliest retained snapshot time is Expired`() {
        // t(S3) predates the earliest retained snapshot (S4).
        assertThatThrownBy { svc.getTable(cat, "ns", "t", atTimestamp = timeOf(3)) }
            .isInstanceOf(HoglakeException.Expired::class.java)
        assertThatThrownBy { svc.listFiles(cat, "ns", "t", atTimestamp = timeOf(3)) }
            .isInstanceOf(HoglakeException.Expired::class.java)
        assertThatThrownBy { scan.planScan(cat, "ns", "t", atTimestamp = timeOf(3)) }
            .isInstanceOf(HoglakeException.Expired::class.java)
        assertThatThrownBy { svc.resolveTimestamp(cat, timeOf(4).minusSeconds(1)) }
            .isInstanceOf(HoglakeException.Expired::class.java)

        // At exactly the earliest retained time it resolves to 4.
        assertThat(svc.resolveTimestamp(cat, timeOf(4))).isEqualTo(4L)
        assertThat(svc.listFiles(cat, "ns", "t", atTimestamp = timeOf(4))).hasSize(1)
    }
}
