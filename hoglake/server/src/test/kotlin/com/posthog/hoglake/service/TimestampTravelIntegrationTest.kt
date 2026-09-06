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
 * Timestamp time travel: resolveTimestamp picks the largest snapshot
 * with snapshot_time <= the timestamp, and getTable/listFiles/planScan
 * accept at_timestamp as an alternative to snapshot. Snapshot times are
 * rewritten to a deterministic base + S minutes ladder via direct SQL.
 *
 * Timeline: S0 catalog, S1 namespace, S2 table t, S3-S4 padding DDL,
 * files f1@S3 and f2@S4.
 */
@Tag("integration")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class TimestampTravelIntegrationTest {
    private val db = PgTestSupport.freshDatabase()
    private val svc = CatalogService(db.jdbi)
    private val scan = ScanService(db.jdbi)
    private val idCol = ColumnDef("id", ColType.LONG, nullable = false)

    private val cat = "ts-cat"
    private var catalogId = 0L
    private val base = Instant.parse("2026-02-01T00:00:00Z")

    private fun timeOf(snapshot: Long): Instant = base.plusSeconds(snapshot * 60)

    @BeforeAll
    fun setUp() {
        catalogId = svc.createCatalog(cat, "s3://bucket/ts").catalogId // S0
        svc.createNamespace(cat, "ns") // S1
        val tableId = svc.createTable(cat, "ns", "t", listOf(idCol)).tableId // S2
        svc.createTable(cat, "ns", "pad3", listOf(idCol)) // S3
        svc.createTable(cat, "ns", "pad4", listOf(idCol)) // S4
        check(svc.getCatalog(cat).headSnapshotId == 4L)

        db.jdbi.withHandleUnchecked { h ->
            for (fid in listOf(1L to 3L, 2L to 4L)) {
                h.createUpdate(
                    """
                    INSERT INTO hog_data_file
                        (catalog_id, data_file_id, table_id, begin_snapshot, path,
                         record_count, file_size_bytes, row_id_start)
                    VALUES (:cid, :fid, :tid, :begin, :path, 50, 4096, :rowIdStart)
                    """,
                )
                    .bind("cid", catalogId)
                    .bind("fid", fid.first)
                    .bind("tid", tableId)
                    .bind("begin", fid.second)
                    .bind("path", "s3://bucket/ts/f${fid.first}.parquet")
                    .bind("rowIdStart", (fid.first - 1) * 100)
                    .execute()
            }
            for (s in 0L..4L) {
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
        }
    }

    @AfterAll
    fun tearDown() = db.close()

    @Test
    fun `resolveTimestamp picks exact, lower-between, and head-after targets`() {
        // Exact snapshot time.
        assertThat(svc.resolveTimestamp(cat, timeOf(3))).isEqualTo(3L)
        // Between two snapshots -> the lower one.
        assertThat(svc.resolveTimestamp(cat, timeOf(3).plusSeconds(30))).isEqualTo(3L)
        assertThat(svc.resolveTimestamp(cat, timeOf(4).minusSeconds(1))).isEqualTo(3L)
        // After head's time -> head.
        assertThat(svc.resolveTimestamp(cat, timeOf(4).plusSeconds(3600))).isEqualTo(4L)
        // Before the earliest retained snapshot (S0) -> Expired.
        assertThatThrownBy { svc.resolveTimestamp(cat, base.minusSeconds(1)) }
            .isInstanceOf(HoglakeException.Expired::class.java)
        // Unknown catalog -> NotFound.
        assertThatThrownBy { svc.resolveTimestamp("nope", timeOf(3)) }
            .isInstanceOf(HoglakeException.NotFound::class.java)
    }

    @Test
    fun `read paths accept at_timestamp and resolve like snapshot ids`() {
        // listFiles at t(S3): only f1; at t(S4)+: both.
        assertThat(svc.listFiles(cat, "ns", "t", atTimestamp = timeOf(3)).map { it.dataFileId })
            .containsExactly(1L)
        assertThat(
            svc.listFiles(cat, "ns", "t", atTimestamp = timeOf(4).plusSeconds(5))
                .map { it.dataFileId },
        ).containsExactly(1L, 2L)

        // getTable resolves (pad4 does not exist yet at t(S3)).
        assertThat(svc.getTable(cat, "ns", "t", atTimestamp = timeOf(2)).name).isEqualTo("t")
        assertThatThrownBy { svc.getTable(cat, "ns", "pad4", atTimestamp = timeOf(3)) }
            .isInstanceOf(HoglakeException.NotFound::class.java)

        // planScan honors the same resolution.
        assertThat(scan.planScan(cat, "ns", "t", atTimestamp = timeOf(3))).hasSize(1)
        assertThat(scan.planScan(cat, "ns", "t", atTimestamp = timeOf(4))).hasSize(2)
    }

    @Test
    fun `snapshot and at_timestamp together are a Validation failure`() {
        assertThatThrownBy { svc.getTable(cat, "ns", "t", snapshot = 3, atTimestamp = timeOf(3)) }
            .isInstanceOf(HoglakeException.Validation::class.java)
            .hasMessageContaining("mutually exclusive")
        assertThatThrownBy { svc.listFiles(cat, "ns", "t", snapshot = 3, atTimestamp = timeOf(3)) }
            .isInstanceOf(HoglakeException.Validation::class.java)
        assertThatThrownBy { scan.planScan(cat, "ns", "t", snapshot = 3, atTimestamp = timeOf(3)) }
            .isInstanceOf(HoglakeException.Validation::class.java)
    }
}
