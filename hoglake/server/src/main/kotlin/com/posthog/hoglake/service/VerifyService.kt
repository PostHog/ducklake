package com.posthog.hoglake.service

import com.posthog.hoglake.model.VerifyCheck
import com.posthog.hoglake.model.VerifyReport
import com.posthog.hoglake.observability.Audit
import com.posthog.hoglake.persistence.CatalogRepo
import org.jdbi.v3.core.Handle
import org.jdbi.v3.core.Jdbi
import org.jdbi.v3.core.kotlin.inTransactionUnchecked

/**
 * POST /v1/catalogs/{catalog}/maintenance/verify (gaps.md B3, absorbing
 * B4): the QE suite's global-invariant SQL promoted to an on-demand,
 * metadata-only endpoint. Read-only, one REPEATABLE READ transaction (a
 * consistent MVCC snapshot across the cross-table checks), no catalog
 * lock — verify never blocks writers.
 *
 * Checks:
 *  1. row_id_tiling — positional (explicit_row_ids = false) file rows
 *     of a table, live AND historical, are non-overlapping intervals
 *     with non-negative starts. Compaction outputs are excluded from
 *     the interval sweep by design: they legitimately CARRY their
 *     inputs' row ids (same logical rows at a later snapshot), and
 *     their row_id_start is only min(input ids). Gaps are legal
 *     (expiry deletes unreachable rows); overlap never is.
 *  2. delete_vectors — at most one live DV per data file (backs the
 *     unique partial index), supersession chains monotone
 *     (delete_count only grows along begin_snapshot), and no DV
 *     counting more deletes than its data file has rows.
 *  3. orphans — live child rows of dead parents: live
 *     hog_data_file / hog_column / hog_table_version rows on a dropped
 *     table (drop end-snapshots all three; a live row there is a lost
 *     write path).
 *  4. removal_queue — undrained hog_file_removal rows whose path is
 *     still referenced by ANY file row (a queue entry is a suggestion,
 *     never an authorization; a referenced suggestion is invariant
 *     violation #4's alert condition at rest).
 *  5. snapshot_density — the true count(*): retained snapshots are
 *     exactly the dense range [earliest_snapshot_id, head] (B4's
 *     always-on approximation, asserted for real here).
 *  6. next_row_id — hog_table_stats.next_row_id is at or past every
 *     positional file's range end (the allocator can never have handed
 *     out a range it doesn't remember).
 *
 * The report carries true violation counts and SAMPLE details capped at
 * [MAX_SAMPLES] per check, so a badly broken catalog cannot produce an
 * unbounded response.
 */
class VerifyService(private val jdbi: Jdbi) {
    fun runOnce(catalog: String): VerifyReport =
        Audit.audited(
            "verify",
            catalog,
            null,
            successOutcome = { r -> if (r.status == "pass") "ok" else "invariant_violation" },
            detail = { r ->
                r.checks.joinToString(" ") { "${it.check}=${it.violations}" }
            },
        ) {
            jdbi.inTransactionUnchecked { h ->
                // First statement of the transaction: a consistent
                // read-only MVCC snapshot for every check below.
                h.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
                val cat = CatalogRepo.require(h, catalog)
                val checks =
                    listOf(
                        rowIdTiling(h, cat.catalogId),
                        deleteVectors(h, cat.catalogId),
                        orphans(h, cat.catalogId),
                        removalQueue(h, cat.catalogId),
                        snapshotDensity(h, cat.catalogId, cat.earliestSnapshotId, cat.headSnapshotId),
                        nextRowId(h, cat.catalogId),
                    )
                VerifyReport(
                    catalog = catalog,
                    status = if (checks.all { it.status == "pass" }) "pass" else "fail",
                    checks = checks,
                )
            }
        }

    private fun check(
        name: String,
        samples: List<String>,
        violations: Long = samples.size.toLong(),
    ) = VerifyCheck(
        check = name,
        status = if (violations == 0L) "pass" else "fail",
        violations = violations,
        samples = samples.take(MAX_SAMPLES),
    )

    /** Run [sql] (bound :c = catalogId) and render each violation row via [render]. */
    private fun violations(
        h: Handle,
        catalogId: Long,
        sql: String,
        render: (java.sql.ResultSet) -> String,
    ): List<String> =
        h.createQuery(sql)
            .bind("c", catalogId)
            .map { rs, _ -> render(rs) }
            .list()

    // ---- 1: row-id tiling --------------------------------------------------

    private fun rowIdTiling(
        h: Handle,
        catalogId: Long,
    ): VerifyCheck {
        // A positional file overlapping ANY earlier positional interval of
        // its table (window max of prior range ends; zero-record files
        // occupy no ids and are skipped).
        val overlaps =
            violations(
                h,
                catalogId,
                """
                SELECT table_id, data_file_id, row_id_start, record_count, prev_end FROM (
                    SELECT table_id, data_file_id, row_id_start, record_count,
                           max(row_id_start + record_count) OVER (
                               PARTITION BY table_id
                               ORDER BY row_id_start, data_file_id
                               ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS prev_end
                    FROM hog_data_file
                    WHERE catalog_id = :c AND NOT explicit_row_ids AND record_count > 0) x
                WHERE prev_end IS NOT NULL AND row_id_start < prev_end
                ORDER BY table_id, row_id_start
                """,
            ) { rs ->
                "table_id=${rs.getLong("table_id")} data_file_id=${rs.getLong("data_file_id")} " +
                    "range=[${rs.getLong("row_id_start")},+${rs.getLong("record_count")}) " +
                    "overlaps prior end ${rs.getLong("prev_end")}"
            }
        val negative =
            violations(
                h,
                catalogId,
                """
                SELECT table_id, data_file_id, row_id_start FROM hog_data_file
                WHERE catalog_id = :c AND row_id_start < 0
                ORDER BY table_id, data_file_id
                """,
            ) { rs ->
                "table_id=${rs.getLong("table_id")} data_file_id=${rs.getLong("data_file_id")} " +
                    "negative row_id_start=${rs.getLong("row_id_start")}"
            }
        return check("row_id_tiling", overlaps + negative)
    }

    // ---- 2: deletion vectors ----------------------------------------------

    private fun deleteVectors(
        h: Handle,
        catalogId: Long,
    ): VerifyCheck {
        val dupLive =
            violations(
                h,
                catalogId,
                """
                SELECT data_file_id, count(*) AS live FROM hog_delete_file
                WHERE catalog_id = :c AND end_snapshot IS NULL
                GROUP BY data_file_id HAVING count(*) > 1
                ORDER BY data_file_id
                """,
            ) { rs ->
                "data_file_id=${rs.getLong("data_file_id")} has ${rs.getLong("live")} live DVs"
            }
        val shrinking =
            violations(
                h,
                catalogId,
                """
                SELECT a.data_file_id, a.begin_snapshot AS older, a.delete_count AS older_count,
                       b.begin_snapshot AS newer, b.delete_count AS newer_count
                FROM hog_delete_file a
                JOIN hog_delete_file b
                  ON b.catalog_id = a.catalog_id AND b.data_file_id = a.data_file_id
                 AND b.begin_snapshot > a.begin_snapshot AND b.delete_count < a.delete_count
                WHERE a.catalog_id = :c
                ORDER BY a.data_file_id, a.begin_snapshot
                """,
            ) { rs ->
                "data_file_id=${rs.getLong("data_file_id")} DV shrank: " +
                    "S${rs.getLong("older")}=${rs.getLong("older_count")} -> " +
                    "S${rs.getLong("newer")}=${rs.getLong("newer_count")}"
            }
        val oversize =
            violations(
                h,
                catalogId,
                """
                SELECT dv.delete_file_id, dv.data_file_id, dv.delete_count, df.record_count
                FROM hog_delete_file dv
                JOIN hog_data_file df
                  ON df.catalog_id = dv.catalog_id AND df.data_file_id = dv.data_file_id
                WHERE dv.catalog_id = :c AND dv.delete_count > df.record_count
                ORDER BY dv.delete_file_id
                """,
            ) { rs ->
                "delete_file_id=${rs.getLong("delete_file_id")} delete_count=" +
                    "${rs.getLong("delete_count")} exceeds record_count=" +
                    "${rs.getLong("record_count")} of data_file_id=${rs.getLong("data_file_id")}"
            }
        return check("delete_vectors", dupLive + shrinking + oversize)
    }

    // ---- 3: orphans --------------------------------------------------------

    private fun orphans(
        h: Handle,
        catalogId: Long,
    ): VerifyCheck {
        fun liveChildrenOfDroppedTable(
            table: String,
            idColumn: String,
        ): List<String> =
            violations(
                h,
                catalogId,
                """
                SELECT x.table_id, x.$idColumn AS child_id
                FROM $table x
                JOIN hog_table t
                  ON t.catalog_id = x.catalog_id AND t.table_id = x.table_id
                WHERE x.catalog_id = :c AND x.end_snapshot IS NULL
                  AND t.dropped_snapshot IS NOT NULL
                ORDER BY x.table_id, x.$idColumn
                """,
            ) { rs ->
                "$table $idColumn=${rs.getLong("child_id")} still live on dropped " +
                    "table_id=${rs.getLong("table_id")}"
            }
        // Table names are compile-time literals (invariant 9 intact).
        val samples =
            liveChildrenOfDroppedTable("hog_data_file", "data_file_id") +
                liveChildrenOfDroppedTable("hog_column", "field_id") +
                liveChildrenOfDroppedTable("hog_table_version", "begin_snapshot")
        return check("orphans", samples)
    }

    // ---- 4: removal queue --------------------------------------------------

    private fun removalQueue(
        h: Handle,
        catalogId: Long,
    ): VerifyCheck {
        val samples =
            violations(
                h,
                catalogId,
                """
                SELECT q.removal_id, q.path FROM hog_file_removal q
                WHERE q.catalog_id = :c AND q.drained_at IS NULL AND EXISTS (
                    SELECT 1 FROM hog_data_file f
                    WHERE f.catalog_id = q.catalog_id AND f.path = q.path
                    UNION
                    SELECT 1 FROM hog_delete_file d
                    WHERE d.catalog_id = q.catalog_id AND d.path = q.path)
                ORDER BY q.removal_id
                """,
            ) { rs ->
                "removal_id=${rs.getLong("removal_id")} path='${rs.getString("path")}' " +
                    "queued but still live-referenced"
            }
        return check("removal_queue", samples)
    }

    // ---- 5: snapshot density -----------------------------------------------

    private fun snapshotDensity(
        h: Handle,
        catalogId: Long,
        earliest: Long,
        head: Long,
    ): VerifyCheck {
        val (count, min, max) =
            h.createQuery(
                "SELECT count(*), min(snapshot_id), max(snapshot_id) FROM hog_snapshot WHERE catalog_id = :c",
            )
                .bind("c", catalogId)
                .map { rs, _ -> Triple(rs.getLong(1), rs.getLong(2), rs.getLong(3)) }
                .one()
        val expected = head - earliest + 1
        val samples = mutableListOf<String>()
        if (count != expected) {
            samples += "count(*)=$count but head($head) - earliest($earliest) + 1 = $expected"
        }
        if (count > 0 && min != earliest) samples += "min(snapshot_id)=$min != earliest_snapshot_id=$earliest"
        if (count > 0 && max != head) samples += "max(snapshot_id)=$max != head=$head"
        return check("snapshot_density", samples)
    }

    // ---- 6: next_row_id consistency ----------------------------------------

    private fun nextRowId(
        h: Handle,
        catalogId: Long,
    ): VerifyCheck {
        val samples =
            violations(
                h,
                catalogId,
                """
                SELECT s.table_id, s.next_row_id, m.max_end
                FROM hog_table_stats s
                JOIN (SELECT table_id, max(row_id_start + record_count) AS max_end
                      FROM hog_data_file
                      WHERE catalog_id = :c AND NOT explicit_row_ids
                      GROUP BY table_id) m ON m.table_id = s.table_id
                WHERE s.catalog_id = :c AND s.next_row_id < m.max_end
                ORDER BY s.table_id
                """,
            ) { rs ->
                "table_id=${rs.getLong("table_id")} next_row_id=${rs.getLong("next_row_id")} " +
                    "below max allocated range end ${rs.getLong("max_end")}"
            }
        return check("next_row_id", samples)
    }

    companion object {
        /** Per-check cap on sample details in the report. */
        const val MAX_SAMPLES = 20
    }
}
