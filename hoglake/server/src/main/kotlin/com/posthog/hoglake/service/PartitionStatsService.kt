package com.posthog.hoglake.service

import com.posthog.hoglake.model.HoglakeException
import com.posthog.hoglake.model.PartitionDebt
import com.posthog.hoglake.model.PartitionStatsReport
import com.posthog.hoglake.model.PartitionValue
import com.posthog.hoglake.persistence.CatalogRepo
import com.posthog.hoglake.persistence.NamespaceRepo
import com.posthog.hoglake.persistence.TableRepo
import org.jdbi.v3.core.Handle
import org.jdbi.v3.core.Jdbi
import org.jdbi.v3.core.kotlin.inTransactionUnchecked
import java.util.UUID

/**
 * GET /v1/catalogs/{catalog}/stats/partitions — leaf partitions ranked
 * by compaction debt, for the operator question "how un-compacted is
 * each partition" (the coarse months-by-team ops model makes leaf
 * partitions few and large, so a full ranking is cheap and primary).
 *
 * Semantics:
 *  - LIVE files only: visible at head under the invariant-6 predicate
 *    (begin_snapshot <= head AND (end_snapshot IS NULL OR head <
 *    end_snapshot)), on live tables in live namespaces — dropped
 *    tables never appear.
 *  - Grouped by (table, spec_id, partition value tuple). Files written
 *    under an older spec than the table's current one group under
 *    THEIR spec_id; an unpartitioned table (or the pre-spec vintage of
 *    a later-partitioned one) is one group with a null spec and empty
 *    partition values.
 *  - "Small" = file_size_bytes < [smallFileThresholdBytes], the SAME
 *    strict-less-than CompactionService applies to candidate inputs
 *    (App wires both from Config.compactionTargetBytes) — so
 *    debt_score (= small_file_count) is exactly the files a sweep
 *    would try to merge.
 *  - dv_count counts live DVs over the group's files (at most one per
 *    file by the unique partial index).
 *  - Ordered by debt_score desc, ties by small_file_bytes desc, then a
 *    stable name/spec/values tiebreak.
 *
 * One aggregation statement per request (no per-table loop), plus one
 * bounded lookup for partition-field display names. Read-only, one
 * REPEATABLE READ MVCC snapshot, no locks — never blocks writers.
 */
class PartitionStatsService(
    private val jdbi: Jdbi,
    /** Compaction target size = the small-file threshold (strict <). */
    private val smallFileThresholdBytes: Long,
) {
    fun partitionStats(
        catalog: String,
        namespace: String?,
        table: String?,
        limit: Int,
    ): PartitionStatsReport {
        if (table != null && namespace == null) {
            throw HoglakeException.Validation("'table' filter requires 'namespace'")
        }
        if (limit <= 0) {
            throw HoglakeException.Validation("limit must be positive (got $limit)")
        }
        val cappedLimit = limit.coerceAtMost(MAX_LIMIT)
        return jdbi.inTransactionUnchecked { h ->
            // First statement of the transaction: one consistent
            // read-only MVCC snapshot for both queries below.
            h.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
            val cat = CatalogRepo.require(h, catalog)
            if (namespace != null) {
                val ns =
                    NamespaceRepo.findLiveByName(h, cat.catalogId, namespace)
                        ?: throw HoglakeException.NotFound(
                            "namespace '$namespace' in catalog '$catalog'",
                        )
                if (table != null && TableRepo.findLive(h, cat.catalogId, ns.namespaceId, table) == null) {
                    throw HoglakeException.NotFound("table '$namespace.$table' in catalog '$catalog'")
                }
            }
            val rows = groupRows(h, cat.catalogId, cat.headSnapshotId, namespace, table, cappedLimit)
            val fieldNames = partitionFieldNames(h, cat.catalogId, rows.map { it.tableId }.distinct())
            PartitionStatsReport(
                partitions = rows.map { it.toDebt(fieldNames) },
                truncated = (rows.firstOrNull()?.totalGroups ?: 0L) > cappedLimit,
                staleSpecGroups = rows.firstOrNull()?.staleSpecGroups ?: 0L,
            )
        }
    }

    // ---- the aggregation ---------------------------------------------------

    private data class GroupRow(
        val namespace: String,
        val table: String,
        val tableUuid: UUID,
        val tableId: Long,
        val specId: Long?,
        val values: List<String?>?,
        val fileCount: Long,
        val smallFileCount: Long,
        val totalBytes: Long,
        val smallFileBytes: Long,
        val dvCount: Long,
        val totalGroups: Long,
        val staleSpecGroups: Long,
    ) {
        fun toDebt(fieldNames: Map<Pair<Long, Long>, List<String>>): PartitionDebt {
            val names = specId?.let { fieldNames[tableId to it] } ?: emptyList()
            return PartitionDebt(
                namespace = namespace,
                table = table,
                tableUuid = tableUuid,
                partitionValues =
                    (values ?: emptyList()).mapIndexed { i, value ->
                        PartitionValue(names.getOrElse(i) { "key_$i" }, value)
                    },
                specId = specId,
                fileCount = fileCount,
                smallFileCount = smallFileCount,
                totalBytes = totalBytes,
                smallFileBytes = smallFileBytes,
                avgFileBytes = if (fileCount > 0) totalBytes / fileCount else 0,
                dvCount = dvCount,
                debtScore = smallFileCount,
            )
        }
    }

    /**
     * The one aggregation: live files -> (table, spec, values) groups
     * with debt aggregates, window totals for truncation and the
     * stale-spec count, ordered and limited in SQL. The optional
     * name filters are static SQL fragments chosen here — every value
     * rides a bind (invariant 9).
     */
    private fun groupRows(
        h: Handle,
        catalogId: Long,
        head: Long,
        namespace: String?,
        table: String?,
        limit: Int,
    ): List<GroupRow> {
        val namespaceFilter = if (namespace != null) "AND ns.name = :namespace" else ""
        val tableFilter = if (table != null) "AND tv.name = :tableName" else ""
        val query =
            h.createQuery(
                """
                WITH files AS (
                    SELECT f.table_id, f.spec_id, f.file_size_bytes,
                           ns.name AS namespace, tv.name AS table_name, t.table_uuid,
                           (SELECT array_agg(pv.value ORDER BY pv.key_index)
                            FROM hog_file_partition_value pv
                            WHERE pv.catalog_id = f.catalog_id
                              AND pv.data_file_id = f.data_file_id) AS partition_values,
                           EXISTS (
                               SELECT 1 FROM hog_delete_file dv
                               WHERE dv.catalog_id = f.catalog_id
                                 AND dv.data_file_id = f.data_file_id
                                 AND dv.begin_snapshot <= :head
                                 AND (dv.end_snapshot IS NULL OR :head < dv.end_snapshot)
                           ) AS has_live_dv
                    FROM hog_data_file f
                    JOIN hog_table t
                      ON t.catalog_id = f.catalog_id AND t.table_id = f.table_id
                    JOIN hog_table_version tv
                      ON tv.catalog_id = f.catalog_id AND tv.table_id = f.table_id
                     AND tv.begin_snapshot <= :head
                     AND (tv.end_snapshot IS NULL OR :head < tv.end_snapshot)
                    JOIN hog_namespace ns
                      ON ns.catalog_id = f.catalog_id AND ns.namespace_id = tv.namespace_id
                    WHERE f.catalog_id = :catalogId
                      AND t.dropped_snapshot IS NULL
                      AND NOT ns.dropped
                      AND f.begin_snapshot <= :head
                      AND (f.end_snapshot IS NULL OR :head < f.end_snapshot)
                      $namespaceFilter
                      $tableFilter
                ),
                current_spec AS (
                    SELECT ps.table_id, max(ps.spec_id) AS spec_id
                    FROM hog_partition_spec ps
                    WHERE ps.catalog_id = :catalogId
                      AND ps.begin_snapshot <= :head
                      AND (ps.end_snapshot IS NULL OR :head < ps.end_snapshot)
                    GROUP BY ps.table_id
                ),
                groups AS (
                    SELECT namespace, table_name, table_uuid, table_id, spec_id, partition_values,
                           count(*) AS file_count,
                           count(*) FILTER (WHERE file_size_bytes < :smallBytes) AS small_file_count,
                           sum(file_size_bytes) AS total_bytes,
                           COALESCE(sum(file_size_bytes)
                                        FILTER (WHERE file_size_bytes < :smallBytes), 0) AS small_file_bytes,
                           count(*) FILTER (WHERE has_live_dv) AS dv_count
                    FROM files
                    GROUP BY namespace, table_name, table_uuid, table_id, spec_id, partition_values
                )
                SELECT g.*,
                       count(*) OVER () AS total_groups,
                       count(*) FILTER (WHERE g.spec_id IS DISTINCT FROM cs.spec_id)
                           OVER () AS stale_spec_groups
                FROM groups g
                LEFT JOIN current_spec cs ON cs.table_id = g.table_id
                ORDER BY small_file_count DESC, small_file_bytes DESC,
                         namespace, table_name, spec_id NULLS FIRST, partition_values
                LIMIT :limit
                """,
            )
                .bind("catalogId", catalogId)
                .bind("head", head)
                .bind("smallBytes", smallFileThresholdBytes)
                .bind("limit", limit)
        if (namespace != null) query.bind("namespace", namespace)
        if (table != null) query.bind("tableName", table)
        return query
            .map { rs, _ ->
                GroupRow(
                    namespace = rs.getString("namespace"),
                    table = rs.getString("table_name"),
                    tableUuid = rs.getObject("table_uuid") as UUID,
                    tableId = rs.getLong("table_id"),
                    specId = rs.getObject("spec_id")?.let { (it as Number).toLong() },
                    values =
                        (rs.getArray("partition_values")?.array as? Array<*>)
                            ?.map { it as String? },
                    fileCount = rs.getLong("file_count"),
                    smallFileCount = rs.getLong("small_file_count"),
                    totalBytes = rs.getLong("total_bytes"),
                    smallFileBytes = rs.getLong("small_file_bytes"),
                    dvCount = rs.getLong("dv_count"),
                    totalGroups = rs.getLong("total_groups"),
                    staleSpecGroups = rs.getLong("stale_spec_groups"),
                )
            }
            .list()
    }

    // ---- partition-field display names ---------------------------------------

    /**
     * (table_id, spec_id) -> field display names in key_index order.
     * Identity fields take the source column's name; other transforms
     * append it ("ts_month"). The column name resolves to the LIVE
     * version when one exists, else the latest version (a dropped
     * column's partitions still need a label); a column with no
     * versions at all (never legal) falls back to "field_<id>".
     */
    private fun partitionFieldNames(
        h: Handle,
        catalogId: Long,
        tableIds: List<Long>,
    ): Map<Pair<Long, Long>, List<String>> {
        if (tableIds.isEmpty()) return emptyMap()

        data class FieldRow(val tableId: Long, val specId: Long, val keyIndex: Int, val name: String)
        return h.createQuery(
            """
            SELECT pf.table_id, pf.spec_id, pf.key_index, pf.transform, pf.source_field_id,
                   c.name AS column_name
            FROM hog_partition_field pf
            LEFT JOIN LATERAL (
                SELECT name FROM hog_column c
                WHERE c.catalog_id = pf.catalog_id
                  AND c.table_id = pf.table_id
                  AND c.field_id = pf.source_field_id
                ORDER BY (c.end_snapshot IS NULL) DESC, c.begin_snapshot DESC
                LIMIT 1
            ) c ON true
            WHERE pf.catalog_id = :catalogId AND pf.table_id IN (<tableIds>)
            ORDER BY pf.table_id, pf.spec_id, pf.key_index
            """,
        )
            .bind("catalogId", catalogId)
            .bindList("tableIds", tableIds)
            .map { rs, _ ->
                val column = rs.getString("column_name") ?: "field_${rs.getLong("source_field_id")}"
                val transform = rs.getString("transform")
                FieldRow(
                    tableId = rs.getLong("table_id"),
                    specId = rs.getLong("spec_id"),
                    keyIndex = rs.getInt("key_index"),
                    name = if (transform == "identity") column else "${column}_$transform",
                )
            }
            .list()
            .groupBy({ it.tableId to it.specId }, { it })
            .mapValues { (_, rows) -> rows.sortedBy { it.keyIndex }.map { it.name } }
    }

    companion object {
        /** Default page size when no `limit` is supplied. */
        const val DEFAULT_LIMIT = 50

        /** Hard cap on `limit`. */
        const val MAX_LIMIT = 500
    }
}
