package com.posthog.hoglake.model

import java.util.UUID

/**
 * One partition key/value pair of a leaf partition, in key_index order.
 * [field] is the partition field's display name: the source column's
 * name for identity transforms, "<column>_<transform>" otherwise
 * (e.g. "ts_month" for month(ts) — the Iceberg naming convention).
 */
data class PartitionValue(
    val field: String,
    /** Transformed partition value; null = null partition value. */
    val value: String?,
)

/**
 * Compaction-debt aggregates for one leaf partition: the live
 * (visible-at-head) data files of one table sharing one
 * (spec_id, partition value tuple). An unpartitioned table (or its
 * pre-spec file vintage) is one row with empty [partitionValues] and a
 * null [specId]. Files written under an older spec than the table's
 * current one group under THEIR spec_id — vintages never mix.
 */
data class PartitionDebt(
    val namespace: String,
    val table: String,
    val tableUuid: UUID,
    val partitionValues: List<PartitionValue>,
    /** Spec the files were written under; null = unpartitioned vintage. */
    val specId: Long?,
    val fileCount: Long,
    /** Files under the compaction target size (the sweep's input threshold). */
    val smallFileCount: Long,
    val totalBytes: Long,
    val smallFileBytes: Long,
    /** totalBytes / fileCount, floored. */
    val avgFileBytes: Long,
    /** Live deletion vectors over the partition's files. */
    val dvCount: Long,
    /** = smallFileCount: the files a compaction sweep would try to merge. */
    val debtScore: Long,
)

/**
 * GET /v1/catalogs/{catalog}/stats/partitions: leaf partitions ranked
 * by compaction debt (debt_score desc, small_file_bytes desc).
 */
data class PartitionStatsReport(
    val partitions: List<PartitionDebt>,
    /** True when more groups existed than the requested limit. */
    val truncated: Boolean,
    /**
     * Groups (across the WHOLE result set, not just the returned page)
     * whose spec vintage is not the table's current live spec —
     * partitions a spec change left behind.
     */
    val staleSpecGroups: Long,
)
