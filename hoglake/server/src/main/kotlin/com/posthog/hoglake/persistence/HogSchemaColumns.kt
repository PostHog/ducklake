package com.posthog.hoglake.persistence

/**
 * The mapping layer's declared view of the schema: for every `hog_*`
 * table, the full set of columns the code knows about. The
 * mapper-coverage gate (MapperCoverageGateIntegrationTest) asserts each
 * set EQUALS the live columns of the migrated database, so adding a
 * column to V1__init.sql/schema.sql without touching the mapping layer
 * fails CI with the table and column named — the drift class where
 * OptionsService.LifecycleCatalog silently fell behind
 * CatalogRepo.CatalogInfo can no longer happen quietly.
 *
 * Each entry names the mapper site(s) that must be reviewed when the
 * column set changes; updating this file IS the acknowledgment that the
 * mapping layer saw the new column.
 */
object HogSchemaColumns {
    val TABLES: Map<String, Set<String>> =
        mapOf(
            // CatalogRepo.catalogMapper (the one hog_catalog row mapping;
            // options/expiry/cleanup read through it). Allocator columns are
            // advanced via UPDATE..RETURNING in CatalogRepo.allocate*.
            "hog_catalog" to
                setOf(
                    "catalog_id", "name", "data_path", "last_snapshot_id", "schema_version",
                    "next_table_id", "next_file_id", "next_namespace_id",
                    "snapshot_retention_seconds", "consumer_floor", "earliest_snapshot_id",
                    "earliest_snapshot_time", "next_view_id", "created_at",
                ),
            // SnapshotRepo (insert + page mappers), TimeTravelRepo.
            "hog_snapshot" to
                setOf(
                    "catalog_id", "snapshot_id", "snapshot_time", "schema_version",
                    "author", "commit_message",
                ),
            // SnapshotRepo.insertChange, CommitService change batch,
            // CommitService.checkConflicts.
            "hog_snapshot_change" to setOf("catalog_id", "snapshot_id", "kind", "object_id"),
            // NamespaceRepo.
            "hog_namespace" to setOf("catalog_id", "namespace_id", "name", "dropped", "created_at"),
            // TableRepo (identity + drop marking).
            "hog_table" to
                setOf(
                    "catalog_id", "table_id", "table_uuid", "created_snapshot",
                    "dropped_snapshot", "next_field_id",
                ),
            // TableRepo versioned-name mappers, CommitService.resolveLiveTable.
            "hog_table_version" to
                setOf(
                    "catalog_id", "table_id", "begin_snapshot", "end_snapshot",
                    "namespace_id", "name",
                ),
            // TableRepo.columnsAt, AlterService, Hydrator.liveColumns.
            "hog_column" to
                setOf(
                    "catalog_id", "table_id", "field_id", "begin_snapshot", "end_snapshot",
                    "name", "col_type", "type_params", "nullable", "ordinal",
                ),
            // CommitService.writeAppends (rollup + row-id allocator).
            "hog_table_stats" to
                setOf("catalog_id", "table_id", "record_count", "file_size_bytes", "next_row_id"),
            // FileRepo mappers, CommitService.writeAppends, Hydrator,
            // CompactionService (candidates + output insert).
            "hog_data_file" to
                setOf(
                    "catalog_id", "data_file_id", "table_id", "begin_snapshot", "end_snapshot",
                    "path", "file_format", "record_count", "file_size_bytes", "footer_size",
                    "row_id_start", "stats_state", "spec_id", "explicit_row_ids",
                    "missing_field_ids",
                ),
            // FileRepo stats mappers, Hydrator.upsertStats,
            // CompactionService.aggregateStats.
            "hog_file_column_stats" to
                setOf(
                    "catalog_id", "data_file_id", "field_id", "value_count", "null_count",
                    "nan_count", "size_bytes", "lower_bound", "upper_bound",
                ),
            // OffsetRepo, ExpiryService floor query, CatalogMetrics lag query.
            "hog_consumer_offset" to
                setOf("catalog_id", "consumer_id", "table_uuid", "committed_snapshot", "updated_at"),
            // SpecRepo, CommitService.liveSpec.
            "hog_partition_spec" to
                setOf("catalog_id", "table_id", "spec_id", "begin_snapshot", "end_snapshot"),
            "hog_partition_field" to
                setOf(
                    "catalog_id", "table_id", "spec_id", "key_index", "source_field_id",
                    "transform", "transform_param",
                ),
            // FileRepo partition-value loads, CommitService/CompactionService inserts.
            "hog_file_partition_value" to setOf("catalog_id", "data_file_id", "key_index", "value"),
            // SortRepo.
            "hog_sort_spec" to
                setOf("catalog_id", "table_id", "sort_id", "begin_snapshot", "end_snapshot"),
            "hog_sort_field" to
                setOf(
                    "catalog_id", "table_id", "sort_id", "key_index", "source_field_id",
                    "direction", "null_order",
                ),
            // DeleteFileReadRepo, CommitService.applyDeletes.
            "hog_delete_file" to
                setOf(
                    "catalog_id", "delete_file_id", "table_id", "data_file_id", "begin_snapshot",
                    "end_snapshot", "path", "file_format", "delete_count", "file_size_bytes",
                ),
            // ViewRepo.
            "hog_view" to
                setOf(
                    "catalog_id", "view_id", "view_uuid", "namespace_id", "name", "dialect",
                    "sql", "begin_snapshot", "end_snapshot",
                ),
            // CleanupService (drain + ledger), ExpiryService queue inserts.
            "hog_file_removal" to
                setOf(
                    "removal_id", "catalog_id", "path", "file_kind", "reason", "scheduled_at",
                    "attempts", "last_attempt_at", "drained_at", "drained_outcome",
                ),
        )
}
