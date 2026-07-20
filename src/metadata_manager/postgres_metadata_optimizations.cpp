#include "metadata_manager/postgres_metadata_manager.hpp"

#include "storage/ducklake_catalog.hpp"
#include "storage/ducklake_transaction.hpp"

#include <atomic>

namespace duckdb {

string DuckLakeMetadataManager::GetSnapshotAndStatsAndChangesQuery(const set<TableIndex> &table_ids) {
	string stats_filter = "FALSE";
	if (!table_ids.empty()) {
		stats_filter = "ducklake_table_stats.table_id IN (";
		for (auto &table_id : table_ids) {
			if (stats_filter.back() != '(') {
				stats_filter += ", ";
			}
			stats_filter += to_string(table_id.index);
		}
		stats_filter += ")";
	}
	auto query = GetSnapshotAndStatsAndChangesQuery();
	auto filtered_query = StringUtil::Replace(query, "ORDER BY table_id NULLS FIRST;",
	                                          "    AND " + stats_filter + "\nORDER BY table_id NULLS FIRST;");
	if (filtered_query == query) {
		throw InternalException("Could not apply the transaction write-set filter to the metadata stats query");
	}
	return filtered_query;
}

SnapshotChangeInfo
DuckLakeMetadataManager::GetSnapshotAndStatsAndChanges(SnapshotAndStats &current_snapshot,
                                                       const std::function<unique_ptr<QueryResult>(string)> &executor,
                                                       const set<TableIndex> &table_ids) {
	auto result = executor(GetSnapshotAndStatsAndChangesQuery(table_ids));
	return ParseSnapshotAndStatsAndChanges(*result, current_snapshot);
}

vector<DuckLakeTableSizeInfo> PostgresMetadataManager::GetTableSizes(DuckLakeSnapshot snapshot) {
	string query = R"SQL(
SELECT tbl.schema_id, tbl.table_id, tbl.table_name, tbl.table_uuid,
       COALESCE(data_files.file_count, 0) AS data_file_count,
       COALESCE(data_files.total_file_size, 0) AS data_total_size,
       COALESCE(delete_files.file_count, 0) AS delete_file_count,
       COALESCE(delete_files.total_file_size, 0) AS delete_total_size
FROM {METADATA_CATALOG}.ducklake_table tbl
LEFT JOIN (
  SELECT table_id, COUNT(*) AS file_count, COALESCE(SUM(file_size_bytes), 0) AS total_file_size
  FROM {METADATA_CATALOG}.ducklake_data_file
  WHERE {SNAPSHOT_ID} >= begin_snapshot AND ({SNAPSHOT_ID} < end_snapshot OR end_snapshot IS NULL)
  GROUP BY table_id
) data_files USING (table_id)
LEFT JOIN (
  SELECT table_id, COUNT(*) AS file_count, COALESCE(SUM(file_size_bytes), 0) AS total_file_size
  FROM {METADATA_CATALOG}.ducklake_delete_file
  WHERE {SNAPSHOT_ID} >= begin_snapshot AND ({SNAPSHOT_ID} < end_snapshot OR end_snapshot IS NULL)
  GROUP BY table_id
) delete_files USING (table_id)
WHERE {SNAPSHOT_ID} >= tbl.begin_snapshot
  AND ({SNAPSHOT_ID} < tbl.end_snapshot OR tbl.end_snapshot IS NULL)
)SQL";
	auto result = PassthroughQuery(snapshot, query);
	if (result->HasError()) {
		result->GetErrorObject().Throw("Failed to get table sizes using the PostgreSQL native path: ");
	}

	vector<DuckLakeTableSizeInfo> table_sizes;
	for (auto &row : *result) {
		DuckLakeTableSizeInfo table_size;
		table_size.schema_id = SchemaIndex(row.GetValue<idx_t>(0));
		table_size.table_id = TableIndex(row.GetValue<idx_t>(1));
		table_size.table_name = row.GetValue<string>(2);
		table_size.table_uuid = row.GetValue<string>(3);
		table_size.file_count = row.GetValue<idx_t>(4);
		table_size.file_size_bytes = row.GetValue<idx_t>(5);
		table_size.delete_file_count = row.GetValue<idx_t>(6);
		table_size.delete_file_size_bytes = row.GetValue<idx_t>(7);
		table_sizes.push_back(std::move(table_size));
	}
	return table_sizes;
}

void PostgresMetadataManager::DeleteSnapshots(const vector<DuckLakeSnapshotInfo> &snapshots) {
	if (snapshots.empty()) {
		return;
	}
	string stats_query = "SELECT DISTINCT table_id FROM {METADATA_CATALOG}.ducklake_table_stats;";
	auto stats_result = PassthroughQuery(stats_query);
	if (stats_result->HasError()) {
		stats_result->GetErrorObject().Throw("Failed to list table stats for cache invalidation in DuckLake: ");
	}
	vector<TableIndex> stats_table_ids;
	for (auto &row : *stats_result) {
		stats_table_ids.push_back(TableIndex(row.GetValue<idx_t>(0)));
	}
	static atomic<idx_t> next_selection_id(0);
	auto expiration_selection = StringUtil::Format("ducklake_postgres_expired_snapshots_%llu", next_selection_id++);
	string selection_values;
	for (auto &snapshot : snapshots) {
		if (!selection_values.empty()) {
			selection_values += ", ";
		}
		selection_values += StringUtil::Format("(%llu)", snapshot.id);
	}
	auto create_selection = StringUtil::Format(R"SQL(
CREATE TEMP TABLE %s(snapshot_id BIGINT PRIMARY KEY) ON COMMIT DROP;
INSERT INTO %s(snapshot_id) VALUES %s;
)SQL",
	                                           expiration_selection, expiration_selection, selection_values);

	string batch = create_selection + R"SQL(
DELETE FROM {METADATA_CATALOG}.ducklake_snapshot_changes c
USING {EXPIRATION_SELECTION} expired
WHERE c.snapshot_id = expired.snapshot_id;
DELETE FROM {METADATA_CATALOG}.ducklake_snapshot s
USING {EXPIRATION_SELECTION} expired
WHERE s.snapshot_id = expired.snapshot_id;

DROP TABLE IF EXISTS pg_temp.ducklake_postgres_dead_tables;
CREATE TEMP TABLE ducklake_postgres_dead_tables ON COMMIT DROP AS
SELECT DISTINCT t.table_id
FROM {METADATA_CATALOG}.ducklake_table t
WHERE t.end_snapshot IS NOT NULL
  AND NOT EXISTS (
      SELECT 1 FROM {METADATA_CATALOG}.ducklake_snapshot s
      WHERE s.snapshot_id >= t.begin_snapshot AND s.snapshot_id < t.end_snapshot)
  AND NOT EXISTS (
      SELECT 1 FROM {METADATA_CATALOG}.ducklake_table t2
      WHERE t2.table_id = t.table_id
        AND (t2.end_snapshot IS NULL OR EXISTS (
            SELECT 1 FROM {METADATA_CATALOG}.ducklake_snapshot s2
            WHERE s2.snapshot_id >= t2.begin_snapshot AND s2.snapshot_id < t2.end_snapshot)));
CREATE UNIQUE INDEX ON ducklake_postgres_dead_tables(table_id);

DROP TABLE IF EXISTS pg_temp.ducklake_postgres_dead_data_files;
CREATE TEMP TABLE ducklake_postgres_dead_data_files ON COMMIT DROP AS
SELECT f.data_file_id,
       CASE
         WHEN NOT f.path_is_relative THEN f.path
         WHEN t.path IS NOT NULL AND NOT t.path_is_relative THEN t.path || f.path
         WHEN s.path IS NOT NULL AND NOT s.path_is_relative THEN s.path || COALESCE(t.path, '') || f.path
         ELSE {DATA_PATH} || COALESCE(s.path, '') || COALESCE(t.path, '') || f.path
       END AS cleanup_path,
       FALSE AS cleanup_path_is_relative
FROM {METADATA_CATALOG}.ducklake_data_file f
JOIN LATERAL (
  SELECT table_id, schema_id, path, path_is_relative
  FROM {METADATA_CATALOG}.ducklake_table table_path
  WHERE table_path.table_id = f.table_id
  ORDER BY begin_snapshot DESC LIMIT 1
) t ON true
JOIN LATERAL (
  SELECT path, path_is_relative
  FROM {METADATA_CATALOG}.ducklake_schema schema_path
  WHERE schema_path.schema_id = t.schema_id
  ORDER BY begin_snapshot DESC LIMIT 1
) s ON true
WHERE EXISTS (SELECT 1 FROM ducklake_postgres_dead_tables dead WHERE dead.table_id = f.table_id)
   OR (f.end_snapshot IS NOT NULL AND NOT EXISTS (
       SELECT 1 FROM {METADATA_CATALOG}.ducklake_snapshot snap
       WHERE snap.snapshot_id >= f.begin_snapshot AND snap.snapshot_id < f.end_snapshot));
CREATE UNIQUE INDEX ON ducklake_postgres_dead_data_files(data_file_id);

INSERT INTO {METADATA_CATALOG}.ducklake_files_scheduled_for_deletion
SELECT data_file_id, cleanup_path, cleanup_path_is_relative, NOW()
FROM ducklake_postgres_dead_data_files;
DELETE FROM {METADATA_CATALOG}.ducklake_file_column_stats x USING ducklake_postgres_dead_data_files dead
WHERE x.data_file_id = dead.data_file_id;
DELETE FROM {METADATA_CATALOG}.ducklake_file_variant_stats x USING ducklake_postgres_dead_data_files dead
WHERE x.data_file_id = dead.data_file_id;
DELETE FROM {METADATA_CATALOG}.ducklake_file_partition_value x USING ducklake_postgres_dead_data_files dead
WHERE x.data_file_id = dead.data_file_id;

DROP TABLE IF EXISTS pg_temp.ducklake_postgres_dead_delete_files;
CREATE TEMP TABLE ducklake_postgres_dead_delete_files ON COMMIT DROP AS
SELECT f.delete_file_id,
       CASE
         WHEN NOT f.path_is_relative THEN f.path
         WHEN t.path IS NOT NULL AND NOT t.path_is_relative THEN t.path || f.path
         WHEN s.path IS NOT NULL AND NOT s.path_is_relative THEN s.path || COALESCE(t.path, '') || f.path
         ELSE {DATA_PATH} || COALESCE(s.path, '') || COALESCE(t.path, '') || f.path
       END AS cleanup_path,
       FALSE AS cleanup_path_is_relative
FROM {METADATA_CATALOG}.ducklake_delete_file f
JOIN LATERAL (
  SELECT table_id, schema_id, path, path_is_relative
  FROM {METADATA_CATALOG}.ducklake_table table_path
  WHERE table_path.table_id = f.table_id
  ORDER BY begin_snapshot DESC LIMIT 1
) t ON true
JOIN LATERAL (
  SELECT path, path_is_relative
  FROM {METADATA_CATALOG}.ducklake_schema schema_path
  WHERE schema_path.schema_id = t.schema_id
  ORDER BY begin_snapshot DESC LIMIT 1
) s ON true
WHERE EXISTS (SELECT 1 FROM ducklake_postgres_dead_tables dead WHERE dead.table_id = f.table_id)
   OR EXISTS (SELECT 1 FROM ducklake_postgres_dead_data_files dead WHERE dead.data_file_id = f.data_file_id)
   OR (f.end_snapshot IS NOT NULL AND NOT EXISTS (
       SELECT 1 FROM {METADATA_CATALOG}.ducklake_snapshot snap
       WHERE snap.snapshot_id >= f.begin_snapshot AND snap.snapshot_id < f.end_snapshot));
CREATE UNIQUE INDEX ON ducklake_postgres_dead_delete_files(delete_file_id);

INSERT INTO {METADATA_CATALOG}.ducklake_files_scheduled_for_deletion
SELECT delete_file_id, cleanup_path, cleanup_path_is_relative, NOW()
FROM ducklake_postgres_dead_delete_files;
DELETE FROM {METADATA_CATALOG}.ducklake_delete_file x USING ducklake_postgres_dead_delete_files dead
WHERE x.delete_file_id = dead.delete_file_id;
DELETE FROM {METADATA_CATALOG}.ducklake_data_file x USING ducklake_postgres_dead_data_files dead
WHERE x.data_file_id = dead.data_file_id;

DO $ducklake_postgres$
DECLARE inlined RECORD;
BEGIN
  FOR inlined IN
    SELECT table_name FROM {METADATA_CATALOG}.ducklake_inlined_data_tables idt
    JOIN ducklake_postgres_dead_tables dead USING (table_id)
  LOOP
    EXECUTE format('DROP TABLE IF EXISTS %%I.%%I', {METADATA_SCHEMA_NAME_LITERAL}, inlined.table_name);
  END LOOP;
END
$ducklake_postgres$;

DELETE FROM {METADATA_CATALOG}.ducklake_table x USING ducklake_postgres_dead_tables dead
WHERE x.table_id = dead.table_id;
DELETE FROM {METADATA_CATALOG}.ducklake_table_stats x USING ducklake_postgres_dead_tables dead
WHERE x.table_id = dead.table_id;
DELETE FROM {METADATA_CATALOG}.ducklake_table_column_stats x USING ducklake_postgres_dead_tables dead
WHERE x.table_id = dead.table_id;
DELETE FROM {METADATA_CATALOG}.ducklake_partition_info x USING ducklake_postgres_dead_tables dead
WHERE x.table_id = dead.table_id;
DELETE FROM {METADATA_CATALOG}.ducklake_partition_column x USING ducklake_postgres_dead_tables dead
WHERE x.table_id = dead.table_id;
DELETE FROM {METADATA_CATALOG}.ducklake_column x USING ducklake_postgres_dead_tables dead
WHERE x.table_id = dead.table_id;
DELETE FROM {METADATA_CATALOG}.ducklake_column_tag x USING ducklake_postgres_dead_tables dead
WHERE x.table_id = dead.table_id;
DELETE FROM {METADATA_CATALOG}.ducklake_sort_info x USING ducklake_postgres_dead_tables dead
WHERE x.table_id = dead.table_id;
DELETE FROM {METADATA_CATALOG}.ducklake_sort_expression x USING ducklake_postgres_dead_tables dead
WHERE x.table_id = dead.table_id;
DELETE FROM {METADATA_CATALOG}.ducklake_schema_versions x USING ducklake_postgres_dead_tables dead
WHERE x.table_id = dead.table_id;
DELETE FROM {METADATA_CATALOG}.ducklake_inlined_data_tables x USING ducklake_postgres_dead_tables dead
WHERE x.table_id = dead.table_id;
DELETE FROM {METADATA_CATALOG}.ducklake_column_mapping x USING ducklake_postgres_dead_tables dead
WHERE x.table_id = dead.table_id;

DELETE FROM {METADATA_CATALOG}.ducklake_schema x
WHERE x.end_snapshot IS NOT NULL AND NOT EXISTS (
  SELECT 1 FROM {METADATA_CATALOG}.ducklake_snapshot s
  WHERE s.snapshot_id >= x.begin_snapshot AND s.snapshot_id < x.end_snapshot);
DELETE FROM {METADATA_CATALOG}.ducklake_view x
WHERE x.end_snapshot IS NOT NULL AND NOT EXISTS (
  SELECT 1 FROM {METADATA_CATALOG}.ducklake_snapshot s
  WHERE s.snapshot_id >= x.begin_snapshot AND s.snapshot_id < x.end_snapshot);
DELETE FROM {METADATA_CATALOG}.ducklake_tag x
WHERE x.end_snapshot IS NOT NULL AND NOT EXISTS (
  SELECT 1 FROM {METADATA_CATALOG}.ducklake_snapshot s
  WHERE s.snapshot_id >= x.begin_snapshot AND s.snapshot_id < x.end_snapshot);
DELETE FROM {METADATA_CATALOG}.ducklake_macro x
WHERE x.end_snapshot IS NOT NULL AND NOT EXISTS (
  SELECT 1 FROM {METADATA_CATALOG}.ducklake_snapshot s
  WHERE s.snapshot_id >= x.begin_snapshot AND s.snapshot_id < x.end_snapshot);
DELETE FROM {METADATA_CATALOG}.ducklake_macro_impl x
WHERE NOT EXISTS (SELECT 1 FROM {METADATA_CATALOG}.ducklake_macro m WHERE m.macro_id = x.macro_id);
DELETE FROM {METADATA_CATALOG}.ducklake_macro_parameters x
WHERE NOT EXISTS (SELECT 1 FROM {METADATA_CATALOG}.ducklake_macro m WHERE m.macro_id = x.macro_id);

DROP TABLE IF EXISTS pg_temp.ducklake_postgres_orphan_mappings;
CREATE TEMP TABLE ducklake_postgres_orphan_mappings ON COMMIT DROP AS
SELECT DISTINCT n.mapping_id
FROM {METADATA_CATALOG}.ducklake_name_mapping n
WHERE NOT EXISTS (
  SELECT 1 FROM {METADATA_CATALOG}.ducklake_column_mapping m WHERE m.mapping_id = n.mapping_id);
DELETE FROM {METADATA_CATALOG}.ducklake_name_mapping n
USING ducklake_postgres_orphan_mappings orphan
WHERE n.mapping_id = orphan.mapping_id
  AND NOT EXISTS (
    SELECT 1 FROM {METADATA_CATALOG}.ducklake_column_mapping m WHERE m.mapping_id = n.mapping_id);
)SQL";

	DuckLakeSnapshot execution_snapshot;
	batch = StringUtil::Replace(batch, "{EXPIRATION_SELECTION}", "pg_temp." + expiration_selection);
	auto result = PassthroughExecute(execution_snapshot, batch);
	if (result->HasError()) {
		result->GetErrorObject().Throw("Failed to expire snapshots using the PostgreSQL native path: ");
	}
	auto &catalog = transaction.GetCatalog();
	for (auto &snapshot : snapshots) {
		for (auto &table_id : stats_table_ids) {
			catalog.InvalidateTableStatsCache(snapshot.next_file_id, table_id);
		}
	}
}

} // namespace duckdb
