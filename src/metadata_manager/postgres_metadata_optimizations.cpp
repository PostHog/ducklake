#include "metadata_manager/postgres_metadata_manager.hpp"

#include "storage/ducklake_catalog.hpp"
#include "storage/ducklake_transaction.hpp"

#include <atomic>

namespace duckdb {

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

void PostgresMetadataManager::DeleteSnapshotRows(const vector<DuckLakeSnapshotInfo> &snapshots) {
	if (snapshots.empty()) {
		return;
	}
	static atomic<idx_t> next_selection_id(0);
	auto selection = StringUtil::Format("ducklake_postgres_expired_snapshots_%llu", next_selection_id++);
	string selection_values;
	for (auto &snapshot : snapshots) {
		if (!selection_values.empty()) {
			selection_values += ", ";
		}
		selection_values += StringUtil::Format("(%llu)", snapshot.id);
	}
	string query = StringUtil::Format(R"SQL(
CREATE TEMP TABLE %s(snapshot_id BIGINT PRIMARY KEY) ON COMMIT DROP;
INSERT INTO %s(snapshot_id) VALUES %s;
DELETE FROM {METADATA_CATALOG}.ducklake_snapshot_changes c
USING %s expired
WHERE c.snapshot_id = expired.snapshot_id;
DELETE FROM {METADATA_CATALOG}.ducklake_snapshot s
USING %s expired
WHERE s.snapshot_id = expired.snapshot_id;
)SQL",
	                                  selection, selection, selection_values, selection, selection);
	DuckLakeSnapshot execution_snapshot;
	auto result = PassthroughExecute(execution_snapshot, query);
	if (result->HasError()) {
		result->GetErrorObject().Throw("Failed to delete snapshots using the PostgreSQL native path: ");
	}
}

} // namespace duckdb
