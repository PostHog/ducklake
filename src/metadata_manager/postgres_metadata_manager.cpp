#include "metadata_manager/postgres_metadata_manager.hpp"
#include "common/ducklake_util.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/planner/filter/constant_filter.hpp"
#include "storage/ducklake_catalog.hpp"
#include "storage/ducklake_transaction.hpp"

namespace duckdb {

static bool IsDigit(char c) {
	return c >= '0' && c <= '9';
}

static bool HasFourDigitDatePrefix(const string &value) {
	return value.size() >= 10 && IsDigit(value[0]) && IsDigit(value[1]) && IsDigit(value[2]) && IsDigit(value[3]) &&
	       value[4] == '-' && IsDigit(value[5]) && IsDigit(value[6]) && value[7] == '-' && IsDigit(value[8]) &&
	       IsDigit(value[9]);
}

PostgresMetadataManager::PostgresMetadataManager(DuckLakeTransaction &transaction)
    : DuckLakeMetadataManager(transaction) {
}

bool PostgresMetadataManager::TypeIsNativelySupported(const LogicalType &type) {
	switch (type.id()) {
	// Unnamed composite types are not supported.
	case LogicalTypeId::STRUCT:
	case LogicalTypeId::MAP:
	case LogicalTypeId::LIST:
	case LogicalTypeId::UBIGINT:
	case LogicalTypeId::HUGEINT:
	case LogicalTypeId::UHUGEINT:
	// Postgres timestamp/date ranges are narrower than DuckDB's
	case LogicalTypeId::DATE:
	case LogicalTypeId::TIMESTAMP:
	case LogicalTypeId::TIMESTAMP_TZ:
	case LogicalTypeId::TIMESTAMP_SEC:
	case LogicalTypeId::TIMESTAMP_MS:
	case LogicalTypeId::TIMESTAMP_NS:
	// Postgres bytea input format differs from DuckDB's blob text format
	case LogicalTypeId::BLOB:
	// Postgres cannot store null bytes in VARCHAR/TEXT columns
	case LogicalTypeId::VARCHAR:
	case LogicalTypeId::VARIANT:
	// If we knew that the Postgres installation has PostGIS installed, we could support GEOMETRY in the future.
	case LogicalTypeId::GEOMETRY:
		return false;
	default:
		return true;
	}
}

bool PostgresMetadataManager::SupportsInlining(const LogicalType &type) {
	if (type.id() == LogicalTypeId::VARIANT) {
		return false;
	}
	return DuckLakeMetadataManager::SupportsInlining(type);
}

string PostgresMetadataManager::GetColumnTypeInternal(const LogicalType &column_type) {
	switch (column_type.id()) {
	case LogicalTypeId::DOUBLE:
		return "DOUBLE PRECISION";
	case LogicalTypeId::TINYINT:
		return "SMALLINT";
	case LogicalTypeId::UTINYINT:
	case LogicalTypeId::USMALLINT:
		return "INTEGER";
	case LogicalTypeId::UINTEGER:
		return "BIGINT";
	case LogicalTypeId::FLOAT:
		return "REAL";
	case LogicalTypeId::BLOB:
	case LogicalTypeId::VARCHAR:
		return "BYTEA";
	case LogicalTypeId::UBIGINT:
	case LogicalTypeId::HUGEINT:
	case LogicalTypeId::UHUGEINT:
	case LogicalTypeId::DATE:
	case LogicalTypeId::TIMESTAMP:
	case LogicalTypeId::TIMESTAMP_TZ:
	case LogicalTypeId::TIMESTAMP_SEC:
	case LogicalTypeId::TIMESTAMP_MS:
	case LogicalTypeId::TIMESTAMP_NS:
		return "VARCHAR";
	default:
		return column_type.ToString();
	}
}

string PostgresMetadataManager::GetPostgresIndexStatements() {
	return R"(
CREATE INDEX IF NOT EXISTS ducklake_data_file_table_snapshot_idx ON {METADATA_CATALOG}.ducklake_data_file(table_id, begin_snapshot, end_snapshot);
CREATE INDEX IF NOT EXISTS ducklake_delete_file_table_snapshot_idx ON {METADATA_CATALOG}.ducklake_delete_file(table_id, begin_snapshot, end_snapshot);
CREATE INDEX IF NOT EXISTS ducklake_file_column_stats_table_column_idx ON {METADATA_CATALOG}.ducklake_file_column_stats(table_id, column_id);
CREATE INDEX IF NOT EXISTS ducklake_schema_versions_table_schema_version_idx ON {METADATA_CATALOG}.ducklake_schema_versions(table_id, schema_version);
CREATE INDEX IF NOT EXISTS ducklake_column_table_snapshot_idx ON {METADATA_CATALOG}.ducklake_column(table_id, begin_snapshot, end_snapshot);
CREATE INDEX IF NOT EXISTS ducklake_table_column_stats_table_column_idx ON {METADATA_CATALOG}.ducklake_table_column_stats(table_id, column_id);
)";
}

void PostgresMetadataManager::InitializeDuckLake(bool has_explicit_schema, DuckLakeEncryption encryption) {
	DuckLakeMetadataManager::InitializeDuckLake(has_explicit_schema, encryption);
	auto index_query = GetPostgresIndexStatements();
	auto result = Execute(index_query);
	if (result->HasError()) {
		result->GetErrorObject().Throw("Failed to initialize DuckLake Postgres metadata indexes: ");
	}
}

string PostgresMetadataManager::GetPostgresStatsType(const LogicalType &type) {
	switch (type.id()) {
	case LogicalTypeId::BOOLEAN:
		return "BOOLEAN";
	case LogicalTypeId::TINYINT:
	case LogicalTypeId::SMALLINT:
		return "SMALLINT";
	case LogicalTypeId::INTEGER:
	case LogicalTypeId::UTINYINT:
	case LogicalTypeId::USMALLINT:
		return "INTEGER";
	case LogicalTypeId::BIGINT:
	case LogicalTypeId::UINTEGER:
		return "BIGINT";
	case LogicalTypeId::UBIGINT:
	case LogicalTypeId::HUGEINT:
	case LogicalTypeId::UHUGEINT:
		return "NUMERIC";
	case LogicalTypeId::FLOAT:
		return "REAL";
	case LogicalTypeId::DOUBLE:
		return "DOUBLE PRECISION";
	case LogicalTypeId::DATE:
		return "DATE";
	case LogicalTypeId::TIMESTAMP:
	case LogicalTypeId::TIMESTAMP_SEC:
	case LogicalTypeId::TIMESTAMP_MS:
		return "TIMESTAMP";
	default:
		return type.ToString();
	}
}

bool PostgresMetadataManager::IsPostgresTemporalStatsType(const LogicalType &type) {
	switch (type.id()) {
	case LogicalTypeId::DATE:
	case LogicalTypeId::TIMESTAMP:
	case LogicalTypeId::TIMESTAMP_SEC:
	case LogicalTypeId::TIMESTAMP_MS:
		return true;
	default:
		return false;
	}
}

bool PostgresMetadataManager::CanCastTemporalValueForValueComparison(const Value &val, const LogicalType &type) {
	auto value = val.ToString();
	if (!HasFourDigitDatePrefix(value)) {
		return false;
	}
	if (type.id() == LogicalTypeId::DATE) {
		return value.size() == 10;
	}
	return IsPostgresTemporalStatsType(type);
}

bool PostgresMetadataManager::CanCastStatsForValueComparison(const LogicalType &type) {
	return type.IsNumeric() || type.id() == LogicalTypeId::BOOLEAN || IsPostgresTemporalStatsType(type);
}

string PostgresMetadataManager::CastValueToTarget(const Value &val, const LogicalType &type) {
	bool value_is_finite = true;
	if (val.type().id() == LogicalTypeId::FLOAT || val.type().id() == LogicalTypeId::DOUBLE) {
		value_is_finite = Value::IsFinite(val.GetValue<double>());
	}
	if (type.IsNumeric() && value_is_finite) {
		return val.ToString();
	}
	auto literal = DuckLakeUtil::SQLLiteralToString(val.ToString());
	if (IsPostgresTemporalStatsType(type) && CanCastTemporalValueForValueComparison(val, type)) {
		return literal + "::" + GetPostgresStatsType(type);
	}
	if (RequiresValueComparison(type) && CanCastStatsForValueComparison(type)) {
		return literal + "::" + GetPostgresStatsType(type);
	}
	return literal;
}

string PostgresMetadataManager::CastStatsToTarget(const string &stats, const LogicalType &type) {
	if (IsPostgresTemporalStatsType(type)) {
		string regex;
		if (type.id() == LogicalTypeId::DATE) {
			regex = "'^[0-9]{4}-(0[1-9]|1[0-2])-([0][1-9]|[12][0-9]|3[01])$'";
		} else {
			regex =
			    "'^[0-9]{4}-(0[1-9]|1[0-2])-([0][1-9]|[12][0-9]|3[01])( [0-9]{2}:[0-9]{2}:[0-9]{2}(\\.[0-9]{1,6})?)?$'";
		}
		return StringUtil::Format("(CASE WHEN %s ~ %s THEN %s::%s END)", stats, regex, stats,
		                          GetPostgresStatsType(type));
	}
	if (RequiresValueComparison(type) && CanCastStatsForValueComparison(type)) {
		return stats + "::" + GetPostgresStatsType(type);
	}
	return stats;
}

string PostgresMetadataManager::GenerateConstantFilter(const ConstantFilter &constant_filter, const LogicalType &type,
                                                       unordered_set<string> &referenced_stats) {
	if (RequiresValueComparison(type) && !CanCastStatsForValueComparison(type)) {
		return string();
	}
	if (IsPostgresTemporalStatsType(type) && !CanCastTemporalValueForValueComparison(constant_filter.constant, type)) {
		return string();
	}
	auto constant_str = CastValueToTarget(constant_filter.constant, type);
	auto min_value = CastStatsToTarget("min_value", type);
	auto max_value = CastStatsToTarget("max_value", type);
	if (IsPostgresTemporalStatsType(type)) {
		auto postgres_type = GetPostgresStatsType(type);
		min_value = StringUtil::Format("COALESCE(%s, '-infinity'::%s)", min_value, postgres_type);
		max_value = StringUtil::Format("COALESCE(%s, 'infinity'::%s)", max_value, postgres_type);
	}
	switch (constant_filter.comparison_type) {
	case ExpressionType::COMPARE_EQUAL:
		referenced_stats.insert("min_value");
		referenced_stats.insert("max_value");
		return StringUtil::Format("%s BETWEEN %s AND %s", constant_str, min_value, max_value);
	case ExpressionType::COMPARE_NOTEQUAL:
		referenced_stats.insert("min_value");
		referenced_stats.insert("max_value");
		return StringUtil::Format("NOT (%s = %s AND %s = %s)", min_value, constant_str, max_value, constant_str);
	case ExpressionType::COMPARE_GREATERTHANOREQUALTO:
		referenced_stats.insert("max_value");
		return StringUtil::Format("%s >= %s", max_value, constant_str);
	case ExpressionType::COMPARE_GREATERTHAN:
		referenced_stats.insert("max_value");
		return StringUtil::Format("%s > %s", max_value, constant_str);
	case ExpressionType::COMPARE_LESSTHANOREQUALTO:
		referenced_stats.insert("min_value");
		return StringUtil::Format("%s <= %s", min_value, constant_str);
	case ExpressionType::COMPARE_LESSTHAN:
		referenced_stats.insert("min_value");
		return StringUtil::Format("%s < %s", min_value, constant_str);
	default:
		return string();
	}
}

unique_ptr<QueryResult> PostgresMetadataManager::ExecuteQuery(DuckLakeSnapshot snapshot, string &query,
                                                              string command) {
	auto &commit_info = transaction.GetCommitInfo();

	query = StringUtil::Replace(query, "{SNAPSHOT_ID}", to_string(snapshot.snapshot_id));
	query = StringUtil::Replace(query, "{SCHEMA_VERSION}", to_string(snapshot.schema_version));
	query = StringUtil::Replace(query, "{NEXT_CATALOG_ID}", to_string(snapshot.next_catalog_id));
	query = StringUtil::Replace(query, "{NEXT_FILE_ID}", to_string(snapshot.next_file_id));
	query = StringUtil::Replace(query, "{AUTHOR}", commit_info.author.ToSQLString());
	query = StringUtil::Replace(query, "{COMMIT_MESSAGE}", commit_info.commit_message.ToSQLString());
	query = StringUtil::Replace(query, "{COMMIT_EXTRA_INFO}", commit_info.commit_extra_info.ToSQLString());

	auto &ducklake_catalog = transaction.GetCatalog();
	auto catalog_identifier = DuckLakeUtil::SQLIdentifierToString(ducklake_catalog.MetadataDatabaseName());
	auto catalog_literal = DuckLakeUtil::SQLLiteralToString(ducklake_catalog.MetadataDatabaseName());
	auto schema_identifier = DuckLakeUtil::SQLIdentifierToString(ducklake_catalog.MetadataSchemaName());
	auto schema_identifier_escaped = StringUtil::Replace(schema_identifier, "'", "''");
	auto schema_literal = DuckLakeUtil::SQLLiteralToString(ducklake_catalog.MetadataSchemaName());
	auto metadata_path = DuckLakeUtil::SQLLiteralToString(ducklake_catalog.MetadataPath());
	auto data_path = DuckLakeUtil::SQLLiteralToString(ducklake_catalog.DataPath());

	query = StringUtil::Replace(query, "{METADATA_CATALOG_NAME_LITERAL}", catalog_literal);
	query = StringUtil::Replace(query, "{METADATA_CATALOG_NAME_IDENTIFIER}", catalog_identifier);
	query = StringUtil::Replace(query, "{METADATA_SCHEMA_NAME_LITERAL}", schema_literal);
	query = StringUtil::Replace(query, "{METADATA_CATALOG}", schema_identifier);
	query = StringUtil::Replace(query, "{METADATA_SCHEMA_ESCAPED}", schema_identifier_escaped);
	query = StringUtil::Replace(query, "{METADATA_PATH}", metadata_path);
	query = StringUtil::Replace(query, "{DATA_PATH}", data_path);

	auto passthrough_query = StringUtil::Format("CALL %s(%s, %s)", command, catalog_literal, SQLString(query));
	auto result = transaction.Query(passthrough_query);
	if (command == "postgres_execute" && !result->HasError()) {
		while (result->Fetch()) {
		}
	}
	return result;
}

unique_ptr<QueryResult> PostgresMetadataManager::ExecuteQuery(string &query, string command) {
	// Snapshot-less metadata queries must not contain snapshot placeholders.
	DuckLakeSnapshot snapshot;
	return ExecuteQuery(snapshot, query, std::move(command));
}
unique_ptr<QueryResult> PostgresMetadataManager::Execute(DuckLakeSnapshot snapshot, string &query) {
	return ExecuteQuery(snapshot, query, "postgres_execute");
}

unique_ptr<QueryResult> PostgresMetadataManager::Execute(string &query) {
	return ExecuteQuery(query, "postgres_execute");
}

unique_ptr<QueryResult> PostgresMetadataManager::Query(DuckLakeSnapshot snapshot, string &query) {
	return ExecuteQuery(snapshot, query, "postgres_query");
}

unique_ptr<QueryResult> PostgresMetadataManager::Query(string &query) {
	return ExecuteQuery(query, "postgres_query");
}

string PostgresMetadataManager::GetLatestSnapshotQuery() const {
	return R"(
		SELECT * FROM postgres_query({METADATA_CATALOG_NAME_LITERAL},
			'SELECT snapshot_id, schema_version, next_catalog_id, next_file_id
			 FROM {METADATA_SCHEMA_ESCAPED}.ducklake_snapshot WHERE snapshot_id = (
			     SELECT MAX(snapshot_id) FROM {METADATA_SCHEMA_ESCAPED}.ducklake_snapshot
			 );')
	)";
}

bool PostgresMetadataManager::InlinedDeletionTableExists(TableIndex, DuckLakeSnapshot snapshot,
                                                         const string &table_name) {
	auto query = StringUtil::Format(R"(
SELECT EXISTS (
	SELECT 1
	FROM information_schema.tables
	WHERE table_schema = {METADATA_SCHEMA_NAME_LITERAL}
	  AND table_name = %s
))",
	                                DuckLakeUtil::SQLLiteralToString(table_name));
	auto result = Query(snapshot, query);
	if (result->HasError()) {
		return false;
	}
	auto chunk = result->Fetch();
	return chunk && chunk->size() > 0 && chunk->GetValue(0, 0).GetValue<bool>();
}

// We need a specialized function here to do a reinterpret for postgres from BLOB to VARCHAR
shared_ptr<DuckLakeInlinedData>
PostgresMetadataManager::TransformInlinedData(QueryResult &result, const vector<LogicalType> &expected_types) {
	bool needs_reinterpret = false;
	if (!expected_types.empty()) {
		D_ASSERT(expected_types.size() == result.types.size());
		for (idx_t i = 0; i < expected_types.size(); i++) {
			if (result.types[i] != expected_types[i]) {
				D_ASSERT(result.types[i].id() == LogicalTypeId::BLOB &&
				         expected_types[i].id() == LogicalTypeId::VARCHAR);
				needs_reinterpret = true;
			}
		}
	}
	if (!needs_reinterpret) {
		return DuckLakeMetadataManager::TransformInlinedData(result, expected_types);
	}

	if (result.HasError()) {
		result.GetErrorObject().Throw("Failed to read inlined data from DuckLake: ");
	}
	auto context = transaction.context.lock();
	auto data = make_uniq<ColumnDataCollection>(*context, expected_types);
	DataChunk reinterpret_chunk;
	reinterpret_chunk.Initialize(*context, expected_types);
	while (true) {
		auto chunk = result.Fetch();
		if (!chunk) {
			break;
		}
		for (idx_t i = 0; i < expected_types.size(); i++) {
			reinterpret_chunk.data[i].Reinterpret(chunk->data[i]);
		}
		reinterpret_chunk.SetCardinality(chunk->size());
		data->Append(reinterpret_chunk);
	}
	auto inlined_data = make_shared_ptr<DuckLakeInlinedData>();
	inlined_data->data = std::move(data);
	return inlined_data;
}

} // namespace duckdb
