#include "metadata_manager/postgres_metadata_manager.hpp"
#include "common/ducklake_util.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/planner/filter/constant_filter.hpp"
#include "storage/ducklake_catalog.hpp"
#include "storage/ducklake_transaction.hpp"
#include "storage/ducklake_metadata_info.hpp"

namespace duckdb {

static constexpr const char *POSTGRES_TYPED_MIN_VALUE = "__ducklake_typed_min_value";
static constexpr const char *POSTGRES_TYPED_MAX_VALUE = "__ducklake_typed_max_value";

static bool IsDigit(char c) {
	return c >= '0' && c <= '9';
}

static bool HasFourDigitDatePrefix(const string &value) {
	return value.size() >= 10 && IsDigit(value[0]) && IsDigit(value[1]) && IsDigit(value[2]) && IsDigit(value[3]) &&
	       value[4] == '-' && IsDigit(value[5]) && IsDigit(value[6]) && value[7] == '-' && IsDigit(value[8]) &&
	       IsDigit(value[9]);
}

static string WithPostgresBinaryCollation(const string &expression) {
	// DuckLake VARCHAR stats use DuckDB's bytewise ordering, independent of the metadata database locale.
	return "(" + expression + " COLLATE \"C\")";
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
	auto result = PassthroughExecute(index_query);
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
	case LogicalTypeId::TIMESTAMP_TZ:
		return "TIMESTAMPTZ";
	default:
		return type.ToString();
	}
}

bool PostgresMetadataManager::IsPostgresTemporalStatsType(const LogicalType &type) {
	switch (type.id()) {
	case LogicalTypeId::DATE:
	case LogicalTypeId::TIMESTAMP:
	case LogicalTypeId::TIMESTAMP_TZ:
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
	if (type.id() == LogicalTypeId::VARCHAR) {
		return WithPostgresBinaryCollation(literal);
	}
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
		} else if (type.id() == LogicalTypeId::TIMESTAMP_TZ) {
			regex = "'^[0-9]{4}-(0[1-9]|1[0-2])-([0][1-9]|[12][0-9]|3[01]) "
			        "([01][0-9]|2[0-3]):[0-5][0-9]:[0-5][0-9](\\.[0-9]{1,6})?"
			        "(Z|[+-](0[0-9]|1[0-5])(:[0-5][0-9])?)$'";
			auto year = StringUtil::Format("substring(%s FROM 1 FOR 4)::INTEGER", stats);
			auto month = StringUtil::Format("substring(%s FROM 6 FOR 2)::INTEGER", stats);
			auto day = StringUtil::Format("substring(%s FROM 9 FOR 2)::INTEGER", stats);
			auto max_day = StringUtil::Format(
			    "(CASE WHEN %s = 2 THEN CASE WHEN mod(%s, 4) = 0 AND (mod(%s, 100) <> 0 OR mod(%s, 400) = 0) "
			    "THEN 29 ELSE 28 END WHEN %s IN (4, 6, 9, 11) THEN 30 ELSE 31 END)",
			    month, year, year, year, month);
			auto valid_date = StringUtil::Format("%s > 0 AND %s <= %s", year, day, max_day);
			return StringUtil::Format("(CASE WHEN %s ~ %s THEN CASE WHEN %s THEN %s::%s END END)", stats, regex,
			                          valid_date, stats, GetPostgresStatsType(type));
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
	if (type.id() == LogicalTypeId::VARCHAR) {
		return WithPostgresBinaryCollation(stats);
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
	auto min_value =
	    IsPostgresTemporalStatsType(type) ? POSTGRES_TYPED_MIN_VALUE : CastStatsToTarget("min_value", type);
	auto max_value =
	    IsPostgresTemporalStatsType(type) ? POSTGRES_TYPED_MAX_VALUE : CastStatsToTarget("max_value", type);
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

	auto passthrough_query =
	    command == "postgres_query" ? StringUtil::Format("SELECT * FROM postgres_query(%s, %s)", catalog_literal,
	                                                     SQLString(query))
	                                : StringUtil::Format("CALL %s(%s, %s)", command, catalog_literal, SQLString(query));
	// Run the fully-formed passthrough query directly on the metadata connection. Routing it back
	// through transaction.Query() would re-enter PostgresMetadataManager::Query and wrap the query
	// in another postgres_query(...) call indefinitely (v1.5.3 delegates transaction.Query -> manager).
	auto result = transaction.ExecuteRaw(passthrough_query);
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
	return PassthroughExecute(snapshot, query);
}

unique_ptr<QueryResult> PostgresMetadataManager::Execute(string &query) {
	return PassthroughExecute(query);
}

unique_ptr<QueryResult> PostgresMetadataManager::PassthroughExecute(DuckLakeSnapshot snapshot, string &query) {
	return ExecuteQuery(snapshot, query, "postgres_execute");
}

unique_ptr<QueryResult> PostgresMetadataManager::PassthroughExecute(string &query) {
	return ExecuteQuery(query, "postgres_execute");
}

unique_ptr<QueryResult> PostgresMetadataManager::PassthroughQuery(DuckLakeSnapshot snapshot, string &query) {
	return ExecuteQuery(snapshot, query, "postgres_query");
}

unique_ptr<QueryResult> PostgresMetadataManager::PassthroughQuery(string &query) {
	return ExecuteQuery(query, "postgres_query");
}

string PostgresMetadataManager::GenerateFileColumnStatsCTEBody(const CTERequirement &req, TableIndex table_id) {
	auto select_list = GenerateFileColumnStatsSelectList(req);
	auto query = StringUtil::Format("SELECT %s\n"
	                                "FROM {METADATA_SCHEMA_ESCAPED}.ducklake_file_column_stats\n"
	                                "WHERE column_id = %d AND table_id = %d",
	                                select_list, req.column_field_index, table_id.index);
	return StringUtil::Format("  SELECT * FROM postgres_query({METADATA_CATALOG_NAME_LITERAL},\n"
	                          "    %s)\n",
	                          DuckLakeUtil::SQLLiteralToString(query));
}

string PostgresMetadataManager::GenerateFileColumnStatsSelectList(const CTERequirement &req) {
	string select_list = "data_file_id";
	for (const auto &stat : req.referenced_stats) {
		select_list += ", " + stat;
	}
	if (!IsPostgresTemporalStatsType(req.column_type)) {
		return select_list;
	}
	if (req.referenced_stats.count("min_value")) {
		select_list +=
		    StringUtil::Format(", %s AS %s", CastStatsToTarget("min_value", req.column_type), POSTGRES_TYPED_MIN_VALUE);
	}
	if (req.referenced_stats.count("max_value")) {
		select_list +=
		    StringUtil::Format(", %s AS %s", CastStatsToTarget("max_value", req.column_type), POSTGRES_TYPED_MAX_VALUE);
	}
	return select_list;
}

string PostgresMetadataManager::GeneratePassthroughFileColumnStatsCTEBody(const CTERequirement &req,
                                                                          TableIndex table_id) {
	return StringUtil::Format("  SELECT %s\n"
	                          "  FROM {METADATA_CATALOG}.ducklake_file_column_stats\n"
	                          "  WHERE column_id = %d AND table_id = %d\n",
	                          GenerateFileColumnStatsSelectList(req), req.column_field_index, table_id.index);
}

// Postgres inlined data is fetched in its storage types; convert it to the expected DuckDB scan types.
shared_ptr<DuckLakeInlinedData>
PostgresMetadataManager::TransformInlinedData(QueryResult &result, const vector<LogicalType> &expected_types) {
	if (result.HasError()) {
		result.GetErrorObject().Throw("Failed to read inlined data from DuckLake: ");
	}

	if (expected_types.empty() || result.types == expected_types) {
		return DuckLakeMetadataManager::TransformInlinedData(result, expected_types);
	}
	if (expected_types.size() != result.types.size()) {
		throw InternalException("Expected %d inlined data columns from Postgres, but received %d", expected_types.size(),
		                        result.types.size());
	}

	auto context = transaction.context.lock();
	auto data = make_uniq<ColumnDataCollection>(*context, expected_types);
	ColumnDataAppendState append_state;
	data->InitializeAppend(append_state);
	DataChunk transform_chunk;
	transform_chunk.Initialize(*context, expected_types);
	while (true) {
		auto chunk = result.Fetch();
		if (!chunk) {
			break;
		}
		transform_chunk.Reset();
		for (idx_t i = 0; i < expected_types.size(); i++) {
			if (result.types[i] == expected_types[i]) {
				transform_chunk.data[i].Reference(chunk->data[i]);
			} else if (result.types[i].id() == LogicalTypeId::BLOB && expected_types[i].id() == LogicalTypeId::VARCHAR) {
				transform_chunk.data[i].Reinterpret(chunk->data[i]);
			} else {
				VectorOperations::Cast(*context, chunk->data[i], transform_chunk.data[i], chunk->size());
			}
		}
		transform_chunk.SetCardinality(chunk->size());
		data->Append(append_state, transform_chunk);
	}
	auto inlined_data = make_shared_ptr<DuckLakeInlinedData>();
	inlined_data->data = std::move(data);
	return inlined_data;
}

} // namespace duckdb
