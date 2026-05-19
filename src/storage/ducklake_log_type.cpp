#include "storage/ducklake_log_type.hpp"

#include "duckdb/common/types/value.hpp"

namespace duckdb {

constexpr LogLevel DuckLakeMetadataLogType::LEVEL;

DuckLakeMetadataLogType::DuckLakeMetadataLogType() : LogType(NAME, LEVEL, GetLogType()) {
}

LogicalType DuckLakeMetadataLogType::GetLogType() {
	child_list_t<LogicalType> child_list = {
	    {"catalog", LogicalType::VARCHAR},
	    {"query", LogicalType::VARCHAR},
	    {"elapsed_ms", LogicalType::BIGINT},
	    {"metadata_api", LogicalType::VARCHAR},
	    {"ducklake_explicit_metadata_transaction", LogicalType::BOOLEAN},
	};
	return LogicalType::STRUCT(child_list);
}

string DuckLakeMetadataLogType::ConstructLogMessage(const string &catalog_name, const string &query,
                                                    int64_t elapsed_ms, const string &metadata_api,
                                                    bool ducklake_explicit_metadata_transaction) {
	child_list_t<Value> child_list = {
	    {"catalog", Value(catalog_name)},
	    {"query", Value(query)},
	    {"elapsed_ms", Value::BIGINT(elapsed_ms)},
	    {"metadata_api", Value(metadata_api)},
	    {"ducklake_explicit_metadata_transaction", Value::BOOLEAN(ducklake_explicit_metadata_transaction)},
	};
	return Value::STRUCT(std::move(child_list)).ToString();
}

} // namespace duckdb
