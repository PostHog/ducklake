//===----------------------------------------------------------------------===//
//                         DuckDB
//
// metadata_manager/postgres_metadata_manager.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "storage/ducklake_metadata_manager.hpp"

namespace duckdb {

class PostgresMetadataManager : public DuckLakeMetadataManager {
public:
	explicit PostgresMetadataManager(DuckLakeTransaction &transaction);

	static unique_ptr<DuckLakeMetadataManager> Create(DuckLakeTransaction &transaction) {
		return make_uniq<PostgresMetadataManager>(transaction);
	}

	bool TypeIsNativelySupported(const LogicalType &type) override;
	bool SupportsInlining(const LogicalType &type) override;
	bool SupportsAppender() const override {
		return false;
	}
	idx_t MaxIdentifierLength() const override {
		return 63;
	}

	string GetColumnTypeInternal(const LogicalType &type) override;
	shared_ptr<DuckLakeInlinedData> TransformInlinedData(QueryResult &result,
	                                                     const vector<LogicalType> &expected_types) override;
	void InitializeDuckLake(bool has_explicit_schema, DuckLakeEncryption encryption) override;

	unique_ptr<QueryResult> Execute(DuckLakeSnapshot snapshot, string &query) override;
	unique_ptr<QueryResult> Execute(string &query) override;

	unique_ptr<QueryResult> Query(DuckLakeSnapshot snapshot, string &query) override;
	unique_ptr<QueryResult> Query(string &query) override;

protected:
	string GetLatestSnapshotQuery() const override;
	bool InlinedDeletionTableExists(TableIndex table_id, DuckLakeSnapshot snapshot, const string &table_name) override;
	string CastValueToTarget(const Value &val, const LogicalType &type) override;
	string CastStatsToTarget(const string &stats, const LogicalType &type) override;
	string GenerateConstantFilter(const ConstantFilter &constant_filter, const LogicalType &type,
	                              unordered_set<string> &referenced_stats) override;

private:
	unique_ptr<QueryResult> ExecuteQuery(DuckLakeSnapshot snapshot, string &query, string command);
	unique_ptr<QueryResult> ExecuteQuery(string &query, string command);
	string GetPostgresIndexStatements();
	string GetPostgresStatsType(const LogicalType &type);
	bool CanCastStatsForValueComparison(const LogicalType &type);
	bool IsPostgresTemporalStatsType(const LogicalType &type);
	bool CanCastTemporalValueForValueComparison(const Value &val, const LogicalType &type);
};

} // namespace duckdb
