//===----------------------------------------------------------------------===//
//                         DuckDB
//
// metadata_manager/quack_metadata_manager.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "storage/ducklake_metadata_manager.hpp"

namespace duckdb {

class QuackMetadataManager : public DuckLakeMetadataManager {
public:
	explicit QuackMetadataManager(DuckLakeTransaction &transaction);

	static unique_ptr<DuckLakeMetadataManager> Create(DuckLakeTransaction &transaction) {
		return make_uniq<QuackMetadataManager>(transaction);
	}

	bool SupportsAppender() const override {
		return false;
	}
	unique_ptr<QueryResult> Execute(DuckLakeSnapshot snapshot, string &query) override;
	unique_ptr<QueryResult> Execute(string &query) override;
	unique_ptr<QueryResult> SnapshotQuery(DuckLakeSnapshot snapshot, string &query) override;
	unique_ptr<QueryResult> CurrentQuery(DuckLakeSnapshot snapshot, string &query) override;
	unique_ptr<QueryResult> CurrentQuery(string &query) override;
	unique_ptr<QueryResult> AttachMetadata(const string &attach_query) override;
	void ClearCache() override;

	bool MetadataExists() override;

protected:
	string MetadataExistsQuery() const override;
};

} // namespace duckdb
