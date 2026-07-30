#include "functions/ducklake_table_functions.hpp"

#include "common/ducklake_commit_stats.hpp"

namespace duckdb {

struct DuckLakeCommitStatsState : public GlobalTableFunctionState {
	DuckLakeCommitStatsState() : offset(0) {
	}

	vector<DuckLakeCommitStatsEntry> entries;
	idx_t offset;
};

static unique_ptr<FunctionData> DuckLakeCommitStatsBind(ClientContext &, TableFunctionBindInput &,
                                                        vector<LogicalType> &return_types, vector<string> &names) {
	names.emplace_back("catalog");
	return_types.emplace_back(LogicalType::VARCHAR);

	names.emplace_back("stat");
	return_types.emplace_back(LogicalType::VARCHAR);

	names.emplace_back("value");
	return_types.emplace_back(LogicalType::BIGINT);

	return make_uniq<TableFunctionData>();
}

static unique_ptr<GlobalTableFunctionState> DuckLakeCommitStatsInit(ClientContext &, TableFunctionInitInput &) {
	auto result = make_uniq<DuckLakeCommitStatsState>();
	result->entries = DuckLakeCommitStatsRegistry::Get().Snapshot();
	return std::move(result);
}

static void DuckLakeCommitStatsExecute(ClientContext &, TableFunctionInput &data_p, DataChunk &output) {
	auto &state = data_p.global_state->Cast<DuckLakeCommitStatsState>();

	idx_t count = 0;
	while (state.offset < state.entries.size() && count < STANDARD_VECTOR_SIZE) {
		auto &entry = state.entries[state.offset++];
		output.SetValue(0, count, Value(entry.catalog));
		output.SetValue(1, count, Value(entry.stat));
		output.SetValue(2, count, Value::BIGINT(entry.value));
		count++;
	}
	output.SetCardinality(count);
}

DuckLakeCommitStatsFunction::DuckLakeCommitStatsFunction()
    : TableFunction("ducklake_commit_stats", {}, DuckLakeCommitStatsExecute, DuckLakeCommitStatsBind,
                    DuckLakeCommitStatsInit) {
}

} // namespace duckdb
