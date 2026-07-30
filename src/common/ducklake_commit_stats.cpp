#include "common/ducklake_commit_stats.hpp"

#include "duckdb/common/string_util.hpp"

namespace duckdb {

void DuckLakeCatalogCommitStats::RecordConflict(DuckLakeCommitConflictCause cause) {
	switch (cause) {
	case DuckLakeCommitConflictCause::PRIMARY_KEY:
		conflicts_primary_key++;
		break;
	case DuckLakeCommitConflictCause::UNIQUE:
		conflicts_unique++;
		break;
	case DuckLakeCommitConflictCause::CONFLICT:
		conflicts_conflict++;
		break;
	case DuckLakeCommitConflictCause::CONCURRENT:
		conflicts_concurrent++;
		break;
	default:
		break;
	}
}

DuckLakeCommitStatsRegistry &DuckLakeCommitStatsRegistry::Get() {
	static DuckLakeCommitStatsRegistry instance;
	return instance;
}

DuckLakeCatalogCommitStats &DuckLakeCommitStatsRegistry::GetStats(const string &catalog) {
	lock_guard<mutex> guard(lock);
	auto entry = stats_map.find(catalog);
	if (entry == stats_map.end()) {
		entry = stats_map.emplace(catalog, make_uniq<DuckLakeCatalogCommitStats>()).first;
	}
	return *entry->second;
}

vector<DuckLakeCommitStatsEntry> DuckLakeCommitStatsRegistry::Snapshot() {
	lock_guard<mutex> guard(lock);
	vector<DuckLakeCommitStatsEntry> result;
	for (auto &entry : stats_map) {
		auto &catalog = entry.first;
		auto &stats = *entry.second;
		result.push_back({catalog, "attempts", stats.attempts.load()});
		result.push_back({catalog, "successes", stats.successes.load()});
		result.push_back({catalog, "retries_exhausted", stats.retries_exhausted.load()});
		result.push_back({catalog, "nonretryable_errors", stats.nonretryable_errors.load()});
		result.push_back({catalog, "backoff_ms", stats.backoff_ms.load()});
		result.push_back({catalog, "total_commit_ms", stats.total_commit_ms.load()});
		result.push_back({catalog, "conflicts.primary_key", stats.conflicts_primary_key.load()});
		result.push_back({catalog, "conflicts.unique", stats.conflicts_unique.load()});
		result.push_back({catalog, "conflicts.conflict", stats.conflicts_conflict.load()});
		result.push_back({catalog, "conflicts.concurrent", stats.conflicts_concurrent.load()});
	}
	return result;
}

DuckLakeCommitConflictCause DuckLakeCommitStatsRegistry::ClassifyCommitError(const string &original_message) {
	auto message = StringUtil::Lower(original_message);
	// the buckets and their order mirror the retry checks in DuckLakeTransaction::RetryOnError
	if (StringUtil::Contains(message, "primary key")) {
		return DuckLakeCommitConflictCause::PRIMARY_KEY;
	}
	if (StringUtil::Contains(message, "unique")) {
		return DuckLakeCommitConflictCause::UNIQUE;
	}
	if (StringUtil::Contains(message, "conflict")) {
		return DuckLakeCommitConflictCause::CONFLICT;
	}
	if (StringUtil::Contains(message, "concurrent")) {
		return DuckLakeCommitConflictCause::CONCURRENT;
	}
	return DuckLakeCommitConflictCause::NONE;
}

} // namespace duckdb
