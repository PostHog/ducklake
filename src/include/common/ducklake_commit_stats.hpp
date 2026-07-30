//===----------------------------------------------------------------------===//
//                         DuckDB
//
// common/ducklake_commit_stats.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/map.hpp"
#include "duckdb/common/mutex.hpp"
#include "duckdb/common/vector.hpp"

#include <atomic>

namespace duckdb {

//! Buckets for commit-conflict errors, matching the substring checks in DuckLakeTransaction::RetryOnError.
enum class DuckLakeCommitConflictCause : uint8_t { NONE, PRIMARY_KEY, UNIQUE, CONFLICT, CONCURRENT };

//! Cumulative, monotonic per-process commit-loop counters for a single catalog.
struct DuckLakeCatalogCommitStats {
	//! One per commit-loop iteration.
	std::atomic<int64_t> attempts {0};
	//! Commit loop finished successfully.
	std::atomic<int64_t> successes {0};
	//! Commit loop gave up after the maximum retry count on a retryable error.
	std::atomic<int64_t> retries_exhausted {0};
	//! Errors for which RetryOnError returned false.
	std::atomic<int64_t> nonretryable_errors {0};
	//! Total milliseconds slept in retry backoff.
	std::atomic<int64_t> backoff_ms {0};
	//! Total wall-clock milliseconds spent inside the commit retry loop (successful or not).
	std::atomic<int64_t> total_commit_ms {0};
	//! Conflicts by cause (see DuckLakeCommitConflictCause).
	std::atomic<int64_t> conflicts_primary_key {0};
	std::atomic<int64_t> conflicts_unique {0};
	std::atomic<int64_t> conflicts_conflict {0};
	std::atomic<int64_t> conflicts_concurrent {0};

	void RecordConflict(DuckLakeCommitConflictCause cause);
};

//! A (catalog, stat, value) row snapshotted from the registry.
struct DuckLakeCommitStatsEntry {
	string catalog;
	string stat;
	int64_t value;
};

//! Process-global registry of per-catalog commit-loop statistics.
class DuckLakeCommitStatsRegistry {
public:
	//! The process-global singleton.
	static DuckLakeCommitStatsRegistry &Get();

	//! Counters for the given catalog label, created on first use. The returned reference lives for the
	//! lifetime of the process.
	DuckLakeCatalogCommitStats &GetStats(const string &catalog);

	//! A consistent snapshot of all counters as (catalog, stat, value) rows.
	vector<DuckLakeCommitStatsEntry> Snapshot();

	//! Classify a commit error message into a conflict-cause bucket, using the same substring checks (in the
	//! same order) as DuckLakeTransaction::RetryOnError. Returns NONE for non-retryable errors.
	static DuckLakeCommitConflictCause ClassifyCommitError(const string &message);
	//! Whether a classified cause is retryable - NONE is the only non-retryable classification.
	static bool IsRetryableCause(DuckLakeCommitConflictCause cause) {
		return cause != DuckLakeCommitConflictCause::NONE;
	}

private:
	mutex lock;
	//! unique_ptr values: the atomics make the struct non-movable, and handed-out references must stay stable.
	map<string, unique_ptr<DuckLakeCatalogCommitStats>> stats_map;
};

} // namespace duckdb
