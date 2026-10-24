# CockroachDB as a DuckLake Metadata Store — Support Report

**Date:** 2026-06-12
**Tested:** DuckDB v1.5.3 (Variegata) + ducklake extension (PostHog fork; patched-build verification on the `v1.5-variegata` branch, DuckDB v1.5.2) against CockroachDB v25.2.19, single node via docker compose (`experiments/crdb/docker-compose.yml`). Postgres 17 used as a control baseline.

## TL;DR

CockroachDB **mostly works** as a DuckLake metadata store today, but is **not production-ready without two fixes**:

1. **Works out of the box: nothing.** The very first attach fails, because DuckDB's postgres scanner defaults to binary `COPY`, which CockroachDB has not implemented. Two global settings fix this (see Workarounds).
2. **With workarounds applied:** the entire core feature surface works — DDL, DML, snapshots, time travel, schema evolution, transactions, views, partitioning, compaction, data inlining, change feeds, multi-process catalogs.
3. **Two real blockers remain:**
   - `ducklake_expire_snapshots()` and `ducklake_flush_inlined_data()` fail (ctid-based DELETE path) — **no workaround**; snapshot/inlined-data cleanup is impossible.
   - Concurrent writers lose ~25% of commits as hard failures (CockroachDB's `40001 restart transaction` errors are not recognized as retryable by DuckLake). **A one-line DuckLake patch fixes this** (verified — see Concurrency).

Verdict: **experimental / near-supportable**. The gaps are small, well-understood, and fixable upstream — none are architectural.

## Architecture context

DuckLake talks to a Postgres-class catalog two ways (this matters because the two paths fail differently):

1. **Raw SQL passthrough** — `PostgresMetadataManager` wraps every metadata query in `CALL postgres_query(...)` / `postgres_execute(...)` and the text is executed verbatim by the server (`src/metadata_manager/postgres_metadata_manager.cpp:112`). This is ~all metadata reads and writes.
2. **DuckDB-planned DML on the attached catalog** — a few maintenance operations (notably `DeleteSnapshots`, `src/storage/ducklake_metadata_manager.cpp:4329`) run `DELETE FROM {METADATA_CATALOG}.tbl` through `transaction.Query(...)`, so DuckDB plans the DELETE and the postgres scanner executes it via **ctid row addressing**.

CockroachDB handles path 1 fine (its SQL dialect covered everything DuckLake sends, including `STRING_AGG`, `NULLS FIRST/LAST`, `::UUID` casts, `CREATE TABLE IF NOT EXISTS`, multi-statement batches). Path 2 is where it breaks.

## Test results

28/28 `ducklake_*` metadata tables created cleanly on first attach. Feature matrix (each test in its own process — also exercises re-attach):

| Feature | Result |
|---|---|
| ATTACH / catalog bootstrap (all DDL + migrations) | ✅ (with COPY workaround) |
| CREATE TABLE / INSERT / SELECT | ✅ |
| UPDATE / DELETE (incl. deletion vectors) | ✅ |
| `snapshots()`, time travel `AT (VERSION => n)` | ✅ |
| Schema evolution (add/drop/rename column, type promotion) | ✅ |
| Multi-schema, views | ✅ |
| Multi-statement transactions, rollback | ✅ |
| 20-type stress (incl. HUGEINT, UUID, INTERVAL, STRUCT, MAP, lists) | ✅ |
| Data inlining (write + read back from CRDB tables) | ✅ |
| Partitioning (`SET PARTITIONED BY`) | ✅ |
| Compaction `merge_adjacent_files()` | ✅ |
| `rewrite_data_files()` | ✅ |
| `cleanup_old_files()` / `delete_orphaned_files()` | ✅ |
| `table_changes()` change feed | ✅ |
| `ducklake_table_info()` / `ducklake_list_files()` | ✅ |
| Concurrent readers during writes (15 readers vs writer loop) | ✅ 0 failures |
| **`ducklake_expire_snapshots()`** | ❌ ctid |
| **`ducklake_flush_inlined_data()`** | ❌ ctid |
| **Concurrent writers (3×15 commits)** | ❌ 24% hard-fail (fixed by patch below) |

### Blocker 1: binary COPY (workaround exists)

First attach fails with:

```
COPY (...) TO STDOUT (FORMAT "binary"): ERROR: at or near "binary":
syntax error: unimplemented  (CockroachDB issue #96590)
```

The postgres scanner reads via binary COPY and parallelizes scans by `ctid`; CockroachDB implements neither. Both have scanner-level escape hatches, but they must be set **GLOBAL** — DuckLake issues metadata queries on its own internal connection, which doesn't see session-local `SET`:

```sql
LOAD postgres_scanner;
SET GLOBAL pg_use_text_protocol = true;  -- slower, but compatible
SET GLOBAL pg_use_ctid_scan = false;
ATTACH 'ducklake:postgres:dbname=ducklakedb host=... port=26257 user=root' AS lake (DATA_PATH '...');
```

### Blocker 2: ctid-based metadata DELETEs (no workaround)

`ducklake_expire_snapshots()` and `ducklake_flush_inlined_data()` fail:

```
Failed to delete snapshots in DuckLake: Failed to execute query
"SELECT "snapshot_id", ctid FROM "public"."ducklake_snapshot" WHERE ...":
ERROR: column "ctid" does not exist
```

These two operations route DELETEs through DuckDB's DML planner (path 2 above) instead of raw passthrough; the scanner identifies rows to delete by `ctid`, which CockroachDB does not expose. `pg_use_ctid_scan=false` does not affect the DML path.

**Consequence:** snapshots and flushed inlined data can never be expired — the catalog grows forever. For an append-heavy production lake this rules CockroachDB out until fixed.

**Fix difficulty: low.** `DeleteSnapshots` already builds plain `DELETE FROM ... WHERE snapshot_id IN (...)` strings (`src/storage/ducklake_metadata_manager.cpp:4341`); routing them through `PostgresMetadataManager::Execute()` (raw `postgres_execute`, like every other metadata write) instead of `transaction.Query()` would fix both functions on CockroachDB and would also be a minor efficiency win on vanilla Postgres (single server-side DELETE instead of scan-ctids-then-delete).

### Blocker 3: concurrent-writer commits hard-fail (one-line fix, verified)

3 writers × 15 sequential single-row commits each:

| Configuration | Failed commits |
|---|---|
| CRDB default (serializable), unpatched DuckLake | 11/45 (24%), **rows lost** |
| CRDB `read committed` cluster default | no change (scanner's explicit `BEGIN ... REPEATABLE READ` overrides it) |
| CRDB `repeatable_read_isolation.enabled` | no change |
| **CRDB default + 1-line DuckLake patch** | **0/45 — all rows landed, retries absorbed every collision** |

Root cause: on commit contention, Postgres surfaces a duplicate-key error on `ducklake_snapshot`'s primary key — DuckLake's `RetryOnError` (`src/storage/ducklake_transaction.cpp:2545`) matches the substring "unique" and retries with backoff. CockroachDB instead aborts the transaction with:

```
ERROR: restart transaction: TransactionRetryWithProtoRefreshError: WriteTooOldError ...
```

(SQLSTATE 40001). No substring matches DuckLake's retry list ("primary key", "unique", "conflict", "concurrent"), so the commit fails permanently and the write is lost. CRDB-side isolation tuning can't help because the postgres scanner hardcodes `BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ` per transaction.

The fix:

```cpp
// in RetryOnError(), src/storage/ducklake_transaction.cpp
// retry on serialization failures (SQLSTATE 40001), e.g. CockroachDB's
// "restart transaction: TransactionRetryWithProtoRefreshError"
if (StringUtil::Contains(message, "restart transaction")) {
    return true;
}
```

(A more principled version would match on SQLSTATE 40001 rather than message text — Postgres serialization failures say "could not serialize access", which is *also* unmatched today, so this gap technically exists for vanilla PG under repeatable read too.)

Verified end to end: rebuilt the extension with this patch (worktree on `v1.5-variegata`) and reran the 3×15 concurrency test against default-config CockroachDB — **0/45 failures, every commit retried to success**.

## Performance (directional only)

10 sequential attach+commit cycles, single-node CRDB vs Postgres 17, same host:

- CockroachDB: ~0.51 s/cycle
- Postgres 17: ~0.27 s/cycle

~2× slower per metadata commit, expected for a consensus-based engine even single-node. Reads of data files (parquet) are unaffected; only catalog round-trips pay the cost. The text-protocol fallback adds overhead on large metadata scans, but DuckLake metadata result sets are small.

## What was NOT tested

- Multi-node CockroachDB cluster (single node only; multi-node adds latency and more 40001 retries — the patch above becomes more important, not less)
- The full DuckLake SQL test suite (`test/configs/postgres.json`) against CRDB
- High-volume snapshot accumulation / catalog growth behavior
- CockroachDB serverless / cloud offerings
- DuckLake catalog migration between versions on CRDB

## Recommendations

To make CockroachDB a supported metadata store:

1. **Ship the `RetryOnError` patch** (1 line; ideally match SQLSTATE 40001 / "could not serialize access" too, which also benefits vanilla Postgres).
2. **Route `DeleteSnapshots` / inlined-data-flush DELETEs through raw `postgres_execute`** instead of DuckDB-planned DML (removes the ctid dependency; also fewer round trips on Postgres).
3. **Auto-set text protocol** when the server is detected as CockroachDB (`SELECT version()` starts with "CockroachDB"), or document the two `SET GLOBAL`s.
4. Optionally: add a CRDB job to CI mirroring the Postgres job (`cockroachdb/cockroach` single-node image, same test config + the two settings).

With (1) and (2) upstreamed, CockroachDB support would be on par with Postgres for correctness, at ~2× metadata-commit latency.

## Repro artifacts

- `experiments/crdb/docker-compose.yml` — CRDB single node
- `experiments/crdb/run_tests.sh`, `run_tests2.sh` — feature matrix
- `experiments/crdb/concurrency_test.sh` — 3-writer concurrency test
- `experiments/crdb/results/` — raw outputs
- Patched build worktree: `/tmp/ducklake-crdb-patch` (branch `v1.5-variegata` + RetryOnError patch)
