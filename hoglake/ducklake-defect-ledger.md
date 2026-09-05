# DuckLake defect ledger — and where hoglake corrects each

Moved from the local working notes (2026-09-04). These were drafted as
upstream tickets against `duckdb/ducklake`; after the hard fork they are
simply our bugs, and this ledger is part of the hoglake case file.
Each entry ends with a **Hoglake correction** stating how the design
eliminates the defect (or its whole class). File:line pointers are into
the fork source (`src/` in this repo).

---

## 1. Inline-table GC is unreachable for any `table_id` with a single `schema_version`

**Symptom**: On a long-running catalog, `ducklake_inlined_data_*`
tables accumulate without bound. Observed 112,947 such tables on one
production catalog (postgres metadata backend); 112,122 of them (~99%)
are the latest `schema_version` for their `table_id`. Most are empty.

**Root cause**:
`DuckLakeTransactionState::DropEmptySupersededInlinedTables`
([`src/storage/ducklake_transaction_state.cpp:1381-1441`](https://github.com/PostHog/hoglake/blob/eee193b7cb18fc4954df4664c3468d75f2d26ceb/src/storage/ducklake_transaction_state.cpp#L1381-L1441)) and the
equivalent in the metadata manager
([`src/storage/ducklake_metadata_manager.cpp:5074-5124`](https://github.com/PostHog/hoglake/blob/eee193b7cb18fc4954df4664c3468d75f2d26ceb/src/storage/ducklake_metadata_manager.cpp#L5074-L5124)) both select
candidates with:

```sql
WHERE idt.schema_version < (
    SELECT MAX(idt2.schema_version)
    FROM ducklake_inlined_data_tables idt2
    WHERE idt2.table_id = idt.table_id
)
```

When a `table_id` only ever has one `schema_version`, `MAX = self`, so
the predicate is `version < version` ⇒ always false. The empty inline
table is never a candidate, regardless of how many post-commit GC
cycles run. This is also the only GC site for inline tables.

**Observed impact**: every commit walks a 112K-row registry and can
only ever drop the 825 superseded entries; the 111,694 single-version
inline tables are structurally invisible.

**→ Hoglake correction**: inlining is **dropped entirely**
([README.md](README.md) Decisions, 2026-09-04). No dynamic per-schema-version tables exist, so
neither the GC bug nor the registry walk is representable. Migration
flushes residual inlined rows to parquet once, at cutover.

---

## 2. Commit path loads stats for the ENTIRE catalog — O(tables × columns) per attempt

**Symptom**: On a catalog with many tables, every commit takes multiple
seconds of pure metadata I/O before it can even check for conflicts,
re-paid on every internal conflict retry. Observed at 59,004 tables /
4.55M columns: the snapshot+stats load runs 5-7s wall per call (2.6s
server-side + binary COPY of 3.7M rows to the client), up to 7
concurrent instances. A writer inserting into a single table saw
190-264s commits under concurrent load (peer commits every ~13s kept
invalidating the snapshot; each retry re-paid the full load).

**Root cause**:
`DuckLakeMetadataManager::GetSnapshotAndStatsAndChanges`
([`src/storage/ducklake_metadata_manager.cpp:3747`](https://github.com/PostHog/hoglake/blob/eee193b7cb18fc4954df4664c3468d75f2d26ceb/src/storage/ducklake_metadata_manager.cpp#L3747)) UNIONs the
latest-snapshot row with the full `ducklake_table_stats ⟕
ducklake_table_column_stats` join — **no `table_id` filter**. The
per-table version of the same query exists (`GetGlobalTableStats`,
`:934`) but the commit path doesn't scope to the write set.

**Mitigations measured**: btree indexes don't help (ORDER BY NULLS
FIRST mismatch); `work_mem=512MB` halves server time at best. The real
fix is the write-set filter.

**→ Hoglake correction**: commit is a server-side operation scoped to
the write set by construction ([README.md](README.md), commit
protocol). A
catalog-global load on the commit path is not an optimization target —
it is structurally impossible to write, and commit admission (§7 of the
experience record) bounds what concurrent load can do to latency.

---

## 3. Schema cache memory estimate counts tables, not columns — LRU never evicts, unbounded RSS

**Symptom**: Long-lived writer RSS grows without bound (~12.6GiB/h
observed) while `duckdb_memory()` reports ~0; confirmed OOM loop on a
48GiB container. Growth tracks the schema-version churn of attached
catalogs, not the write rate.

**Root cause**: `DuckLakeCatalog::GetSchemaCacheEntry`
([`src/storage/ducklake_catalog.cpp:220`](https://github.com/PostHog/hoglake/blob/eee193b7cb18fc4954df4664c3468d75f2d26ceb/src/storage/ducklake_catalog.cpp#L220)) caches one full catalog set
per `schema_version` in DuckDB's ObjectCache (8GiB hardcoded cap).
Eviction is driven by `GetEstimatedCacheMemory()`, which counts **table
entries, not columns** (`:42-44`) — at 728 tables / 85K columns the
estimate is ~3MB vs ~40MB real, 10-15× under, so the LRU never evicts
before the container OOMs. Trigger math: 315 new schema versions/hour
(tenant DDL) × ~40MB pinned per version = 12.6GiB/h — matches the
observed slope exactly.

**→ Hoglake correction**: the class dies with the client-embedded
engine. There is no per-connection catalog cache in every writer;
schema state lives once, in the service, sized and bounded
deliberately. (The related per-connection native-memory leak — freed
only on DETACH — that forced viaduck's connection-recycling
workarounds dies with it.)

---

## 4. Stats rows for dropped tables are never cleaned until snapshot expiry — commit cost grows with DDL history

**Symptom**: On a catalog with heavy table churn (report tables using a
create/drop replace pattern, ~24 tables/hour), stats tables accumulate
rows for dropped tables without bound. Observed: 99.4% of 55,010
table_stats rows and 99.3% of ~3.71M column_stats rows belonged to
dropped tables — and defect #2 made every writer pay for them on every
commit attempt.

**Root cause**: `DROP TABLE` only sets `end_snapshot` on the
`ducklake_table` row ([`src/storage/ducklake_metadata_manager.cpp:2232`](https://github.com/PostHog/hoglake/blob/eee193b7cb18fc4954df4664c3468d75f2d26ceb/src/storage/ducklake_metadata_manager.cpp#L2232));
stats rows are deleted only by `DeleteSnapshots` (`:4742`), i.e. at
snapshot-expiry time. Where expiry lags DDL churn, orphans persist
indefinitely; global stats are unversioned and unreadable for
non-live tables — pure dead weight.

**Production fix that shipped (2026-07-03)**: backup-scoped orphan
purge — stats join 3.71M → 56K rows (66×); writer flush durations
60-264s → 3.8-6.4s (~30-50×). Made recurring (NOT EXISTS anti-join
form; never NOT IN — NULL-key semantics) because orphans regrow at
~29K rows/14h on the churn pattern.

**→ Hoglake correction**: stats rows are deleted **in the DROP TABLE
commit itself** (nothing can read them after the drop), and the schema
carries real FKs so the cascade is declarative, not hand-rolled. The
recurring purge cron and its metric disappear; any orphan count &gt; 0 is
exported as an invariant violation (orphan count over zero pages),
not run as a recurring workload.

---

## 5. `TransformGlobalStatsRow` crashes (INTERNAL Error) on NULL `column_id` / `next_row_id`

**Symptom**: `InternalException: Calling GetValueInternal on a value
that is NULL`, raised during query *planning* (join-order optimizer via
`GetCardinality`). Killed two consecutive compaction runs mid-merge on
a high-DDL-churn catalog, then stopped reproducing — transient row
shape, most likely from a concurrent multi-minute DDL transaction. An
InternalException invalidates the whole DuckDB instance.

**Root cause**: `TransformGlobalStatsRow`
([`src/storage/ducklake_metadata_manager.cpp:862`](https://github.com/PostHog/hoglake/blob/eee193b7cb18fc4954df4664c3468d75f2d26ceb/src/storage/ducklake_metadata_manager.cpp#L862)) reads `column_id` and
the `record_count`/`next_row_id`/`table_size_bytes` trio via `GetValue`
with no IsNull guard (only the column-stat fields at positions 5+ are
guarded). A table_stats row with zero column_stats rows LEFT JOINs to
NULL `column_id`; the stats query's WHERE never covers `next_row_id`.
Either shape hard-crashes whatever query is being planned.

**Operational aggravator**: two engines share one catalog — the
maintenance image runs the upstream extension while writers run the
fork, so fork fixes never reach the maintenance path.

**→ Hoglake correction**: three layers. The schema declares NOT NULL
where the code assumes it, so the row shape is unrepresentable; the
service validates its own metadata reads (typed, no unguarded
GetValue); and there is exactly one implementation — the
two-engines-one-catalog drift that let the fixed and unfixed code
share a catalog cannot happen.

---

## 6. Expire-snapshots reimplementation drift: residual leak classes vs the built-in cascade

**Context (2026-07-10)**: a Postgres-native port of expire-snapshots
(built because the built-in is too slow at scale — see the expiry
entries in the API map) went through adversarial review; three
loss-class defects were found and fixed (missing head-snapshot guard
that bricked idle catalogs; a time-based bridging predicate that could
queue live-referenced files for S3 deletion; non-atomic per-batch
cascade). Two LEAK classes remained versus the built-in cascade:

1. **Batch-scoped dead-file scan** vs the built-in's global unbridged
   scan: under snapshot time/id inversion (ids follow commit order,
   times are txn-start, and long-held transactions invert them), a
   file's end_snapshot row can be deleted while a bridge survives —
   when the bridge later expires the file is never re-examined →
   permanent catalog-row + S3 leak.
2. **Omitted cascades**: dropped-table cleanup across the ~12 lifecycle
   tables, dropped tables' live-marked data files, unbridged
   schema/view/tag/macro rows (fork cascade at
   [`src/storage/ducklake_metadata_manager.cpp:4590-4799`](https://github.com/PostHog/hoglake/blob/eee193b7cb18fc4954df4664c3468d75f2d26ceb/src/storage/ducklake_metadata_manager.cpp#L4590-L4799)). The port
   never deletes a dropped table's live-marked files → S3 + catalog
   leak; the 54K-dropped-tables incident shows the class accumulates.

Also noted for the record: cleanup (`GetOldFilesForCleanup`, `:4309`)
trusts the deletion queue absolutely — age filter only, no liveness
join — so a bad queue entry deletes a live file.

**→ Hoglake correction**: this defect is really "two implementations
of one cascade drifted." The service owns exactly one expiry
implementation with the full cascade; FKs make most of the cascade
declarative; expiry is incremental/continuous (retention as catalog
policy) so the scale pressure that motivated a parallel port never
builds; and physical deletion is soft (bucket versioning + delayed
permanent delete) behind a mandatory liveness check, so even a wrong
queue entry is recoverable.
