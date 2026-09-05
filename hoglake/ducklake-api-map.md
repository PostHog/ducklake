# DuckLake extension — SQL-facing API surface map

Survey of the PostHog fork (`~/src/hoglake`, merged with upstream main @
2026-08-26) for the hoglake control-plane design. Companion to
[pyducklake-api-map.md](pyducklake-api-map.md) and [README.md](README.md). Paths are repo-relative.

Registration entry point: [`src/ducklake_extension.cpp:20`](https://github.com/PostHog/hoglake/blob/eee193b7cb18fc4954df4664c3468d75f2d26ceb/src/ducklake_extension.cpp#L20)
(`LoadInternal`). Everything a SQL client can reach is one of: (a) a
globally-registered table/scalar function, (b) a per-catalog table macro
auto-materialized inside an attached DuckLake schema, (c) `ATTACH`
options / secret parameters, (d) global `SET` extension options, or (e)
DDL/DML routed through `DuckLakeCatalog`.

---

## 1. Registered functions

### 1.1 Global table functions

All "metadata" functions take the **attached DuckLake catalog name as a
VARCHAR first argument** ([`src/functions/base_metadata_function.cpp:8`](https://github.com/PostHog/hoglake/blob/eee193b7cb18fc4954df4664c3468d75f2d26ceb/src/functions/base_metadata_function.cpp#L8) —
errors if the name is not an attached catalog whose
`GetCatalogType() == "ducklake"`).

| Function | Positional args | Named params | Returns | Description | Registered at | Issues/defects we've hit |
|---|---|---|---|---|---|---|
| `ducklake_snapshots` | `catalog VARCHAR` | — | `snapshot_id BIGINT, snapshot_time TIMESTAMPTZ, schema_version BIGINT, changes MAP(VARCHAR, VARCHAR[]), author VARCHAR, commit_message VARCHAR, commit_extra_info VARCHAR` | Lists every snapshot. `changes` maps change-kind → affected ids/names (`schemas_created`, `tables_dropped`, `tables_altered`, `tables_inserted_into`, `tables_deleted_from`, `views_*`, `scalar_macros_*`, `table_macros_*`, `inlined_insert`, `inlined_delete`, `flushed_inlined`, `merge_adjacent`, `rewrite_delete`). | [`src/functions/ducklake_snapshots.cpp:172`](https://github.com/PostHog/hoglake/blob/eee193b7cb18fc4954df4664c3468d75f2d26ceb/src/functions/ducklake_snapshots.cpp#L172); columns `:38-58`; change-map `:70-152` | No filter/pagination — full catalog list. At megaduck scale (15.3M snapshots) clients OOM; viaduck bypasses with raw `MIN/MAX(snapshot_id)` SQL against the metadata schema. |
| `ducklake_table_info` | `catalog VARCHAR` | — | `table_name, schema_id, table_id, table_uuid UUID, file_count, file_size_bytes, delete_file_count, delete_file_size_bytes` | Per-table file/byte counts at the current snapshot. | [`src/functions/ducklake_table_info.cpp:55`](https://github.com/PostHog/hoglake/blob/eee193b7cb18fc4954df4664c3468d75f2d26ceb/src/functions/ducklake_table_info.cpp#L55); columns `:29-51` | — |
| `ducklake_table_insertions` | `catalog, schema, table VARCHAR, start, end` (both `BIGINT` versions or both `TIMESTAMPTZ`) | — | table columns + virtual `snapshot_id`, `rowid` | CDC: rows inserted between two snapshots. Overload set of 2. | [`src/functions/ducklake_table_insertions.cpp:91`](https://github.com/PostHog/hoglake/blob/eee193b7cb18fc4954df4664c3468d75f2d26ceb/src/functions/ducklake_table_insertions.cpp#L91) | Read cost scales with the snapshot *span*, not the rows returned; fork changefeed scans read the entire firehose with no destination pruning (~2× macro amplification, full-hose experiment 2026-07). |
| `ducklake_table_deletions` | same | — | same shape | Rows deleted between two snapshots. | [`src/functions/ducklake_table_insertions.cpp:101`](https://github.com/PostHog/hoglake/blob/eee193b7cb18fc4954df4664c3468d75f2d26ceb/src/functions/ducklake_table_insertions.cpp#L101) | Same profile as insertions. |
| `ducklake_table_changes` | `catalog, schema_name, table_name, start_snapshot, end_snapshot` | — | `snapshot_id, rowid, change_type, <table columns>` | **Table macro, not a C++ function** — SQL-defined join of insertions/deletions producing `change_type ∈ {insert, delete, update_preimage, update_postimage}`. | [`src/functions/ducklake_table_changes.cpp:8-24`](https://github.com/PostHog/hoglake/blob/eee193b7cb18fc4954df4664c3468d75f2d26ceb/src/functions/ducklake_table_changes.cpp#L8-L24), registered [`src/ducklake_extension.cpp:92`](https://github.com/PostHog/hoglake/blob/eee193b7cb18fc4954df4664c3468d75f2d26ceb/src/ducklake_extension.cpp#L92) | Deterministically stalls the DuckDB executor on a large fast-advancing catalog (UNION ALL of two LEFT JOINs; no runnable tasks, never completes; repros at threads=1) — viaduck runs the branches separately (`VIADUCK_CDC_SPLIT_READ`). Also the surface where rowid reuse on upsert-recreate corrupts insert/delete pairing downstream. |
| `ducklake_merge_adjacent_files` | `(catalog)` or `(catalog, table)` | `min_file_size UBIGINT`, `max_file_size UBIGINT`, `max_compacted_files UBIGINT`, `newer_than TIMESTAMPTZ`, `schema VARCHAR` (2-arg form) | `schema_name, table_name, files_processed, files_created` | Compacts adjacent small data files. Honors per-schema/table `auto_compact`. | [`src/functions/ducklake_compaction_functions.cpp:948`](https://github.com/PostHog/hoglake/blob/eee193b7cb18fc4954df4664c3468d75f2d26ceb/src/functions/ducklake_compaction_functions.cpp#L948); names `:939-946` | Sorted compaction silently **remaps rowids** (positional reassignment; empirically confirmed) — contradicts the lineage doc. Zero-output pathology: singletons + row-id non-contiguity → 0 groups despite 15.8K candidates. Adjacency is row-id order, not key range; large unsorted files are never re-sorted. Commit phase deletes via giant IN-lists → 60–180s catalog commits (the 2026-09-04 writer convoy). No advisory lock vs concurrent maintenance. |
| `ducklake_rewrite_data_files` | `(catalog)` or `(catalog, table)` | `delete_threshold DOUBLE` (0–1), `max_compacted_files UBIGINT`, `schema VARCHAR` | same shape | Rewrites files whose deleted-row fraction exceeds the threshold, dropping delete files. | [`src/functions/ducklake_compaction_functions.cpp:975`](https://github.com/PostHog/hoglake/blob/eee193b7cb18fc4954df4664c3468d75f2d26ceb/src/functions/ducklake_compaction_functions.cpp#L975) | Always materializes row ids (the safe path) — but shares the compaction commit-cost profile and has no advisory lock. |
| `ducklake_cleanup_old_files` | `catalog VARCHAR` | `older_than TIMESTAMPTZ`, `cleanup_all BOOLEAN`, `dry_run BOOLEAN` | `path VARCHAR` | Physically deletes files no longer referenced by any live snapshot. Requires exactly one of `cleanup_all`/`older_than`, unless catalog option `delete_older_than` set (fallback `'2 days'`). | [`src/functions/ducklake_cleanup_files.cpp:165`](https://github.com/PostHog/hoglake/blob/eee193b7cb18fc4954df4664c3468d75f2d26ceb/src/functions/ducklake_cleanup_files.cpp#L165); validation `:78-84` | All-or-nothing S3 deletion: one 5xx rolls back the catalog DELETE, leaving phantom queue entries (the 9.99M-row phantom queue, 14h recovery). Trusts the deletion queue absolutely — age filter only, no liveness join. No advisory lock (the same incident was three concurrent maintenance contexts). |
| `ducklake_delete_orphaned_files` | `catalog VARCHAR` | same | `path VARCHAR` | Same contract, for files in the data path the catalog never referenced. | [`src/functions/ducklake_cleanup_files.cpp:173`](https://github.com/PostHog/hoglake/blob/eee193b7cb18fc4954df4664c3468d75f2d26ceb/src/functions/ducklake_cleanup_files.cpp#L173) | Orphan scan ships the ~30M-row `ducklake_data_file` table to the client (no NOT EXISTS pushdown) to usually match 0 rows, TOASTed `path` columns included. No advisory lock. |
| `ducklake_expire_snapshots` | `catalog VARCHAR` | `older_than TIMESTAMPTZ`, `versions UBIGINT[]`, `dry_run BOOLEAN` | same columns as `ducklake_snapshots` | Metadata-only expiry (file removal is `cleanup_old_files`). `versions`/`older_than` mutually exclusive (`:124`). Empty `versions`, or no criteria + no `expire_older_than` option → **silent no-op** (`:127-136`). | [`src/functions/ducklake_expire_snapshots.cpp:151`](https://github.com/PostHog/hoglake/blob/eee193b7cb18fc4954df4664c3468d75f2d26ceb/src/functions/ducklake_expire_snapshots.cpp#L151) | ~14ms/snapshot regardless of chunk size (~50h against 15.3M snapshots). IN-list deletes where the `older_than` path always yields a contiguous range. No self-batching — 500K deletes in one transaction; killing it rolls back hours. Global-only: no table-scoped purge, so dropped tables leak files indefinitely (the 54K-dropped-tables case). Dropped-table stats rows are cleaned ONLY here — when expiry lags, orphans accumulate (3.7M rows; 30–50× commit-cost purge win). Silent no-op on empty criteria. No advisory lock. |
| `ducklake_flush_inlined_data` | `catalog VARCHAR` | `schema_name`, `table_name` | `schema_name, table_name, rows_flushed` | Materializes inlined rows into Parquet; also flushes inlined file-deletions. Gated by `auto_compact`. Implemented as a `bind_operator` producing a plan. | [`src/functions/ducklake_flush_inlined_data.cpp:786`](https://github.com/PostHog/hoglake/blob/eee193b7cb18fc4954df4664c3468d75f2d26ceb/src/functions/ducklake_flush_inlined_data.cpp#L786); bind `:608` | Companion GC (`DropEmptySupersededInlinedTables`) is structurally unreachable for any table with a single schema_version (`MAX = self`) — 112,947 inline tables accumulated on one catalog, 99% uncollectable; the registry is walked on every commit. (Moot in hoglake: inlining dropped.) |
| `ducklake_set_option` | `catalog, option VARCHAR, value ANY` | `table_name`, `schema` | `Success BOOLEAN` | Sets a persisted catalog option at global/schema/table scope. §2.2. | [`src/functions/ducklake_set_option.cpp:234`](https://github.com/PostHog/hoglake/blob/eee193b7cb18fc4954df4664c3468d75f2d26ceb/src/functions/ducklake_set_option.cpp#L234); dispatch `:87-172` | — |
| `ducklake_options` | `catalog VARCHAR` | — | `option_name, description, value, scope, scope_entry` | Enumerates options with effective values + scope. | [`src/functions/ducklake_options.cpp:174`](https://github.com/PostHog/hoglake/blob/eee193b7cb18fc4954df4664c3468d75f2d26ceb/src/functions/ducklake_options.cpp#L174); catalog `:15-41` | — |
| `ducklake_settings` | `catalog VARCHAR` | — | `catalog_type, extension_version, data_path` | Resolved metadata backend (normalized), version, data path. | [`src/functions/ducklake_settings.cpp:48`](https://github.com/PostHog/hoglake/blob/eee193b7cb18fc4954df4664c3468d75f2d26ceb/src/functions/ducklake_settings.cpp#L48) | — |
| `ducklake_set_commit_message` | `catalog, author, commit_message VARCHAR` | `extra_info VARCHAR` | `Success BOOLEAN` | Attaches author/message to the pending snapshot. Required when `require_commit_message`. | [`src/functions/ducklake_set_commit_message.cpp:55`](https://github.com/PostHog/hoglake/blob/eee193b7cb18fc4954df4664c3468d75f2d26ceb/src/functions/ducklake_set_commit_message.cpp#L55) | — |
| `ducklake_current_snapshot` | `catalog VARCHAR` | — | `id UBIGINT` | Snapshot the current transaction reads. | [`src/functions/ducklake_current_snapshot.cpp:27`](https://github.com/PostHog/hoglake/blob/eee193b7cb18fc4954df4664c3468d75f2d26ceb/src/functions/ducklake_current_snapshot.cpp#L27) | — |
| `ducklake_last_committed_snapshot` | `catalog VARCHAR` | — | `id UBIGINT` | Snapshot from the most recent successful commit on this handle (NULL if none); cached in-process ([`src/include/storage/ducklake_catalog.hpp:250-269`](https://github.com/PostHog/hoglake/blob/eee193b7cb18fc4954df4664c3468d75f2d26ceb/src/include/storage/ducklake_catalog.hpp#L250-L269)). | [`src/functions/ducklake_last_committed_snapshot.cpp:29`](https://github.com/PostHog/hoglake/blob/eee193b7cb18fc4954df4664c3468d75f2d26ceb/src/functions/ducklake_last_committed_snapshot.cpp#L29) | — |
| `ducklake_list_files` | `catalog, table VARCHAR` | `schema`, `snapshot_version BIGINT`, `snapshot_time TIMESTAMPTZ` | `data_file, data_file_size_bytes, data_file_footer_size, data_file_encryption_key BLOB, delete_file, delete_file_size_bytes, delete_file_footer_size, delete_file_encryption_key BLOB` | Physical files backing a table at a snapshot, incl. encryption keys — the primary hook for external readers. | [`src/functions/ducklake_list_files.cpp:108`](https://github.com/PostHog/hoglake/blob/eee193b7cb18fc4954df4664c3468d75f2d26ceb/src/functions/ducklake_list_files.cpp#L108); columns `:40-62` | — |
| `ducklake_add_data_files` | `catalog, table, files` (`VARCHAR` or `VARCHAR[]` globs) | `allow_missing`, `ignore_extra_columns`, `hive_partitioning`, `schema` | `filename VARCHAR` | Registers pre-existing Parquet files without rewriting: parses footers, derives types/field ids/stats, maps hive partition values. | [`src/functions/ducklake_add_data_files.cpp:1385`](https://github.com/PostHog/hoglake/blob/eee193b7cb18fc4954df4664c3468d75f2d26ceb/src/functions/ducklake_add_data_files.cpp#L1385); bind `:37` | Registered files get NULL `row_id_start` — reading their rowid throws, and they sit outside the lineage guarantee hoglake commits to. |
| `ducklake_commit` | `metadata_schema VARCHAR, schema_version BIGINT` | `max_retry_count BIGINT`, `retry_wait_ms BIGINT`, `retry_backoff DOUBLE` | `committed_snapshot_id, committed_schema_version, had_flushes BOOLEAN` | **Server-side commit**: reads staged commit rows from temp staging tables and performs the whole commit (incl. the OCC retry loop) inside the metadata server. THE key precedent for hoglake. | [`src/functions/ducklake_commit.cpp:70`](https://github.com/PostHog/hoglake/blob/eee193b7cb18fc4954df4664c3468d75f2d26ceb/src/functions/ducklake_commit.cpp#L70); driver [`src/include/storage/ducklake_server_side_commit.hpp:27`](https://github.com/PostHog/hoglake/blob/eee193b7cb18fc4954df4664c3468d75f2d26ceb/src/include/storage/ducklake_server_side_commit.hpp#L27); staging enum [`src/include/storage/ducklake_staged_commit.hpp:36-54`](https://github.com/PostHog/hoglake/blob/eee193b7cb18fc4954df4664c3468d75f2d26ceb/src/include/storage/ducklake_staged_commit.hpp#L36-L54); batch builder `:68-115` | The commit path (client- and server-side) loads stats for the ENTIRE catalog per attempt — 5–7s at 59K tables/3.7M stats rows, re-paid on every OCC retry; observed 190–264s single-table commits. `TransformGlobalStatsRow` reads NULL `column_id`/`next_row_id` unguarded → `InternalException` that invalidates the whole DuckDB instance (killed consecutive compaction runs; viaduck ships a mitigation). Retryability decided by error-string matching. On duckgres, worker connections leak idle-in-transaction, blocking catalog DDL. |
| `ducklake_scan` | inherits `parquet_scan` | inherits | table columns | Registered only so serialized plans deserialize; clones `parquet_scan` with `DuckLakeMultiFileReader` + DuckLake stats/virtual-column callbacks. Not for direct calls. | [`src/storage/ducklake_scan.cpp:217`](https://github.com/PostHog/hoglake/blob/eee193b7cb18fc4954df4664c3468d75f2d26ceb/src/storage/ducklake_scan.cpp#L217); registered [`src/ducklake_extension.cpp:114`](https://github.com/PostHog/hoglake/blob/eee193b7cb18fc4954df4664c3468d75f2d26ceb/src/ducklake_extension.cpp#L114) | — |

### 1.2 Scalar functions

| Function | Signature | Description | Location |
|---|---|---|---|
| `murmur3_32` | `murmur3_32(ANY) -> INTEGER` | Iceberg-compatible Murmur3 x86_32 for `bucket(...)` transforms. Ints/bools sign-extend to int64; float/double via `doubleToLongBits` with `-0.0` normalized; VARCHAR hashes UTF-8 bytes. **Limitation**: nested types hash the value's *string representation* — not Iceberg-spec. | [`src/functions/ducklake_murmur3.cpp:101`](https://github.com/PostHog/hoglake/blob/eee193b7cb18fc4954df4664c3468d75f2d26ceb/src/functions/ducklake_murmur3.cpp#L101); fallback `:87-93` |

### 1.3 Secret type

`CREATE SECRET ... (TYPE ducklake, ...)`, provider `config`, default
secret `__default_ducklake`. `ATTACH 'ducklake:<secret_name>'` or bare
`ATTACH 'ducklake:'` resolves from the secret. Parameters: `data_path`,
`metadata_schema`, `metadata_catalog`, `metadata_path` (**required**),
`metadata_parameters MAP(VARCHAR,VARCHAR)`, `encrypted BOOLEAN`,
`ducklake_version VARCHAR` — [`src/storage/ducklake_secret.cpp:40-50`](https://github.com/PostHog/hoglake/blob/eee193b7cb18fc4954df4664c3468d75f2d26ceb/src/storage/ducklake_secret.cpp#L40-L50);
requirement `:9-11`; name detection `:18-30`.

### 1.4 Per-catalog table macros

Materialized lazily inside every DuckLake schema
([`src/storage/ducklake_default_functions.cpp:10-24`](https://github.com/PostHog/hoglake/blob/eee193b7cb18fc4954df4664c3468d75f2d26ceb/src/storage/ducklake_default_functions.cpp#L10-L24); substitution
`:27-38`): `merge_adjacent_files()`, `set_option(...)`,
`set_commit_message(...)`, `options()`, `settings()`,
`current_snapshot()`, `last_committed_snapshot()`, `snapshots()`,
`table_info()`, `table_changes(...)`, `table_deletions(...)`,
`table_insertions(...)`.

No macro wrapper exists for: `rewrite_data_files`, `expire_snapshots`,
`cleanup_old_files`, `delete_orphaned_files`, `flush_inlined_data`,
`list_files`, `add_data_files` — explicit catalog-name calls only.

### 1.5 `CHECKPOINT` as a composite maintenance verb

`CHECKPOINT <ducklake_db>` runs a fixed six-step pipeline on a fresh
connection: `flush_inlined_data` → `expire_snapshots` →
`merge_adjacent_files` → `rewrite_data_files` → `cleanup_old_files` →
`delete_orphaned_files` ([`src/storage/ducklake_checkpoint.cpp:14-34`](https://github.com/PostHog/hoglake/blob/eee193b7cb18fc4954df4664c3468d75f2d26ceb/src/storage/ducklake_checkpoint.cpp#L14-L34)).
Comment at `:17-20` admits the merge/rewrite ordering is unresolved.

---

## 2. Options

### 2.1 `ATTACH` options (and secret keys)

Parsed in `HandleDuckLakeOption`, [`src/storage/ducklake_storage.cpp:13-71`](https://github.com/PostHog/hoglake/blob/eee193b7cb18fc4954df4664c3468d75f2d26ceb/src/storage/ducklake_storage.cpp#L13-L71).
Unknown keys throw. Backing struct
[`src/include/common/ducklake_options.hpp:24-42`](https://github.com/PostHog/hoglake/blob/eee193b7cb18fc4954df4664c3468d75f2d26ceb/src/include/common/ducklake_options.hpp#L24-L42).

| Option | Type | Meaning / notes |
|---|---|---|
| `data_path` | VARCHAR | Root for Parquet files. Auto-loads the required filesystem extension ([`ducklake_initializer.cpp:139-159`](https://github.com/PostHog/hoglake/blob/eee193b7cb18fc4954df4664c3468d75f2d26ceb/src/storage/ducklake_initializer.cpp#L139-L159)). Must match stored value unless `override_data_path`. |
| `override_data_path` | BOOLEAN | Allow `data_path` to differ from the catalog-recorded value. |
| `metadata_schema` | identifier | Schema inside the metadata DB holding the DuckLake tables. |
| `metadata_catalog` | VARCHAR | Attach name for the metadata DB; empty → internal `__ducklake_metadata_<name>`, HIDDEN ([`ducklake_storage.cpp:119-122`](https://github.com/PostHog/hoglake/blob/eee193b7cb18fc4954df4664c3468d75f2d26ceb/src/storage/ducklake_storage.cpp#L119-L122), [`ducklake_initializer.cpp:61-63`](https://github.com/PostHog/hoglake/blob/eee193b7cb18fc4954df4664c3468d75f2d26ceb/src/storage/ducklake_initializer.cpp#L61-L63)). |
| `metadata_path` | VARCHAR | Connection string of the metadata DB (also the bare ATTACH payload). |
| `metadata_parameters` | MAP | Passed through verbatim to the metadata DB attach; key `type` selects the backend ([`ducklake_catalog.cpp:198-207`](https://github.com/PostHog/hoglake/blob/eee193b7cb18fc4954df4664c3468d75f2d26ceb/src/storage/ducklake_catalog.cpp#L198-L207)). |
| `meta_*` prefix | any | Sugar for `metadata_parameters['*']`. |
| `encrypted` | BOOLEAN | Encrypt written Parquet (tri-state; AUTOMATIC → unencrypted on create). |
| `data_inlining_row_limit` | UBIGINT | Rows inlined into the catalog instead of Parquet. |
| `snapshot_version` / `snapshot_time` | BIGINT / TIMESTAMPTZ | Pin the whole attach to a snapshot; **forces READ_ONLY**; mutually exclusive. |
| `write_deletion_vectors` | BOOLEAN | Experimental: Iceberg V3 puffin deletion vectors. |
| `create_if_not_exists` | BOOLEAN | Default true; forced false under READ_ONLY unless explicit. |
| `automatic_migration` | BOOLEAN | Permit in-place metadata migration; else version mismatch is a hard error ([`ducklake_initializer.cpp:191-249`](https://github.com/PostHog/hoglake/blob/eee193b7cb18fc4954df4664c3468d75f2d26ceb/src/storage/ducklake_initializer.cpp#L191-L249)). |
| `busy_timeout` | UBIGINT | Default 5000 ms; forwarded to metadata attach. |
| `ducklake_version` | VARCHAR | Pin the spec version (≥ '1.0'); downgrades rejected. |

### 2.2 Persisted catalog options (`ducklake_set_option`)

Scope resolution table → schema → global
([`ducklake_catalog.hpp:141-153`](https://github.com/PostHog/hoglake/blob/eee193b7cb18fc4954df4664c3468d75f2d26ceb/src/include/storage/ducklake_catalog.hpp#L141-L153)).

| Key | Value | Notes |
|---|---|---|
| `data_inlining_row_limit` | UBIGINT | 0 disables; validates no `_ducklake_*` column collisions (`ducklake_set_option.cpp:16-53,129-135`) |
| `parquet_compression` | VARCHAR | `uncompressed, snappy, gzip, zstd, brotli, lz4, lz4_raw` |
| `parquet_version` | 1 or 2 | |
| `parquet_compression_level` | UBIGINT | |
| `parquet_row_group_size` / `_bytes` | UBIGINT / mem-string | Cannot be 0 |
| `target_file_size` | mem-string | Insert + compaction output target; default 512 MB ([`ducklake_catalog.hpp:100`](https://github.com/PostHog/hoglake/blob/eee193b7cb18fc4954df4664c3468d75f2d26ceb/src/include/storage/ducklake_catalog.hpp#L100)) |
| `hive_file_pattern` | BOOLEAN | Hive-style partitioned directories |
| `require_commit_message` | BOOLEAN | Commit fails without `set_commit_message` |
| `rewrite_delete_threshold` | DOUBLE 0–1 | Default for `rewrite_data_files` |
| `delete_older_than` / `expire_older_than` | interval string | Cleanup/expiry defaults; **global scope only** |
| `auto_compact` | BOOLEAN | Gate for maintenance functions per schema/table |
| `per_thread_output` | BOOLEAN | Separate output file per thread on parallel insert |
| `write_deletion_vectors` | BOOLEAN | Experimental |
| `sort_on_insert` | BOOLEAN | Default true; consumer [`ducklake_insert.cpp:818-840`](https://github.com/PostHog/hoglake/blob/eee193b7cb18fc4954df4664c3468d75f2d26ceb/src/storage/ducklake_insert.cpp#L818-L840) |
| `version`, `created_by`, `data_path`, `encrypted` | — | Read-only in `ducklake_options()`; not settable |

### 2.3 Global DuckDB extension settings (`SET`)

[`src/ducklake_extension.cpp:29-51`](https://github.com/PostHog/hoglake/blob/eee193b7cb18fc4954df4664c3468d75f2d26ceb/src/ducklake_extension.cpp#L29-L51), all GLOBAL scope:
`ducklake_max_retry_count` (10), `ducklake_retry_wait_ms` (100),
`ducklake_retry_backoff` (1.5), `ducklake_default_data_inlining_row_limit`
(10), `ducklake_default_version`, `ducklake_target_file_size`,
`ducklake_write_deletion_vectors` (false). Also a
`DuckLakeMetadataLogType` log type surfacing metadata queries to
`duckdb_logs` (`:24`).

**Defect we've hit (fleet-wide outages, twice)**: extension settings
live in a registry slot-matched between engine and extension build.
Running the fork *engine* with a stock-channel *extension* misaligned
the registries — `SET ducklake_max_retry_count` wrote into the core
`TimeZone` slot (every TIMESTAMPTZ fetch raised
`pytz.UnknownTimeZoneError('20')`) and, in the other direction,
`SET TimeZone='UTC'` landed in the UINT64 retry slot ("Could not
convert string 'UTC' to UINT64", zero-flush outage). viaduck now SETs
these one at a time and diffs `duckdb_settings()` before/after. A
service with typed config makes this entire failure class
unrepresentable.

---

## 3. DDL / DML routed through the catalog

### 3.1 Supported

`CREATE/DROP SCHEMA`; `CREATE TABLE` (+AS, with partition/sort at
create); `CREATE VIEW` (catalog name rewritten to a
`{DUCKLAKE_CATALOG}.` placeholder so views survive re-attach —
[`ducklake_schema_entry.cpp:147-168`](https://github.com/PostHog/hoglake/blob/eee193b7cb18fc4954df4664c3468d75f2d26ceb/src/storage/ducklake_schema_entry.cpp#L147-L168)); `CREATE [TABLE] MACRO`; `INSERT`,
`DELETE`, `UPDATE`, `MERGE INTO` (dedicated physical plans:
[`ducklake_insert.cpp:802`](https://github.com/PostHog/hoglake/blob/eee193b7cb18fc4954df4664c3468d75f2d26ceb/src/storage/ducklake_insert.cpp#L802), [`ducklake_delete.cpp:701`](https://github.com/PostHog/hoglake/blob/eee193b7cb18fc4954df4664c3468d75f2d26ceb/src/storage/ducklake_delete.cpp#L701),
[`ducklake_update.cpp:260`](https://github.com/PostHog/hoglake/blob/eee193b7cb18fc4954df4664c3468d75f2d26ceb/src/storage/ducklake_update.cpp#L260), [`ducklake_merge_into.cpp:621`](https://github.com/PostHog/hoglake/blob/eee193b7cb18fc4954df4664c3468d75f2d26ceb/src/storage/ducklake_merge_into.cpp#L621)); `DROP` (no
CASCADE); `COMMENT ON TABLE/VIEW/COLUMN`; time travel `AT (VERSION|
TIMESTAMP => ...)`; virtual scan columns `filename`, `rowid`,
`snapshot_id` ([`ducklake_table_entry.cpp:402-419`](https://github.com/PostHog/hoglake/blob/eee193b7cb18fc4954df4664c3468d75f2d26ceb/src/storage/ducklake_table_entry.cpp#L402-L419)).

### 3.2 `ALTER TABLE` subtypes (dispatch [`ducklake_table_entry.cpp:1377-1405`](https://github.com/PostHog/hoglake/blob/eee193b7cb18fc4954df4664c3468d75f2d26ceb/src/storage/ducklake_table_entry.cpp#L1377-L1405))

`RENAME TABLE/COLUMN`, `ADD/DROP COLUMN`, `ALTER COLUMN TYPE`,
`SET/DROP NOT NULL`, `SET DEFAULT`, `ADD/DROP/RENAME FIELD` (nested
struct evolution), `SET PARTITIONED BY`, `SET SORTED BY`.

- `SET SORTED BY` is present in this fork (`:1330`, `:1402`); runtime
  effect gated by `sort_on_insert`.
- Partition transforms: identity, `year/month/day/hour`,
  `epoch_year/month/day/hour` (require ≥1.1 metadata), `bucket(n, col)`
  (`:578-618`).
- With transaction-local inlined data, only a restricted ALTER subset is
  permitted (`:1364-1376`). `RENAME FIELD` is top-level-only (`:1277`).
  Type evolution follows a promotion lattice; unsupported pairs throw.

### 3.3 Explicitly unsupported (throws)

Indexes; CHECK constraints; PRIMARY KEY/UNIQUE; FOREIGN KEY; generated
columns; per-column compression; sequences; user-defined
table/copy/pragma functions; collations; user-defined types; `DROP ...
CASCADE`; non-comment column tags. ([`ducklake_schema_entry.cpp:144-372`](https://github.com/PostHog/hoglake/blob/eee193b7cb18fc4954df4664c3468d75f2d26ceb/src/storage/ducklake_schema_entry.cpp#L144-L372),
[`ducklake_table_entry.cpp:113-128`](https://github.com/PostHog/hoglake/blob/eee193b7cb18fc4954df4664c3468d75f2d26ceb/src/storage/ducklake_table_entry.cpp#L113-L128), `ducklake_catalog.cpp:555,919,1079`.)

---

## 4. Multi-backend metadata support

Backend from `metadata_parameters['type']` or the `metadata_path` prefix
([`ducklake_catalog.cpp:196-207`](https://github.com/PostHog/hoglake/blob/eee193b7cb18fc4954df4664c3468d75f2d26ceb/src/storage/ducklake_catalog.cpp#L196-L207)). Registry
[`ducklake_metadata_manager.cpp:46-49`](https://github.com/PostHog/hoglake/blob/eee193b7cb18fc4954df4664c3468d75f2d26ceb/src/storage/ducklake_metadata_manager.cpp#L46-L49):

| `type` | Manager | Notes |
|---|---|---|
| `postgres(_scanner)` | `PostgresMetadataManager` | |
| `sqlite(_scanner)` | `SQLiteMetadataManager` | |
| `quack(_scanner)` | `QuackMetadataManager` | Duckgres metadata server — the backend with **server-side commits** |
| else / `duckdb` / MotherDuck | base manager | DuckDB file/in-memory |

**MySQL is not supported** — no manager or code path exists.
Version-layered `DuckLakeMetadataManagerV1_1<Base>` wraps each backend
for ≥1.1 metadata ([`ducklake_initializer.cpp:308-330`](https://github.com/PostHog/hoglake/blob/eee193b7cb18fc4954df4664c3468d75f2d26ceb/src/storage/ducklake_initializer.cpp#L308-L330)).

Postgres-specific paths: attach scoped to `METADATA_SCHEMA` (else the
postgres extension reflects every schema — "very slow on large or
multi-tenant catalogs", [`ducklake_initializer.cpp:51-60`](https://github.com/PostHog/hoglake/blob/eee193b7cb18fc4954df4664c3468d75f2d26ceb/src/storage/ducklake_initializer.cpp#L51-L60)); 63-char
identifier cap; type-mapping overrides; inlined-data cast pass
([`ducklake_inlined_data_reader.cpp:74-76`](https://github.com/PostHog/hoglake/blob/eee193b7cb18fc4954df4664c3468d75f2d26ceb/src/storage/ducklake_inlined_data_reader.cpp#L74-L76)); custom latest-snapshot and
file-column-stats queries using `postgres_query()` pushdown; no
Appender. Quack-specific: `ProbeServerCapabilities`,
`CanSkipSnapshotFetch`, `FlushChangesServerSide`
([`quack_metadata_manager.hpp:25-35`](https://github.com/PostHog/hoglake/blob/eee193b7cb18fc4954df4664c3468d75f2d26ceb/src/include/metadata_manager/quack_metadata_manager.hpp#L25-L35)); a `query_lock` mutex serializes
metadata statements because a failed statement aborts the server-side
transaction (`:41-46`).

---

## 5. Transaction / OCC model surface

**Commit dispatch** — `DuckLakeTransaction::FlushChanges`
([`ducklake_transaction.cpp:1383-1405`](https://github.com/PostHog/hoglake/blob/eee193b7cb18fc4954df4664c3468d75f2d26ceb/src/storage/ducklake_transaction.cpp#L1383-L1405)): (1) quack + skippable snapshot
fetch → fully server-side; (2) server-side retrials → server-side with
read snapshot via `ducklake_commit`; (3) else client-side
`RunCommitLoop` (`:1431`). Base manager throws on server-side flush for
unsupported backends ([`ducklake_metadata_manager.cpp:83-87`](https://github.com/PostHog/hoglake/blob/eee193b7cb18fc4954df4664c3468d75f2d26ceb/src/storage/ducklake_metadata_manager.cpp#L83-L87)).

**Isolation / conflicts**: snapshot isolation with per-snapshot schema
cache; at commit, re-read newer snapshots and `CheckForConflicts`
against their unioned `changes_made`
([`ducklake_transaction_state.cpp:142`](https://github.com/PostHog/hoglake/blob/eee193b7cb18fc4954df4664c3468d75f2d26ceb/src/storage/ducklake_transaction_state.cpp#L142), called `:1934`). Conflict matrix:
drop-vs-drop, create-vs-create (by name), insert-vs-{drop, alter,
delete-from, inlined-delete}, delete-vs-{drop, alter, merge_adjacent,
rewrite_delete, insert, inlined-insert}, per-file delete-vs-delete
(`:243-249`), flush-inlined-vs-{drop, inlined-delete, flushed-inlined}.

**Retry behavior**: `DuckLakeRetryConfig {10, 100ms, 1.5}`
([`ducklake_transaction.hpp:168-174`](https://github.com/PostHog/hoglake/blob/eee193b7cb18fc4954df4664c3468d75f2d26ceb/src/include/storage/ducklake_transaction.hpp#L168-L174)), overridable per-`ducklake_commit`
call. Loop `max_retry_count + 1` attempts
([`ducklake_transaction_state.cpp:1924`](https://github.com/PostHog/hoglake/blob/eee193b7cb18fc4954df4664c3468d75f2d26ceb/src/storage/ducklake_transaction_state.cpp#L1924)). **Retryability is decided by
string-matching the error message** (pkey errors, "conflict",
concurrent-access — [`ducklake_transaction.cpp:1351-1366`](https://github.com/PostHog/hoglake/blob/eee193b7cb18fc4954df4664c3468d75f2d26ceb/src/storage/ducklake_transaction.cpp#L1351-L1366)) — notable
fragility. Backoff `wait * jitter * pow(backoff, i)` (`:1998-1999`).
Per-retry reset: rollback, clear inlined caches, fresh snapshot
([`ducklake_transaction.cpp:1461-1465`](https://github.com/PostHog/hoglake/blob/eee193b7cb18fc4954df4664c3468d75f2d26ceb/src/storage/ducklake_transaction.cpp#L1461-L1465)).

**Commit-context surface** (what hoglake's server must provide):
`RunCommitLoop` populates a `DuckLakeCommitContext` of ~18 closures —
conflict query, snapshot fetch, commit-batch executor, cache
flush/invalidate hooks, `try_append_data_files`,
`write_inlined_tables`, `write_inlined_file_deletes`,
`set_catalog_version`, `set_committed_snapshot_id`,
`invalidate_table_stats_cache`, plus commit info and the v1.1 flag
([`ducklake_transaction.cpp:1432-1556`](https://github.com/PostHog/hoglake/blob/eee193b7cb18fc4954df4664c3468d75f2d26ceb/src/storage/ducklake_transaction.cpp#L1432-L1556)). The server-side equivalent
builds the same context from staged temp tables
([`ducklake_server_side_commit.hpp:84`](https://github.com/PostHog/hoglake/blob/eee193b7cb18fc4954df4664c3468d75f2d26ceb/src/include/storage/ducklake_server_side_commit.hpp#L84)).
