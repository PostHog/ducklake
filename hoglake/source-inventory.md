# DuckLake extension source inventory

LOC survey of the fork (`src/`, C++), 2026-09-04. Not reuse material —
the point is knowing what the extension actually *does* and where each
piece goes in the hoglake world. Companion to [ducklake-api-map.md](ducklake-api-map.md)
(what it exposes) and [metadata-schema.md](metadata-schema.md) (what it stores).

## Totals

| | Files | LOC |
|---|---|---|
| `.cpp` | 69 | 30,395 |
| `.hpp` (src/include) | 60 | 6,123 |
| **Total** | **129** | **36,518** |

| Directory | LOC (.cpp) | Files | What lives there |
|---|---|---|---|
| `src/storage` | 23,083 | 36 | Catalog, transaction/OCC, metadata SQL generation, physical operators, scan machinery |
| `src/functions` | 4,866 | 19 | The registered table/scalar functions |
| `src/common` | 1,055 | 7 | Types, utils, parquet footer scanning, name mapping |
| `src/metadata_manager` | 445 | 4 | Postgres/quack/sqlite backend overrides + v1.1 overlay |
| `src/` root | 151 | 1 | Extension registration |

Headers (`src/include`, 6.1K LOC) mirror the cpp layout; not itemized —
they are declarations plus a few meaty inline pieces already covered by
the API map ([`ducklake_catalog.hpp`](https://github.com/PostHog/hoglake/blob/eee193b7cb18fc4954df4664c3468d75f2d26ceb/src/include/storage/ducklake_catalog.hpp) option/scope resolution,
[`ducklake_server_side_commit.hpp`](https://github.com/PostHog/hoglake/blob/eee193b7cb18fc4954df4664c3468d75f2d26ceb/src/include/storage/ducklake_server_side_commit.hpp) context builder,
[`ducklake_staged_commit.hpp`](https://github.com/PostHog/hoglake/blob/eee193b7cb18fc4954df4664c3468d75f2d26ceb/src/include/storage/ducklake_staged_commit.hpp) staging-table schemas).

## Per-file inventory, by disposition in hoglake

### A. Reimplement server-side — the essence (~12.6K LOC)

The catalog/commit/OCC core. This is what hoglake's service *is*.

| File | LOC | Role |
|---|---|---|
| [`storage/ducklake_metadata_manager.cpp`](https://github.com/PostHog/hoglake/blob/eee193b7cb18fc4954df4664c3468d75f2d26ceb/src/storage/ducklake_metadata_manager.cpp) | 5,795 | THE file: all metadata SQL generation — schema DDL, snapshot/commit inserts, file registration, stats, changefeed queries, expiry cascades, migrations. One-fifth of all cpp LOC; becomes hoglake's queries/repositories layer, with FKs and bound parameters instead of string-built batches. |
| [`storage/ducklake_transaction.cpp`](https://github.com/PostHog/hoglake/blob/eee193b7cb18fc4954df4664c3468d75f2d26ceb/src/storage/ducklake_transaction.cpp) | 2,217 | Transaction lifetime, commit dispatch (client-side vs server-side), retry-on-error string matching, metadata-connection management. |
| [`storage/ducklake_transaction_state.cpp`](https://github.com/PostHog/hoglake/blob/eee193b7cb18fc4954df4664c3468d75f2d26ceb/src/storage/ducklake_transaction_state.cpp) | 2,012 | The commit loop: conflict detection matrix, snapshot allocation, retry/backoff, per-attempt state reset, CommitChanges orchestration. |
| [`storage/ducklake_server_side_commit.cpp`](https://github.com/PostHog/hoglake/blob/eee193b7cb18fc4954df4664c3468d75f2d26ceb/src/storage/ducklake_server_side_commit.cpp) | 897 | Server-side commit driver (quack backend): rebuilds the commit context from staged tables — the direct precedent for hoglake's commit endpoint. |
| [`storage/ducklake_staged_commit.cpp`](https://github.com/PostHog/hoglake/blob/eee193b7cb18fc4954df4664c3468d75f2d26ceb/src/storage/ducklake_staged_commit.cpp) | 532 | Client half of server-side commit: stages the write set into 16 temp tables + generates the SQL batch. Becomes the wire protocol. |
| [`storage/ducklake_stats.cpp`](https://github.com/PostHog/hoglake/blob/eee193b7cb18fc4954df4664c3468d75f2d26ceb/src/storage/ducklake_stats.cpp) | 369 | Table/column stats model + merge logic. |
| [`storage/ducklake_initializer.cpp`](https://github.com/PostHog/hoglake/blob/eee193b7cb18fc4954df4664c3468d75f2d26ceb/src/storage/ducklake_initializer.cpp) | 332 | Attach-time init: version resolution, migration chain, backend selection. Becomes service startup + migration runner. |
| [`storage/ducklake_transaction_changes.cpp`](https://github.com/PostHog/hoglake/blob/eee193b7cb18fc4954df4664c3468d75f2d26ceb/src/storage/ducklake_transaction_changes.cpp) | 221 | `changes_made` encoding/parsing — the OCC conflict vocabulary. |
| [`metadata_manager/postgres_metadata_manager.cpp`](https://github.com/PostHog/hoglake/blob/eee193b7cb18fc4954df4664c3468d75f2d26ceb/src/metadata_manager/postgres_metadata_manager.cpp) | 195 | PG overrides: `postgres_execute` wrapping, type mapping, pushdown queries. Dissolves into the (PG-only) core. |
| [`metadata_manager/quack_metadata_manager.cpp`](https://github.com/PostHog/hoglake/blob/eee193b7cb18fc4954df4664c3468d75f2d26ceb/src/metadata_manager/quack_metadata_manager.cpp) | 154 | Server-side-commit capability probing. |
| [`metadata_manager/sqlite_metadata_manager.cpp`](https://github.com/PostHog/hoglake/blob/eee193b7cb18fc4954df4664c3468d75f2d26ceb/src/metadata_manager/sqlite_metadata_manager.cpp) | 50 | Dies (PG-only). |
| [`metadata_manager/ducklake_metadata_manager_v1_1.cpp`](https://github.com/PostHog/hoglake/blob/eee193b7cb18fc4954df4664c3468d75f2d26ceb/src/metadata_manager/ducklake_metadata_manager_v1_1.cpp) | 46 | v1.1 schema overlay. |
| [`common/ducklake_types.cpp`](https://github.com/PostHog/hoglake/blob/eee193b7cb18fc4954df4664c3468d75f2d26ceb/src/common/ducklake_types.cpp) | 160 | Type serialization (the `column_type` strings). |
| [`common/ducklake_util.cpp`](https://github.com/PostHog/hoglake/blob/eee193b7cb18fc4954df4664c3468d75f2d26ceb/src/common/ducklake_util.cpp) | 531 | SQL escaping, path joining, encryption-key handling, misc. |
| [`common/ducklake_version.cpp`](https://github.com/PostHog/hoglake/blob/eee193b7cb18fc4954df4664c3468d75f2d26ceb/src/common/ducklake_version.cpp) | 57 | Version enum/parsing. |
| [`common/ducklake_snapshot.cpp`](https://github.com/PostHog/hoglake/blob/eee193b7cb18fc4954df4664c3468d75f2d26ceb/src/common/ducklake_snapshot.cpp) / [`ducklake_data_file.cpp`](https://github.com/PostHog/hoglake/blob/eee193b7cb18fc4954df4664c3468d75f2d26ceb/src/common/ducklake_data_file.cpp) | 23 / 63 | Core value types. |

### B. Dies with DuckDB — engine integration (~9.3K LOC)

DuckDB catalog bindings and physical operators. In hoglake, writes are
"client writes parquet + calls commit" and reads are Trino/engine-side,
so none of this is reimplemented as such.

| File | LOC | Role |
|---|---|---|
| [`storage/ducklake_table_entry.cpp`](https://github.com/PostHog/hoglake/blob/eee193b7cb18fc4954df4664c3468d75f2d26ceb/src/storage/ducklake_table_entry.cpp) | 1,506 | DuckDB table catalog entry: ALTER dispatch, type-promotion lattice, partition transforms, virtual columns. (The *rules* survive as API validation; the entry machinery dies.) |
| [`storage/ducklake_catalog.cpp`](https://github.com/PostHog/hoglake/blob/eee193b7cb18fc4954df4664c3468d75f2d26ceb/src/storage/ducklake_catalog.cpp) | 1,149 | DuckDB catalog implementation, schema-per-snapshot cache (the leak-estimate bug lives here). |
| [`storage/ducklake_insert.cpp`](https://github.com/PostHog/hoglake/blob/eee193b7cb18fc4954df4664c3468d75f2d26ceb/src/storage/ducklake_insert.cpp) | 887 | INSERT/CTAS physical operator, parquet writing, sort-on-insert, target file size. |
| [`storage/ducklake_multi_file_reader.cpp`](https://github.com/PostHog/hoglake/blob/eee193b7cb18fc4954df4664c3468d75f2d26ceb/src/storage/ducklake_multi_file_reader.cpp) | 741 | Scan-side: row-id/snapshot column materialization, delete-file application. |
| [`storage/ducklake_delete.cpp`](https://github.com/PostHog/hoglake/blob/eee193b7cb18fc4954df4664c3468d75f2d26ceb/src/storage/ducklake_delete.cpp) | 716 | DELETE operator, delete-file writing. |
| [`storage/ducklake_merge_into.cpp`](https://github.com/PostHog/hoglake/blob/eee193b7cb18fc4954df4664c3468d75f2d26ceb/src/storage/ducklake_merge_into.cpp) | 652 | MERGE INTO operator. |
| [`storage/ducklake_schema_entry.cpp`](https://github.com/PostHog/hoglake/blob/eee193b7cb18fc4954df4664c3468d75f2d26ceb/src/storage/ducklake_schema_entry.cpp) | 634 | Schema catalog entry: create table/view/macro, unsupported-feature throws. |
| [`storage/ducklake_delete_filter.cpp`](https://github.com/PostHog/hoglake/blob/eee193b7cb18fc4954df4664c3468d75f2d26ceb/src/storage/ducklake_delete_filter.cpp) | 511 | Applying positional deletes during scans. |
| [`storage/ducklake_multi_file_list.cpp`](https://github.com/PostHog/hoglake/blob/eee193b7cb18fc4954df4664c3468d75f2d26ceb/src/storage/ducklake_multi_file_list.cpp) | 471 | File-list resolution for scans. |
| [`storage/ducklake_field_data.cpp`](https://github.com/PostHog/hoglake/blob/eee193b7cb18fc4954df4664c3468d75f2d26ceb/src/storage/ducklake_field_data.cpp) | 402 | Field-id ↔ DuckDB column binding. |
| [`storage/ducklake_scan.cpp`](https://github.com/PostHog/hoglake/blob/eee193b7cb18fc4954df4664c3468d75f2d26ceb/src/storage/ducklake_scan.cpp) | 341 | `ducklake_scan` = parquet_scan clone + callbacks. |
| [`storage/ducklake_update.cpp`](https://github.com/PostHog/hoglake/blob/eee193b7cb18fc4954df4664c3468d75f2d26ceb/src/storage/ducklake_update.cpp) | 335 | UPDATE operator (delete+insert). |
| [`storage/ducklake_partition_data.cpp`](https://github.com/PostHog/hoglake/blob/eee193b7cb18fc4954df4664c3468d75f2d26ceb/src/storage/ducklake_partition_data.cpp) | 309 | Partition-value computation on write. (Transform *semantics* survive.) |
| [`storage/ducklake_view_entry.cpp`](https://github.com/PostHog/hoglake/blob/eee193b7cb18fc4954df4664c3468d75f2d26ceb/src/storage/ducklake_view_entry.cpp) | 172 | View catalog entry. |
| [`storage/ducklake_storage.cpp`](https://github.com/PostHog/hoglake/blob/eee193b7cb18fc4954df4664c3468d75f2d26ceb/src/storage/ducklake_storage.cpp) | 145 | ATTACH option parsing. |
| [`storage/ducklake_catalog_set.cpp`](https://github.com/PostHog/hoglake/blob/eee193b7cb18fc4954df4664c3468d75f2d26ceb/src/storage/ducklake_catalog_set.cpp) | 72 | Catalog-set container. |
| [`storage/ducklake_sort_data.cpp`](https://github.com/PostHog/hoglake/blob/eee193b7cb18fc4954df4664c3468d75f2d26ceb/src/storage/ducklake_sort_data.cpp) | 60 | Sort-spec plumbing for insert. |
| [`storage/ducklake_secret.cpp`](https://github.com/PostHog/hoglake/blob/eee193b7cb18fc4954df4664c3468d75f2d26ceb/src/storage/ducklake_secret.cpp) | 58 | Secret type. |
| [`storage/ducklake_default_functions.cpp`](https://github.com/PostHog/hoglake/blob/eee193b7cb18fc4954df4664c3468d75f2d26ceb/src/storage/ducklake_default_functions.cpp) | 54 | Per-catalog macro wrappers. |
| [`storage/ducklake_autoload_helper.cpp`](https://github.com/PostHog/hoglake/blob/eee193b7cb18fc4954df4664c3468d75f2d26ceb/src/storage/ducklake_autoload_helper.cpp) | 50 | fs-extension autoload. |
| [`storage/ducklake_transaction_manager.cpp`](https://github.com/PostHog/hoglake/blob/eee193b7cb18fc4954df4664c3468d75f2d26ceb/src/storage/ducklake_transaction_manager.cpp) | 46 | DuckDB transaction-manager shim. |
| [`storage/ducklake_log_type.cpp`](https://github.com/PostHog/hoglake/blob/eee193b7cb18fc4954df4664c3468d75f2d26ceb/src/storage/ducklake_log_type.cpp) | 31 | Metadata-query log type. |
| [`ducklake_extension.cpp`](https://github.com/PostHog/hoglake/blob/eee193b7cb18fc4954df4664c3468d75f2d26ceb/src/ducklake_extension.cpp) | 151 | Registration + global settings. |

### C. Dies with the inlining decision (~1.6K LOC)

| File | LOC |
|---|---|
| [`functions/ducklake_flush_inlined_data.cpp`](https://github.com/PostHog/hoglake/blob/eee193b7cb18fc4954df4664c3468d75f2d26ceb/src/functions/ducklake_flush_inlined_data.cpp) | 793 |
| [`storage/ducklake_inline_data.cpp`](https://github.com/PostHog/hoglake/blob/eee193b7cb18fc4954df4664c3468d75f2d26ceb/src/storage/ducklake_inline_data.cpp) | 414 |
| [`storage/ducklake_inlined_data_reader.cpp`](https://github.com/PostHog/hoglake/blob/eee193b7cb18fc4954df4664c3468d75f2d26ceb/src/storage/ducklake_inlined_data_reader.cpp) | 313 |
| [`storage/ducklake_inlined_data.cpp`](https://github.com/PostHog/hoglake/blob/eee193b7cb18fc4954df4664c3468d75f2d26ceb/src/storage/ducklake_inlined_data.cpp) | 65 |

### D. Becomes service background jobs (~1.4K LOC)

| File | LOC | Role |
|---|---|---|
| [`functions/ducklake_compaction_functions.cpp`](https://github.com/PostHog/hoglake/blob/eee193b7cb18fc4954df4664c3468d75f2d26ceb/src/functions/ducklake_compaction_functions.cpp) | 991 | merge_adjacent + rewrite_data_files planning/execution. Rowid materialization becomes unconditional (lineage decision). |
| [`functions/ducklake_cleanup_files.cpp`](https://github.com/PostHog/hoglake/blob/eee193b7cb18fc4954df4664c3468d75f2d26ceb/src/functions/ducklake_cleanup_files.cpp) | 181 | Deletion-queue drain + orphan scan. Gets per-batch commit + advisory-lock semantics for free once server-owned. |
| [`functions/ducklake_expire_snapshots.cpp`](https://github.com/PostHog/hoglake/blob/eee193b7cb18fc4954df4664c3468d75f2d26ceb/src/functions/ducklake_expire_snapshots.cpp) | 159 | Expiry entry point (cascade lives in A). |
| [`storage/ducklake_checkpoint.cpp`](https://github.com/PostHog/hoglake/blob/eee193b7cb18fc4954df4664c3468d75f2d26ceb/src/storage/ducklake_checkpoint.cpp) | 36 | The six-step maintenance pipeline. |

### E. Becomes API endpoints (~1.2K LOC)

| File | LOC | Endpoint |
|---|---|---|
| [`functions/ducklake_snapshots.cpp`](https://github.com/PostHog/hoglake/blob/eee193b7cb18fc4954df4664c3468d75f2d26ceb/src/functions/ducklake_snapshots.cpp) | 176 | list snapshots |
| [`functions/ducklake_table_insertions.cpp`](https://github.com/PostHog/hoglake/blob/eee193b7cb18fc4954df4664c3468d75f2d26ceb/src/functions/ducklake_table_insertions.cpp) | 110 | changefeed (insertions/deletions) |
| [`functions/ducklake_table_changes.cpp`](https://github.com/PostHog/hoglake/blob/eee193b7cb18fc4954df4664c3468d75f2d26ceb/src/functions/ducklake_table_changes.cpp) | 31 | changefeed (the SQL macro — rewritten server-side, killing the executor-stall wedge) |
| [`functions/ducklake_list_files.cpp`](https://github.com/PostHog/hoglake/blob/eee193b7cb18fc4954df4664c3468d75f2d26ceb/src/functions/ducklake_list_files.cpp) | 116 | file listing / read planning |
| [`functions/ducklake_table_info.cpp`](https://github.com/PostHog/hoglake/blob/eee193b7cb18fc4954df4664c3468d75f2d26ceb/src/functions/ducklake_table_info.cpp) | 59 | table stats |
| [`functions/ducklake_options.cpp`](https://github.com/PostHog/hoglake/blob/eee193b7cb18fc4954df4664c3468d75f2d26ceb/src/functions/ducklake_options.cpp) / [`ducklake_set_option.cpp`](https://github.com/PostHog/hoglake/blob/eee193b7cb18fc4954df4664c3468d75f2d26ceb/src/functions/ducklake_set_option.cpp) | 180 / 241 | options get/set |
| [`functions/ducklake_settings.cpp`](https://github.com/PostHog/hoglake/blob/eee193b7cb18fc4954df4664c3468d75f2d26ceb/src/functions/ducklake_settings.cpp) | 52 | catalog info |
| [`functions/ducklake_set_commit_message.cpp`](https://github.com/PostHog/hoglake/blob/eee193b7cb18fc4954df4664c3468d75f2d26ceb/src/functions/ducklake_set_commit_message.cpp) | 60 | commit metadata |
| [`functions/ducklake_current_snapshot.cpp`](https://github.com/PostHog/hoglake/blob/eee193b7cb18fc4954df4664c3468d75f2d26ceb/src/functions/ducklake_current_snapshot.cpp) / [`ducklake_last_committed_snapshot.cpp`](https://github.com/PostHog/hoglake/blob/eee193b7cb18fc4954df4664c3468d75f2d26ceb/src/functions/ducklake_last_committed_snapshot.cpp) | 31 / 33 | snapshot pointers |
| [`functions/ducklake_commit.cpp`](https://github.com/PostHog/hoglake/blob/eee193b7cb18fc4954df4664c3468d75f2d26ceb/src/functions/ducklake_commit.cpp) | 78 | THE commit endpoint (see A) |
| [`functions/base_metadata_function.cpp`](https://github.com/PostHog/hoglake/blob/eee193b7cb18fc4954df4664c3468d75f2d26ceb/src/functions/base_metadata_function.cpp) | 68 | shared binding |

### F. Port faithfully — format/algorithm code (~1.8K LOC)

Small but load-bearing; must be behavior-identical in the new language.

| File | LOC | Role |
|---|---|---|
| [`functions/ducklake_add_data_files.cpp`](https://github.com/PostHog/hoglake/blob/eee193b7cb18fc4954df4664c3468d75f2d26ceb/src/functions/ducklake_add_data_files.cpp) | 1,400 | Parquet-footer parsing → types/field-ids/stats derivation + hive-partition mapping. The registration path; hoglake's commit endpoint needs the validation half, clients the stats-derivation half. |
| [`functions/ducklake_murmur3.cpp`](https://github.com/PostHog/hoglake/blob/eee193b7cb18fc4954df4664c3468d75f2d26ceb/src/functions/ducklake_murmur3.cpp) | 107 | Iceberg-compatible bucket hash — must match bit-for-bit or bucket pruning breaks (fix the nested-type string-repr fallback while at it). |
| [`storage/ducklake_puffin.cpp`](https://github.com/PostHog/hoglake/blob/eee193b7cb18fc4954df4664c3468d75f2d26ceb/src/storage/ducklake_puffin.cpp) | 310 | Puffin deletion-vector file format (keep only if deletion vectors stay in scope). |
| [`storage/ducklake_deletion_vector.cpp`](https://github.com/PostHog/hoglake/blob/eee193b7cb18fc4954df4664c3468d75f2d26ceb/src/storage/ducklake_deletion_vector.cpp) | 228 | Deletion-vector application. |
| [`common/parquet_file_scanner.cpp`](https://github.com/PostHog/hoglake/blob/eee193b7cb18fc4954df4664c3468d75f2d26ceb/src/common/parquet_file_scanner.cpp) | 113 | Footer scanning support. |
| [`common/ducklake_name_map.cpp`](https://github.com/PostHog/hoglake/blob/eee193b7cb18fc4954df4664c3468d75f2d26ceb/src/common/ducklake_name_map.cpp) | 108 | Name-mapping resolution for externally-added files. |

## The shape of the answer

~36.5K LOC total, but the *catalog* — the part hoglake reimplements —
is ~12.6K, of which 5.8K is string-built SQL that a parameterized,
FK-backed Postgres layer shrinks substantially. Another ~9.3K is
DuckDB-engine integration that has no successor (Trino + client
parquet writers replace it), ~1.6K dies with inlining, and ~4.5K
becomes jobs/endpoints whose logic mostly lives in the core anyway.
The genuinely fiddly ports are small and isolated: footer→stats
derivation, the murmur3 bucket hash, and (optionally) puffin.
