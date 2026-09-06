# DuckLake metadata schema — reference for a Postgres-first rebuild

Survey of the catalog schema as defined in the fork source, for the
hoglake design. Companion to [ducklake-api-map.md](ducklake-api-map.md),
[pyducklake-api-map.md](pyducklake-api-map.md), [README.md](README.md).
Sections 1–5 describe the *predecessor* (`ducklake_*`) schema; §6 is
the inventory of the hoglake schema as built (`hog_*`), kept in sync
with [`server/schema.sql`](server/schema.sql) — which, not this doc, is
the authoritative artifact.

Source of truth: [`src/storage/ducklake_metadata_manager.cpp`](https://github.com/PostHog/hoglake/blob/eee193b7cb18fc4954df4664c3468d75f2d26ceb/src/storage/ducklake_metadata_manager.cpp)
(`GetCreateTableStatements`, lines 237–310) plus the v1.1 overlay in
[`src/metadata_manager/ducklake_metadata_manager_v1_1.cpp`](https://github.com/PostHog/hoglake/blob/eee193b7cb18fc4954df4664c3468d75f2d26ceb/src/metadata_manager/ducklake_metadata_manager_v1_1.cpp). There are no
`.sql` files — the entire schema is C++ string literals with a
`{METADATA_CATALOG}` placeholder substituted per backend. Types are
DuckDB-dialect DDL handed verbatim to the metadata store.

---

## 1. Metadata tables

### Core catalog / versioning

| Table | Columns (type) | Keys declared | Role |
|---|---|---|---|
| `ducklake_metadata` | `key VARCHAR NOT NULL`, `value VARCHAR NOT NULL`, `scope VARCHAR`, `scope_id BIGINT` | none | Key/value options. Holds `version`, `created_by`, `data_path`, `encrypted` globally (`scope IS NULL`) and per-schema/table settings via scope+scope_id. Seeded `:212`; read by `LoadDuckLake` `:516`; written by `SetConfigOption` `:5745`. Its existence is the "is this a DuckLake" probe (`:177`). |
| `ducklake_snapshot` | `snapshot_id BIGINT PRIMARY KEY`, `snapshot_time TIMESTAMPTZ`, `schema_version BIGINT`, `next_catalog_id BIGINT`, `next_file_id BIGINT` | PK | The commit log / MVCC anchor; also carries the ID allocators. Snapshot 0 inserted at init (`:210`). |
| `ducklake_snapshot_changes` | `snapshot_id BIGINT PRIMARY KEY`, `changes_made VARCHAR`, `author`, `commit_message`, `commit_extra_info` | PK | Per-snapshot change summary for OCC conflict detection (`changes_made` = comma-joined encoded list) + commit metadata. |
| `ducklake_schema_versions` | `begin_snapshot`, `schema_version`, `table_id` | none | Per-table map schema version → beginning snapshot (`GetBeginSnapshotForSchemaVersion` `:664`). `table_id` added 0.3→0.4; pre-0.4 global rows backfilled/deleted `:401-423`. |

### Schema objects

| Table | Columns | Keys | Role |
|---|---|---|---|
| `ducklake_schema` | `schema_id PK`, `schema_uuid UUID`, `begin_snapshot`, `end_snapshot`, `schema_name`, `path`, `path_is_relative` | PK | Versioned SQL schemas; first hop of the 3-level relative-path chain. |
| `ducklake_table` | `table_id`, `table_uuid UUID`, `begin_snapshot`, `end_snapshot`, `schema_id`, `table_name`, `path`, `path_is_relative` | **none** | Versioned tables — rename/alter closes the row and inserts a new one with the same `table_id`; `(table_id, begin_snapshot)` is the real key. |
| `ducklake_column` | `column_id`, `begin_snapshot`, `end_snapshot`, `table_id`, `column_order`, `column_name`, `column_type VARCHAR`, `initial_default`, `default_value`, `nulls_allowed`, `parent_column`, `default_value_type`, `default_value_dialect` | none | Versioned columns; `column_id` = stable field id; `parent_column` self-reference for nested types. |
| `ducklake_view` | `view_id`, `view_uuid`, `begin_snapshot`, `end_snapshot`, `schema_id`, `view_name`, `dialect`, `sql`, `column_aliases` | none | Versioned views; SQL stored with dialect. |
| `ducklake_macro` | `schema_id`, `macro_id`, `macro_name`, `begin_snapshot`, `end_snapshot` | none | Versioned macros (0.3→0.4). |
| `ducklake_macro_impl` | `macro_id`, `impl_id`, `dialect`, `sql`, `type` | none | Per-overload/dialect implementation. NOT snapshot-versioned — orphan-swept (`:5554`). |
| `ducklake_macro_parameters` | `macro_id`, `impl_id`, `column_id`, `parameter_name`, `parameter_type`, `default_value`, `default_value_type` | none | Macro params; same orphan GC. |

### Comments / tags

| Table | Columns | Role |
|---|---|---|
| `ducklake_tag` | `object_id`, `begin_snapshot`, `end_snapshot`, `key`, `value` | Object tags. `object_id` is **polymorphic** (table id *or* view id — both `DropTables` and `DropViews` expire by it, `:2608`, `:2617`). Split or discriminate in the rebuild. |
| `ducklake_column_tag` | `table_id`, `column_id`, `begin_snapshot`, `end_snapshot`, `key`, `value` | Column comments/tags, versioned. |
| `ducklake_view_column_tag` | `view_id`, `column_name`, `begin_snapshot`, `end_snapshot`, `key`, `value` | **v1.1-dev1 only** (`_v1_1.cpp:29`, created by `MigrateV10` `:439`). Keyed by column *name*, unlike `ducklake_column_tag`. Gated on `SupportsV1_1Metadata()`. |

### Data files and statistics

| Table | Columns | Keys | Role |
|---|---|---|---|
| `ducklake_data_file` | `data_file_id PK`, `table_id`, `begin_snapshot`, `end_snapshot`, `file_order`, `path`, `path_is_relative`, `file_format`, `record_count`, `file_size_bytes`, `footer_size`, `row_id_start`, `partition_id`, `encryption_key`, `mapping_id`, `partial_max`, `row_group_count` (v1.1) | PK | The file manifest; alive for `[begin_snapshot, end_snapshot)`. `row_id_start` anchors the global row-id range; `mapping_id` links name mappings for externally-added files; `partial_max` marks multi-snapshot files. DDL `:223` / v1.1 `:10`. |
| `ducklake_delete_file` | `delete_file_id PK`, `table_id`, `begin_snapshot`, `end_snapshot`, `data_file_id`, `path`, `path_is_relative`, `format`, `delete_count`, `file_size_bytes`, `footer_size`, `encryption_key`, `partial_max`, `row_group_count` (v1.1) | PK | Positional delete files (Parquet or Puffin); each points at one `data_file_id`; at most one live delete file per data file per range. |
| `ducklake_file_column_stats` | `data_file_id`, `table_id`, `column_id`, `column_size_bytes`, `value_count`, `null_count`, `min_value VARCHAR`, `max_value VARCHAR`, `contains_nan`, `extra_stats` | none | Per-file zone maps — the hot pruning table (`GenerateFileColumnStatsCTEBody` `:1636`; PG override [`postgres_metadata_manager.cpp:134`](https://github.com/PostHog/hoglake/blob/eee193b7cb18fc4954df4664c3468d75f2d26ceb/src/metadata_manager/postgres_metadata_manager.cpp#L134)). Min/max as text, cast in generated SQL. Renamed from `..._statistics` in 0.2→0.3 (`:366`). |
| `ducklake_file_variant_stats` | same + `variant_path`, `shredded_type` | none | Per shredded VARIANT sub-path (0.3→0.4, `:383`). |
| `ducklake_table_stats` | `table_id`, `record_count`, `next_row_id`, `file_size_bytes` | none | Table rollup and the **row-id allocator**. Hand-rolled upsert (`UpdateGlobalTableStatsSql` `:4932`). |
| `ducklake_table_column_stats` | `table_id`, `column_id`, `contains_null`, `contains_nan`, `min_value`, `max_value`, `extra_stats` | none | All-files column stats for metadata-answered MIN/MAX (`GlobalTableStatsQuery` `:1185`). |

### Partitioning and sorting

| Table | Columns | Role |
|---|---|---|
| `ducklake_partition_info` | `partition_id`, `table_id`, `begin_snapshot`, `end_snapshot` | Versioned spec header. |
| `ducklake_partition_column` | `partition_id`, `table_id`, `partition_key_index`, `column_id`, `transform` | Ordered keys + transform. 0.1→0.2 rewrote `column_id` from ordinal to field id (`:328`). |
| `ducklake_file_partition_value` | `data_file_id`, `table_id`, `partition_key_index`, `partition_value VARCHAR` | Materialized per-file partition values; sole input to bucket pruning (`BuildBucketPartitionPruningClause` `:1810` — string equality only, for backend portability). |
| `ducklake_sort_info` | `sort_id`, `table_id`, `begin_snapshot`, `end_snapshot` | Versioned sort spec header (0.3→0.4). |
| `ducklake_sort_expression` | `sort_id`, `table_id`, `sort_key_index`, `expression`, `dialect`, `sort_direction`, `null_order` | Ordered sort expressions. |

### Name mapping (externally added files)

| Table | Columns | Role |
|---|---|---|
| `ducklake_column_mapping` | `mapping_id`, `table_id`, `type` | Mapping-strategy header for `add_data_files` registrations. |
| `ducklake_name_mapping` | `mapping_id`, `column_id`, `source_name`, `target_field_id`, `parent_column`, `is_partition` | Source-name → field-id entries, nested via `parent_column`; `is_partition` (0.2→0.3) flags hive-derived columns. Orphan-GC'd (`:5572`). |

### File lifecycle

| Table | Columns | Role |
|---|---|---|
| `ducklake_files_scheduled_for_deletion` | `data_file_id`, `path`, `path_is_relative`, `schedule_start TIMESTAMPTZ` | Deferred physical-delete queue; fed by compaction, delete-file overwrite, expiry; drained by `cleanup_old_files` (`:5017`, `:5158`). |

### Data inlining (dynamic tables)

| Table | Columns | Role |
|---|---|---|
| `ducklake_inlined_data_tables` | `table_id`, `table_name`, `schema_version` | Registry of physical inlined-data tables. |
| `ducklake_inlined_data_<table_id>_<schema_version>` | `_ducklake_row_id`, `_ducklake_begin_snapshot`, `_ducklake_end_snapshot`, + user columns | **Dynamically created per table+schema version — user data living in the catalog DB.** Name `InlinedTableNameFor` `:2816`; DDL `:2824`. The `_ducklake_` prefix is a v1.1 rename (`MigrateInlinedColumnNames` `:449`). |
| `ducklake_inlined_delete_<table_id>` | `file_id`, `row_id`, `begin_snapshot` | Dynamic, per table: small deletes against Parquet files as rows. Created lazily; existence probed by running a SELECT and inspecting the error — flagged fragile in-source (`:3381`). |
| `ducklake_staged_*` (16 names) | see [`ducklake_staged_commit.cpp:27-59`](https://github.com/PostHog/hoglake/blob/eee193b7cb18fc4954df4664c3468d75f2d26ceb/src/storage/ducklake_staged_commit.cpp#L27-L59) | **Not catalog tables** — temp staging for server-side `ducklake_commit()` (quack only). |

---

## 2. Versioning and migration today

**Version location**: one `ducklake_metadata` row, `key='version'`,
value ∈ `0.1, 0.2, 0.3-dev1, 0.3, 0.4-dev1, 0.4, 1.0, 1.1-dev1`
([`common/ducklake_version.cpp:6-55`](https://github.com/PostHog/hoglake/blob/eee193b7cb18fc4954df4664c3468d75f2d26ceb/src/common/ducklake_version.cpp#L6-L55);
`DUCKLAKE_LATEST_VERSION = V1_1_DEV_1`).

**Migration is imperative, forward-only, in the metadata manager.**
Each step is a virtual emitting a multi-statement string ending with
`UPDATE ducklake_metadata SET value='<next>' WHERE key='version'`:
`MigrateV01` `:317` (paths, scope, mapping tables, partition column-id
rewrite); `MigrateV02` `:357` (is_partition, commit-author,
schema_versions, stats-table rename, extra_stats); `MigrateV03` `:373`
(macros, sort tables, variant stats, `partial_file_info`→`partial_max`
via temp table, per-table schema_versions fanout); `MigrateV04` `:426`
(bump only); `MigrateV10` `:435` (row_group_count, view_column_tag,
inlined column-name rename — which runs *first* so a user-column
collision aborts while still consistently at 1.0).

**`allow_failures` / idempotency**: `ExecuteMigration` (`:337`)
textually substitutes `{IF_NOT_EXISTS}`/`{IF_EXISTS}`/`{WHERE_EMPTY}` —
real clauses for re-runnable `-dev` migration, empty otherwise. **There
is no applied-migrations ledger, no advisory lock, and no per-step
transaction boundary.** Mid-migration failure leaves a partially
migrated catalog reconstructable only from the version string.

**Mismatch behavior** ([`ducklake_initializer.cpp:187-286`](https://github.com/PostHog/hoglake/blob/eee193b7cb18fc4954df4664c3468d75f2d26ceb/src/storage/ducklake_initializer.cpp#L187-L286)): newer
catalog → "Cannot downgrade"; older without `AUTOMATIC_MIGRATION` →
error; target resolution: explicit `ducklake_version` wins, else
automatic ⇒ latest, else catalog's own if ≥1.0, else hard error.
Runtime dispatch is by C++ type: `SetVersionedMetadataManager` (`:308`)
swaps in `DuckLakeMetadataManagerV1_1<Backend>`; feature checks via
`SupportsV1_1Metadata()`.

---

## 3. Relationships and invariants

**Snapshot ↔ schema version**: `ducklake_snapshot.schema_version` bumps
only on schema-changing commits (`transaction_state.cpp:1940-1943`);
`ducklake_schema_versions` records per-table begin snapshots, fallback
to the table's own `begin_snapshot` (`:664`).

**Versioned-row pattern** (schema, table, view, column, tags,
partition_info, sort_info, macro, data_file, delete_file): visible at
snapshot S iff

```
S >= begin_snapshot AND (S < end_snapshot OR end_snapshot IS NULL)
```

(exact predicate in `GetFilesForTable` `:1970`, `GetTableSizes` `:5714`,
etc.). Expiry = `UPDATE ... SET end_snapshot = {S} WHERE end_snapshot IS
NULL AND <id> IN (...)` via the `FlushDrop` template (`:2584`).

**Cascade on DROP TABLE** (`DropTables` `:2600`): expires
`ducklake_table` and — for a real drop — `partition_info`, `column`,
`column_tag`, `data_file`, `delete_file`, `tag` (by object_id),
`sort_info`. A rename closes only the table row. Hand-rolled cascade, no
FK. **Note the gap: stats tables are NOT in this cascade** — the
orphaned-stats commit-cost pathology documented in DUCKLAKE_TICKETS.md.

**Row-id ranges**: `ducklake_table_stats.next_row_id` is the per-table
allocator; a file's rows are `row_id_start + file_row_number` unless the
file physically carries a row-id column
(`multi_file_reader.cpp:589-694`). `row_id_start` is nullable
(externally-added files); reading such a row id throws (`:602`).
Flushing inlined data preserves original row ids via
`flush_row_id_start` ([`ducklake_insert.cpp:170`](https://github.com/PostHog/hoglake/blob/eee193b7cb18fc4954df4664c3468d75f2d26ceb/src/storage/ducklake_insert.cpp#L170)).

**Partial files (`partial_max`)**: a file whose rows span several
snapshots stores the max contributing snapshot; at read,
`SetSnapshotFilter` (`:1286`) either admits the whole file
(`partial_max <= S`) or row-filters by the embedded snapshot column.
Changefeeds additionally admit via `partial_max >= <snapshot>`
(`:2059`).

**Deletes → data files**: `delete_file.data_file_id` is a bare BIGINT
(no FK); visibility is a LEFT JOIN with both rows' ranges applied
independently (`:1964-1970`). Inlined deletes are merged into the file
list in-process, not in SQL (`ReadInlinedFileDeletions` `:3265`).

**Path resolution**: `path_is_relative` chains file → table → schema →
catalog `data_path` (nested CASE in `GetKnownFilesForCleanupQuery`
`:5042-5065`). An absolute-path redesign must migrate all three levels
together.

**Orphan-based GC (no FKs)**: `macro_impl`/`macro_parameters` (`:5554`)
and `name_mapping` (`:5572`) cleaned by `NOT EXISTS` sweeps at expiry —
exactly where FK `ON DELETE CASCADE` takes over in the rebuild.

---

## 4. Postgres-specific handling

- Postgres does **not** execute through the attached-catalog path:
  `PostgresMetadataManager::ExecuteQuery`
  ([`postgres_metadata_manager.cpp:83`](https://github.com/PostHog/hoglake/blob/eee193b7cb18fc4954df4664c3468d75f2d26ceb/src/metadata_manager/postgres_metadata_manager.cpp#L83)) substitutes placeholders and
  wraps the batch in `CALL postgres_execute('<catalog>', '<sql>')`
  (`:113`) — native execution inside Postgres. Reads use the base query
  path; two reads rewritten to `postgres_query(...)` pushdown:
  latest-snapshot (`:124`) and the file-column-stats CTE (`:134`).
- `{METADATA_CATALOG}` expands to the **schema identifier only** on
  Postgres (`:108`), plus `{METADATA_SCHEMA_ESCAPED}` for embedding in
  quoted `postgres_query` strings.
- Inlined-data type mapping (`GetColumnTypeInternal` `:51`): DOUBLE→
  `DOUBLE PRECISION`, TINYINT→SMALLINT, UTINYINT/USMALLINT→INTEGER,
  UINTEGER→BIGINT, FLOAT→REAL, **BLOB/VARCHAR→BYTEA** (TEXT can't hold
  NUL), UBIGINT/HUGEINT/UHUGEINT/all DATE-TIME→VARCHAR (range).
  Reads need `TransformInlinedData` (`:147`) to reinterpret BLOB→VARCHAR.
  `TypeIsNativelySupported` (`:14`) false for
  STRUCT/MAP/LIST/unsigned-big/date-time/BLOB/VARCHAR/VARIANT/GEOMETRY;
  `SupportsInlining` (`:44`) additionally refuses VARIANT.
- `SupportsAppender() → false` (SQL-batch path always);
  `MaxIdentifierLength() → 63`; `pg_experimental_filter_pushdown`
  disabled on the metadata connection
  ([`ducklake_transaction.cpp:819-824`](https://github.com/PostHog/hoglake/blob/eee193b7cb18fc4954df4664c3468d75f2d26ceb/src/storage/ducklake_transaction.cpp#L819-L824)); attach scoped with
  `SCHEMA '<metadata_schema>'` when unset
  ([`ducklake_initializer.cpp:51-60`](https://github.com/PostHog/hoglake/blob/eee193b7cb18fc4954df4664c3468d75f2d26ceb/src/storage/ducklake_initializer.cpp#L51-L60)).
- Dialect accommodations in shared code: ANSI `CAST` not `::` (SQLite),
  one UPDATE per column instead of `UPDATE ... FROM (VALUES ...)`
  (`:4957-4976`), bucket pruning by string equality only
  ([`ducklake_metadata_manager.hpp:524-528`](https://github.com/PostHog/hoglake/blob/eee193b7cb18fc4954df4664c3468d75f2d26ceb/src/include/storage/ducklake_metadata_manager.hpp#L524-L528)).
- **Transactions/locking**: one metadata-DB transaction per DuckLake
  transaction ([`ducklake_transaction.cpp:825`](https://github.com/PostHog/hoglake/blob/eee193b7cb18fc4954df4664c3468d75f2d26ceb/src/storage/ducklake_transaction.cpp#L825)), purely optimistic —
  snapshot-id PK collision fails the commit; retry re-runs
  `CheckForConflicts` with jittered exponential backoff. No
  `SELECT ... FOR UPDATE`, no advisory locks, no serializable request.

---

## 5. Write patterns per operation

All builders append into one `batch_queries` blob executed as a single
multi-statement command inside the metadata transaction
(`transaction_state.cpp:1947-1953`). Entry:
`DuckLakeTransactionState::CommitChanges` (`:1575`), preceded by
`InsertSnapshotSql()`, followed by `WriteSnapshotChanges`.

**Typical append**: `ducklake_snapshot` INSERT (`:4416`);
`ducklake_data_file` INSERT×n (`WriteNewDataFilesSqlBatch` `:4165`;
DuckDB-only Appender fast path `:3904`); `file_column_stats` (`:4250`),
`file_variant_stats` (`:4258`), `file_partition_value` (`:4254`);
`table_stats`/`table_column_stats` insert-or-update (`:4932`);
`snapshot_changes` INSERT (`:4427`); inlined path instead writes
`ducklake_inlined_data_*` (`:3062`, `:3144`); `schema_versions` INSERT
per altered table (`:5690`). DDL emitted from `CommitChanges`
(`transaction_state.cpp:1616-1685`).

**Delete/update**: `DeleteOverwrittenDeleteFiles` (`:4272`) DELETEs
superseded delete-file rows + queues their paths; `WriteNewDeleteFiles`
(`:4311`) INSERTs new ones. Small deletes INSERT into
`ducklake_inlined_delete_<t>` (`:3205`) or UPDATE
`_ducklake_end_snapshot` on inlined rows (`:3170`). Dropped files get an
`end_snapshot` UPDATE, not a DELETE (`:4264`, `:4268`).

**Compaction** (`WriteCompactions` `:5283`): merge-adjacent = hard
DELETE of source rows from `data_file`, `file_column_stats`,
`delete_file`, `file_partition_value`, `file_variant_stats` + queue
paths (`:5195`); delete-rewrite = `end_snapshot`/`begin_snapshot`
UPDATEs + exact stats recompute
(`RecomputeGlobalStatsAfterRewrite`, `transaction_state.cpp:1783-1793`).

**Snapshot expiry** (`DeleteSnapshots` `:5295`) — a sequence of
*separately executed* statements (not one batch): delete snapshot rows;
find dead tables; delete unreachable data files + stats + partition
values (queue paths); same for delete files; per-dead-table deletes
across 12 lifecycle tables + `DROP TABLE` of orphaned inlined tables;
delete unreachable schema/view/tag/macro rows; orphan sweeps.
`DropEmptySupersededInlinedTables` (`:5604`) runs separately. Cleanup
drains the deletion queue (`:5017`, `:5158`).

---

## Notes to carry into the hoglake design

- Only **five PKs** in the whole schema (`snapshot`,
  `snapshot_changes`, `schema`, `data_file`, `delete_file`) and **zero**
  FKs, unique constraints, indexes, NOT NULLs (beyond
  `ducklake_metadata`), or checks. Every hot lookup —
  `data_file(table_id, begin, end)`, `file_column_stats(table_id,
  column_id, data_file_id)`, `delete_file(data_file_id)` — is a full
  scan on Postgres unless ops adds indexes by hand (we do).
- Identity allocation is client-side via `next_catalog_id` /
  `next_file_id` / `next_row_id` — not sequences. A server can keep the
  allocator-in-snapshot model (it's what makes OCC conflicts cheap to
  detect) or move to sequences; decide explicitly.
- Every value is inlined into SQL text via `StringUtil::Format` — no
  bound parameters anywhere, capping practical commit-batch size;
  Postgres-native COPY/prepared statements are an easy structural win.
- No migration ledger; no advisory lock during migration; partial
  migration is representable. The hoglake migration story
  (versioned SQL files + applied-migrations table + advisory lock)
  fixes all three.
- The dynamic per-schema-version inlined-data tables are the schema's
  biggest wart (the 112K-table registry incident). DECIDED 2026-09-04:
  inlining is dropped from hoglake entirely; migration flushes any
  residual inlined rows to parquet at cutover (see [README.md](README.md)
  Decisions).

---

## 6. The hoglake schema as built (`hog_*`)

The rebuild the notes above argue for exists. Canonical DDL:
[`server/schema.sql`](server/schema.sql) (complete desired state; a CI
gate asserts fold(migrations) == that file, and a mapper-coverage gate
keeps the Kotlin row mappers in lockstep with the live column set).
Inventory as of 2026-09-06 — every versioned table carries the
`[begin_snapshot, end_snapshot)` pattern with a
`CHECK (end_snapshot > begin_snapshot)`, and every child rides an FK
with `ON DELETE CASCADE`:

| Table | Columns | Integrity highlights | Role |
|---|---|---|---|
| `hog_catalog` | `catalog_id` (identity PK), `name`, `data_path`, allocators (`last_snapshot_id`, `next_table_id`, `next_file_id`, `next_namespace_id`, `next_view_id`, `schema_version`), `snapshot_retention_seconds`, `consumer_floor`, `earliest_snapshot_id`, `earliest_snapshot_time`, `created_at` | `name` UNIQUE + CHECK (lower-case, ≤63); retention CHECK (> 0 or NULL) | Catalog root + id allocators (advanced only inside the commit tail). `earliest_snapshot_time` = when the expiry floor was reached — survives the deleted snapshot rows, cited in 410s |
| `hog_snapshot` | `catalog_id`, `snapshot_id`, `snapshot_time`, `schema_version`, `author`, `commit_message` | PK (catalog_id, snapshot_id); FK→catalog CASCADE | The commit log; ids dense per catalog; `snapshot_time` stamped `clock_timestamp()` inside the serialized tail |
| `hog_snapshot_change` | `catalog_id`, `snapshot_id`, `kind`, `object_id` | `kind` CHECK (closed 10-kind vocabulary); `object_id` NOT NULL (a NULL would be an invisible OCC bypass); conflict index (catalog_id, object_id, kind, snapshot_id) | Typed OCC vocabulary — conflict detection is one indexed anti-join |
| `hog_namespace` | `catalog_id`, `namespace_id`, `name`, `dropped`, `created_at` | PK; identifier CHECK on `name`; live-name partial unique | Namespaces (not snapshot-versioned in v1: rename disallowed, drop requires emptiness) |
| `hog_table` | `catalog_id`, `table_id`, `table_uuid`, `created_snapshot`, `dropped_snapshot`, `next_field_id` | PK (catalog_id, table_id); UNIQUE (catalog_id, table_uuid) | Immutable identity; `table_uuid` survives rename, changes on drop+recreate — the consumer-cursor contract |
| `hog_table_version` | `catalog_id`, `table_id`, `begin_snapshot`, `end_snapshot`, `namespace_id`, `name` | PK (…, begin_snapshot); FKs CASCADE; identifier CHECK; live-name partial unique per namespace | Versioned mutable bits (name, namespace) |
| `hog_column` | `catalog_id`, `table_id`, `field_id`, `begin_snapshot`, `end_snapshot`, `name`, `col_type`, `type_params` (jsonb), `nullable`, `ordinal` | `col_type` CHECK (closed 13-type set, every member Iceberg-mappable); identifier CHECK; `ordinal >= 0` CHECK + live-ordinal partial unique (duplicate live ordinal = parquet-writer corruption vector, refused by the DB) | Versioned columns; `field_id` is the stable parquet binding (`PARQUET:field_id`) |
| `hog_table_stats` | `catalog_id`, `table_id`, `record_count`, `file_size_bytes`, `next_row_id` | CHECKs `>= 0` on both counters (DB backstop against overflow-wrapped rollups/allocators; the commit tail rejects overflow first via `addExact`) | Head-scoped rollup + the row-id allocator |
| `hog_data_file` | `catalog_id`, `data_file_id`, `table_id`, `begin/end_snapshot`, `path`, `file_format`, `record_count`, `file_size_bytes`, `footer_size`, `row_id_start`, `stats_state`, `spec_id`, `explicit_row_ids`, `missing_field_ids` | PK; `stats_state` CHECK (`provided\|pending\|failed`); count/size CHECKs; live + pending partial indexes | The manifest. `row_id_start` NOT NULL (lineage guarantee); `explicit_row_ids` marks compaction outputs carrying `_hog_row_id`; `missing_field_ids` flags id-less files (rename guard) |
| `hog_file_column_stats` | `catalog_id`, `data_file_id`, `field_id`, `value_count`, `null_count`, `nan_count`, `size_bytes`, `lower_bound`, `upper_bound` | PK; count CHECKs; FK→data_file CASCADE | Zone maps; bounds in Iceberg single-value binary (bytea, typed — never text) |
| `hog_consumer_offset` | `catalog_id`, `consumer_id`, `table_uuid`, `committed_snapshot`, `updated_at` | PK (catalog, consumer, table_uuid); consumer_id length CHECK; retention-floor index | Log primitives: offsets keyed by `table_uuid`, respected by expiry's consumer floor; rows outlive table drops by design |
| `hog_partition_spec` / `hog_partition_field` | spec header (`spec_id`, begin/end) + ordered fields (`key_index`, `source_field_id`, `transform`, `transform_param`) | `transform` CHECK (`identity\|bucket\|year\|month\|day\|hour`); bucket-param pairing CHECK; FKs CASCADE | Versioned partition specs; files remember their spec vintage |
| `hog_file_partition_value` | `catalog_id`, `data_file_id`, `key_index`, `value` | PK; FK→data_file CASCADE (the 50M-orphan class from §Notes, made impossible) | Transformed per-file partition values (NULL = null value) |
| `hog_sort_spec` / `hog_sort_field` | header (`sort_id`, begin/end) + ordered fields (`source_field_id`, `direction`, `null_order`) | direction/null_order CHECKs; FKs CASCADE | Versioned sort orders — advisory for writers, binding for compaction rewrites |
| `hog_delete_file` | `catalog_id`, `delete_file_id`, `table_id`, `data_file_id`, `begin/end_snapshot`, `path`, `file_format`, `delete_count`, `file_size_bytes` | one-live-DV-per-data-file partial unique; `delete_count > 0` CHECK; FKs CASCADE | Deletion vectors (puffin `deletion-vector-v1`); supersession end-snapshots, growth-monotone |
| `hog_view` | `catalog_id`, `view_id`, `view_uuid`, `namespace_id`, `name`, `dialect`, `sql`, `begin/end_snapshot` | PK; identifier CHECK; live-name partial unique | Versioned views, SQL stored verbatim |
| `hog_file_removal` | `removal_id` (identity PK), `catalog_id`, `path`, `file_kind`, `reason`, `scheduled_at`, `attempts`, `last_attempt_at`, `drained_at`, `drained_outcome` | `reason` CHECK (`snapshot_expiry\|table_drop_gc\|compaction_staging`); `drained_outcome` CHECK (`deleted\|absent\|registered`) paired-null with `drained_at`; undrained partial drain index | Physical-deletion queue AND forensics ledger: drains soft-delete (outcome + timestamp survive), skips/transients bump `attempts`; `compaction_staging` rows are the compactor's output-path claim tickets, settled `'registered'` on group commit |

What the predecessor survey flagged, answered: every hot lookup is a
partial index on `end_snapshot IS NULL`; every orphan class from §3's
hand-rolled cascades is an FK CASCADE; the stats tables are IN the
cascade; identifiers are CHECK-constrained at the DB; and there are no
dynamic tables of any kind — inlining is gone, so the schema is closed
and enumerable, exactly what this table proves.
