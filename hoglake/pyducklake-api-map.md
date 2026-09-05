# pyducklake API map

Survey of the installed client (`pyducklake-1.0.18`, from viaduck's venv)
for the hoglake control-plane design. Companion to [ducklake-api-map.md](ducklake-api-map.md)
(the extension surface) and [README.md](README.md). Line pointers are relative to
the installed package; caller-side pointers are into `~/src/viaduck`.

Note: [`__init__.py:116`](https://github.com/jghoman/pyducklake/blob/v1.0.18/src/pyducklake/__init__.py#L116) still declares `__version__ = "0.1.0"` — the
in-package version string is stale and unusable for compatibility gating.

Architecture in one line: `Catalog` owns a single in-process
`duckdb.connect()` with `INSTALL ducklake; LOAD ducklake;` plus one
`ATTACH 'ducklake:<uri>'`; **every** object below (Table, DataScan,
Inspect, Maintenance, Transaction, the evolution builders) is a thin
f-string SQL generator that executes against `catalog.connection`. There
is no client/server boundary anywhere — the "client" *is* the DuckDB
process.

---

## 1. Public classes and functions

### Module layout ([`__init__.py:95-114`](https://github.com/jghoman/pyducklake/blob/v1.0.18/src/pyducklake/__init__.py#L95-L114))

| Module | Role |
|---|---|
| [`catalog.py`](https://github.com/jghoman/pyducklake/blob/v1.0.18/src/pyducklake/catalog.py) | Connection + namespace/table/view CRUD, options, transactions |
| [`table.py`](https://github.com/jghoman/pyducklake/blob/v1.0.18/src/pyducklake/table.py) | Table object: snapshots, rollback, write paths, CDC, spec/sort |
| [`scan.py`](https://github.com/jghoman/pyducklake/blob/v1.0.18/src/pyducklake/scan.py) | `DataScan` immutable read builder and output conversions |
| [`inspect.py`](https://github.com/jghoman/pyducklake/blob/v1.0.18/src/pyducklake/inspect.py) | Snapshot/file/partition metadata as Arrow |
| [`maintenance.py`](https://github.com/jghoman/pyducklake/blob/v1.0.18/src/pyducklake/maintenance.py) | Compaction, expiry, cleanup, checkpoint |
| [`cdc.py`](https://github.com/jghoman/pyducklake/blob/v1.0.18/src/pyducklake/cdc.py) | `ChangeSet` result wrapper |
| [`transaction.py`](https://github.com/jghoman/pyducklake/blob/v1.0.18/src/pyducklake/transaction.py), [`view.py`](https://github.com/jghoman/pyducklake/blob/v1.0.18/src/pyducklake/view.py), [`schema.py`](https://github.com/jghoman/pyducklake/blob/v1.0.18/src/pyducklake/schema.py), [`schema_evolution.py`](https://github.com/jghoman/pyducklake/blob/v1.0.18/src/pyducklake/schema_evolution.py), [`partitioning.py`](https://github.com/jghoman/pyducklake/blob/v1.0.18/src/pyducklake/partitioning.py), [`sorting.py`](https://github.com/jghoman/pyducklake/blob/v1.0.18/src/pyducklake/sorting.py), [`types.py`](https://github.com/jghoman/pyducklake/blob/v1.0.18/src/pyducklake/types.py), [`expressions.py`](https://github.com/jghoman/pyducklake/blob/v1.0.18/src/pyducklake/expressions.py), [`exceptions.py`](https://github.com/jghoman/pyducklake/blob/v1.0.18/src/pyducklake/exceptions.py), [`snapshot.py`](https://github.com/jghoman/pyducklake/blob/v1.0.18/src/pyducklake/snapshot.py), [`cli.py`](https://github.com/jghoman/pyducklake/blob/v1.0.18/src/pyducklake/cli.py) | Supporting types/builders |

### `Catalog` ([`catalog.py:198`](https://github.com/jghoman/pyducklake/blob/v1.0.18/src/pyducklake/catalog.py#L198))

`Catalog(name, uri, *, data_path=None, properties=None, encrypted=False)`
— [`catalog.py:213`](https://github.com/jghoman/pyducklake/blob/v1.0.18/src/pyducklake/catalog.py#L213). Opens a fresh in-memory DuckDB connection,
installs/loads the ducklake extension, applies `properties` as bare
`SET k = 'v'`, then `ATTACH 'ducklake:{uri}' AS "{name}" (DATA_PATH
'...', ENCRYPTED)`.

| Member | Signature | Description |
|---|---|---|
| `name` | property → `str` | Catalog (attachment) name. [`catalog.py:249`](https://github.com/jghoman/pyducklake/blob/v1.0.18/src/pyducklake/catalog.py#L249) |
| `encrypted` | property → `bool` | Whether Parquet encryption was requested. [`catalog.py:254`](https://github.com/jghoman/pyducklake/blob/v1.0.18/src/pyducklake/catalog.py#L254) |
| **`connection`** | property → `duckdb.DuckDBPyConnection` | **Public escape hatch to the raw DuckDB connection.** The single biggest obstacle to a control-plane rewrite. [`catalog.py:258`](https://github.com/jghoman/pyducklake/blob/v1.0.18/src/pyducklake/catalog.py#L258) |
| `append_profiler` | property → `AppendProfiler` | Internal profiler shared with tables. [`catalog.py:263`](https://github.com/jghoman/pyducklake/blob/v1.0.18/src/pyducklake/catalog.py#L263) |
| `list_namespaces()` | → `list[str]` | `SELECT schema_name FROM information_schema.schemata`. [`catalog.py:270`](https://github.com/jghoman/pyducklake/blob/v1.0.18/src/pyducklake/catalog.py#L270) |
| `create_namespace(namespace)` | → `None` | Existence check then `CREATE SCHEMA`; raises `NamespaceAlreadyExistsError`. [`catalog.py:279`](https://github.com/jghoman/pyducklake/blob/v1.0.18/src/pyducklake/catalog.py#L279) |
| `create_namespace_if_not_exists(namespace)` | → `None` | `CREATE SCHEMA IF NOT EXISTS`. [`catalog.py:285`](https://github.com/jghoman/pyducklake/blob/v1.0.18/src/pyducklake/catalog.py#L285) |
| `drop_namespace(namespace)` | → `None` | Refuses if it contains tables or views. [`catalog.py:289`](https://github.com/jghoman/pyducklake/blob/v1.0.18/src/pyducklake/catalog.py#L289) |
| `namespace_exists(namespace)` | → `bool` | [`catalog.py:299`](https://github.com/jghoman/pyducklake/blob/v1.0.18/src/pyducklake/catalog.py#L299) |
| `list_tables(namespace="main")` | → `list[tuple[str,str]]` | `(namespace, table)` pairs, `table_type != 'VIEW'`. [`catalog.py:309`](https://github.com/jghoman/pyducklake/blob/v1.0.18/src/pyducklake/catalog.py#L309) |
| `create_table(identifier, schema)` | → `Table` | Renders `CREATE TABLE` from `Schema`, then re-reads canonical types via `information_schema.columns`. [`catalog.py:320`](https://github.com/jghoman/pyducklake/blob/v1.0.18/src/pyducklake/catalog.py#L320) |
| `create_table_if_not_exists(identifier, schema)` | → `Table` | Load-or-create. [`catalog.py:361`](https://github.com/jghoman/pyducklake/blob/v1.0.18/src/pyducklake/catalog.py#L361) |
| `load_table(identifier)` | → `Table` | Existence check + schema rebuild; raises `NoSuchTableError`. [`catalog.py:372`](https://github.com/jghoman/pyducklake/blob/v1.0.18/src/pyducklake/catalog.py#L372) |
| `drop_table(identifier)` | → `None` | [`catalog.py:380`](https://github.com/jghoman/pyducklake/blob/v1.0.18/src/pyducklake/catalog.py#L380) |
| `rename_table(from_id, to_id)` | → `Table` | `ALTER TABLE ... RENAME TO`; cross-namespace rename rejected. [`catalog.py:388`](https://github.com/jghoman/pyducklake/blob/v1.0.18/src/pyducklake/catalog.py#L388) |
| `table_exists(identifier)` | → `bool` | [`catalog.py:404`](https://github.com/jghoman/pyducklake/blob/v1.0.18/src/pyducklake/catalog.py#L404) |
| `create_view(identifier, sql)` / `create_or_replace_view` | → `View` | `CREATE [OR REPLACE] VIEW {fqn} AS {sql}` — **caller-supplied SQL passed through verbatim**. `catalog.py:416,434` |
| `load_view` / `rename_view` / `drop_view` / `list_views` / `view_exists` | | View analogues of the table ops. `catalog.py:445,464,479,487,498` |
| `fully_qualified_name(namespace, table)` | → `str` | `"cat"."ns"."tbl"`. [`catalog.py:521`](https://github.com/jghoman/pyducklake/blob/v1.0.18/src/pyducklake/catalog.py#L521) |
| `fetchall(sql)` | → `list[tuple]` | **Public arbitrary-SQL execution.** [`catalog.py:531`](https://github.com/jghoman/pyducklake/blob/v1.0.18/src/pyducklake/catalog.py#L531) |
| `build_schema_from_describe(ns, table)` | → `Schema` | Reconstructs a `Schema` (with synthesized sequential field IDs) from `information_schema.columns`. [`catalog.py:537`](https://github.com/jghoman/pyducklake/blob/v1.0.18/src/pyducklake/catalog.py#L537) |
| `set_commit_message(message, *, author=None)` | → `None` | `CALL {cat}.set_commit_message(author, msg)`; only recorded inside an explicit transaction. [`catalog.py:559`](https://github.com/jghoman/pyducklake/blob/v1.0.18/src/pyducklake/catalog.py#L559) |
| `set_option(key, value, *, scope=None)` | → `None` | `CALL {cat}.set_option(...)`, optionally table-scoped. Key/scope validated against `^[a-zA-Z_][a-zA-Z0-9_]*$`. [`catalog.py:577`](https://github.com/jghoman/pyducklake/blob/v1.0.18/src/pyducklake/catalog.py#L577) |
| `get_options()` | → `pa.Table` | `SELECT * FROM {cat}.options()`. [`catalog.py:627`](https://github.com/jghoman/pyducklake/blob/v1.0.18/src/pyducklake/catalog.py#L627) |
| `begin_transaction()` | → `Transaction` | [`catalog.py:642`](https://github.com/jghoman/pyducklake/blob/v1.0.18/src/pyducklake/catalog.py#L642) |
| `close()` / `__enter__` / `__exit__` | | Best-effort `DETACH` then `conn.close()` — see §4. [`catalog.py:659`](https://github.com/jghoman/pyducklake/blob/v1.0.18/src/pyducklake/catalog.py#L659) |

Module helpers: `quote_identifier(name)` ([`catalog.py:188`](https://github.com/jghoman/pyducklake/blob/v1.0.18/src/pyducklake/catalog.py#L188)),
`escape_string_literal(value)` ([`catalog.py:193`](https://github.com/jghoman/pyducklake/blob/v1.0.18/src/pyducklake/catalog.py#L193)), and the private
DuckDB-type-string parser `_duckdb_type_to_ducklake` ([`catalog.py:102`](https://github.com/jghoman/pyducklake/blob/v1.0.18/src/pyducklake/catalog.py#L102))
with its DECIMAL/LIST/MAP/STRUCT regexes ([`catalog.py:96-99`](https://github.com/jghoman/pyducklake/blob/v1.0.18/src/pyducklake/catalog.py#L96-L99)).

### `Table` ([`table.py:52`](https://github.com/jghoman/pyducklake/blob/v1.0.18/src/pyducklake/table.py#L52))

`Table(identifier: tuple[str,str], schema: Schema, catalog: Catalog, *,
sort_order: SortOrder | None = None)` — constructed by the catalog, not
by users.

**Identity / metadata**

| Member | Description |
|---|---|
| `name`, `namespace`, `identifier`, `schema`, `catalog`, `fully_qualified_name` | Properties. [`table.py:68-96`](https://github.com/jghoman/pyducklake/blob/v1.0.18/src/pyducklake/table.py#L68-L96) |
| `current_snapshot() -> Snapshot \| None` | Returns the last element of `snapshots()` — i.e. loads *all* snapshots to get one. [`table.py:98`](https://github.com/jghoman/pyducklake/blob/v1.0.18/src/pyducklake/table.py#L98) |
| `snapshots() -> list[Snapshot]` | **Catalog-wide**, not per-table. Reads `__ducklake_metadata_<cat>.ducklake_snapshot` directly. [`table.py:105`](https://github.com/jghoman/pyducklake/blob/v1.0.18/src/pyducklake/table.py#L105) |
| `refresh() -> Table` | Re-reads schema, invalidates sort-order cache. [`table.py:143`](https://github.com/jghoman/pyducklake/blob/v1.0.18/src/pyducklake/table.py#L143) |
| `spec -> PartitionSpec` | Property; 5-way join over ducklake metadata tables. [`table.py:517`](https://github.com/jghoman/pyducklake/blob/v1.0.18/src/pyducklake/table.py#L517) |
| `sort_order -> SortOrder` | Property, memoized in `_sort_order_cache`. [`table.py:582`](https://github.com/jghoman/pyducklake/blob/v1.0.18/src/pyducklake/table.py#L582) |
| `update_spec()`, `update_sort_order()`, `update_schema()` | Return builders. `table.py:574,643,857` |
| `inspect() -> InspectTable`, `maintenance() -> MaintenanceTable` | `table.py:670,678` |

**Read paths**

| Member | Description |
|---|---|
| `scan(row_filter=AlwaysTrue(), selected_fields=("*",), snapshot_id=None, limit=None) -> DataScan` | Str filters are wrapped in `RawSQL` and interpolated unchanged. [`table.py:207`](https://github.com/jghoman/pyducklake/blob/v1.0.18/src/pyducklake/table.py#L207) |
| `to_arrow_dataset(*, snapshot_id=None) -> ds.Dataset` | **Fully materializes** via `scan().to_arrow()` then wraps — not a lazy dataset. [`table.py:651`](https://github.com/jghoman/pyducklake/blob/v1.0.18/src/pyducklake/table.py#L651) |

**Write paths**

| Member | Description |
|---|---|
| `append(df: ArrowCompatible) -> None` | Registers the Arrow table as `_pyducklake_tmp_append`, `INSERT INTO {fqn} SELECT * FROM ...` (+ `ORDER BY` if the table has a sort order), unregisters in `finally`. Wraps the insert in the append profiler. [`table.py:320`](https://github.com/jghoman/pyducklake/blob/v1.0.18/src/pyducklake/table.py#L320) |
| `append_batches(batches, *, schema=None) -> None` | Streaming variant registering a `RecordBatchReader`; no profiling. [`table.py:368`](https://github.com/jghoman/pyducklake/blob/v1.0.18/src/pyducklake/table.py#L368) |
| `overwrite(df, overwrite_filter=AlwaysTrue()) -> None` | `DELETE [WHERE ...]` then `INSERT`. Deliberately does **not** open its own transaction. [`table.py:402`](https://github.com/jghoman/pyducklake/blob/v1.0.18/src/pyducklake/table.py#L402) |
| `delete(delete_filter) -> None` | `DELETE FROM {fqn} [WHERE ...]`; `AlwaysFalse()` is a no-op. [`table.py:446`](https://github.com/jghoman/pyducklake/blob/v1.0.18/src/pyducklake/table.py#L446) |
| `upsert(df, join_cols) -> UpsertResult` | Builds a `MERGE INTO ... WHEN MATCHED THEN UPDATE ... WHEN NOT MATCHED THEN INSERT`. Derives counts via full `COUNT(*)` **before and after** (`table.py:487,509`) — three round trips, and the counts are wrong under concurrency. [`table.py:469`](https://github.com/jghoman/pyducklake/blob/v1.0.18/src/pyducklake/table.py#L469) |
| `add_files(file_paths, *, allow_missing=False, ignore_extra_columns=False)` | One `CALL ducklake_add_data_files(...)` per path. [`table.py:253`](https://github.com/jghoman/pyducklake/blob/v1.0.18/src/pyducklake/table.py#L253) |
| `rollback_to_snapshot(snapshot_id)` | Not a real rollback: copies `AT (VERSION => id)` into a TEMP table, `DELETE` all, re-`INSERT`, drop temp. 4 statements, no transaction. [`table.py:151`](https://github.com/jghoman/pyducklake/blob/v1.0.18/src/pyducklake/table.py#L151) |
| `rollback_to_timestamp(timestamp)` | Picks the latest snapshot ≤ ts (with ad-hoc tz normalization) then delegates. [`table.py:177`](https://github.com/jghoman/pyducklake/blob/v1.0.18/src/pyducklake/table.py#L177) |

`UpsertResult(rows_updated, rows_inserted)` frozen dataclass —
[`table.py:44`](https://github.com/jghoman/pyducklake/blob/v1.0.18/src/pyducklake/table.py#L44). `ArrowCompatible = pa.Table | ArrowStreamExportable`
(PyCapsule protocol) — [`table.py:32-39`](https://github.com/jghoman/pyducklake/blob/v1.0.18/src/pyducklake/table.py#L32-L39); conversion in `_to_arrow_table`
([`table.py:299`](https://github.com/jghoman/pyducklake/blob/v1.0.18/src/pyducklake/table.py#L299)).

**CDC / changefeed wrappers**

| Member | Underlying function | Meta columns projected |
|---|---|---|
| `table_changes(start_snapshot=None, end_snapshot=None, *, start_time=None, end_time=None, columns=None, filter_expr=None) -> ChangeSet` | `ducklake_table_changes` | `snapshot_id, rowid, change_type`. [`table.py:686`](https://github.com/jghoman/pyducklake/blob/v1.0.18/src/pyducklake/table.py#L686) |
| `table_insertions(...same...) -> ChangeSet` | `ducklake_table_insertions` | none. [`table.py:716`](https://github.com/jghoman/pyducklake/blob/v1.0.18/src/pyducklake/table.py#L716) |
| `table_deletions(...same...) -> ChangeSet` | `ducklake_table_deletions` | none. [`table.py:743`](https://github.com/jghoman/pyducklake/blob/v1.0.18/src/pyducklake/table.py#L743) |

All three funnel into `_cdc_query` ([`table.py:796`](https://github.com/jghoman/pyducklake/blob/v1.0.18/src/pyducklake/table.py#L796)). Bound rules enforced
by `_validate_cdc_bounds` ([`table.py:770`](https://github.com/jghoman/pyducklake/blob/v1.0.18/src/pyducklake/table.py#L770)): snapshot and time bounds
cannot be mixed; a start bound is mandatory; a missing `end_snapshot`
resolves via `current_snapshot()` (which loads every snapshot), a missing
`end_time` becomes `CURRENT_TIMESTAMP`.

### `DataScan` ([`scan.py:49`](https://github.com/jghoman/pyducklake/blob/v1.0.18/src/pyducklake/scan.py#L49))

Immutable builder; every method returns a new instance. `__slots__`-based,
holds a `Table | View`.

| Member | Description |
|---|---|
| `filter(expr)` | `And`-combines; str → `RawSQL`. [`scan.py:75`](https://github.com/jghoman/pyducklake/blob/v1.0.18/src/pyducklake/scan.py#L75) |
| `select(*fields)` | Replaces the projection. [`scan.py:96`](https://github.com/jghoman/pyducklake/blob/v1.0.18/src/pyducklake/scan.py#L96) |
| `with_snapshot(id)` / `with_timestamp(ts)` | Time travel; mutually exclusive (raises at SQL-build time, [`scan.py:249`](https://github.com/jghoman/pyducklake/blob/v1.0.18/src/pyducklake/scan.py#L249)). `scan.py:107,118` |
| `with_limit(n)` | [`scan.py:132`](https://github.com/jghoman/pyducklake/blob/v1.0.18/src/pyducklake/scan.py#L132) |
| `to_arrow() -> pa.Table` | `execute(sql).fetch_arrow_table()`. [`scan.py:145`](https://github.com/jghoman/pyducklake/blob/v1.0.18/src/pyducklake/scan.py#L145) |
| `to_pandas()` / `to_polars()` / `to_ray()` | Optional-dependency conversions, guarded by `importlib.util.find_spec`. `scan.py:151,178,193` |
| `to_duckdb(*, connection=None) -> DuckDBPyRelation` | Returns a live relation on the catalog connection (or a supplied one) — leaks lazy execution state to the caller. [`scan.py:163`](https://github.com/jghoman/pyducklake/blob/v1.0.18/src/pyducklake/scan.py#L163) |
| `to_arrow_batch_reader() -> pa.RecordBatchReader` | Streaming read. [`scan.py:172`](https://github.com/jghoman/pyducklake/blob/v1.0.18/src/pyducklake/scan.py#L172) |
| `to_arrow_dataset()` | Materializes then wraps. [`scan.py:207`](https://github.com/jghoman/pyducklake/blob/v1.0.18/src/pyducklake/scan.py#L207) |
| `count() -> int` | Separate `SELECT COUNT(*)` statement. [`scan.py:216`](https://github.com/jghoman/pyducklake/blob/v1.0.18/src/pyducklake/scan.py#L216) |

SQL generation: `_build_sql` / `_build_count_sql` / `_format_table_ref`
([`scan.py:224-265`](https://github.com/jghoman/pyducklake/blob/v1.0.18/src/pyducklake/scan.py#L224-L265)). `RawSQL(sql)` is a public `BooleanExpression`
passthrough ([`scan.py:31`](https://github.com/jghoman/pyducklake/blob/v1.0.18/src/pyducklake/scan.py#L31)).

### `ChangeSet` ([`cdc.py:18`](https://github.com/jghoman/pyducklake/blob/v1.0.18/src/pyducklake/cdc.py#L18))

`ChangeSet(arrow_table: pa.Table, change_type_col: str | None =
"change_type")`. Pure client-side Arrow post-processing — no SQL, so it
transfers unchanged to a service client.

`to_arrow()`, `to_pandas()`, `num_rows`, `column_names`, `inserts()`,
`deletes()`, `update_preimages()`, `update_postimages()`,
`updates() -> list[(old,new)]` (pairs pre/post images by `rowid` in
Python, [`cdc.py:87`](https://github.com/jghoman/pyducklake/blob/v1.0.18/src/pyducklake/cdc.py#L87)), `has_updates()`, `summary() -> dict[str,int]`.
Methods needing `change_type` raise on `table_insertions`/
`table_deletions` results ([`cdc.py:58`](https://github.com/jghoman/pyducklake/blob/v1.0.18/src/pyducklake/cdc.py#L58)).

### `MaintenanceTable` ([`maintenance.py:22`](https://github.com/jghoman/pyducklake/blob/v1.0.18/src/pyducklake/maintenance.py#L22)) — via `table.maintenance()`

Every method is **catalog-scoped** despite hanging off a table
(explicitly noted at `maintenance.py:37,63`).

| Method | SQL |
|---|---|
| `compact(*, min_file_size=None, max_file_size=None, max_compacted_files=None)` | `CALL ducklake_merge_adjacent_files('{cat}', ...::UBIGINT)`. [`maintenance.py:28`](https://github.com/jghoman/pyducklake/blob/v1.0.18/src/pyducklake/maintenance.py#L28) |
| `rewrite_data_files(*, delete_threshold=None)` | `CALL ducklake_rewrite_data_files(...)`. [`maintenance.py:55`](https://github.com/jghoman/pyducklake/blob/v1.0.18/src/pyducklake/maintenance.py#L55) |
| `expire_snapshots(*, older_than=None, versions=None, dry_run=False)` | `CALL ducklake_expire_snapshots(...)`; `older_than`/`versions` mutually exclusive. [`maintenance.py:76`](https://github.com/jghoman/pyducklake/blob/v1.0.18/src/pyducklake/maintenance.py#L76) |
| `cleanup_files(*, older_than=None, dry_run=False)` | `CALL ducklake_cleanup_old_files(...)`. [`maintenance.py:114`](https://github.com/jghoman/pyducklake/blob/v1.0.18/src/pyducklake/maintenance.py#L114) |
| `delete_orphaned_files(*, dry_run=False)` | `CALL ducklake_delete_orphaned_files(...)`. [`maintenance.py:141`](https://github.com/jghoman/pyducklake/blob/v1.0.18/src/pyducklake/maintenance.py#L141) |
| `checkpoint()` | `CHECKPOINT "{catalog}"` — despite the docstring claiming "all maintenance operations sequentially". [`maintenance.py:155`](https://github.com/jghoman/pyducklake/blob/v1.0.18/src/pyducklake/maintenance.py#L155) |

`validate_older_than(value)` ([`maintenance.py:16`](https://github.com/jghoman/pyducklake/blob/v1.0.18/src/pyducklake/maintenance.py#L16)) is the only input
validation: a `YYYY-MM-DD[ HH:MM:SS[.f]]` regex ([`maintenance.py:13`](https://github.com/jghoman/pyducklake/blob/v1.0.18/src/pyducklake/maintenance.py#L13)).

**No `flush_inlined` helper exists** anywhere in the package.
Inlined-data flushing/reading is entirely absent from the public API;
consumers reach into the metadata DB for it (see §2, caller-side).

### `InspectTable` ([`inspect.py:30`](https://github.com/jghoman/pyducklake/blob/v1.0.18/src/pyducklake/inspect.py#L30)) — via `table.inspect()`

| Method | SQL |
|---|---|
| `snapshots() -> pa.Table` | `SELECT * FROM "{cat}".snapshots() ORDER BY snapshot_id`. [`inspect.py:39`](https://github.com/jghoman/pyducklake/blob/v1.0.18/src/pyducklake/inspect.py#L39) |
| `history() -> pa.Table` | Same, `DESC`. [`inspect.py:94`](https://github.com/jghoman/pyducklake/blob/v1.0.18/src/pyducklake/inspect.py#L94) |
| `files(snapshot_id=None, snapshot_time=None) -> pa.Table` | `SELECT * FROM ducklake_list_files('{cat}','{tbl}', schema := '{ns}'[, snapshot_version := N][, snapshot_time := '...'])`. Args mutually exclusive. [`inspect.py:51`](https://github.com/jghoman/pyducklake/blob/v1.0.18/src/pyducklake/inspect.py#L51) |
| `partitions() -> pa.Table` | Two hand-rolled queries against `__ducklake_metadata_*` tables; builds the Arrow table column-by-column in Python. [`inspect.py:102`](https://github.com/jghoman/pyducklake/blob/v1.0.18/src/pyducklake/inspect.py#L102) |

`_to_arrow_table(result)` ([`inspect.py:16`](https://github.com/jghoman/pyducklake/blob/v1.0.18/src/pyducklake/inspect.py#L16)) normalizes `.arrow()`
returning either a `Table` or a `RecordBatchReader` "depending on the
version" — a defensive version-drift workaround duplicated in
[`catalog.py:633`](https://github.com/jghoman/pyducklake/blob/v1.0.18/src/pyducklake/catalog.py#L633) and [`table.py:848`](https://github.com/jghoman/pyducklake/blob/v1.0.18/src/pyducklake/table.py#L848).

### `Transaction` ([`transaction.py:16`](https://github.com/jghoman/pyducklake/blob/v1.0.18/src/pyducklake/transaction.py#L16))

Constructor immediately issues `BEGIN TRANSACTION` on the catalog
connection ([`transaction.py:43`](https://github.com/jghoman/pyducklake/blob/v1.0.18/src/pyducklake/transaction.py#L43)). `load_table(identifier)` just
delegates to the catalog — **there is no transactional isolation of
objects**; the "transaction" is purely the connection's session state.
`commit()` / `rollback()` issue `COMMIT` / `ROLLBACK`, guarded by
`_committed`/`_rolled_back` flags. Context manager auto-commits on clean
exit, rolls back on exception ([`transaction.py:71`](https://github.com/jghoman/pyducklake/blob/v1.0.18/src/pyducklake/transaction.py#L71)).

### `View` ([`view.py:21`](https://github.com/jghoman/pyducklake/blob/v1.0.18/src/pyducklake/view.py#L21))

`View(identifier, schema, sql, catalog)`. Properties `name`,
`namespace`, `identifier`, `schema`, `sql_text`,
`fully_qualified_name`, `catalog`. Methods `scan(row_filter,
selected_fields, limit)` (no time travel), `to_arrow()`, `to_pandas()`,
`to_arrow_dataset()`, `refresh()`.

### Builders

- **`UpdateSchema`** ([`schema_evolution.py:51`](https://github.com/jghoman/pyducklake/blob/v1.0.18/src/pyducklake/schema_evolution.py#L51)) — `add_column(name,
  field_type, doc=None, required=False)`, `drop_column(name)`,
  `rename_column(name, new_name)`, `update_column(name, new_type)`,
  `set_nullability(name, required)`, `commit()`. Each change becomes one
  `ALTER TABLE` statement executed in a plain loop
  ([`schema_evolution.py:102-104`](https://github.com/jghoman/pyducklake/blob/v1.0.18/src/pyducklake/schema_evolution.py#L102-L104)) — **no transaction wrapping, so a
  mid-list failure leaves partial evolution**. `doc` is accepted and
  silently dropped (never appears in `_change_to_sql`,
  [`schema_evolution.py:109`](https://github.com/jghoman/pyducklake/blob/v1.0.18/src/pyducklake/schema_evolution.py#L109)). Context manager auto-commits.
- **`UpdateSpec`** ([`partitioning.py:186`](https://github.com/jghoman/pyducklake/blob/v1.0.18/src/pyducklake/partitioning.py#L186)) — `add_field(source_column,
  transform=IDENTITY)`, `clear()`, `commit()` → `ALTER TABLE ... SET
  PARTITIONED BY (...)` / `RESET PARTITIONED BY`. Transforms:
  `IDENTITY, YEAR, MONTH, DAY, HOUR` ([`partitioning.py:119-123`](https://github.com/jghoman/pyducklake/blob/v1.0.18/src/pyducklake/partitioning.py#L119-L123));
  `PartitionField`, `PartitionSpec`, `UNPARTITIONED`.
- **`UpdateSortOrder`** ([`sorting.py:82`](https://github.com/jghoman/pyducklake/blob/v1.0.18/src/pyducklake/sorting.py#L82)) — `add_field(source_column,
  direction=ASC, null_order=NULLS_LAST)`, `clear()`, `commit()` →
  `ALTER TABLE ... SET SORTED BY (...)` / `RESET SORTED BY`.
  `SortDirection`, `NullOrder`, `SortField.to_sql()`, `SortOrder`,
  `UNSORTED`.

### Value types

- **`Schema`** ([`schema.py:73`](https://github.com/jghoman/pyducklake/blob/v1.0.18/src/pyducklake/schema.py#L73)) with `Schema.of(*fields | dict)`
  ([`schema.py:103`](https://github.com/jghoman/pyducklake/blob/v1.0.18/src/pyducklake/schema.py#L103)), `fields`, `schema_id`, `find_field`, `find_type`,
  `find_column_name`, `column_names()`, `field_ids()`,
  `highest_field_id`, `as_struct()`, `as_arrow()`, `select(*names)`.
  Helpers `required(name, type, doc=None)` / `optional(...)`
  (`schema.py:21,46`).
- **`Snapshot`** ([`snapshot.py:12`](https://github.com/jghoman/pyducklake/blob/v1.0.18/src/pyducklake/snapshot.py#L12)) frozen dataclass: `snapshot_id`,
  `timestamp`, `schema_version`, `changes`, `author`, `commit_message`.
  `Table.snapshots()` only ever populates the first three
  ([`table.py:134`](https://github.com/jghoman/pyducklake/blob/v1.0.18/src/pyducklake/table.py#L134)); the rest are reachable only through
  `inspect().snapshots()` as raw Arrow.
- **[`types.py`](https://github.com/jghoman/pyducklake/blob/v1.0.18/src/pyducklake/types.py)** — `DucklakeType` hierarchy (primitives + `DecimalType`,
  `ListType`, `MapType`, `StructType`, `NestedField`) plus
  `arrow_type_to_ducklake`, `ducklake_type_to_arrow`,
  `ducklake_type_to_sql` (`types.py:362,417,495`).
- **[`expressions.py`](https://github.com/jghoman/pyducklake/blob/v1.0.18/src/pyducklake/expressions.py)** — `BooleanExpression` ABC with `to_sql()`;
  `AlwaysTrue`/`AlwaysFalse` factories, `Not`, `And`, `Or`, `EqualTo`,
  `NotEqualTo`, `GreaterThan(OrEqual)`, `LessThan(OrEqual)`, `In`,
  `NotIn`, `IsNull`, `NotNull`, `IsNaN`, `NotNaN`, `Reference`. Values
  rendered by `_format_value` ([`expressions.py:37`](https://github.com/jghoman/pyducklake/blob/v1.0.18/src/pyducklake/expressions.py#L37)), columns by
  `_quote_column` ([`expressions.py:55`](https://github.com/jghoman/pyducklake/blob/v1.0.18/src/pyducklake/expressions.py#L55)). This tree is a pure SQL-string
  compiler — a service needs an equivalent wire representation.
- **[`exceptions.py`](https://github.com/jghoman/pyducklake/blob/v1.0.18/src/pyducklake/exceptions.py)** — `DucklakeError` base; `NoSuchTableError`,
  `TableAlreadyExistsError`, `NoSuchNamespaceError`,
  `NamespaceAlreadyExistsError`, `NamespaceNotEmptyError`,
  `CommitFailedError` (**defined but never raised anywhere in the
  package**), `NoSuchViewError`, `ViewAlreadyExistsError`.

### CLI ([`cli.py`](https://github.com/jghoman/pyducklake/blob/v1.0.18/src/pyducklake/cli.py))

`pyducklake --uri ... [--catalog NAME] [--data-path P] [--output
text|json]` with subcommands `list-namespaces`, `create-namespace`,
`drop-namespace`, `list-tables`, `describe`, `schema`, `spec`,
`snapshots`, `files`, `compact`, `expire-snapshots`, `checkpoint`,
`version`. The CLI bypasses `MaintenanceTable` and issues the `CALL`
statements itself (`cli.py:276,325,340`), and its `checkpoint` runs only
`ducklake_merge_adjacent_files` ([`cli.py:340`](https://github.com/jghoman/pyducklake/blob/v1.0.18/src/pyducklake/cli.py#L340)) — divergent from
`MaintenanceTable.checkpoint()`.

---

## 2. Raw-SQL and private-API leak surface

### 2a. Inside pyducklake — everything is raw SQL

There is no non-SQL code path. The list below covers only what a
control-plane service must replace with *dedicated endpoints* (ordinary
DDL/DML the service naturally owns).

**Direct reads of the DuckLake metadata schema
(`__ducklake_metadata_<catalog>.*`)** — bypass every ducklake-provided
function and bind to the physical catalog schema. Highest-risk leaks; a
service must expose first-class equivalents.

| Location | Method | Metadata tables read |
|---|---|---|
| [`table.py:113-119`](https://github.com/jghoman/pyducklake/blob/v1.0.18/src/pyducklake/table.py#L113-L119) | `Table.snapshots()` | `ducklake_snapshot` (`snapshot_id, snapshot_time, schema_version`, full scan + `ORDER BY`) |
| [`table.py:533-552`](https://github.com/jghoman/pyducklake/blob/v1.0.18/src/pyducklake/table.py#L533-L552) | `Table.spec` | `ducklake_partition_column` ⋈ `ducklake_partition_info` ⋈ `ducklake_table` ⋈ `ducklake_schema` ⋈ `ducklake_column`, filtered `end_snapshot IS NULL` |
| [`table.py:604-620`](https://github.com/jghoman/pyducklake/blob/v1.0.18/src/pyducklake/table.py#L604-L620) | `Table._fetch_sort_order()` | `ducklake_sort_expression` ⋈ `ducklake_sort_info` ⋈ `ducklake_table` ⋈ `ducklake_schema` |
| [`inspect.py:111-142`](https://github.com/jghoman/pyducklake/blob/v1.0.18/src/pyducklake/inspect.py#L111-L142) | `InspectTable.partitions()` | `ducklake_table` ⋈ `ducklake_schema` (for `table_id`), then `ducklake_partition_column` ⋈ `ducklake_partition_info` |

All four swallow `duckdb.CatalogException`/`BinderException` and return
an empty/UNPARTITIONED/UNSORTED result (`table.py:120,553,621`;
[`inspect.py:143`](https://github.com/jghoman/pyducklake/blob/v1.0.18/src/pyducklake/inspect.py#L143)) — a metadata-schema rename or version change degrades
**silently** rather than failing.

**`information_schema` reads** (portable but still schema
introspection): `list_namespaces` ([`catalog.py:272`](https://github.com/jghoman/pyducklake/blob/v1.0.18/src/pyducklake/catalog.py#L272)), `namespace_exists`
([`catalog.py:300`](https://github.com/jghoman/pyducklake/blob/v1.0.18/src/pyducklake/catalog.py#L300)), `list_tables` ([`catalog.py:311`](https://github.com/jghoman/pyducklake/blob/v1.0.18/src/pyducklake/catalog.py#L311)), `table_exists`
([`catalog.py:406`](https://github.com/jghoman/pyducklake/blob/v1.0.18/src/pyducklake/catalog.py#L406)), `list_views` ([`catalog.py:489`](https://github.com/jghoman/pyducklake/blob/v1.0.18/src/pyducklake/catalog.py#L489)), `view_exists`
([`catalog.py:501`](https://github.com/jghoman/pyducklake/blob/v1.0.18/src/pyducklake/catalog.py#L501)), `load_view`'s definition fetch ([`catalog.py:455`](https://github.com/jghoman/pyducklake/blob/v1.0.18/src/pyducklake/catalog.py#L455)),
and `build_schema_from_describe` ([`catalog.py:539`](https://github.com/jghoman/pyducklake/blob/v1.0.18/src/pyducklake/catalog.py#L539)) — the last is the
**sole** source of table schemas, including field IDs, which are
*fabricated* client-side from `ordinal_position`
([`catalog.py:547-554`](https://github.com/jghoman/pyducklake/blob/v1.0.18/src/pyducklake/catalog.py#L547-L554)) and therefore do not match DuckLake's real
column IDs.

**Ducklake extension `CALL`/table functions** — a service could proxy
one-for-one:

| Location | Statement |
|---|---|
| [`catalog.py:229`](https://github.com/jghoman/pyducklake/blob/v1.0.18/src/pyducklake/catalog.py#L229) | `INSTALL ducklake; LOAD ducklake;` |
| [`catalog.py:237`](https://github.com/jghoman/pyducklake/blob/v1.0.18/src/pyducklake/catalog.py#L237) | `SET {key} = '{value}'` per property (pre-ATTACH) |
| [`catalog.py:239-247`](https://github.com/jghoman/pyducklake/blob/v1.0.18/src/pyducklake/catalog.py#L239-L247) | `ATTACH 'ducklake:{uri}' AS "{name}" (DATA_PATH '...', ENCRYPTED)` |
| [`catalog.py:570`](https://github.com/jghoman/pyducklake/blob/v1.0.18/src/pyducklake/catalog.py#L570) | `CALL {cat}.set_commit_message(author, msg)` |
| [`catalog.py:618-625`](https://github.com/jghoman/pyducklake/blob/v1.0.18/src/pyducklake/catalog.py#L618-L625) | `CALL {cat}.set_option(key, value[, table_name := , schema := ])` |
| [`catalog.py:633`](https://github.com/jghoman/pyducklake/blob/v1.0.18/src/pyducklake/catalog.py#L633) | `SELECT * FROM {cat}.options()` |
| [`catalog.py:672`](https://github.com/jghoman/pyducklake/blob/v1.0.18/src/pyducklake/catalog.py#L672) | `DETACH "{name}"` |
| [`table.py:293-295`](https://github.com/jghoman/pyducklake/blob/v1.0.18/src/pyducklake/table.py#L293-L295) | `CALL ducklake_add_data_files(cat, tbl, path[, schema :=][, allow_missing :=][, ignore_extra_columns :=])` |
| [`table.py:843`](https://github.com/jghoman/pyducklake/blob/v1.0.18/src/pyducklake/table.py#L843) | `SELECT {cols} FROM ducklake_table_changes\|insertions\|deletions(cat, ns, tbl, start, end) [WHERE {filter_expr}]` |
| `inspect.py:47,98` | `SELECT * FROM "{cat}".snapshots() ORDER BY snapshot_id [DESC]` |
| [`inspect.py:83-90`](https://github.com/jghoman/pyducklake/blob/v1.0.18/src/pyducklake/inspect.py#L83-L90) | `SELECT * FROM ducklake_list_files(cat, tbl, schema := [, snapshot_version :=][, snapshot_time :=])` |
| `maintenance.py:52,73,111,138,152,159` | `ducklake_merge_adjacent_files`, `ducklake_rewrite_data_files`, `ducklake_expire_snapshots`, `ducklake_cleanup_old_files`, `ducklake_delete_orphaned_files`, `CHECKPOINT "{cat}"` |
| `cli.py:276,325,340` | Same CALLs, re-issued directly from the CLI |

**Connection-object side effects no RPC can carry over:**

| Location | What it does |
|---|---|
| `table.py:346,366` / `392,400` / `422,442` / `489,507` | `conn.register(...)` / `conn.unregister(...)` of a local Arrow table or `RecordBatchReader` under `_pyducklake_tmp_*`, then `INSERT ... SELECT * FROM <registered>`. **The entire write path depends on zero-copy in-process Arrow registration.** A service needs an upload/stream protocol here. |
| [`table.py:170-175`](https://github.com/jghoman/pyducklake/blob/v1.0.18/src/pyducklake/table.py#L170-L175) | `rollback_to_snapshot` creates/drops `TEMP TABLE _pyducklake_rollback` on the shared connection |
| [`scan.py:170`](https://github.com/jghoman/pyducklake/blob/v1.0.18/src/pyducklake/scan.py#L170) | `to_duckdb()` hands back a `DuckDBPyRelation` bound to the connection |
| `transaction.py:43,53,60` | `BEGIN TRANSACTION` / `COMMIT` / `ROLLBACK` as bare session statements |
| [`profiling.py:112-160`](https://github.com/jghoman/pyducklake/blob/v1.0.18/src/pyducklake/profiling.py#L112-L160) | `SET enable_profiling`, `PRAGMA disable_profiling`, `conn.get_profiling_information("json")`, etc. |

**Interpolation / injection notes** (relevant to any wire-protocol
design): `Catalog._execute` accepts `params` ([`catalog.py:525`](https://github.com/jghoman/pyducklake/blob/v1.0.18/src/pyducklake/catalog.py#L525)) but **no
call site anywhere in the package passes them** — 100% of statements are
f-string built. Only `escape_string_literal`/`quote_identifier` guard
user values, and three inputs bypass them entirely:
`create_view`/`create_or_replace_view`'s `sql` (`catalog.py:431,442`),
`DataScan` string filters via `RawSQL` ([`scan.py:82`](https://github.com/jghoman/pyducklake/blob/v1.0.18/src/pyducklake/scan.py#L82)), and CDC
`filter_expr` ([`table.py:846`](https://github.com/jghoman/pyducklake/blob/v1.0.18/src/pyducklake/table.py#L846)).

### 2b. Caller-side leaks (viaduck) — the ones a service must absorb

These matter more than the internal ones: they show which parts of the
API were *insufficient* in production.

| Location | Leak | Reason given in code |
|---|---|---|
| [`viaduck/source.py:453-459`](https://github.com/PostHog/viaduck/blob/5933ba531ab08440ca9e3e2ea77f6fcd0c51a287/viaduck/source.py#L453-L459) | `table._catalog.connection.execute('SELECT MAX(snapshot_id) FROM "__ducklake_metadata_<cat>".ducklake_snapshot')` | `current_snapshot()` loads every snapshot row into Python; "on a catalog with hundreds of thousands of snapshots ... RSS climbs linearly per poll cycle" |
| [`source.py:524-531`](https://github.com/PostHog/viaduck/blob/5933ba531ab08440ca9e3e2ea77f6fcd0c51a287/viaduck/source.py#L524-L531) | `MIN(snapshot_id)` direct | "safe on large catalogs where `table.snapshots()` would OOM" |
| [`source.py:540-546`](https://github.com/PostHog/viaduck/blob/5933ba531ab08440ca9e3e2ea77f6fcd0c51a287/viaduck/source.py#L540-L546) | `SELECT MIN(snapshot_id), MAX(snapshot_id)` in one statement | "DuckDB's postgres scanner does no aggregate pushdown" |
| [`source.py:486-513`](https://github.com/PostHog/viaduck/blob/5933ba531ab08440ca9e3e2ea77f6fcd0c51a287/viaduck/source.py#L486-L513) | snapshot_time lookup by id + `MIN(snapshot_time)` clamp | No pyducklake API for it |
| [`source.py:663`](https://github.com/PostHog/viaduck/blob/5933ba531ab08440ca9e3e2ea77f6fcd0c51a287/viaduck/source.py#L663) | Hand-written `ducklake_table_insertions`/`deletions` LEFT-JOIN branches executed separately and `pa.concat_tables`'d | Replaces `Table.table_changes()` entirely — see §4 |
| [`viaduck/destination.py:524`](https://github.com/PostHog/viaduck/blob/5933ba531ab08440ca9e3e2ea77f6fcd0c51a287/viaduck/destination.py#L524) | `SELECT 1 FROM {fqn} LIMIT 1` on the raw connection | Emptiness probe; `scan().count()` is a full count |
| `viaduck/main.py:535,1893,1906,2261,2347` | Passes `._catalog.connection` around as a general-purpose read/progress connection | Progress polling (`conn.query_progress()`), feed reads |
| [`viaduck/feed.py:62-682`](https://github.com/PostHog/viaduck/blob/5933ba531ab08440ca9e3e2ea77f6fcd0c51a287/viaduck/feed.py#L62-L682) | **Bypasses DuckDB entirely** — dedicated `psycopg` connection to the metadata Postgres, reading `ducklake_metadata`, `ducklake_table`, `ducklake_schema`, `ducklake_snapshot`, `ducklake_data_file`, `ducklake_inlined_data_tables` and the per-table inlined stores | Parquet-file planning + inlined-row reads; requires custom index `viaduck_data_file_range` ([`feed.py:245-253`](https://github.com/PostHog/viaduck/blob/5933ba531ab08440ca9e3e2ea77f6fcd0c51a287/viaduck/feed.py#L245-L253)); pins `REPEATABLE READ` ([`feed.py:587`](https://github.com/PostHog/viaduck/blob/5933ba531ab08440ca9e3e2ea77f6fcd0c51a287/viaduck/feed.py#L587)) |
| [`source.py:273`](https://github.com/PostHog/viaduck/blob/5933ba531ab08440ca9e3e2ea77f6fcd0c51a287/viaduck/source.py#L273), [`source.py:197-227`](https://github.com/PostHog/viaduck/blob/5933ba531ab08440ca9e3e2ea77f6fcd0c51a287/viaduck/source.py#L197-L227) | Raw connection used to re-apply and verify `ducklake_*` settings post-ATTACH | See §4 |

The inlined-data reads in [`feed.py`](https://github.com/PostHog/viaduck/blob/5933ba531ab08440ca9e3e2ea77f6fcd0c51a287/viaduck/feed.py) are precisely the "flush_inlined"
capability the package does not expose.

---

## 3. Configuration surface

### What the package offers

| Knob | Where | Behavior |
|---|---|---|
| `uri` | [`catalog.py:239`](https://github.com/jghoman/pyducklake/blob/v1.0.18/src/pyducklake/catalog.py#L239) | Interpolated into `ATTACH 'ducklake:{uri}'`. Backends per [`__init__.py:48-53`](https://github.com/jghoman/pyducklake/blob/v1.0.18/src/pyducklake/__init__.py#L48-L53): DuckDB file, postgres, mysql, sqlite. **Not escaped** — a URI containing `'` breaks the statement. |
| `data_path` | [`catalog.py:242`](https://github.com/jghoman/pyducklake/blob/v1.0.18/src/pyducklake/catalog.py#L242) | `DATA_PATH '<escaped>'` ATTACH option. Only settable at construction. |
| `encrypted` | [`catalog.py:244`](https://github.com/jghoman/pyducklake/blob/v1.0.18/src/pyducklake/catalog.py#L244) | Adds bare `ENCRYPTED` to the ATTACH options. |
| `properties: dict[str,str]` | [`catalog.py:232-237`](https://github.com/jghoman/pyducklake/blob/v1.0.18/src/pyducklake/catalog.py#L232-L237) | Each entry becomes `SET {key} = '{escaped value}'` **before** ATTACH, in dict order. Keys validated against `^[a-zA-Z_][a-zA-Z0-9_]*$`. The **only** channel for S3/storage config — no typed storage-properties model, no credential-chain support, no secret redaction. |
| `pyducklake_profile_append` | `profiling.py:15,64-79` | The one pyducklake-owned property; stripped out of the DuckDB property loop. Conflicts with any manual profiling setting → `ValueError`. |
| Runtime ducklake options | [`catalog.py:577`](https://github.com/jghoman/pyducklake/blob/v1.0.18/src/pyducklake/catalog.py#L577) | Post-attach `CALL {cat}.set_option(...)`; `scope="schema.table"` for table-level. Readable via `get_options()`. |

### What the package does **not** offer

- **No retry logic of any kind** (zero hits for retry/backoff). OCC
  handling is left to the extension's internal loop and the caller.
- **No connection pooling or caching.** Each `Catalog(...)` is a
  brand-new `duckdb.connect()` ([`catalog.py:228`](https://github.com/jghoman/pyducklake/blob/v1.0.18/src/pyducklake/catalog.py#L228)) with unbounded
  lifetime until `close()`.
- **No timeouts, cancellation, health check, or reconnect.**
- **No external-file-cache handling.**
- **`close()` is the only lifecycle hook** (with the DETACH dance, §4).

### Caller-side configuration a control-plane service would need to own

[`viaduck/source.py:56-116`](https://github.com/PostHog/viaduck/blob/5933ba531ab08440ca9e3e2ea77f6fcd0c51a287/viaduck/source.py#L56-L116) (`_CONNECTION_DEFAULTS`) is the de-facto
missing configuration layer:

| Setting | Value | Rationale |
|---|---|---|
| `pg_connection_limit` | `64` | Parallel CDC scan claimed every pool slot; concurrent state write hit the 30s pool timeout. |
| `arrow_large_buffer_size` | `true` | 32-bit Arrow string offsets cap an exported buffer at 2 GiB; a 12.5M-file seed scan died. |
| `enable_progress_bar(_print)` | `true`/`false` | DuckDB only computes progress when the bar is on. |
| **`enable_external_file_cache`** | **`false`** | "climbed at ~5 GiB/h ... hit the container's 48 GiB limit in ~8h". |
| `ducklake_max_retry_count`/`ducklake_retry_wait_ms`/`ducklake_retry_backoff` | `20`/`50`/`1.0` | Extension retry loop is "the ONLY layer that can retry a commit cheaply"; backoff=1.0 defeats the uncapped exponent ([`ducklake_transaction.cpp:2669`](https://github.com/PostHog/hoglake/blob/eee193b7cb18fc4954df4664c3468d75f2d26ceb/src/storage/ducklake_transaction.cpp#L2669)). |
| `temp_directory` | fresh `mkdtemp` per connection | Spill filenames carry no instance token; two instances sharing a temp dir corrupt temp accounting and crash. `sweep_spill_dirs()` at process start. |

Connection caching/pooling also lives caller-side: destination catalog
pool with force-eviction on write-retry, source-connection recycle on an
interval — explicitly labelled memory-leak workarounds.

---

## 4. Known workarounds, hacks, and version pins

### In-package

| Location | Workaround |
|---|---|
| [`catalog.py:659-675`](https://github.com/jghoman/pyducklake/blob/v1.0.18/src/pyducklake/catalog.py#L659-L675) | **`close()` DETACHes before closing** — `conn.close()` alone skips the extension's per-connection cleanup when the catalog carries conflict residue, "orphaning the allocation for the life of the process". Failing DETACH swallowed at debug. |
| [`inspect.py:16-27`](https://github.com/jghoman/pyducklake/blob/v1.0.18/src/pyducklake/inspect.py#L16-L27) + 2 copies | `.arrow()` returns `Table` or `RecordBatchReader` "depending on the version" — normalize-with-`isinstance`, copy-pasted three times. |
| `table.py:120,553,621`; [`inspect.py:143`](https://github.com/jghoman/pyducklake/blob/v1.0.18/src/pyducklake/inspect.py#L143) | Blanket catch-and-return-empty around every `__ducklake_metadata_*` query — silent wrong answers on schema drift. |
| [`catalog.py:400`](https://github.com/jghoman/pyducklake/blob/v1.0.18/src/pyducklake/catalog.py#L400) | `RENAME TO` must take a bare name, not an FQN. |
| [`table.py:189-199`](https://github.com/jghoman/pyducklake/blob/v1.0.18/src/pyducklake/table.py#L189-L199) | Ad-hoc tz normalization in `rollback_to_timestamp`. |
| [`table.py:412-417`](https://github.com/jghoman/pyducklake/blob/v1.0.18/src/pyducklake/table.py#L412-L417) | `overwrite()` avoids its own transaction — DuckDB's lack of nested transactions leaks into the API contract. |
| `maintenance.py:37,63` | Compaction/rewrite are catalog-level ("ducklake does not support per-table compaction") behind a table-scoped API. |
| [`profiling.py:104-149`](https://github.com/jghoman/pyducklake/blob/v1.0.18/src/pyducklake/profiling.py#L104-L149) | All profiling best-effort warn-and-continue; capture only on successful write. |
| [`scan.py:44-46`](https://github.com/jghoman/pyducklake/blob/v1.0.18/src/pyducklake/scan.py#L44-L46) | `_is_always_true` equality-compares "without importing private class". |
| [`catalog.py:546-554`](https://github.com/jghoman/pyducklake/blob/v1.0.18/src/pyducklake/catalog.py#L546-L554) | **Field IDs invented client-side** from `ordinal_position` — not DuckLake's real column IDs. Hoglake must decide whether to preserve the fiction. |

### Caller-side (viaduck) — workarounds *against* pyducklake

| Location | Workaround |
|---|---|
| `source.py:16-22,560-680` | **`VIADUCK_CDC_SPLIT_READ`**: the `ducklake_table_changes` macro (UNION ALL of two LEFT JOINs) "deterministically stalls the DuckDB executor" on a large fast-advancing catalog; each branch alone is fine, so viaduck runs them as two statements and concats in Arrow — reimplementing `table_changes()` by hand. |
| [`source.py:178-227`](https://github.com/PostHog/viaduck/blob/5933ba531ab08440ca9e3e2ea77f6fcd0c51a287/viaduck/source.py#L178-L227) | **Verified extension-SET**: fork engine + stock extension have mismatched setting registries — `SET ducklake_max_retry_count` wrote into the core `TimeZone` slot (`pytz.UnknownTimeZoneError('20')` fleet-wide). Fix: SET post-attach one at a time, diff `duckdb_settings()` before/after, revert on collateral change. |
| [`source.py:240-251`](https://github.com/PostHog/viaduck/blob/5933ba531ab08440ca9e3e2ea77f6fcd0c51a287/viaduck/source.py#L240-L251) | No `SET TimeZone` pin: aliasing is bidirectional — `'UTC'` landed in a UINT64 retry slot, fleet-wide zero-flush outage. |
| [`source.py:253-268`](https://github.com/PostHog/viaduck/blob/5933ba531ab08440ca9e3e2ea77f6fcd0c51a287/viaduck/source.py#L253-L268) | Bounded retry around `Catalog(...)` construction: concurrent CREATE TABLE races surface PG duplicate-key errors through IF NOT EXISTS. |
| [`source.py:316-322`](https://github.com/PostHog/viaduck/blob/5933ba531ab08440ca9e3e2ea77f6fcd0c51a287/viaduck/source.py#L316-L322) | Bare `duckdb.connect()` + httpfs read pool for the parquet data plane, not pyducklake — "a DuckDB connection has a single query slot, so N parallel unit reads need N connections". |
| [`source.py`](https://github.com/PostHog/viaduck/blob/5933ba531ab08440ca9e3e2ea77f6fcd0c51a287/viaduck/source.py) `_UNREPLICATABLE_TYPES` | Column types excluded because the `_duckdb_type_to_ducklake` parser raises on them, which is fatal at `load_table`. |
| [`config.py:307`](https://github.com/PostHog/viaduck/blob/5933ba531ab08440ca9e3e2ea77f6fcd0c51a287/viaduck/config.py#L307), [`destination.py:555-592`](https://github.com/PostHog/viaduck/blob/5933ba531ab08440ca9e3e2ea77f6fcd0c51a287/viaduck/destination.py#L555-L592) | Populated-table safety gate around `SET PARTITIONED BY`. |
| [`feed.py:245-253`](https://github.com/PostHog/viaduck/blob/5933ba531ab08440ca9e3e2ea77f6fcd0c51a287/viaduck/feed.py#L245-L253) | Custom index `viaduck_data_file_range` on `ducklake_data_file` required for feed planning. |

### Version pins ([`viaduck/pyproject.toml:18-78`](https://github.com/PostHog/viaduck/blob/5933ba531ab08440ca9e3e2ea77f6fcd0c51a287/pyproject.toml#L18-L78))

- `pyducklake>=1.0.18`; `duckdb==1.5.5` hard pin (extension-channel
  freeze rationale in the comment); `pytz` as a declared runtime dep
  (duckdb invokes it parsing timestamps); a dependency-cooldown
  carve-out for `pyducklake` with a fixed date.

---

## Implications for the hoglake client (summary)

1. **The write path is the hardest boundary.** All writes depend on
   `conn.register()` of in-process Arrow objects. Every one needs an
   upload or Arrow-Flight-style stream — or the hoglake model (client
   writes parquet, registers footers) which removes the problem.
2. **Four metadata queries bind directly to the physical catalog
   schema** (snapshots, partition spec, sort order, partition info) and
   must become service endpoints.
3. **`Catalog.connection`, `Catalog.fetchall`, and `DataScan.to_duckdb`
   are public** and load-bearing for viaduck — deprecating them is a
   breaking-change negotiation, not a refactor.
4. **Three known-costly APIs need cheaper endpoints on day one**:
   `current_snapshot()` (loads all snapshots), `scan().count()` (full
   count; `upsert` calls it twice), `table_changes()` (the
   executor-stall macro viaduck already replaced by hand).
5. **Filters, view SQL, and CDC `filter_expr` are raw SQL strings** — a
   wire protocol needs a serialized expression tree (the
   [`expressions.py`](https://github.com/jghoman/pyducklake/blob/v1.0.18/src/pyducklake/expressions.py) AST is already the right shape) or an explicit
   trusted-SQL-fragment contract.
6. **Retry, pooling, cache control, spill isolation, and
   setting-application safety are all absent from the package** and
   reimplemented in the caller — the control-plane service is the
   natural owner of every one.
