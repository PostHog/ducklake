# A DuckDB Read Extension for Hoglake (2026-09-05)

Design sketch. DuckDB returns as **a client, never the engine the
catalog lives inside** (README non-goal preserved). The extension talks
REST to the control plane and reads parquet from object storage itself;
Postgres is never visible. The model is the DuckLake extension's shape
with the client-library parts amputated: `ducklake` proves the UX,
hoglake removes everything that made it a fleet liability (embedded
catalog SQL, per-process metadata caches, fork drift).

## Interface

```sql
INSTALL hoglake; LOAD hoglake;
SET hoglake_endpoint = 'https://hoglake.internal';
SET hoglake_token    = '…';                    -- static bearer until auth lands

ATTACH 'hoglake:analytics' AS lake;            -- catalog name, not a URI

SELECT * FROM lake.events.events
WHERE ts >= '2026-09-01';                      -- stats pruning pushes the range

SELECT * FROM lake.events.events AT SNAPSHOT 412;        -- time travel
SELECT * FROM lake.events.events AT TIMESTAMP '2026-09-04T00:00Z';

SELECT * FROM hoglake_changes('lake', 'events', 'events', 400, 412);
```

Write path (if any) is `COPY ... TO 's3://…'` (parquet, field ids
stamped) + a commit function shipping footer stats — the pyhoglake
payload over the same endpoint. **Read-only v1** matches the Trino
connector's posture and the Iceberg-facade decision; the extension is a
reader first.

## Anatomy

Standard DuckDB extension layout (out-of-tree, `extension-ci-tools` —
the enclosing repo is literally a DuckDB extension, so the build
machinery is a known quantity):

```
hoglake-ext/
├── src/
│   ├── hoglake_extension.cpp        -- entry point, catalog registration
│   ├── hoglake_catalog.cpp          -- duckdb::Catalog impl (schema/table list)
│   ├── hoglake_table_entry.cpp      -- binds a hoglake table at a snapshot
│   ├── hoglake_scan.cpp             -- TableFunction: plans via /scan, reads parquet
│   ├── hoglake_client.cpp           -- REST client (httplib/cpr), typed DTOs
│   ├── hoglake_transaction.cpp      -- snapshot pinning, MVCC integration
│   └── hoglake_storage.cpp          -- optional duckdb::StorageExtension for ATTACH
├── src/include/…
├── test/sql/…                       -- sqllogictests against a mock server
└── CMakeLists.txt
```

Two integration surfaces DuckDB offers, both used:

| Surface | Role |
|---|---|
| `duckdb::Catalog` (+ `StorageExtension` for `ATTACH 'hoglake:'`) | Namespaces → schemas, tables → table entries, snapshot binding, secret/config plumbing |
| `TableFunction` per table | Scan planning → HTTP `/scan` → file list → DuckDB's native parquet reader with projection/filter pushdown; DVs applied as a filter operator |

The REST client is the only hoglake-specific code: thin, typed,
generated-or-mirrored DTOs from `openapi/hoglake.yaml`. Everything
downstream of the file list is stock DuckDB machinery — parquet scan,
S3 filesystem, stats-based filter pushdown.

## Request flow (a query)

```
ATTACH hoglake:analytics
  → GET /v1/catalogs/analytics                      (exists, head, data_path)
  → GET /v1/catalogs/analytics/namespaces           (schema list)
  → per schema: GET …/tables                        (table list)

Query on lake.events.events:
  1. Bind:     GET …/namespaces/events/tables/events?snapshot=<pinned>
               → columns (field ids, types), spec, table_uuid
  2. Plan:     POST …/tables/events/scan  { snapshot, columns, filter? }
               → files: path, record_count, row_id_start,
                 explicit_row_ids, stats_state, bounds, live DV
  3. Read:     DuckDB parquet reader per file; zone-map pruning from
               hoglake stats (skip before open) + parquet's own row-group
               pruning after open
  4. DV mask:  files carrying a live DV get the positional mask applied
               (row_id_start + offset → bitmap) before filters
  5. pending/failed stats files: always scanned (never pruned) —
               correctness by construction
```

Snapshot pinning: one snapshot id per DuckDB transaction (first table
touch pins it, `BEGIN`/`SET TRANSACTION`-style), so multi-table reads
are consistent — the property DuckLake got via being in the same
Postgres transaction, reproduced with a pinned `read_snapshot`.

## What maps well

| Hoglake concept | DuckDB mechanism |
|---|---|
| Snapshot / time travel | `AT SNAPSHOT n` → `?snapshot=` param; timestamp → server resolves |
| Versioned schema | Bind-time column fetch at the pinned snapshot |
| Stats pruning | File-skip on bounds before open; parquet RG pruning after |
| DV deletes | Positional bitmap mask in the scan operator (DuckDB has the machinery from its own delete vectors) |
| `explicit_row_ids` files | Read the reserved `_hog_row_id` column when CDC-shaped queries need lineage |
| Changefeed | `hoglake_changes()` table function → `GET …/changes` → file ranges + DV diffs, Arrow IPC on the wire when the service speaks it |
| Partition transforms | Server hands transformed values; reader never computes them |

## What does not map / costs

- **Snapshot pinning vs. DuckDB's transaction model**: DuckDB MVCC is
  per-database; a hoglake catalog is external. Pin-at-first-touch is
  the pragmatic answer (same as ducklake's attach semantics), but a
  long-lived DuckDB session holds an old snapshot — hedgerow-style
  consumers must re-ATTACH or accept the pin. Document it.
- **Filter pushdown fidelity**: hoglake stats prune at *file*
  granularity; DuckDB wants to push expression subtrees. The /scan
  endpoint takes an optional filter, but the honest v1 is "prune on
  bounds client-side from the returned stats" — the server sending the
  full file list + stats and DuckDB doing the pruning keeps the server
  dumb and the client fast. Server-side filter evaluation is a later
  optimization.
- **DV read path**: hoglake DVs are puffin-dv files; the extension must
  read them (small bitmaps) — one native reader, no server involvement.
  Cheap; the format is simple.
- **Secrets**: DuckDB's `SECRET` mechanism maps cleanly onto
  endpoint+token; the S3 side reuses DuckDB's native S3 secret. Two
  credential domains (control plane, object store) — matches the
  credential-vending future.
- **No catalog mutation from DuckDB in v1**: DDL stays on the REST API.
  Tempting to add `CREATE TABLE` sugar; resist until the facade's
  write posture is decided — two write paths into one catalog is the
  DuckLake disease.

## Why this is worth having (and when)

It's the **debugger's REPL** and the third leg of the reader story:
Trino for federation at scale, the web console for operators, DuckDB
for "attach and poke at it" — the workflow the DuckLake extension
accidentally made excellent. It also dogfoods the read API surface
(scan/files/changes) from a third runtime (the first two being the
Trino connector and pyhoglake's readers), which historically is how
you find out your API has an assumption only its first client shares.

Prerequisites, in order: the Iceberg-facade snapshot-pinning semantics
(they're the same problem), `/scan` carrying stats in the response
(it does), and the auth decision (even static tokens) so the extension
isn't born with "whoever has the endpoint" as its security model.
Effort estimate: a focused engineer-weeks project for read-only —
the hard parts (parquet, S3, MVCC, extension CI) are all reused; the
hoglake-specific surface is one REST client and one catalog adapter.
