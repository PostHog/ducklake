# pyhoglake

Python client for [hoglake](../README.md), the Postgres-native
lakehouse-catalog control plane. A **thin API wrapper**: no embedded
engine, no SQL, no direct catalog-database access — ever. The client
writes parquet to object storage itself and registers it with the
control plane via footer-shipping commits.

## Install

```sh
# from this directory
uv add pyhoglake            # or: pip install .
```

Dependencies: `httpx`, `pyarrow`. Development uses the flox env in this
directory:

```sh
flox activate -- uv sync
flox activate -- uv run pytest                      # unit + integration
flox activate -- uv run pytest -m "not integration" # unit only
```

Integration tests need a live server (`HOGLAKE_URL`, default
`http://localhost:8080`) and S3 credentials (`HOGLAKE_S3_ENDPOINT`,
`HOGLAKE_S3_ACCESS_KEY`, `HOGLAKE_S3_SECRET_KEY`); they skip cleanly
when the server is unreachable.

## Quickstart — the append path end to end

```python
import pyarrow as pa
from pyhoglake import HoglakeClient, S3Config

client = HoglakeClient(
    "http://localhost:8080",
    s3=S3Config(
        access_key="hoglake",
        secret_key="hoglake123",
        endpoint_override="http://localhost:19000",  # MinIO; omit for AWS
        region="us-east-1",
    ),
)

catalog = client.create_catalog("demo", "s3://my-bucket/demo/")
ns = catalog.create_namespace("analytics")

table = ns.create_table(
    "events",
    pa.schema(
        [
            pa.field("id", pa.int64(), nullable=False),
            pa.field("name", pa.string()),
            pa.field("amount", pa.decimal128(10, 2)),
        ]
    ),
)

# THE writer path: writes one parquet file (with catalog field ids
# embedded in the parquet schema) to
#   s3://my-bucket/demo/data/analytics/events/<uuid>.parquet
# extracts per-column footer stats (value/null counts, Iceberg
# single-value binary min/max bounds), and registers the file in one
# commit. The server never opens the file.
result = table.append(
    pa.table({"id": [1, 2], "name": ["a", None], "amount": [None, None]}),
    author="me",
    message="first batch",
)
print(result.snapshot_id)

# reads are metadata-only planning; you fetch the parquet yourself
for f in table.files():
    print(f.path, f.record_count, f.stats_state, f.row_id_start)

# changefeed + consumer offsets
plan = table.changes(from_snapshot=0)
catalog.commit_offset("my-consumer", table.table_uuid, plan.to_snapshot)
catalog.offset("my-consumer", table.table_uuid)  # one offset; None if unset
```

More surface:

```python
from pyhoglake import ops

table.alter([ops.add_column("score", pa.float64())])  # schema evolution
table.info(snapshot=5)  # time travel
table.files(at_timestamp=some_datetime)  # by timestamp
table.append(big_table, deferred_stats=True)  # register as pending
catalog.set_retention(7 * 86400, consumer_floor=True)  # retention policy
catalog.expire()
catalog.cleanup()  # maintenance sweeps
ns.create_view("v", "SELECT 1", dialect="trino")
for s in catalog.snapshots(limit=1000):
    ...  # auto-paginated
for s in catalog.snapshots(before=head + 1):
    ...  # descending walk
    # (mutually exclusive
    # with non-zero after)
```

## Configuration

| What | How |
|---|---|
| Server | `HoglakeClient(base_url, timeout=30.0)` — `/v1` is appended |
| Object store | `S3Config(access_key, secret_key, endpoint_override, region, allow_bucket_creation)`; the write path uses `pyarrow.fs.S3FileSystem` (path-style with an endpoint override) |
| Errors | Typed: `NotFoundError`, `AlreadyExistsError`, `CommitConflictError` (`retryable=True` — refresh read snapshot and retry), `ValidationError`, `OffsetRegressionError`, `ExpiredError` (410 — reconcile from a full scan), `IncarnationChangedError` (the append incarnation guard, enforced server-side at commit, see below), `MalformedResponseError` (every wire-parse failure — a structurally defective response body, an unexpected redirect (3xx is never success), a field of the wrong shape — one exception type naming the model and field), all under `HoglakeError` |

## Type mapping

| pyarrow | hoglake |
|---|---|
| `bool_` | `boolean` |
| `int32` / `int64` | `int` / `long` |
| `float32` / `float64` | `float` / `double` |
| `string` / `large_string` | `string` |
| `binary` / `large_binary` | `binary` |
| `date32` | `date` |
| `time64("us")` | `time` |
| `timestamp("us")` / `timestamp("us", tz)` | `timestamp` / `timestamptz` |
| `decimal128(p, s)` | `decimal` (`type_params: {precision, scale}`) |
| `binary(16)` (fixed) or `pa.uuid()` | `uuid` — 16 big-endian bytes, i.e. `uuid.UUID(...).bytes` |

Anything else is rejected with an error listing the supported set.

Identifiers (namespace/table/view/column names) must match
`^[A-Za-z_][A-Za-z0-9_-]{0,127}$` — the server 422s anything else, and
the constraint is also a CHECK in its schema; catalog names are
stricter (lower-case start, max 63).

Column names starting with `_hog` are additionally reserved for hoglake
internals (`_hog_row_id` is compaction's row-id carrier) — the server
422s them at create/add/rename, and the client fast-fails them at
table-create and append time before any request or upload;
namespace/table/view names are not affected. Deletion-vector positions
are per-file **physical row ordinals** (0-based position within that
parquet file), not row ids.

## Partitioned writes — client-side transforms, fanout appends

When a table has a live partition spec, `Table.append` computes each
row's partition tuple client-side (the server never opens data files),
splits the batch by tuple, writes **one parquet file per partition**,
and registers them all in **one atomic commit** — each file carrying its
`partition_values` (transformed values as strings, by key_index). The
coarse-grained layout the near-term consumers use is months per team:

```python
from pyhoglake import ops

table = ns.create_table(
    "events",
    pa.schema(
        [
            pa.field("team_id", pa.int64(), nullable=False),
            pa.field("ts", pa.timestamp("us")),
            pa.field("payload", pa.string()),
        ]
    ),
)
fid = {c.name: c.field_id for c in table.columns}
table.alter(
    [
        ops.set_partition_spec(
            [
                ops.partition_field(fid["team_id"], "identity"),
                ops.partition_field(fid["ts"], "month"),
            ]
        )
    ]
)

# a batch spanning 3 months x 2 teams -> 6 files, ONE commit
res = table.append(batch)
for f in res.files:  # AppendResult exposes the computed tuples
    print(f.path, f.record_count, f.partition_values)
    # ('101', '672') = identity(team_id)=101, month(ts)=2026-01
    # (months are epoch-relative ints per Iceberg: 672 = (2026-1970)*12)
```

Transforms (`pyhoglake.transforms`) follow the Iceberg spec exactly and
are pinned against its published test vectors:

| Transform | Semantics |
|---|---|
| `identity` | the source value |
| `year` / `month` / `day` / `hour` | epoch-relative ints (years/months since 1970, days/hours since the epoch, floored — pre-1970 is negative) |
| `bucket` (param N) | `(murmur3_x86_32(iceberg_encode(v)) & Integer.MAX_VALUE) % N` — bit-compatible with Iceberg/server bucketing |
| `truncate` (param W) | ints floored to a multiple of W; strings to W codepoints (client-side, ahead of server vocabulary support) |

A null source value yields a null partition value forming its own
partition group (Iceberg semantics). The tuple is computed under the
table's **current** spec (re-resolved pre-flight); if the spec changes
before the commit lands, the server refuses the commit (409 concurrent
DDL / 422 arity mismatch) — the client never silently recomputes under
a different spec. Grouping runs arrow-native where possible and per
*unique* value (never per row) otherwise. Compaction groups only within
`(spec_id, partition_values)` server-side, so partition-local file
layout is preserved end to end.

## The append incarnation guard — atomic at commit

Commit payloads are addressed by **(namespace, table) name**, so an
append lands on whatever table currently holds that name. If the table
is dropped and recreated (a new `table_uuid`) between your resolve and
your append, a naive client would write into the new incarnation's
history without noticing.

`Table.append(..., expected_table_uuid=...)` guards against this
**atomically, at commit time**. Every commit carries an
`expected_table_uuid` field (default: the `table_uuid` the `Table`
object was resolved as; pass one explicitly to pin a specific
incarnation), and the server rejects the whole commit with 409 — zero
writes — when the live table's uuid differs. The client maps that 409
(its message says "the table was recreated") to
`IncarnationChangedError`; ordinary commit conflicts remain
`CommitConflictError` (retryable). There is no window in which a
recreated table can accept rows from a guarded append.

The client also keeps **one** cheap pre-flight re-resolve before the
parquet upload. That is purely an optimization — it fast-fails an
already-dead incarnation before paying for the S3 write — not the
safety mechanism. A commit-time refusal orphans the uploaded parquet
(cleanup's problem, never the catalog's). A refused append never
rebases the `Table` object's pinned identity, so a blind retry trips
the guard again rather than silently adopting the new incarnation.

To opt out entirely (name-only resolution), pass
`expected_table_uuid=pyhoglake.UNGUARDED`; the commit then carries no
`expected_table_uuid` field and no pre-flight check runs.

## Not in 0.1

Deletion-vector writes and Iceberg-facade reads.

## Comparison with pyiceberg

pyhoglake follows pyiceberg's ergonomics where they fit, but the
architecture differs on purpose: the catalog is a *service* — pyhoglake
is a thin REST client that owns only the writer path (parquet + footer
stats), and everything transactional happens server-side.

| Feature | pyhoglake | pyiceberg |
|---------|-----------|-----------|
| **Metadata storage** | Postgres, behind a REST control plane (never touched by clients) | Files (JSON, Avro manifests) via catalog |
| **Catalog backends** | 1 (the hoglake service) | 7 (REST, Hive, Glue, DynamoDB, SQL, BigQuery, In-memory) |
| **Commit protocol** | Footer-shipping: client writes parquet, ships stats, server registers + OCC | Client writes manifests + metadata, catalog swaps pointer |
| **Deferred statistics** | Yes (`deferred_stats=True`; server hydrates async) | No |
| **Field IDs in files** | Written (`PARQUET:field_id`), verified round-trip | Written |
| **Append** | Yes (`Table.append(arrow_table)`) | Yes |
| **Streaming/batch inputs** | Arrow Table (batching via `row_group_size`) | Arrow only |
| **Row-level deletes** | Deletion vectors (server-registered, superseding, conflict-checked); DV *writing* not yet in the client | Position/equality delete files (v0.7+, partial) |
| **Upsert / merge / overwrite** | No (append + DV only, by design v1) | Overwrite yes; upsert yes (v0.7+) |
| **Schema evolution** | Typed ops: add, drop, rename, promote (int→long, float→double), rename table, set partition spec | Add, drop, rename, widen, reorder, union-by-name |
| **Partitioning** | identity, bucket (Murmur3, Iceberg-compatible), year/month/day/hour — spec DDL + partitioned appends (client-side transforms, one file per partition, one commit) | identity, bucket, truncate, year/month/day/hour |
| **Time travel** | Snapshot id or timestamp, on tables/files/scan | Snapshot id, ref name, or timestamp |
| **Snapshot branches/tags** | No | Yes |
| **Change data capture** | First-class: `changes()` plan (files + DVs per snapshot range), 410 on expired ranges | Not implemented |
| **Consumer offsets** | First-class catalog state, monotonic, retention-aware | Not implemented |
| **Row lineage** | Server-assigned contiguous row-id ranges, never reused | Row lineage (v3 spec, partial) |
| **Retention / expiry** | Catalog property; consumer-offset floor; client can trigger + tune | Expire snapshots (limited) |
| **Table maintenance** | Server-side (expiry, cleanup, compaction — DV-aware, row-id-preserving) — client just triggers | Client-side, limited |
| **Views** | Full CRUD (SQL stored verbatim + dialect) | Not implemented |
| **Multi-table transactions** | Yes — one commit may span tables (atomic) | Single-table only |
| **Concurrency** | Server-side OCC; typed 409s with `retryable=True`; appends never conflict with appends | Optimistic, client-side, no retry |
| **Metrics/observability** | Server `/metrics` + audit log; client stays thin | N/A |
| **Zero-infrastructure quickstart** | No — requires the service (docker compose up) | Yes with SQL/memory catalogs |
| **Package size** | 2 deps (httpx, pyarrow) | ~200MB with PyArrow + optional deps |
