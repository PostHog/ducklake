# hoglake Trino connector

A native, **read-only** Trino connector for the hoglake control plane.
Trino plans from hoglake's REST metadata and reads the parquet directly
from object storage; the catalog server is never in the data path.

This is the *interim* path. The long-term plan is Trino's stock Iceberg
connector pointed at a hoglake Iceberg REST facade
([iceberg-federation.md](../../iceberg-federation.md)); this connector
exists because the facade is not built yet and compute needs to read the
lake today. Its read contract is written down as §0 of
[trino-integration.md](../../trino-integration.md) — that document is
the charter, this README is the operator's guide.

## How it fits together

```
Trino coordinator                          hoglake server
  getTableHandle ──── GET /v1/catalogs/{c} ─────► head_snapshot_id
                 ──── GET .../tables/{t}?snapshot=N ─► columns, table_uuid
  getSplits      ──── GET .../scan?snapshot=N ──────► ScanFile[]
                                                 (data file + DV, if any)
Trino workers
  page source    ──── S3 GetObject (ranged) ───────► parquet row groups
```

Two planes, two libraries:

| Plane | What | Library |
|---|---|---|
| Control | catalog/schema/table listing, column resolution, scan planning | `java.net.http` + Jackson against the REST API |
| Data | row-group reads, predicate-free column decode | `trino-parquet` + `trino-filesystem-s3` — the same libraries the bundled hive/iceberg/delta connectors use |

The connector is plain Java (the Trino SPI is Java-first). Kotlin is
applied to the test source set only, so an integration test can boot the
hoglake server in-process from the root project's classes.

## The read contract

Four commitments, each pinned by tests:

1. **One query, one snapshot.** `getTableHandle` resolves the catalog
   head *once* and pins the snapshot id, `table_uuid`, and resolved
   column list into the table handle. Later metadata calls serve from the
   handle; split planning scans at the pinned snapshot. A concurrent
   commit — including a DROP+CREATE incarnation change — can never
   rebind an in-flight query: it reads the analyzed incarnation's
   consistent data, or fails typed. (Before this, a drop+recreate between
   analysis and execution silently returned the *new* incarnation's
   values under the old column names, because field ids restart at 1.)
2. **Typed failures**, never a bare `GENERIC_INTERNAL_ERROR`:

   | Condition | Error |
   |---|---|
   | control plane unreachable / 5xx | `HOGLAKE_CATALOG_UNAVAILABLE` (EXTERNAL) |
   | malformed or unexpected response | `HOGLAKE_INVALID_RESPONSE` (EXTERNAL) |
   | pinned snapshot below the expiry floor (410) | `HOGLAKE_SNAPSHOT_EXPIRED` (EXTERNAL) |
   | configured `hoglake.catalog` missing | `HOGLAKE_CATALOG_NOT_FOUND` (USER_ERROR) |
   | table/schema vanished mid-query | the SPI's `TableNotFoundException` / `SchemaNotFoundException` |

   `HOGLAKE_SNAPSHOT_EXPIRED` is the expiry invariant's engine-side face:
   re-run the query, never silently skip.
3. **Deletion-vector refusal at planning.** A scan pairing any data file
   with a live deletion vector is refused in `getSplits`, before a single
   split reaches the engine — so no partial results precede the failure.
   (v1 reads no row-level deletes at all; a page-source guard remains as
   worker-side defense in depth.)
4. **Id-authoritative column binding.** Columns bind by parquet field id,
   never by name. Renames are therefore free for files that carry field
   ids. The hazard for files *without* them is closed catalog-side: field
   ids are a registration contract, and the server refuses `RENAME
   COLUMN` while any id-less file is live in the table.

## Configuration

`etc/catalog/<name>.properties` in the Trino installation:

```properties
connector.name=hoglake
hoglake.uri=http://hoglake:8080          # REST base, no /v1 suffix
hoglake.catalog=lake                     # the hoglake catalog to expose

hoglake.client.request-timeout=2m        # optional, airlift duration (500ms/30s/2m/1h)
hoglake.s3.endpoint=http://minio:9000    # optional; empty = AWS default resolution
hoglake.s3.region=us-east-1
hoglake.s3.access-key=...
hoglake.s3.secret-key=...
hoglake.s3.path-style=true
```

One hoglake catalog per Trino catalog; hoglake namespaces appear as Trino
schemas. Configuration is validated **at catalog load**, not per query:
`hoglake.uri` must have an http/https scheme and a parseable host,
trailing slashes are collapsed, and an empty `hoglake.catalog` is
rejected. A typo fails the catalog, not every subsequent query.

Credentials fall back to the AWS default provider chain when unset. Note
that `trino-filesystem-s3` never sends anonymous requests, so a public
bucket with no credentials fails at first read rather than at load.

## Type mapping

| hoglake | Trino |
|---|---|
| `boolean` | `BOOLEAN` |
| `int` / `long` | `INTEGER` / `BIGINT` |
| `float` / `double` | `REAL` / `DOUBLE` |
| `string` / `binary` | `VARCHAR` / `VARBINARY` |
| `date` | `DATE` |
| `time` | `TIME(6)` |
| `timestamp` / `timestamptz` | `TIMESTAMP(6)` / `TIMESTAMP(6) WITH TIME ZONE` |
| `uuid` | `UUID` |
| `decimal(p,s)` | `DECIMAL(p,s)` |

`int32`-written columns later promoted to `long` coerce correctly on
read. A narrowing mismatch (an `int64` file read as `INTEGER`) succeeds
for in-range values and throws on overflow — reachable only through
catalog corruption, not through supported schema evolution.

## Build and install

```bash
cd server
flox activate -- ./gradlew :trino:trinoPlugin
```

That assembles `trino/build/trino-plugin/hoglake/` — the connector jar
plus its full runtime dependency closure, in the layout Trino expects.
Mount or copy it to `/usr/lib/trino/plugin/hoglake` and restart the
server.

**Version pin:** Trino/SPI **446**, the last line whose artifacts ship
Java 21 bytecode (447+ needs Java 22). The flox env provides JDK 21, so
446 is the newest SPI this build can compile against. The runtime is the
`trinodb/trino:446` container, which bundles its own JDK.

## Tests

```bash
flox activate -- ./gradlew :trino:test
```

68 tests: unit tests over the client, config, splits, error taxonomy,
parquet binding, and the head-race replay (a snapshot-aware mock control
plane that mimics versioned-row visibility), plus 9 integration tests
that stand up the real thing — Postgres, MinIO, the hoglake server
in-process, and a `trinodb/trino:446` container with the assembled
plugin — and query it over JDBC.

The parquet-binding fixtures write files with **real field ids** via
`trino-parquet`'s writer, which is what makes the id-authoritative
binding matrix testable at all.

## Limits (v1)

- **Read-only.** No `INSERT`, no DDL. Writes stay on hoglake's own API;
  see [trino-integration.md](../../trino-integration.md) §2 for how the
  commit endpoint is shaped so a write adapter stays a translation
  rather than a redesign.
- **No predicate or projection pushdown.** Splits are planned from
  metadata only; Trino does the filtering. Partition and min/max pruning
  arrive with the Iceberg facade, whose manifests carry the typed bounds
  hoglake already stores.
- **One split per file**, never per row group — no intra-file
  parallelism for large files.
- **Deletion vectors are refused**, not applied.
- **No credential vending.** Workers use the catalog's configured S3
  credentials; per-table scoped credentials are a facade-era feature.

## See also

[trino-integration.md](../../trino-integration.md) (charter and write
roadmap) · [iceberg-federation.md](../../iceberg-federation.md) (the
facade that supersedes this) · [server README](../README.md) (the
control plane this reads from) · [AGENT.md](../../AGENT.md) (invariants).
