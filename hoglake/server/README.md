# hoglake server

A lakehouse catalog as a service. Hoglake owns the metadata for tables
whose data lives as parquet in object storage: what tables exist, which
files back them at every point in time, per-file statistics for query
pruning, row-level deletes, and where every consumer of a table's
change stream is. Clients talk REST; nobody talks to the backing
Postgres but the service itself.

Kotlin/JVM, Ktor, Postgres via JDBI + Flyway, S3-compatible object
storage, Prometheus metrics, structured audit log. The OpenAPI contract
lives at `src/main/resources/openapi/hoglake.yaml` and is served at
`/openapi.yaml`.

```
 writers ──► POST /commit ──┐            ┌──► GET /files /scan /changes
 (parquet → S3, then        │            │    (read planning: metadata only)
  register w/ footer stats) │            │
                      ┌─────▼────────────┴─────┐
                      │      hoglake server     │
                      │  DDL · commits (OCC) ·  │──► Postgres (the catalog)
                      │  changefeed · offsets · │
                      │  retention · hydrator · │──► S3 (footer reads +
                      │  metrics · audit        │       physical cleanup only)
                      └─────────────────────────┘
```

The server never opens a data file to admit it, never writes data
files, and touches object storage in exactly two places: the hydrator's
footer reads and cleanup's physical deletes.

## The data model

Everything hangs off five ideas (`db/migration/V1__init.sql`, mirrored
by the canonical `schema.sql`; an integration test asserts the two
stay structurally identical):

**Snapshots are the clock.** Every mutation — DDL, append, delete —
commits exactly one `hog_snapshot` row with a **dense, per-catalog
monotonic id**. A snapshot carries its schema version, timestamp,
author/message, and a set of **typed change rows**
(`hog_snapshot_change`: `table_created`, `table_inserted_into`,
`table_deleted_from`, …, each with an object id). Those change rows are
simultaneously the commit history humans read and the machine
vocabulary conflict detection joins against.

**Versioned rows give time travel.** Mutable metadata (table
name/namespace, columns, partition specs, data files, deletion vectors,
views) is stored as rows with `[begin_snapshot, end_snapshot)` validity.
One predicate defines all reads:
`begin_snapshot <= S AND (end_snapshot IS NULL OR S < end_snapshot)`.
Reading the past is reading the present with a different `S`.

**Identity is split from versions.** `hog_table` is immutable identity:
`table_id` plus a `table_uuid` that survives rename and *changes* on
drop+recreate — the contract external consumers key their cursors to,
so a recreated table is visibly a different thing. `hog_table_version`
and `hog_column` carry the mutable, versioned parts. Column `field_id`s
are stable for the life of the table and are embedded by writers into
the parquet schema itself (`PARQUET:field_id`), so file columns bind to
catalog columns by id, never by name.

**Allocators are rows, advanced under the commit lock.** Snapshot ids,
table/file/view ids, per-table field ids, and per-table row-id ranges
all come from counter columns (`hog_catalog.last_snapshot_id`,
`next_file_id`, `hog_table_stats.next_row_id`, …) advanced with
`UPDATE … RETURNING` inside the serialized commit tail. No sequences,
no gaps, ids ordered exactly like commits.

**Integrity is declared.** Foreign keys with `ON DELETE CASCADE`,
`NOT NULL` where code assumes it, partial unique indexes enforcing
things like "one live table of this name per namespace" and "one live
deletion vector per data file", CHECK-constrained vocabularies. The
database rejects states the code shouldn't have to defend against.

## Feature walkthroughs

### Commits and concurrency control

`commit/CommitService.kt`. A commit is one SQL transaction that opens
by taking a **per-catalog advisory transaction lock**
(`persistence/Locks.kt`: `pg_advisory_xact_lock(4740871, catalog_id)`).
Every DDL and data commit tail serializes on it, which is what makes
snapshot ids dense and allocator math trivial; everything expensive a
client does (writing parquet) happened before the request, so the
serialized section is milliseconds of metadata writes.

Conflicts are optimistic and typed. A writer says which snapshot it
planned against (`read_snapshot`); the service runs one indexed query
over `hog_snapshot_change` for changes after that point that actually
matter to the write set — for appends, only `table_dropped` /
`table_altered` on the touched tables (**appends never conflict with
appends**); for deletes, additionally a per-file check that the
deletion vector being superseded wasn't itself replaced after the read
snapshot. A hit is HTTP 409 with the offending table named; the client
refreshes and retries. Omitting `read_snapshot` is a blind append with
no conflict window. Validation failures (unknown table, bad field id,
malformed stats) are 422 and roll back the entire commit — a
multi-table commit is atomic.

### File registration: footer-shipping and deferred stats

The writer just wrote the parquet file, so it holds the footer in
memory: it ships `record_count`, sizes, and per-column stats
(value/null/nan counts and typed min/max bounds) in the commit body,
and the server registers the file without ever opening it
(`hog_data_file` + `hog_file_column_stats`). Bounds are stored in
Iceberg single-value binary form (`stats/IcebergSingleValue.kt`) —
typed bytes, not text — so downstream metadata consumers re-encode
mechanically.

Stats may also be **deferred**: a file registers with only
`record_count` (mandatory — row-id assignment needs it) and
`stats_state='pending'`. The **hydrator** (`hydrator/Hydrator.kt`)
sweeps pending files in the background: two ranged S3 reads fetch the
parquet footer (never data pages), stats aggregate across row groups,
columns map by embedded field id (name fallback with a warning), bounds
encode by catalog type, and the file flips to `provided` — or `failed`,
loudly, if the footer contradicts the registration (a lying
`record_count` is fraud, not a discrepancy). One bad file never wedges
a sweep. Until hydrated, a pending file simply matches every scan:
correctness holds, pruning quality lags.

### Row lineage

Every append gets a contiguous row-id range per file
(`row_id_start`, width `record_count`) allocated server-side from
`hog_table_stats.next_row_id` under the commit lock. Ranges tile
`[0, total-rows-ever)` per table with no overlap and **no reuse, ever**
— a row's id is stable for the life of the table incarnation, and the
incarnation itself is pinned by `table_uuid`. This is what lets change
consumers and (future) compaction reason about identity without
guessing.

### Row-level deletes: deletion vectors

Deletes never rewrite data files. A client writes a **deletion vector**
file (a bitmap of deleted positions) to object storage and registers it
against a specific data file in a commit (`hog_delete_file`). Rules,
enforced in `CommitService` and by a unique partial index:

- At most **one live DV per data file**. A new DV supersedes the old
  (end-snapshots it) and must **cover** it — `delete_count` only grows.
- Registering a DV requires `read_snapshot`, and if the live DV was
  itself replaced after that snapshot, the commit 409s: you built on a
  stale vector, and merging bitmaps is the client's job. Lost updates
  are structurally impossible.
- A DV must target a live file of the right table, can't shrink, can't
  exceed the file's `record_count`, and can't target a file created in
  the same commit.

Read planning pairs each data file with its DV *as of the requested
snapshot* (`service/ScanService.kt`, `GET /scan`) — historical scans
see historical vectors, so time travel is delete-correct.

### Partitioning

A table carries a versioned **partition spec** (`hog_partition_spec` /
`hog_partition_field`): ordered fields of (source `field_id`,
transform, optional param), where transforms are the closed set
`identity | bucket(n) | year | month | day | hour`. `bucket` uses a
32-bit Murmur3 with pinned semantics so every writer hashes
identically. Writers compute transformed values themselves and ship
one opaque string per spec field with each file registration; the
server validates arity against the live spec, stamps the file with its
`spec_id`, and stores values in `hog_file_partition_value`. Files
remember the spec they were written under, so spec evolution never
rewrites history — pruning just knows which vintage each file is.

### Schema evolution

`service/AlterService.kt`, `POST /alter`: a list of typed operations —
`add_column`, `drop_column`, `rename_column`, `promote_column`,
`rename_table`, `set_partition_spec` — applied **in order, atomically,
as one DDL commit** (one snapshot, one `table_altered` change row, one
schema-version bump). Renames keep the `field_id` (end the old row,
begin a new one with the same id), so files written before a rename
still bind correctly. Type promotion is a strict widening lattice
(`int→long`, `float→double`) chosen so existing files remain readable
under the new schema. Validation runs against the state produced by
earlier ops in the same request, so `[add tmp, rename tmp→final]` works
and `[drop x, rename x→y]` fails precisely. You can't drop the last
column, or a column the live partition spec depends on.

### Time travel

Every point-in-time read (`getTable`, `/files`, `/scan`) takes
`?snapshot=` or `?at_timestamp=` (mutually exclusive). Timestamp
resolution is one indexed lookup: the largest snapshot with
`snapshot_time <= t`; after head resolves to head; before the earliest
*retained* snapshot is 410. Table-level aggregates (`record_count`,
`file_count`, `file_size_bytes`) are always computed from the files
visible at the requested snapshot — never from head-scoped counters.

### The changefeed and consumer offsets

`GET /changes?from_snapshot&to_snapshot` returns the metadata plan for
what happened in `(from, to]`: data files appended and deletion vectors
registered, in snapshot order, with row-id ranges. The client reads the
parquet itself — the feed is planning, not data. Paired with it,
**consumer offsets are catalog state**
(`PUT /consumers/{id}/offsets/{table_uuid}`): monotonic (regression is
a 409), keyed by `table_uuid` so incarnation changes are visible, and —
critically — **respected by retention** (below). Together these are the
log primitives: a replicator's entire state machine is
"changes → apply → commit offset."

### Retention, expiry, and safe file removal

Retention is a **catalog property** (`PATCH /options`:
`snapshot_retention_seconds`, `consumer_floor`), enforced continuously
by background sweeps (`service/ExpiryService.kt`), not by an external
scheduler. Each sweep is one bounded transaction under the commit lock:
compute the new floor as the minimum of (age cutoff, head−1, current
floor + batch, and — when `consumer_floor` — the **minimum consumer
offset**, so expiry can never outrun a lagging consumer; the pinning
consumer is named in the result and the audit line). Then: unreachable
file rows are deleted (FK cascades take their stats and partition
values) with their paths queued, and snapshots are removed with **range
deletes**, never id lists. `earliest_snapshot_id` advances;
requests reaching below it get **410 Gone** with reconcile
instructions — a consumer is told its feed has a hole rather than
silently skipping one.

Physical deletion is decoupled and paranoid
(`service/CleanupService.kt`): the queue is a *suggestion*. At drain
time every path is re-checked against live references — a
still-referenced path is skipped and counted as an **invariant
violation** (alertable), never deleted. Missing objects count as done.
S3 deletes run in sub-batches whose queue rows commit independently, so
a mid-drain failure never rolls back completed work.

### Views

Versioned name + SQL text + dialect (`hog_view`), stored verbatim —
the catalog never parses view SQL. Create/drop are DDL commits with
their own change kinds, so views appear in snapshot history and time
travel like everything else.

### Observability

Three surfaces, none of which ever writes to the catalog or rides a
transaction (`observability/`):

- **`/metrics`** (Prometheus): per-catalog health gauges sampled by a
  background loop in one batched query pass — head snapshot and age,
  expiry floor, removal-queue depth, pending-stats count, live table
  count, per-consumer lag (cardinality-capped) — plus source-side
  counters: commits by outcome, snapshots expired, files removed,
  hydrations by result. HTTP server metrics come with the Ktor
  Micrometer plugin.
- **Audit log**: every consequential action (DDL, commits with
  outcome, options changes, expiry/cleanup runs, offset commits) emits
  one structured JSON line on the `hoglake.audit` logger — actor,
  action, object, outcome, request id — strictly *after* its
  transaction resolves. Request ids ride `X-Request-Id` in and out.
- **Health**: `/healthz` proves the catalog is reachable (`SELECT 1`,
  503 within the pool's fail-fast timeout when it isn't); `/livez` is
  process liveness only.

### Background assembly

`App.kt` wires services into Ktor and `startBackground()` runs the
loops — hydrator, expiry, cleanup, metrics sampler — each an
independent daemon with its own interval knob (`Config.kt`, all
env-sourced, `<= 0` disables), per-catalog failure isolation, and a
close handle. `Main.kt` = migrate (under an advisory lock, so replicas
don't race DDL) → assemble → start loops → serve.

### Specified, not yet implemented

**CDC publications** — the WAL tap: a catalog-managed publication tails
a table's changefeed and produces rows to Kafka, its progress tracked
as a first-class consumer offset (so the retention floor protects
unpublished ranges automatically). Fully specified in the OpenAPI
(endpoints return 501) so clients can build against the shape.

## Dev environment

Toolchain via [flox](https://flox.dev) (JDK 21); Gradle from the host;
recipes via `just` (see `justfile`, composed into `../justfile`):

```sh
just            # list recipes
just test       # full suite (Docker required)
just unit       # no Docker
just one 'com.posthog.hoglake.commit.*'
just schema-check
just compose-up # Postgres 16 + MinIO (ports overridable via HOGLAKE_*_PORT)
just run        # server on :8080
```

## Layout

- `src/main/resources/db/migration/` — Flyway (single squashed V1
  pre-release; `schema.sql` is the canonical twin, equivalence-tested).
- `src/main/kotlin/com/posthog/hoglake/`
  - `model/` — domain types (mirror the OpenAPI schemas).
  - `persistence/` — JDBI repositories; all SQL parameterized.
  - `service/` — DDL, alter, scan, views, options, expiry, cleanup.
  - `commit/` — the commit path (OCC, lock tail, row-id assignment).
  - `hydrator/` — deferred-stats hydration.
  - `stats/` — Iceberg single-value bounds codec.
  - `observability/` — metrics, audit, request ids.
  - `api/` — Ktor routes implementing the spec.

## Testing

JUnit 5 + AssertJ; property tests via kotest-property (strategy:
[../fuzzing.md](../fuzzing.md)). Integration tests use Testcontainers
(Postgres 16, MinIO), tagged `integration`, and need Docker
(`docker-java.properties` in test resources pins the Docker API version
for recent daemons). The schema-equivalence test and an OCC concurrency
torture suite run with everything else in `just test`.
