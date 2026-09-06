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

The server never opens a data file to admit it, and touches object
storage in exactly three places: the hydrator's footer reads, cleanup's
physical deletes, and compaction's rewrite (the one background job that
reads and writes data files — always outside the commit path).

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
no conflict window — but a *stated* `read_snapshot` below the expiry
floor is **410 Gone** (the change rows that would prove the window
clean were expired with their snapshots; the message names the floor
and when it was reached), never a silently truncated conflict check.
Validation failures (unknown table, bad field id, malformed stats) are
422 and roll back the entire commit — a multi-table commit is atomic.

Two more admission gates run under the lock. A commit may carry an
`expected_table_uuid` per table — the incarnation guard: if the live
table's uuid differs (drop+recreate raced the writer), the whole
commit 409s with zero writes. And every registered path (data file or
DV) is checked against **undrained `hog_file_removal` rows**: a path
still scheduled for physical deletion is refused with 409 — otherwise
cleanup's drain could delete an object a newer commit just made live
(path reuse). Time itself is honest too: `snapshot_time` is stamped
`clock_timestamp()` — statement time inside the serialized tail, after
the lock — so recorded times order exactly like snapshot ids even when
commits queue.

Admission is backpressured, not implicit: the commit transaction sets
a `lock_timeout` (`HOGLAKE_COMMIT_LOCK_TIMEOUT_MS`, default 30s), and
a writer that can't get the tail in time receives **503
`commit_queue_timeout` + Retry-After** — an explicit, retryable "the
catalog is convoyed" signal instead of unbounded queueing. Lock waits
are measured (`hoglake_commit_lock_wait_seconds`) on every commit, DDL,
and maintenance tail, so a forming convoy is visible before it's an
incident.

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
`stats_state='pending'`. The **hydrator** (`hydrator/Hydrator.kt`,
parquet-java) sweeps pending files in the background: a ranged S3 read
fetches the parquet footer (never data pages), stats aggregate across
row groups, columns map by embedded field id (name fallback with a
warning — resolved against the schema **at the file's
`begin_snapshot`**, never live-at-hydration, so a drop+add-same-name
between commit and sweep can't write the old incarnation's stats under
a new field id), bounds encode by catalog type, and the file flips to
`provided` — or `failed`, loudly, if the footer contradicts the
registration (a lying `record_count` is fraud, not a discrepancy). One
bad file never wedges a sweep, and a bound the codec can't represent
safely (e.g. a timestamp whose unit conversion would overflow) stores
as NULL — bounds are never guessed. Until hydrated, a pending file
simply matches every scan: correctness holds, pruning quality lags.

Hydration failure has a **two-class taxonomy**. Transient fetch errors
(S3 5xx/SlowDown, timeouts, connection resets) leave the file
`pending` — logged and counted
(`hoglake_hydrator_transient_errors_total`), retried on the next sweep.
Structural errors (object missing, unparseable footer, footer
contradicting the registration) go to `failed` — terminal to the sweep
but operator-recoverable: `POST /maintenance/rehydrate` flips
`failed` → `pending` (catalog-wide or scoped to one namespace+table),
for when the cause was fixed (object re-uploaded, cap raised). Two
guard rails on the fetch itself: files without a usable `footer_size`
fall back to a whole-object read **capped** at
`HOGLAKE_HYDRATOR_MAX_WHOLE_OBJECT_BYTES` (default 256 MiB — a larger
file fails structurally with a fix-then-rehydrate message instead of
becoming an OOM vector), and the sweep claims its batch
`FOR UPDATE SKIP LOCKED`, so concurrent replicas hydrate disjoint sets
instead of amplifying S3 GETs against the same head.

The footer read doubles as the **field-id contract check**: any leaf
without a `PARQUET:field_id` flags the row
(`hog_data_file.missing_field_ids`, gauged as
`hoglake_missing_field_id_files{catalog}`; the reserved `_hog_row_id`
id on compacted files is fine). Id-less files bind columns by name, so
while one is LIVE the table's `rename_column` fails with 409
`idless_files_present` — renaming would silently NULL that column's
history in readers. The guard blocks on live **`pending`** files too:
their id state is unknown until the footer is read, so a rename can't
slip through the commit-to-hydration window. `rename_table` is
unaffected, and the flag clears from relevance when the file is
compacted away, dropped, or expired.

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
see historical vectors, so time travel is delete-correct. Lifecycle is
airtight at the edges too: `DROP TABLE` end-snapshots live DVs along
with the data files (so expiry reclaims both row and object — nothing
`end_snapshot IS NULL` survives a drop), and compaction end-snapshots
a group's DVs together with the inputs they mask.

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
`rename_table`, `set_partition_spec`, `set_sort_order` — applied **in
order, atomically, as one DDL commit** (one snapshot, one
`table_altered` change row, one schema-version bump). Renames keep the
`field_id` (end the old row, begin a new one with the same id), so
files written before a rename still bind correctly. Type promotion is
a strict widening lattice (`int→long`, `float→double`) chosen so
existing files remain readable under the new schema — and a promote
**re-encodes the table's existing stats bounds** (4-byte → 8-byte
Iceberg encoding) in the same transaction, so nothing downstream ever
decodes old-width bounds under the new type. Validation runs against the state produced by
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
a 409), never below the expiry floor (410 — an expired offset would
otherwise wedge the consumer-floor sweep at zero work forever), keyed
by `table_uuid` so incarnation changes are visible, and — critically —
**respected by retention** (below). Together these are the
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
deletes**, never id lists. `earliest_snapshot_id` advances — capturing
the new floor snapshot's `snapshot_time` into
`hog_catalog.earliest_snapshot_time` in the same update, so once the
rows below the floor are gone the catalog can still say WHEN the floor
was reached; requests reaching below it get **410 Gone** with
reconcile instructions citing that time — a consumer is told its feed
has a hole (and since when) rather than silently skipping one. A fifth
step applies the same reachability rule to the accumulating versioned
DDL tables (`hog_table_version`, `hog_column`, `hog_partition_spec`,
`hog_sort_spec`, `hog_view`): rows with
`end_snapshot <= earliest_snapshot_id` are invisible at every retained
snapshot and are deleted (spec fields cascade), so DDL churn cannot
grow the metadata without bound.

Physical deletion is decoupled and paranoid
(`service/CleanupService.kt`): the queue is a *suggestion*. At drain
time every path is re-checked against live references — a
still-referenced path is skipped and counted as an **invariant
violation** (alertable), never deleted. Missing objects count as done.
S3 deletes run in sub-batches (25 paths) whose ledger updates commit
independently, so a mid-drain failure never rolls back completed work
— and each sub-batch's check-then-delete pair runs **under the
per-catalog commit lock**, paired with commit's refusal to register a
path that has an undrained removal row: the two sides together make
delete-under-path-reuse structurally impossible (the liveness answer
can't go stale between check and delete, and a new commit can't slip a
live file under a queued path). The lock hold is bounded — sub-batch
size × one S3 round-trip — which is why the sub-batch is kept small.

Draining **soft-deletes**: a settled `hog_file_removal` row keeps its
place with `drained_at` + `drained_outcome` (`'deleted'` for a
physical removal, `'absent'` for verified-already-gone), so "what did
cleanup touch, when, after how many attempts" is queryable instead of
dying with the row; skips and transient failures bump
`attempts`/`last_attempt_at` and stay queued. The drain reads only
undrained rows (partial index), and each sweep purges drained rows
older than `HOGLAKE_REMOVAL_LEDGER_RETENTION_SECONDS` (default 30
days) so the ledger never becomes its own unbounded-growth problem.

### Sort orders and compaction

A table may carry a versioned **sort order** (`hog_sort_spec` /
`hog_sort_field`, set via the `set_sort_order` alter op) — advisory for
writers (the server never verifies file sortedness), binding for
compaction. **Compaction** (`compaction/CompactionService.kt`,
`POST /maintenance/compact` + an off-by-default loop) merges small live
files: candidates share (spec_id, partition_values) and group greedily
under a byte target — row-id adjacency is NOT required, which is why
outputs materialize their row ids as an explicit `_hog_row_id` int64
column (reserved field id 2147483646, flagged by
`data_file.explicit_row_ids`) instead of relying on position. That
makes sorting on rewrite safe — the predecessor's sorted-compaction
rowid remap is structurally impossible. The rewrite (parquet-java —
the project's one parquet library, shared with the hydrator's footer
reads) happens entirely before the commit transaction; the commit
re-verifies each input is still live under the exact planned identity
(plan-to-commit races skip the group), end-snapshots inputs (time
travel keeps them; expiry reclaims them later), aggregates stats from
typed decoded bounds — treating an undecodable bound as absent, never
wedging on it — and the changefeed excludes compacted outputs so
consumers never see merged rows re-appear as fresh appends.

Coverage is 100% of live layouts, not just the easy ones:

- **DV-bearing inputs compact.** The rewrite reads each input's live
  deletion vector (Iceberg-v3 puffin `deletion-vector-v1` blobs —
  roaring bitmap, magic and CRC verified; `PuffinDeletionVector` is
  the one server-side reader of DV content) and drops the deleted
  positions: survivors keep their original ids in `_hog_row_id`,
  deleted ids are gone forever, and the inputs AND their DVs
  end-snapshot together. The commit re-verifies the exact DV identity
  it planned against — a vector that grew or appeared since planning
  skips the group (`dv_superseded` in the result), so a post-plan
  delete is never dropped. Because the registered stats of a DV'd
  input describe pre-delete data, outputs with DVs applied register as
  `stats_state='pending'` and the hydrator re-derives honest stats.
- **Heterogeneous-schema groups compact.** Inputs written under
  different schema versions rewrite under the LIVE schema, mapped by
  field id: a live column absent from an input null-fills, promoted
  columns up-cast (int32→int64, float→double), dropped field ids drop
  their data. Only a live column *unproducible* from an input's
  physical type skips the group (`unconvertible_schema` — detected
  before any bytes are staged); the unproducible set is narrowings,
  physical mismatches, decimal-scale changes, non-micros time(stamp)
  units, nested inputs, and INT96.
- **Aborted uploads can't orphan objects.** Before uploading, the
  output path pre-registers as an undrained `hog_file_removal` row
  (reason `compaction_staging`) — a claim ticket. A successful group
  commit settles it (`drained_outcome='registered'`) in the same
  transaction that makes the path live; an aborted group leaves it for
  the normal cleanup drain to reclaim. The commit re-claims the ticket
  first, so a drain that won the race just aborts the group — cleanup
  can reclaim aborted outputs and can never reclaim committed ones.

### Views

Versioned name + SQL text + dialect (`hog_view`), stored verbatim —
the catalog never parses view SQL. Create/drop are DDL commits with
their own change kinds, so views appear in snapshot history and time
travel like everything else.

### Self-knowledge: verify, partition debt, consumers, identity

The catalog reports on itself instead of waiting for ops SQL:

- **`POST /maintenance/verify`** (`service/VerifyService.kt`) — the QE
  suite's global-invariant SQL as a read-only, metadata-only endpoint
  (one REPEATABLE READ MVCC snapshot, no catalog lock). Six checks:
  row-id tiling (positional overlap; `explicit_row_ids` compaction
  outputs exempt by design), deletion vectors (one live per file,
  monotone supersession chains, `delete_count <= record_count`),
  orphaned live rows on dropped tables, still-referenced removal-queue
  entries, true snapshot density (`count(*)` equals the dense
  `[earliest, head]` range), and `next_row_id` allocator consistency.
  The JSON report carries per-check status, true violation counts, and
  samples capped at 20.
- **`POST /maintenance/rehydrate`** — the operator requeue for the
  hydrator's structural failures (above): flips `failed` → `pending`,
  catalog-wide or scoped to one namespace+table.
- **`GET /stats/partitions`** (`service/PartitionStatsService.kt`) —
  leaf partitions ranked by compaction debt: `debt_score` is the
  small-file count under exactly the threshold the compactor plans
  with (so the ranking predicts what a sweep would do), plus
  stale-spec-group counts. The webui's compaction-debt page renders
  this.
- **`GET /consumers`** — every consumer in the catalog with per-table
  offsets, table names resolved, dropped tables flagged (offsets
  outlive drops by design — an offset on a dropped table is data, not
  garbage).
- **`GET /v1/info`** — instance identity: the operator-configured
  display name (`HOGLAKE_INSTANCE_NAME`, e.g. "GigaHog"), shown in the
  webui topbar so nobody mistakes prod for dev.
- **`GET /export`** — the DR manifest (snapshot range + live-file
  manifest + consumer offsets, consistent at head) is fully specified
  in the OpenAPI and answers **501** until built (gaps.md B5).

### Observability

Three surfaces, none of which ever writes to the catalog or rides a
transaction (`observability/`):

- **`/metrics`** (Prometheus): per-catalog health gauges sampled by a
  background loop in one batched query pass — head snapshot and age,
  expiry floor, removal-queue depth (undrained entries only),
  pending-stats AND failed-stats counts
  (`hoglake_stats_failed_files`), live id-less-file count
  (`hoglake_missing_field_id_files`), live table
  count, per-consumer lag (cardinality-capped) — plus source-side
  counters: commits by outcome, snapshots expired, files removed,
  hydrations by result (transient errors counted separately), and the
  `hoglake_commit_lock_wait_seconds` histogram on every commit/DDL/
  maintenance tail (the convoy early-warning). HTTP server metrics
  come with the Ktor Micrometer plugin. The webui renders a `/metrics`
  snapshot visually on its metrics page.
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
loops — hydrator, expiry, cleanup, compaction (default off:
`HOGLAKE_COMPACTION_INTERVAL_MS=0` — flipping it on is an ops
decision), metrics sampler — as **coroutines under one supervisor
scope** (`BackgroundLoops`), each with its own interval knob
(`Config.kt`, all env-sourced, `<= 0` disables), per-catalog failure
isolation (a failed iteration is logged + counted and the loop keeps
running), and structured, bounded shutdown (cancel + join, 5s cap).
`Main.kt` = migrate (under an advisory lock, so replicas don't race
DDL) → assemble → start loops → serve.

### Specified, not yet implemented

**CDC publications** — the WAL tap: a catalog-managed publication tails
a table's changefeed and produces rows to Kafka, its progress tracked
as a first-class consumer offset (so the retention floor protects
unpublished ranges automatically). Fully specified in the OpenAPI
(endpoints return 501) so clients can build against the shape.

## Dev environment

Toolchain via [flox](https://flox.dev) (JDK 21); Gradle via the
**checked-in wrapper** (`./gradlew`, version pinned in
`gradle/wrapper/gradle-wrapper.properties` — never a system gradle);
recipes via `just` (see `justfile`, composed into `../justfile`):

```sh
just            # list recipes
just test       # full suite (Docker required)
just unit       # no Docker
just one 'com.posthog.hoglake.commit.*'
just schema-check
just compose-up # Postgres 16 + MinIO (ports overridable via HOGLAKE_*_PORT)
just run        # server on :8080
just docs       # OpenAPI spec in Swagger UI on :8090 (HOGLAKE_SWAGGER_PORT)
```

## Layout

- `src/main/resources/db/migration/` — Flyway (single squashed V1
  pre-release; `schema.sql` is the canonical twin, equivalence-tested).
- `src/main/kotlin/com/posthog/hoglake/`
  - `model/` — domain types (mirror the OpenAPI schemas).
  - `persistence/` — JDBI repositories; all SQL parameterized.
  - `service/` — DDL, alter, scan, views, options, expiry, cleanup.
  - `commit/` — the commit path (OCC, lock tail, row-id assignment).
  - `compaction/` — small-file merging (the only parquet-java user).
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
