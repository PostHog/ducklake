# README.md — hoglake

2026-09-04. The plan for building hoglake: a DuckLake-shaped lakehouse
catalog rebuilt as a Postgres-native service behind a Lakekeeper-style
control plane. Companion docs in this directory:

- [ducklake-api-map.md](ducklake-api-map.md) — every API the DuckLake extension + pyducklake
  expose today, what it does, what's wrong with it.
- [iceberg-federation.md](iceberg-federation.md) — what v1 must get
  right for the Iceberg REST facade to work (field IDs, transforms,
  delete encoding, stats bounds, metadata artifacts).
- [trino-integration.md](trino-integration.md) — Trino as the facade's
  first consumer, and the commit-shape choices that keep append-only
  Trino writes a translation away.
- [metadata-schema.md](metadata-schema.md) — the current `ducklake_*` metadata schema, its
  invariants, and the migration story (current: none).
- [fuzzing.md](fuzzing.md) — the property-testing/fuzzing strategy
  (hypothesis + kotest-property + cross-language codec vectors).

Implementation lives alongside the docs (`justfile` composes the
per-component recipes; `just test-all`):

- [server/](server/README.md) — the control plane (Kotlin/Ktor/PG).
- [pyhoglake/](pyhoglake/README.md) — the Python client (the thin-API
  successor to pyducklake; owns the parquet writer path).
- [webui/](webui/README.md) — the management console.
- [hedgerow/](hedgerow/) — the hoglake-native replication daemon
  (viaduck's successor; append-only, single-destination v1).

## What hoglake is

The DuckLake architecture — snapshots, schema evolution, data files,
and stats as rows in a transactional RDBMS; parquet in object storage —
kept intact, with three structural changes:

1. **The catalog is a service, not a client library.** Clients never
   see the backing Postgres. All catalog SQL moves server-side behind
   an API (the Lakekeeper model). Commit/OCC, compaction, snapshot
   expiry, GC, stats, and metrics become service concerns with one
   owner, one version, one deployment.
2. **Postgres is THE backend, not A backend.** FKs with ON DELETE
   CASCADE, advisory locks, partial indexes, range deletes, server-side
   procedures — everything DuckLake forbade itself for
   SQLite/MySQL portability is now load-bearing design material.
3. **No DuckDB anywhere in the required path.** Readers reach data via
   open interfaces (Iceberg federation first, `o.a.h.fs.FileSystem`
   eventually); writers speak the control-plane API and write parquet
   themselves. DuckDB remains *a* possible client, never the engine the
   catalog lives inside.

Non-goals: DuckLake compatibility (wire, SQL, or metadata), multi-RDBMS
backends, keeping the DuckDB extension alive.

## Hoglake vs. DuckLake at a glance

| | Hoglake | DuckLake |
|---|---|---|
| **Architecture** | Catalog as a service behind REST; clients never see the metadata DB | Client library: a DuckDB extension every process embeds |
| **Metadata backend** | Postgres, exclusively — FKs with CASCADE, advisory locks, partial indexes are load-bearing | DuckDB/Postgres/SQLite (portability subset: no FKs, no locks, no indexes shipped) |
| **Catalog implementations in the fleet** | Exactly one, deployed once | One per client build (fork vs upstream drift on the same catalog) |
| **Commit cost** | Scoped to the write set; O(catalog) loads structurally impossible | Loads stats for the entire catalog per attempt (observed 5–7s at 59K tables, re-paid per OCC retry) |
| **Conflict detection** | Typed change rows, one indexed anti-join; appends never conflict with appends | Comma-encoded string parsed client-side; retryability by error-message matching |
| **Commit serialization** | Per-catalog advisory-lock tail (ms); dense snapshot ids | Snapshot-id PK collision as the conflict signal; retry re-pays the full commit |
| **File registration** | Footer-shipping (writer sends stats; server never opens data files); deferred-stats mode with async hydration | Engine writes files and stats in-process |
| **Row lineage** | Server-assigned contiguous ranges, never reused; `table_uuid` incarnation contract | Rowids reused on upsert-recreate; sorted compaction silently remaps them |
| **Row-level deletes** | Deletion vectors: one live DV per file, growth-monotonic, stale-DV = typed 409 | Positional delete files (+ experimental DVs); delete-vs-delete races surface as generic conflicts |
| **Time travel** | Snapshot id or timestamp; aggregates snapshot-scoped; 410 below the retention floor | Snapshot version or timestamp via ATTACH/AT |
| **CDC / changefeed** | First-class plan API (files + DVs per range); expired ranges refuse loudly | `table_changes()` SQL macro (executor-stall wedge at scale; silent gaps possible) |
| **Consumer offsets** | Catalog state: monotonic, `table_uuid`-keyed, retention-aware | Not a concept; every consumer builds its own cursor store |
| **Retention/expiry** | Catalog property, continuous incremental sweeps, consumer-offset floor, range deletes | Client-invoked global procedure (~14ms/snapshot; ~50h at 15M snapshots); blind to consumers |
| **Physical file deletion** | Queued + liveness-checked at drain; sub-batch commits; still-referenced = alert, never delete | Queue trusted absolutely (age filter only); one S3 5xx rolls back the catalog delete (phantom queues) |
| **Maintenance ownership** | Service background jobs (expiry, cleanup live; compaction landing) | Client procedures, no advisory locks — concurrent runs corrupt shared state |
| **Inlined data** | None (dropped by design; Arrow-blob design reserved if ever needed) | Dynamic per-schema-version tables in the catalog (unreachable-GC class) |
| **Schema evolution** | Typed ops, atomic multi-op DDL commits, field-id-stable renames, strict promotion lattice | ALTER via engine; broader type lattice; struct field ops |
| **Partition transforms** | identity, bucket(n) (Murmur3, Iceberg-bit-compatible), year/month/day/hour; files remember their spec vintage | identity, bucket(n) (murmur3; nested types hash a string repr), calendar + epoch date variants |
| **Sort orders** | Landing with compaction (M4): versioned sort spec, applied on rewrite **with row ids preserved** | `SET SORTED BY` applied at insert/flush/compaction — but sorted compaction silently remaps rowids |
| **Views** | SQL text + dialect, versioned | Yes, incl. macros |
| **Encryption** | Not yet | Per-file parquet encryption |
| **Readers** | REST + native Trino connector (read-only) + web console; Iceberg REST facade designed | DuckDB (only) |
| **Observability** | `/metrics` (per-catalog health, outcome counters) + structured audit log, built in | External scripts/daemons querying the catalog |
| **Migration story** | Flyway + canonical schema.sql with CI equivalence check | Imperative C++ string migrations; no ledger; partial migration representable |
| **Integrity** | PKs, FKs+CASCADE, NOT NULL, CHECK vocabularies, partial unique indexes | 5 PKs total; zero FKs/indexes/constraints beyond them |

## Measured (2026-09-05)

The claims above are benchmarked, not asserted — `bench/` runs these as
regression guards (`just bench quick`; results journal to JSONL).
Numbers below are from `all --quick` on a dev laptop against the local
compose stack; treat them as shape, not capacity planning:

| Guard | Predecessor pathology | Hoglake, measured |
|---|---|---|
| Commit latency vs catalog size | 5–7s stats load per attempt at 59K tables; 190–264s single-table commits | **Flat**: p50 ratio ~1.0 with 10K preseeded snapshots (guard flags > 1.5×) |
| Snapshot expiry | ~14ms/snapshot (~50h per 15M backlog) | **14,462 snapshots/s** (~200×), 14,885 files queued/s |
| Concurrent appends | Superlinear collapse on busy catalogs; commit-storm convoys | 0 conflicts at K=1–8; per-writer p99 grows linearly (3.3→22.5ms); aggregate plateaus ~380 commits/s at the advisory-lock tail (~2.5ms hold) |
| Delete races | Generic conflicts, lost-update risk | 0 lost updates under deliberate hot-file contention; retry-to-success p50 5.6ms |
| Changefeed reads | Cost scaled with snapshot span/catalog | Latency correlates 0.998 with rows returned, not catalog size |
| DDL churn | Dropped-table stats taxed every commit forever | Post-churn commit p50 ratio 0.64 — no residual tax |
| Writer path (real parquet end-to-end) | — | 202K rows/s through `Table.append` |
| Physical cleanup | Blind queue trust; all-or-nothing S3 | 100% liveness-checked, ~900 paths/s drained (the knob to watch at depth) |

Every bench scenario asserts catalog invariants after load (dense
snapshot ids, row-range tiling, DV accounting, `still_referenced == 0`)
and exits loudly if the numbers can't be trusted.

## Why — the operating experience this encodes

Operating DuckLake in production (megaduck + the duckling fleet +
viaduck/millpond writers) has produced a bug ledger
([ducklake-defect-ledger.md](ducklake-defect-ledger.md) in this directory, plus the fork-fix
backlog) whose entries almost
all reduce to a handful of architectural facts. Hoglake's design
answers each.

### 1. Per-commit costs that scale with the catalog, not the transaction

The commit path loads stats for the ENTIRE catalog per attempt
(observed: 59K tables / 3.7M stats rows = 5-7s per attempt, re-paid on
every OCC retry; 190-264s commits for single-table writes). Dropped
tables' stats rows survive until snapshot expiry, so DDL churn grows
this forever (99.4% of stats rows on one catalog were for dropped
tables; purging them bought 30-50×). **Service answer**: commit is a
server-side operation scoped to the write set; catalog-global loads
are structurally impossible to write by accident.

### 2. PG-allergy: integrity and performance left on the table

- 50M orphaned `ducklake_file_partition_value` rows — a declared FK
  with CASCADE would have made the class impossible.
- No advisory locks in any maintenance procedure — concurrent
  maintenance from two contexts produced a 9.99M-row phantom deletion
  queue and a 14-hour recovery.
- 500K-element IN-lists where a contiguous range scan was available;
  no filter pushdown on the orphan scan (30M-row table shipped to the
  client to match ~0 rows); snapshot expiry at ~14ms/snapshot means
  ~50h against a 15M-snapshot production catalog.

**Service answer**: FKs, advisory locks, partial indexes, and range
predicates are the *foundation*, and the service owns coordination so
most cross-client locking disappears outright.

### 3. The client-embedded engine is a fleet liability

- Schema-cache memory misaccounting (counts tables, not columns) →
  LRU never evicts → unbounded RSS → OOM loops on writers.
- Per-connection native memory proportional to catalog metadata
  volume, freed only on connection close → connection-recycling
  workarounds in every long-lived writer.
- Two engines, one catalog: millpond runs upstream ducklake, viaduck
  runs the fork — different conflict rules and stats loaders against
  the same Postgres, and fork fixes never reach the maintenance path.
- Extension distribution pain: fork wheels vs stock extensions,
  core-slot alias collisions, unpinnable community-extension installs.

**Service answer**: exactly one catalog implementation, deployed once,
upgraded once. Client libraries become thin API wrappers with no
embedded query engine to leak, drift, or fork.

### 4. Maintenance as client-side procedures doesn't survive production

Compaction deadline-deaths compounding into backlog, then a recovery
run emitting 60-180s catalog commits that convoyed every writer on the
shard (2026-09-04 incident); all-or-nothing S3 deletion that turns one
5xx into phantom queue state; GC predicates that are structurally
unreachable (single-schema-version inline tables can never be
collected); no table-scoped purge, so dropped tables leak files
indefinitely. **Service answer**: compaction/expiry/GC/metrics are
service-owned background jobs — resumable, incremental,
rate-aware of foreground commit traffic, and fixable in one place.

### 5. Semantics gaps our writers had to paper over

- **Row identity is not stable**: rowids are reused on
  upsert-recreate-of-deleted-key, and sorted `merge_adjacent_files`
  silently remaps rowids (empirically confirmed). Every CDC consumer
  downstream had to defend against identity reuse.
- **No log primitives**: `ducklake_table_changes` reads made viaduck's
  CDC path a read-barrier-bound crawl, and exactly-once required
  hand-built sink-side cursor tables. A catalog that knows about
  consumers (per-consumer offsets committed transactionally with the
  lake) collapses that entire problem.
- **NULL-shaped landmines**: unguarded metadata reads crash the client
  engine (`GetValueInternal on NULL` invalidating whole instances).

**Service answer**: stable row lineage as a contract; changefeed and
consumer offsets as first-class API; the server validates its own
metadata.

### 6. Snapshot lifecycle doesn't scale, and expiry is blind to consumers

Megaduck reached 15.3M snapshots; expiry runs ~14ms/snapshot regardless
of chunk size (~50h to drain a backlog), and a busy writer mints ~138K
snapshots/day. Retention is a cron bolted on outside the catalog — and
it doesn't know consumers exist: expiring a lagging CDC consumer's
unread range is silent, so viaduck grew the retention-clamp machinery
to detect and acknowledge its own data loss after the fact.
**Service answer**: retention is a *catalog property* the service
enforces continuously (incremental daily-step expiry, range deletes),
with snapshot count/age as first-class health signals; and because
consumer offsets live in the catalog, expiry holds a bounded floor at
the min consumer offset and pages when a consumer pins retention —
clients stop defending themselves against their own catalog.

### 7. Every client fights for the catalog with no arbiter

The team-2 flush tax (write throughput superlinearly degrading on a
busy catalog), the duckgres query_log flood (per-query commits starving
OCC writers with 0.4s commit gaps), and the compaction commit-storm
convoy (60–180s commits → downstream liveness kills, 13 restarts) are
one disease: unmediated catalog contention. Adjacent: every writer,
cron, and metrics job holds its own Postgres connections — hence
idle-in-transaction sessions blocking `CREATE INDEX CONCURRENTLY`,
pool-limit races, and pooler-sizing as a recurring ops topic.
**Service answer**: the service is the arbiter. Commit admission —
maintenance yields to foreground, fair queuing across tenants,
backpressure as an explicit API response instead of latency inference.
Observability writes (query_log-class data) never enter the commit
path. All catalog access goes through one owned connection pool with
statement timeouts and idle-in-txn policing; third-party sessions on
the catalog stop existing.

### 8. Recovery and self-knowledge have been artisanal

The split-brain incident was survivable only because S3 versioning let
us hand-remove delete markers for 131 files; `cleanup_old_files` trusts
its deletion queue absolutely (age filter, no liveness join). The
metrics estate (per-tenant CronJobs, an abandoned daemon, orphan-count
gauges, hand-built catalog-SQL dashboards) exists because the catalog
cannot report on itself. Table identity (`table_id` AND `table_uuid`
both change on drop+recreate, survive rename) is tribal knowledge
consumers must encode in cursor guards. And every recovery — zz_
backup tables, split-brain repair, engine-version drift — was
improvised.
**Service answer**: physical deletion is soft by design (bucket
versioning + delayed permanent delete) and always liveness-checked
against live references first. The service exports its own health —
snapshot count/age, deletion-queue depth, stats-pending, commit
latency per tenant, and orphan counts as *invariant violations* —
retiring the metrics-cron layer. The changefeed carries `table_uuid`
so consumers see incarnation changes instead of deducing them. DR is
specified up front: PITR on the catalog Postgres + a consistent export
(catalog dump + file manifest), cheap because the catalog stays small
once orphans are structurally impossible.

## Architecture sketch

```
                       ┌──────────────────────────────┐
   writers (viaduck,   │   hoglake control plane      │
   millpond, jobs) ───▶│  - commit/OCC (write-set     │──▶ Postgres
                       │    scoped)                   │   (the catalog,
   readers (Trino  ───▶│  - snapshot/schema/file APIs │    with FKs)
   via Iceberg REST,   │  - changefeed + consumer     │
   hadoop-fs, duckdb   │    offsets                   │
   as-a-client) ◀──────│  - Iceberg REST facade       │
                       │  - background: compaction,   │
        │              │    expiry, GC, stats/metrics │
        ▼              └──────────────────────────────┘
   object store (parquet; Vortex/native-duckdb files later
   behind a format tag per data file)
```

Key moves:

- **Commit protocol**: client stages parquet to the object store, then
  calls commit with file paths + footer-derived stats (the Iceberg
  `DataFile`+`Metrics` registration model — the server never scans
  data to admit it). Server runs write-set-scoped conflict checks and
  owns retry. Appends never pay O(catalog). **There is an in-fork
  precedent**: the quack backend's server-side commit — client stages
  the whole commit into `ducklake_staged_*` temp tables and calls
  `ducklake_commit()`, which runs the full OCC retry loop inside the
  metadata server ([`src/functions/ducklake_commit.cpp`](https://github.com/PostHog/hoglake/blob/eee193b7cb18fc4954df4664c3468d75f2d26ceb/src/functions/ducklake_commit.cpp),
  [`ducklake_server_side_commit.hpp`](https://github.com/PostHog/hoglake/blob/eee193b7cb18fc4954df4664c3468d75f2d26ceb/src/include/storage/ducklake_server_side_commit.hpp)). Hoglake's commit endpoint is
  that idea promoted from "one backend's optimization" to the only
  path, with a real wire API instead of staged temp tables. See
  [ducklake-api-map.md](ducklake-api-map.md) §5 for the ~18-closure commit context the
  server must cover.
- **Iceberg federation is the read story, near-term**: expose tables
  through an Iceberg REST catalog facade (the Lakekeeper precedent) so
  Trino/Spark read hoglake with their stock Iceberg connectors — no
  custom Trino plugin as a prerequisite. This constrains the metadata
  model to stay Iceberg-mappable (snapshot → snapshot, data_file →
  manifest entry, stats → metrics), which is worth honoring anyway.
- **Changefeed as API**: `changes(table, from_snapshot, to_snapshot)`
  server-side (replacing client `ducklake_table_changes` reads), plus
  per-consumer committed offsets stored in the catalog — the log
  primitives the viaduck redesign wants. Responses carry `table_uuid`
  so incarnation changes (drop+recreate) are visible, not deduced.
- **Retention as consumer-aware catalog policy**: per-catalog retention
  the service enforces continuously (incremental expiry, range
  deletes), floored — within a bound — at the min registered consumer
  offset, paging when a consumer pins it.
- **CDC publications — the WAL tap (specified 2026-09-05, implement
  later).** A catalog-managed publication tails a table's changefeed
  and produces its rows directly to Kafka — the "inverse TableFlow /
  Kafka fanout" sketch from the original architecture rationale, now an
  API surface (see the publications section of openapi/hoglake.yaml;
  endpoints 501 until built). Semantics locked in the spec: the
  publisher is a service background worker whose progress is a
  first-class consumer offset (`publication:{name}`) — so the retention
  floor protects an unpublished range exactly as it protects any
  lagging consumer, and publication lag is visible with the same
  metrics; delivery is at-least-once, records keyed by
  (table_uuid, row_id) so compacted topics converge; deletes emit from
  DV diffs as tombstone-shaped records; incarnation change or a 410
  halts the publication rather than skipping (the hedgerow rules).
  Formats: Arrow IPC batches (the Arrow ground rule) or per-row JSON.
- **Commit admission**: the service arbitrates catalog access —
  maintenance yields to foreground commits, per-tenant fair queuing,
  explicit backpressure responses. Observability data never rides the
  commit path.
- **Commit-serialization refinements (possible direction, not a
  promise).** The catalog-global snapshot chain stays — re-engineering
  it is a bridge too far. But within that model, server-side ownership
  opens candidate improvements to evaluate at phase-2 spec time: split
  snapshot-id allocation (sequence) from conflict detection so a PK
  collision is no longer the conflict signal; serialize only the
  commit tail under a per-catalog advisory xact lock (milliseconds of
  queuing instead of whole-commit retry storms, and id order = commit
  order by construction); normalize `changes_made` into a typed,
  indexed `snapshot_change` table so conflict checks are one anti-join
  and retry re-validation is incremental; an append fast path (per
  the conflict matrix, appends only conflict with DDL/deletes on the
  same tables — one indexed lookup for our dominant traffic); and
  decouple the id allocators (`next_catalog_id`/`next_file_id`/
  `next_row_id`) from the snapshot row via sequences. Each is
  separable; none is load-bearing for the v1 design — the v1 commit
  endpoint may simply reproduce today's OCC semantics behind the API
  and take these as follow-ups.
- **Audit log as a first-class feature — and never rows in a
  database.** Every consequential action emits a structured audit
  event: actor (the authenticated principal), verb, object
  (catalog/table/snapshot/file), outcome, request id. Coverage: DDL,
  commit summaries, maintenance runs, retention/expiry decisions,
  **physical file deletions** (the split-brain forensics we had to
  reconstruct from S3 delete markers), option changes, and authz
  denials. Emission is to the process's structured log stream
  (stdout/OTel → the log pipeline), asynchronously and outside every
  transaction — the duckgres query_log lesson generalized: logging
  synchronously into an operational database is how a log becomes a
  writer-starving workload. If a queryable archive is wanted, it's an
  *async sink* from the pipeline (optionally into an append-only
  hoglake table — dogfood, but downstream of the pipeline, never in
  line with the action it describes). Distinct from
  `snapshot_changes`/commit messages, which are catalog *semantics*
  (OCC vocabulary, time travel) and stay in the catalog; the audit
  log is the operational who-did-what trail keyed to auth principals.
- **Format extensibility**: `data_file.format` tag from day one
  (parquet now; Vortex, raw DuckDB files later). Readers negotiate.
- **Arrow wherever data moves.** Arrow is the interchange spine, as it
  already is in viaduck/millpond: client write buffers are Arrow
  before they become parquet; every data-bearing service response
  (changefeed, inspect/files, any future scan hand-back) is Arrow IPC
  on the wire — Arrow Flight (or Flight SQL) is the natural transport
  for those endpoints rather than JSON-wrapping row data. Metadata
  small-object endpoints stay plain REST/JSON. This also weighs on the
  language choice: first-class Arrow (arrow-rs, arrow-java) is a hard
  requirement, and parquet support via the Arrow implementations is
  the specific thing to evaluate, not "JVM parquet" in the abstract.

## Schema + migrations (the "modern way to define and upgrade it")

Details in [metadata-schema.md](metadata-schema.md); the position:

- The `ducklake_*` schema is largely right as a *shape* (it's the part
  of DuckLake we're keeping). Rebuild it with: real PKs/FKs/CASCADEs,
  partial indexes for the hot predicates (`end_snapshot IS NULL`),
  NOT NULL where the code already assumes it, and no
  per-schema-version inlined-data tables — inlining is **dropped**
  (decided 2026-09-04; usage near-zero, GC history ugly). The
  112K-table registry incident becomes unrepresentable, and the
  flush-inlined machinery, inlined-delete tables, and their entire
  conflict-matrix rows disappear from the design. Migration converts
  any residual inlined rows to parquet once, at cutover.
- Migrations: versioned, ordered SQL files applied by the service at
  startup under an advisory lock, tracked in a `hoglake_migrations`
  table — the boring, standard thing DuckLake never had (its answer is
  a version string check that refuses to open). Plain SQL is the
  contract; no ORM DSL, no portable schema language (that is the
  multi-backend trap again — the representable subset excludes
  everything we picked Postgres for). The concrete mechanism, four
  pieces:
  1. **A canonical `schema.sql`** — complete desired state, plain
     Postgres DDL, the one reviewable artifact where every
     FK/index/constraint is visible in one place.
  2. **Numbered migration files** — hand-written (or diff-generated
     then hand-edited): the operational content (backfills,
     expand/contract sequencing, batching, lock choices) is authored,
     not derivable from a state diff.
  3. **A CI equivalence check** — apply all migrations to a scratch
     DB, diff against `schema.sql` (migra or Atlas), fail on drift.
     This keeps the two representations honest with each other.
  4. **A migration linter** (squawk, or Atlas lint) for lock hazards —
     an ACCESS EXCLUSIVE `ALTER` on the data_file table during a busy
     commit window is the compaction-convoy incident wearing a
     different hat.
  Runner chosen with the impl language (Flyway on JVM, sqlx/refinery
  on Rust, golang-migrate on Go); the four artifacts above are
  language-independent.
- **Scope boundary**: the machinery above governs hoglake's own schema
  from v1 forward. The ducklake→hoglake transition is NOT migration
  file #1 — the schemas differ in tables and relationships, so it's a
  **one-time conversion script** that reads a ducklake catalog and
  writes a fresh hoglake database (see phase 7). Migrations evolve
  hoglake; the converter escapes ducklake. Keeping them separate means
  the migration chain never carries ducklake compatibility shims.

## Implementation language

Not decided; criteria that matter, given the above:

| Criterion | JVM | Rust | Go |
|---|---|---|---|
| Parquet write quality | parquet-java (the pain you know) | arrow-rs/parquet-rs: excellent | weakest of the three |
| Iceberg REST facade leverage | iceberg-java: best | iceberg-rust: maturing | iceberg-go: partial |
| Trino affinity (future native connector) | native | via REST only | via REST only |
| Postgres story | mature | sqlx/tokio-postgres: mature | mature |
| Team fit | strong (Jakob) | intermediate | learning, org momentum |

Note the escape hatch: if the server never touches parquet bytes
(clients write files; server only registers footers — the commit
protocol above), the JVM's parquet weakness mostly stops mattering,
and the language choice becomes an API-server choice. That argues for
deciding the commit protocol *before* the language.

## Phases

1. **Docs (this branch, now)**: API map, schema reference, this plan.
2. **Spec**: metadata schema DDL v1 + the control-plane API (commit,
   snapshot/schema/file read, changefeed, offsets, maintenance
   triggers) + Iceberg-mapping notes. TLA for the commit protocol —
   we know from viaduck that the model checker pays rent.
3. **MVP service**: schema + migrations, read APIs, append commit with
   footer-stats registration, against a copy of a real duckling
   catalog's converted metadata.
4. **Iceberg REST facade** → Trino reads a hoglake table.
5. **First writer**: viaduck's append path (post consumer-group
   refactor it's already shaped as "write parquet, commit offsets" —
   the sink-side cursor becomes a hoglake consumer offset).
6. **Maintenance parity**: compaction, expiry, GC as service jobs —
   retiring the cron fleet and both maintenance codepaths.
7. **Migration tooling**: the one-time `ducklake_*` → hoglake
   converter. Not a schema migration — a transform between different
   table sets and relationships, run per catalog at cutover. It must:
   remap versioned rows into the new FK-backed shape (and *prove* the
   FKs hold on real data before cutover — every orphan class in the
   defect ledger is a row the new constraints will reject); flatten
   the 3-level relative-path chain if we change path semantics; carry
   the row-id allocators and per-file `row_id_start` unchanged (the
   lineage guarantee spans the conversion); flush any residual inlined
   data to parquet first (inlining doesn't exist on the other side);
   convert consumer state (viaduck cursors → hoglake consumer
   offsets); and decide snapshot-history depth explicitly (full
   history vs head+retention window — at 15.3M snapshots, converting
   everything is a choice, not a default). Verification = dual-run
   comparison (row counts, file manifests, stats spot-checks, cursor
   parity) before the duckling flips. Fork enters maintenance-only
   mode at phase 5, dies after 7.

## Decisions (2026-09-04)

- **Inlined data: DROPPED from v1** — consequences folded into the
  schema section above. Refinement (same day): if inlining ever
  returns, it returns as **Arrow-IPC blobs, not row-splatting** — an
  inlined batch is a `data_file` row with `format='arrow_ipc'` whose
  storage is a catalog blob instead of an object-store path. That
  shape fixes everything that made DuckLake's inlining rot: no dynamic
  per-schema-version tables (the 112K-registry/unreachable-GC class
  dies), no per-backend type mapping (IPC is self-describing, carries
  STRUCT/VARIANT/everything), row-id ranges and shipped stats work
  exactly like any file, and "flush" is just compaction rewriting
  inline-format files to parquet. Costs to accept if adopted: rows in
  a blob aren't individually addressable, so inline data is
  append-only and deletes/updates against the inlined range force a
  flush first; and the Iceberg facade can't represent path-less files,
  so its freshness is bounded by flush cadence. The `data_file.format`
  tag already leaves this slot open — no v1 schema work needed to keep
  the option.
- **Row lineage: GUARANTEED.** Stable rowids survive compaction and
  recreation. Two obligations follow: (1) hoglake compaction always
  materializes row ids when rewriting files — the fork's
  `merge_adjacent_files` rowid-remap hazard (positional reassignment
  under SORTED BY) becomes a bug class the service cannot have; (2)
  row-id allocation moves to the commit endpoint, handing out
  monotonic per-table ranges that are **never reused** — which also
  kills the reuse-on-upsert-recreate identity bug that viaduck's
  Phase 2 had to defend against. CDC keeps rowid as a first-class,
  trustworthy identity.
- **Iceberg facade: READ-ONLY.** The metadata model stays
  Iceberg-mappable for reads (snapshot → snapshot, data_file →
  manifest entry, stats → metrics); write-path Iceberg compatibility
  is out of scope. Trino/Spark read through their stock Iceberg
  connectors; all writes go through hoglake's own API.
- **Footer-shipping file registration is day one.** The commit/insert
  API takes file paths + footer-derived stats supplied by the writer
  (the Iceberg `DataFiles.Builder.withMetrics` /
  `ParquetUtil.footerMetrics` model): the writer just wrote the file
  and holds the footer in memory, so the server never opens a parquet
  file to admit it. This is the *only* insertion path — not an
  optimization next to a scan-based one. The `add_data_files`-style
  import of pre-existing files is the same endpoint with the client
  library doing a footer-only read first. Server-side validation stays
  cheap and structural (schema/field-id compatibility, stats shape,
  row-id range assignment); trust-but-verify deep checks belong to a
  background job, not the commit path.
  **Plus a stats-deferred mode**: a file may register *without* stats,
  which the service then fetches asynchronously (footer-only read in a
  background hydrator). Each data file carries a stats state
  (`provided | pending | failed`); a pending file is
  never pruned — it matches every scan and exports to the Iceberg
  facade with null stats (legal; readers just can't skip it) — so
  correctness holds and only pruning quality lags until hydration.
  This makes bulk import of pre-existing files (the case where footer
  reads are the dominant cost — 100K files on S3) fast at
  registration time. One hard constraint from the row-lineage
  decision: `record_count` cannot be deferred, because the commit
  endpoint sizes the file's row-id range at registration — writers
  always know their row count, and imports get it from the manifest
  or accept a footer-count-only read (still far cheaper than full
  stats assembly). Hydration failure is loud (state + metric), and
  the hydrator doubles as the trust-but-verify pass for
  writer-shipped stats.
- **Tenancy: one service, many catalogs** — with the explicit note
  that this is an area to explore. The operational leverage (one
  deploy, one upgrade, fleet-wide fixes land once) is the point of the
  service; blast radius gets managed inside it (per-catalog connection
  pools, per-tenant admission/rate limits, catalog-scoped circuit
  breakers) rather than by process isolation. Revisit if a noisy
  tenant demonstrates the isolation model matters more than the
  leverage.

## AuthN/Z (called out; decision pending)

Today's model is "whoever has the Postgres URI can do anything,
including DDL and raw metadata writes" — one of the things hoglake
exists to fix. The decision space to work through before the API spec
(phase 2):

- **Writer ↔ control plane**: per-writer credentials (static tokens vs
  short-lived OIDC/service-account tokens vs in-cluster mTLS). The
  fleet is all in-cluster today, which makes mTLS or projected service
  account tokens cheap; external access (operators, notebooks) needs
  the token path anyway.
- **Authorization granularity**: catalog-scoped at minimum;
  table-scoped and verb-scoped (read / append / ddl / maintenance)
  worth designing in even if v1 ships coarse. Maintenance verbs should
  be separately grantable — the compaction job must not hold DDL.
- **Data-plane credentials**: the control plane can vend scoped,
  short-lived object-store credentials per table/prefix (the Iceberg
  REST credential-vending model). That removes bucket-wide S3 keys
  from every writer and reader — arguably a bigger security win than
  the API auth itself, and it falls out naturally once the facade
  speaks Iceberg REST.
- **The Iceberg facade inherits whatever we pick** — its read
  endpoints need the same token check and can reuse the vending path.
- **Whatever we pick feeds the audit log**: every audit event carries
  the authenticated principal, which is what makes the trail worth
  having — "the Postgres URI did it" is the anti-pattern we're
  retiring on both fronts at once.
