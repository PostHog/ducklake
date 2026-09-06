# Hoglake — Postgres-Native Lakehouse Catalog Control Plane

Agent guidance for the `hoglake/` subtree. The enclosing repo is the
PostHog DuckLake C++ fork; everything hoglake lives under this
directory and does not touch the fork's `src/`.

## Pre-push checklist

**Never push broken code.** Before every commit and push:

```bash
cd server && flox activate -- ./gradlew :test         # server suite via the wrapper (Docker required)
just lint-all        # ktlint + ruff (check & format) across both Python trees
just pyhoglake test  # pyhoglake suite
just webui test      # vitest (no server needed)
just hedgerow test   # unit; integration needs a live server
```

`ruff` is **pinned** in both Python trees' dev groups and run through
`uv run` — never `uvx ruff`, which resolves a different rule set per
machine and silently ignores the projects' `per-file-ignores`.

The server builds through the **checked-in Gradle wrapper**
(`server/gradlew`, pinned in
`server/gradle/wrapper/gradle-wrapper.properties`) — never a
system-installed gradle. The `just server ...` recipes route through it
too, so `just test-all` (server + pyhoglake) stays equivalent.

For the full end-to-end pass (client/hedgerow integration tests against
a real server): `just server compose-up && just server run` in another
terminal first — integration tests skip cleanly when no server is up,
so a green run without one is NOT a full verification. Say which you
ran.

The server suite includes the **schema equivalence gate**
(`just server schema-check`): fold(migrations) must equal `schema.sql`.
If you touch a migration, update `schema.sql` in the same change or
this fails. It also includes the **mapper-coverage gate**
(`MapperCoverageGateIntegrationTest`): every hog_* table's live columns
must equal the set declared in `persistence/HogSchemaColumns.kt` — a
new column means updating the row mapper(s) named there AND the
declaration, or the gate fails naming the table and column.

Prefer fixup commits over amending and force-pushing.

## What this is

The catalog is a **service, not a client library** (see
[README.md](README.md) — the design doc — and the defect ledger for
why). Clients never see the backing Postgres. Writers write parquet to
object storage themselves and register it via footer-shipping commits;
readers plan from metadata. Kotlin/Ktor/JDBI server, Python client,
React console, Python replication daemon:

| Component | What | Stack | Tests |
|---|---|---|---|
| `server/` | The control plane: DDL, commits (OCC + admission backpressure), scans, changefeed, offsets, retention/expiry/cleanup, hydrator, compaction, verify, metrics, audit | Kotlin 2.2 / JDK 21 (flox) / Ktor / JDBI / Flyway / parquet-java (footer reads + compaction writes) | JUnit5 + Testcontainers (PG16, MinIO) + kotest-property |
| `pyhoglake/` | Thin API client; owns the Python writer path (parquet with field IDs, footer stats, Iceberg bounds codec) | Python 3.12 (flox) / uv / httpx / pyarrow | pytest + pytest-httpx + hypothesis |
| `webui/` | Lakekeeper-style management console: catalog browser (namespaces/tables/files/scan with time travel), newest-first snapshot timeline (`before` paging), consumers (grouped, names resolved, dropped badges), compaction-debt page, `/metrics` visualizer, instance-name badge; int64 wire fields carried as strings (lossless above 2^53) | Vite / React / TS | vitest (mocked fetch) |
| `hedgerow/` | viaduck's successor: source table → destination table replication, append-only, single-destination | Python / uv / pyhoglake | pytest; scripted-fake unit + live integration |

The REST contract is `server/src/main/resources/openapi/hoglake.yaml`
— it is the single source of truth for wire shapes; server routes,
pyhoglake, webui fixtures, and hedgerow all conform to it. Change the
spec and the implementations together.

## Invariants (violating any of these is a bug, full stop)

1. **Snapshot ids are dense per catalog** and ordered with commit order
   (assigned inside the per-catalog advisory-lock commit tail:
   `pg_advisory_xact_lock((4740871::bigint << 32) | (catalog_id & 4294967295))`
   — every DDL/commit tail uses `persistence/Locks.kt`; the key must be
   computed identically everywhere or serialization silently breaks).
2. **Row-id ranges** are assigned server-side at commit, contiguous per
   file, tile `[0, total)` per table, never reused — the lineage
   guarantee. `table_uuid` changes on drop+recreate; consumers key on
   it and must SEE incarnation changes (hedgerow halts on them).
   Compaction preserves ids by materializing them: outputs carry an
   explicit physical `_hog_row_id` int64 column under the **reserved
   parquet field id 2147483646** (never allocatable to a real column),
   flagged by `hog_data_file.explicit_row_ids` — when set,
   `row_id_start` is only min(input ids), not positional. Sorting on
   rewrite is safe *only* because of this; positional reassignment of
   merged rows is the predecessor's rowid-remap bug and must never
   return.
3. **One live deletion vector per data file** (unique partial index);
   supersessions only grow (`delete_count` monotone); a DV newer than
   your `read_snapshot` is a 409, never a lost update.
4. **Physical deletion is never authorized by the queue**: cleanup
   liveness-checks every path against live references at drain time;
   `still_referenced > 0` is an invariant violation, alerted, not
   deleted. Draining soft-deletes: settled `hog_file_removal` rows keep
   `drained_at`/`drained_outcome` (the queryable forensics ledger,
   purged past `HOGLAKE_REMOVAL_LEDGER_RETENTION_SECONDS`, default 30d);
   undrained rows accumulate `attempts`/`last_attempt_at`. The ledger
   is also commit-integrated both ways: commits 409 any registered path
   with an undrained row (path-reuse guard), drain sub-batches run
   under the per-catalog commit lock, and compaction pre-registers its
   output path as a `compaction_staging` claim settled
   `'registered'` on group commit.
5. **Expiry never passes head or (when `consumer_floor`) the min
   consumer offset**, and names the pinning consumer. Ranges below
   `earliest_snapshot_id` are 410 Gone — consumers reconcile, never
   silently skip; the floor advance captures
   `hog_catalog.earliest_snapshot_time` so 410s can say WHEN the floor
   was reached. A fifth sweep step deletes versioned DDL rows
   (`hog_table_version`/`hog_column`/`hog_partition_spec`/`hog_sort_spec`/
   `hog_view`) whose `end_snapshot <= earliest_snapshot_id` — invisible
   at every retained snapshot, so DDL churn cannot grow them unbounded.
6. **Versioned-row visibility**: a row is visible at S iff
   `begin_snapshot <= S AND (end_snapshot IS NULL OR S < end_snapshot)`.
   Every read path uses exactly this predicate.
7. **TableInfo aggregates come from files visible at the requested
   snapshot**, never from `hog_table_stats` (that row is the gross
   append counter / row-id allocator anchor — head-scoped by nature).
8. **Audit/observability never rides a transaction** and never writes
   to any database. Audit emits after commit/rollback; metrics are
   passive.
9. **All SQL is parameterized.** No string-built values, anywhere.
10. **Rows-then-offset everywhere** (server offset API is monotonic;
    hedgerow commits offsets only after destination durability).

## Working conventions

- **Migrations**: plain SQL in `server/src/main/resources/db/migration/`.
  **Pre-release: one squashed `V1__init.sql`** — edit it directly and
  keep `schema.sql` identical (the equivalence test enforces it). The
  chain becomes append-only at the first real release. FKs with
  CASCADE, partial indexes for hot predicates, CHECK-constrained
  vocabularies (deliberate choice over PG enums while the vocabulary
  churns). No migration ledger hacks — Flyway owns it.
- **Change kinds** (`hog_snapshot_change.kind`) are the typed OCC
  vocabulary; adding one = migration + schema.sql + `ChangeKind` enum +
  conflict-rule review in `CommitService`.
- **Errors**: services throw `HoglakeException.*`; the API maps them
  (404/409/410/422; commit admission timeout 503 + Retry-After; parse
  failures 400). New failure modes get a typed exception, not a status
  code sprinkled in a route.
- **Background loops are coroutines**: every periodic job (hydrator,
  expiry, cleanup, compaction, metrics sampler) registers with
  `BackgroundLoops` (one supervisor scope owned by
  `App.startBackground()`) — never a raw daemon thread. Contracts:
  `intervalMs <= 0` = disabled; a failed iteration is logged + counted
  (`hoglake_background_loop_failures_total{loop}`) and the loop (and
  its siblings) keeps running; shutdown is structured and bounded
  (cancel + join, 5s hard cap). Tests drive the services'
  `runOnce`/`sampleOnce` entry points directly, not the scheduler.
- **Multi-agent work**: partition by package/file ownership; frozen
  shared files (build files, Model.kt, migrations, spec) change only
  through the integrating session; agents report needed changes rather
  than making them. Concurrent gradle runs contend on the build dir —
  EOFException in `:test` results is contention, rerun.
- **Cross-language codec vectors**:
  `pyhoglake/tests/vectors/bounds_vectors.json` keeps the Kotlin and
  Python Iceberg single-value codecs bit-identical. A fuzzer-found
  nasty value gets promoted into it. See [fuzzing.md](fuzzing.md).
- **QE culture**: substantive changes get an adversarial review or QE
  agent pass before merge; bugs found by tests/fuzzing become pinned
  regression tests + (design-class ones) defect-ledger entries.

## Known deferrals / open items

- **Compaction (M4) — 100% implemented**: `server/compaction/` —
  planning is metadata-only (live, same spec + partition values, under
  target bytes; adjacency NOT required), rewrite via **parquet-java**
  (the project's one parquet library — decision 2026-09-05: Hardwood is
  out entirely; parquet-java handles footer reads in the hydrator AND
  the compaction writer), commit under the catalog lock with input
  re-verification. **DV-bearing files compact — LANDED**: the rewrite
  APPLIES each input's live DV (puffin `deletion-vector-v1`, read via
  `PuffinDeletionVector`) — survivors keep their ids in `_hog_row_id`,
  deleted ids are gone forever, the DV rows end-snapshot with their
  files, and the commit re-verifies the exact planned DV identity (a
  vector that grew/appeared since planning skips the group:
  `dv_superseded` — a post-plan delete is never dropped).
  **Heterogeneous-schema groups — LANDED**: inputs map to the LIVE
  schema by field id (missing columns null-fill, int→long/float→double
  up-cast, dropped field ids drop their data); only a live column
  unproducible from an input's physical type skips the group
  (`unconvertible_schema`). **Aborted-upload orphans — LANDED**: the
  output path pre-registers as an undrained `hog_file_removal` row
  (reason `compaction_staging`) before upload; a successful group
  commit settles it (`drained_outcome='registered'`) in the same
  transaction, an aborted group leaves it for the normal cleanup drain
  to reclaim, and the commit re-claims the ticket first so a drain that
  won the race just aborts the group. Still deliberate: the background
  loop defaults OFF (`HOGLAKE_COMPACTION_INTERVAL_MS=0`) — flipping it
  on is an ops decision, not a code gap. Remaining rewrite deferrals
  (all surface as `unconvertible_schema` skips, never wrong bytes):
  nested schemas, INT96, decimal-scale changes, non-micros time(stamp)
  units.
- **Field ids are a contract**: the hydrator's footer read flags files
  whose parquet schema has any leaf without `PARQUET:field_id`
  (`hog_data_file.missing_field_ids`; gauge
  `hoglake_missing_field_id_files{catalog}`; the reserved `_hog_row_id`
  id 2147483646 is fine). While a flagged file — or a still-`pending`
  file, whose id state is unknown until the footer is read — is LIVE,
  `rename_column` is refused with 409 `idless_files_present` (id-less
  files bind columns by name; renaming would silently NULL their
  history in readers). `rename_table` is unaffected. Files registered
  with inline stats never pass through the hydrator, so only
  deferred-stats files get checked — still a known gap: the verify
  endpoint is metadata-only by design and can't do the S3 footer reads
  this check needs.
- **Maintenance verify — LANDED**: `POST
  /v1/catalogs/{c}/maintenance/verify` (gaps.md B3, absorbing B4) — the
  QE suite's global-invariant SQL as a metadata-only, read-only
  endpoint (REPEATABLE READ MVCC snapshot, no catalog lock): row-id
  tiling (explicit_row_ids-aware), DV uniqueness/monotonicity/bounds,
  orphaned live rows on dropped tables, still-referenced removal-queue
  entries, true snapshot density, next_row_id consistency.
- **CDC publications to Kafka (the WAL tap)**: fully specified in the
  OpenAPI (501s) + README; not implemented.
- **DR/export**: `GET /v1/catalogs/{c}/export` specified in the OpenAPI
  (snapshot range + live-file manifest + consumer offsets, consistent
  at head); 501 stub until built (gaps.md B5).
- **Iceberg REST facade + Trino**: design obligations in
  [iceberg-federation.md](iceberg-federation.md) /
  [trino-integration.md](trino-integration.md); v1 schema already
  conforms (typed bounds, Iceberg transforms, DV-only deletes).
- **Auth**: out of scope for v1; audit actor is `anonymous` until it
  lands. Decision space in README §AuthN/Z.
- pyhoglake implements `truncate[W]` partition transforms; the server's
  Transform vocabulary doesn't include truncate yet (client-ready,
  server gap).
- No catalog delete API (QE/integration runs leave disposable
  `qe-*`/`pyhog-*`/`hedgerow-*` catalogs behind on dev stacks).
- `bench/` is outside `just lint-all`: it passes `ruff check` but 12
  files fail `ruff format --check`. Format it and wire it in (pin ruff
  in its dev group, add the `lint` recipe) as its own change, so the
  reformat lands as a reviewable diff rather than noise inside another.

## Doc index

**Design and decisions** — [README.md](README.md) (the design doc) ·
[metadata-schema.md](metadata-schema.md) (the schema, table by table) ·
[iceberg-federation.md](iceberg-federation.md) /
[trino-integration.md](trino-integration.md) (engine surfaces) ·
[split-validation-commit.md](split-validation-commit.md) (the commit
tail's escape hatch — designed, not scheduled) ·
[duckdb-read-extension.md](duckdb-read-extension.md) (DuckDB back as a
client, sketch).

**Assessments** — [operational-notes.md](operational-notes.md) (what
changes, and what honestly doesn't, at 2PB/1T) ·
[sql-suggestions.md](sql-suggestions.md) (schema review) ·
[suggestions.md](suggestions.md) (language/stack retrospective) ·
[paimon-compare.md](paimon-compare.md) (the closest comparable).

**Predecessor and process** —
[ducklake-defect-ledger.md](ducklake-defect-ledger.md) (the bugs this
architecture answers) · [ducklake-api-map.md](ducklake-api-map.md) /
[pyducklake-api-map.md](pyducklake-api-map.md) (predecessor surfaces) ·
[fuzzing.md](fuzzing.md) · [source-inventory.md](source-inventory.md).

**Per-component** — [server](server/README.md) ·
[trino connector](server/trino/README.md) ·
[pyhoglake](pyhoglake/README.md) · [webui](webui/README.md) ·
[hedgerow](hedgerow/README.md) · [bench](bench/README.md).

Bug-hunt writeups and in-flight worklists stay local-only (they are
snapshots of a moving tree, not documentation).
