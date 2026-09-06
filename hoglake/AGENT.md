# Hoglake — Postgres-Native Lakehouse Catalog Control Plane

Agent guidance for the `hoglake/` subtree. The enclosing repo is the
PostHog DuckLake C++ fork; everything hoglake lives under this
directory and does not touch the fork's `src/`.

## Pre-push checklist

**Never push broken code.** Before every commit and push:

```bash
cd server && flox activate -- ./gradlew :test         # server suite via the wrapper (Docker required)
cd server && flox activate -- ./gradlew :ktlintCheck  # ktlint check (server Kotlin style gate)
just pyhoglake test  # pyhoglake suite
just webui test      # vitest (no server needed)
just hedgerow test   # unit; integration needs a live server
```

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
| `webui/` | Lakekeeper-style management console | Vite / React / TS | vitest (mocked fetch) |
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
   undrained rows accumulate `attempts`/`last_attempt_at`.
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

- **Compaction (M4) — LANDED**: `server/compaction/` — planning is
  metadata-only (live, DV-free, same spec + partition values, under
  target bytes; adjacency NOT required), rewrite via **parquet-java**
  (the project's one parquet library — decision 2026-09-05: Hardwood is
  out entirely; parquet-java handles footer reads in the hydrator AND
  the compaction writer), commit under the catalog lock with input
  re-verification. Deferred: DV-bearing files are never compacted
  (rewriting deleted rows away would change row-id semantics),
  heterogeneous-schema groups stay uncompacted, background loop
  defaults OFF (`HOGLAKE_COMPACTION_INTERVAL_MS=0`), aborted-group
  uploads orphan in the bucket (lifecycle rules are the backstop).
- **Field ids are a contract**: the hydrator's footer read flags files
  whose parquet schema has any leaf without `PARQUET:field_id`
  (`hog_data_file.missing_field_ids`; gauge
  `hoglake_missing_field_id_files{catalog}`; the reserved `_hog_row_id`
  id 2147483646 is fine). While a flagged file is LIVE, `rename_column`
  is refused with 409 `idless_files_present` (id-less files bind
  columns by name; renaming would silently NULL their history in
  readers). `rename_table` is unaffected. Files registered with inline
  stats never pass through the hydrator, so only deferred-stats files
  get checked — a known gap until a verify endpoint exists.
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
- pyhoglake wants: `expected_table_uuid` guard on append (closes a
  name-rebind race hedgerow flagged), single-table offset GET,
  partition-value transforms (partitioned appends currently raise).
- No catalog delete API (QE/integration runs leave disposable
  `qe-*`/`pyhog-*`/`hedgerow-*` catalogs behind on dev stacks).

## Doc index

[README.md](README.md) (design + decisions) ·
[ducklake-defect-ledger.md](ducklake-defect-ledger.md) (the bugs this
architecture answers) · [metadata-schema.md](metadata-schema.md) ·
[ducklake-api-map.md](ducklake-api-map.md) /
[pyducklake-api-map.md](pyducklake-api-map.md) (predecessor surfaces) ·
[fuzzing.md](fuzzing.md) · [source-inventory.md](source-inventory.md) ·
per-component READMEs.
