# Hoglake — Postgres-Native Lakehouse Catalog Control Plane

Agent guidance for the `hoglake/` subtree. The enclosing repo is the
PostHog DuckLake C++ fork; everything hoglake lives under this
directory and does not touch the fork's `src/`.

## Pre-push checklist

**Never push broken code.** Before every commit and push:

```bash
just test-all        # server suite (Docker required) + pyhoglake suite
just server lint     # ktlint check (server Kotlin style gate)
just webui test      # vitest (no server needed)
just hedgerow test   # unit; integration needs a live server
```

For the full end-to-end pass (client/hedgerow integration tests against
a real server): `just server compose-up && just server run` in another
terminal first — integration tests skip cleanly when no server is up,
so a green run without one is NOT a full verification. Say which you
ran.

The server suite includes the **schema equivalence gate**
(`just server schema-check`): fold(migrations) must equal `schema.sql`.
If you touch a migration, update `schema.sql` in the same change or
this fails.

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
| `server/` | The control plane: DDL, commits (OCC), scans, changefeed, offsets, retention/expiry/cleanup, hydrator, metrics, audit | Kotlin 2.2 / JDK 21 (flox) / Ktor / JDBI / Flyway | JUnit5 + Testcontainers (PG16, MinIO) + kotest-property |
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
3. **One live deletion vector per data file** (unique partial index);
   supersessions only grow (`delete_count` monotone); a DV newer than
   your `read_snapshot` is a 409, never a lost update.
4. **Physical deletion is never authorized by the queue**: cleanup
   liveness-checks every path against live references at drain time;
   `still_referenced > 0` is an invariant violation, alerted, not
   deleted.
5. **Expiry never passes head or (when `consumer_floor`) the min
   consumer offset**, and names the pinning consumer. Ranges below
   `earliest_snapshot_id` are 410 Gone — consumers reconcile, never
   silently skip.
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
  (404/409/410/422; parse failures 400). New failure modes get a typed
  exception, not a status code sprinkled in a route.
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

- **Compaction (M4) — UNBLOCKED (decision 2026-09-05)**: Hardwood
  1.1.0.Beta1 reads `PARQUET:field_id` but drops it on write (verified
  to bytecode), so the compaction rewrite writer is **parquet-java**,
  quarantined in the compaction module — the Hadoop dependency tree
  does not leak elsewhere. Hardwood stays for footer reads (the
  hydrator; read side is correct and tested). An upstream Hardwood
  field-id-write fix is being pursued; when it lands, swap the
  compaction writer back and drop parquet-java. pyarrow (pyhoglake)
  writes field ids correctly already.
- **CDC publications to Kafka (the WAL tap)**: fully specified in the
  OpenAPI (501s) + README; not implemented.
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
