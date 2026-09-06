# Operational Notes — hoglake at 2PB / 1T rows (2026-09-05)

Scale context: the DuckLake deployment hoglake replaces stands at 2PB
and ~1T rows. The straight answer to "how does hoglake improve on
this": at this scale hoglake changes almost nothing about the *data
plane* and nearly everything about the *metadata plane* — which is
where every production scar came from. The parquet bytes are the same
bytes. What changes is that the catalog stops being the bottleneck,
the liability, and the incident generator.

## What doesn't change (be honest about this)

- **Storage cost / scan throughput of the data itself.** 2PB of
  parquet is 2PB of parquet. Trino/Spark scan speed is the engine's
  problem and the file layout's problem — hoglake helps the layout
  only indirectly (compaction, stats quality).
- **The object store.** S3 throughput, listing costs, egress —
  unchanged.

## What changes, quantified against DuckLake's observed numbers

| DuckLake at scale (observed) | Hoglake | How |
|---|---|---|
| Commit loads stats for the entire catalog: 5–7s/attempt at 59K tables / 3.7M stats rows; 190–264s commits under OCC retry | p50 2.0–3.6ms commits, flat vs. preseeded history (bench) | Write-set-scoped commit; O(catalog) loads structurally impossible. ~5 orders of magnitude on the pathological end |
| OCC conflict = snapshot-PK collision; retry re-pays the whole commit | Typed change rows, one indexed anti-join; appends never conflict with appends | `hog_snapshot_change` + `(catalog, object, kind, snapshot)` index; bench: k=8 writers, 368 commits/s, 0 conflicts |
| Expiry ~14ms/snapshot → ~50h to drain a 15.3M-snapshot backlog; DDL churn grows it forever | 5.6–14.5K snapshots/s (bench), continuous incremental sweeps | Range deletes + cascade; ~100–200× drain rate, continuous instead of cron-batched |
| 99.4% of stats rows for dropped tables; purging them bought 30–50× | Unrepresentable | FK `ON DELETE CASCADE`; drop/expiry removes stats rows with the file rows |
| 50M orphaned partition-value rows; 9.99M-row phantom deletion queue | Unrepresentable | FKs + drain-time liveness check; the queue is a suggestion, never an authorization |
| Compaction recovery emitting 60–180s commits → convoy, 13 restarts | `max_groups_per_run` (default 1) per sweep; commit is small metadata under the lock; execution entirely outside any transaction | The 2026-09-04 incident shape designed out: foreground writers wait ms, never on S3 IO |
| Two engines, one catalog (millpond upstream + viaduck fork); drift; fork fixes never reaching maintenance | Exactly one catalog implementation, deployed once | Fork-drift / extension-distribution / OOM-loop class dies |
| CDC: `table_changes()` read-barrier crawl; every consumer hand-builds cursors; expiry silently destroys unread ranges | Changefeed API + catalog-resident consumer offsets + retention floor + 410 | hedgerow rows-then-offset, halt-on-incarnation; expiry *pages* when a consumer pins instead of silently losing its data |
| Rowid reuse on upsert-recreate; sorted compaction silently remaps rowids | Server-assigned, never-reused, `explicit_row_ids` through compaction | Every CDC consumer's defensive machinery at 1T rows — retired |
| Recovery artisanal (split-brain reconstructed from S3 delete markers) | Liveness-checked soft deletion + forensics ledger + audit trail keyed to principals | The drain ledger keeps what was deleted, when, why, after how many attempts |

## The catalog's own size at 2PB / 1T rows (the sizing question)

- **Files**: 2PB at ~256MB/file ≈ ~8M `hog_data_file` rows. Trivial
  for Postgres with the partial live index.
- **Stats** — the real volume driver: 8M files × ~50 columns ≈ ~400M
  `hog_file_column_stats` rows, ~60–80GB with indexes. The catalog's
  dominant table by far. Postgres handles it (narrow PK-clustered
  btree, cascade-deleted with its file), but note it: the catalog
  stays small relative to 2PB (~0.004%), and stats are what you'd
  partition first if it ever stopped being small. The DuckLake
  version of this table was smaller only because it was broken (3.7M
  rows, 99.4% garbage).
- **Row-id allocator**: 1T rows against a bigint `next_row_id` — 2^63
  headroom, a non-issue by design, with the CHECK + `addExact`
  two-layer defense against overflow.
- **Snapshots**: the busiest writer mints ~138K/day; the advisory-lock
  tail sustains hundreds/s. ~100× headroom, and bench says commit
  latency doesn't move with history depth.

## The honest ceilings to watch

1. **One Postgres per service; one advisory-lock tail per catalog.**
   Commit throughput per catalog is capped at 1/tail-latency. At the
   observed ~1.6 mints/s average (bursts higher), fine. If a single
   catalog ever needs sustained >100 commits/s, that's the wall —
   [split-validation-commit.md](split-validation-commit.md) is the
   escape hatch (validation in front of the serialization point rather
   than inside it). Designed, not scheduled.
2. **Read scaling.** Facade/Trino planning reads hit the service →
   one PG. Snapshot-pinned reads are replica-able (lag-tolerant by
   construction), but that's not built yet.
3. **Compaction IO cost at 2PB.** The never-convoy design (one group
   per sweep, full download+rewrite locally, heap-materialized
   survivors) deliberately throttles compaction *throughput*. At 2PB
   with continuous small-file ingestion, whether
   `max_groups_per_run=1` keeps up with the small-file accumulation
   rate is a real capacity-planning question — the knob exists; the
   tradeoff is intentional.
4. **Consumer-floor pinning at 2PB.** A stuck consumer pins expiry →
   end-snapshotted files accumulate as storage. DuckLake's answer was
   silent loss; hoglake's is bounded staleness + a page. Correct
   tradeoff, but at 2PB the pinned-storage bill grows faster.

## The migration note

2PB doesn't move — the converter is metadata-only (paths carried,
row-id allocators preserved, lineage spans the cutover). The real
decision is snapshot-history depth: converting 15.3M snapshots is a
choice, not a default — at this scale, head+retention-window. And the
FK proof matters: every orphan class in the defect ledger is a row the
new constraints will reject, so the converter *is* the audit of how
dirty the old catalog actually is.

## Bottom line

At 2PB/1T, DuckLake's failures were never about the data being big —
they were about every commit, expiry, and maintenance run paying
O(catalog) against a metadata store with no integrity and no arbiter.
Hoglake's numbers at bench scale (flat commits, 0 conflicts, 100–200×
expiry, still_referenced=0) are the structural answers to exactly
those numbers. The data plane was never the problem.
