# Trino integration — reads now, writes without a redesign later

Compute is moving out to Trino, so Trino is the facade's first and most
important consumer — and eventually a producer. This doc covers what
the v1 design does to make reads excellent and to keep the write door
open without reversing the read-only-facade decision.

Companions: [README.md](README.md) (decisions),
[iceberg-federation.md](iceberg-federation.md) (the facade's design
obligations — all of which are prerequisites here).

## 1. Reads: everything rides the Iceberg connector

No custom Trino plugin. Trino's stock Iceberg connector pointed at the
hoglake REST facade gets: snapshot isolation, time travel
(`FOR VERSION AS OF` / `FOR TIMESTAMP AS OF` map to our snapshots),
partition + min/max pruning (as good as our manifests — hence the
typed-bounds obligation), and credential vending (the REST spec's
vended-credentials flow, which the AuthN/Z section already plans for —
Trino workers get scoped, short-lived object-store credentials per
table instead of bucket-wide keys).

Read-quality obligations beyond the federation doc:

- **Manifest quality is query performance.** Trino's planning cost and
  split pruning depend directly on manifest granularity and bound
  tightness. Deferred-stats (`pending`) files appear with null bounds
  and are never pruned — fine for correctness, but a table that lives
  mostly-pending will plan poorly in Trino; the hydrator's SLO is
  effectively a Trino-performance SLO.
- **Predictable snapshot pointers.** Trino caches table metadata;
  the facade must serve stable `metadata.json` locations per snapshot
  and a cheap current-pointer lookup, or planning latency eats the
  gains.
- **Type semantics parity.** TIMESTAMPTZ vs TIMESTAMP behavior,
  decimal scale/precision, and UTF-8 collation expectations should be
  validated against Trino's Iceberg type mapping in the phase-4
  milestone ("Trino reads a hoglake table") with a conformance test
  table containing every supported type.

## 2. Writes: design the commit endpoint so the adapter is a translation

The decision stands: v1 facade is read-only, all writes through
hoglake's API. But "Trino as a read/write source" stays reachable
because of two v1 choices:

**2a. The commit shapes are deliberately isomorphic.** An Iceberg REST
commit is *requirements + updates*: assert the snapshot state the
writer built against, append a new snapshot registering new files. The
hoglake commit is: declare the read snapshot, register files with
stats, server runs OCC. Same shape. Keep it that way — specifically:

- The registration API's stats payload is **Iceberg-`Metrics`-shaped**
  (value/null/nan counts, typed bounds, split offsets). Then a
  manifest entry from a Trino write *is* a valid hoglake registration
  with zero transformation.
- Map hoglake OCC conflicts to the Iceberg REST error contract
  (409/`CommitFailedException`) — Trino already retries those
  correctly. Our typed conflicts (README, commit-serialization
  refinements) make this mapping honest rather than string-matched.

**2b. The universal-producer invariant.** Adopt as a design rule:

> Any Iceberg-conformant parquet file is a valid hoglake data file.

Trino's Iceberg writer already embeds field IDs and produces
footer stats. If hoglake requires nothing beyond conformance — no
bespoke footer fields, encryption optional, no writer-side catalog
calls during the write — then Trino, Spark, and anything else that
writes correct Iceberg parquet is a valid *producer*, and enabling
writes is flipping on an adapter, not teaching engines about hoglake.

**2c. Scope external writes to append-only.** Server-side row-id range
assignment works identically for Trino-written files (they register
like any others and get ranges at commit). What does NOT translate:
Trino's delete/update strategies — copy-on-write rewrites and its own
position-delete emission — collide with the row-lineage guarantee
(rewritten files would need server-side rowid remapping Trino knows
nothing about). So the reachable end-state is:

| Operation from Trino | Status |
|---|---|
| SELECT (incl. time travel) | v1, via facade |
| INSERT / CTAS | reachable via REST adapter — append commit, no lineage hazard |
| DELETE / UPDATE / MERGE | not planned — hoglake-native only, lineage-preserving |

Append-only is also the actual requirement for moved-out compute:
transformation jobs write new tables/partitions; row-level mutation
stays with the CDC machinery that understands lineage.

## 3. Operational seams

- **Admission applies to adapter writes too.** A Trino CTAS
  registering 10K files is a maintenance-sized commit; it goes through
  the same commit-admission and fair-queuing as everything else, and
  gets the same explicit backpressure (which Trino surfaces as
  retryable commit failure).
- **Audit**: adapter commits carry the Trino principal through
  credential vending, so the audit log attributes engine writes to a
  real actor, not "the trino service account did something."
- **Conformance testing**: phase 4 grows a two-way suite — hoglake
  writes / Trino reads (v1 gate), and Trino writes-via-adapter /
  hoglake + Trino read-back (gate for enabling 2b in anger).
