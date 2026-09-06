# Hoglake vs Apache Paimon (2026-09-05)

Paimon 1.x is the streaming-lakehouse incumbent; 2.0 is in discussion
(data evolution, row tracking, multimodal/vector, global indexes, DV
for data-evolution tables merged June 2026, paimon-vortex in flight).
It's the most interesting compare for hoglake because it's the only
major table format that *also* made streaming CDC a first-class
citizen — and it did so with a fundamentally different bet about where
the intelligence lives.

## The one-sentence difference

**Paimon puts the merge machinery in the storage format; hoglake puts
it in the catalog service.** Paimon's answer to "how do I update rows
in a lake" is "structure the files so updates are cheap" (LSM tree,
buckets, merge engines, changelog producers). Hoglake's answer is
"structure the *catalog* so commits are cheap and consumers are known"
(typed OCC, consumer offsets, DVs as catalog state). Same problem,
opposite layers.

## Compare / contrast

| Axis | Hoglake | Paimon |
|---|---|---|
| **Architecture** | Catalog-as-service; clients never see the metadata DB | Catalog-as-library; pluggable backends (FileSystem / Hive / JDBC / REST) |
| **Storage model** | Immutable parquet files + DVs; no merge-on-read tree | LSM tree per bucket (sorted runs), merge-on-read by design |
| **Update model** | Deletion vectors only (append + mask); no in-place merge | Full merge engines: dedup / partial-update / aggregate / first-row |
| **Row identity** | Server-assigned, never reused, `explicit_row_ids` survives compaction | Row tracking (`_ROW_ID`) exists but `rewrite-row-ids=true` compaction **reassigns** them (their own docs warn: "invalidates row-id based references") |
| **CDC** | Catalog-native: changefeed API + per-consumer offsets in the catalog + retention floor | Changelog producers (none/input/lookup/full-compaction) — files, not consumers; the consumer's cursor is its own problem |
| **Commit conflict** | Typed change rows, one indexed anti-join; appends never conflict | Catalog lock factory (hive/jdbc/zk); conflict surface is the snapshot file |
| **Stats / metadata** | Rows in Postgres; one indexed read | Manifest files in object storage (snapshot → manifest-list → manifest) |
| **Query engines** | REST facade (Iceberg-mappable) + native Trino connector; DuckDB/Hadoop-FS sketched | Flink-native first, Spark second, Hive via storage handler |
| **File format** | Parquet now, format tag for later (Vortex etc.) | Parquet/ORC/Avro + paimon-vortex/lance/mosaic in 2.0 |

## Where Paimon's bet wins

- **Updates at streaming rate.** Hoglake's DV model is append + mask;
  Paimon's LSM genuinely *merges* — partial updates, aggregates,
  first-row-wins — with read-time or write-time merge selectable per
  table. If viaduck-class workloads ever need "upsert 50K keys/sec into
  the lake," Paimon does it natively and hoglake answers with "write
  DVs and compact." That's a real gap, not a styling preference.
- **No server to run.** FileSystemCatalog needs nothing but object
  storage. Hoglake's whole point is the opposite, but it's worth
  saying plainly: Paimon's operational floor is lower.
- **Changelog producers are a genuinely good idea hoglake lacks.** The
  `input`/`lookup`/`full-compaction` producer spectrum is a richer
  answer to "how does a consumer get a complete changelog" than
  hoglake's file-list + DV-diff changefeed — particularly
  full-compaction's "compare two snapshots and emit the delta," which
  hoglake cannot do today without the consumer diffing manifests.

## Where hoglake's bet wins

- **Row identity is a contract, not a mode.** Paimon's row tracking is
  real, but its own Data Evolution docs carve out
  `rewrite-row-ids=true` compaction as the escape hatch that
  "invalidates row-id based references and drops global indexes."
  Hoglake made the opposite call (README decision 2026-09-04: lineage
  GUARANTEED, `explicit_row_ids`, never-reuse) precisely because the
  DuckLake experience proved every downstream CDC consumer ends up
  defending against identity reuse. Paimon kept the footgun and
  documented it; hoglake removed it.
- **The catalog knows consumers exist.** Paimon's changelog is files;
  consumption state is entirely the consumer's problem (the same gap
  that made viaduck build its own cursor store over DuckLake). Hoglake
  consumer offsets + the retention floor + 410-on-expiry is the
  collapse of that entire problem class. Paimon has no equivalent and
  its architecture (library, no service) makes one hard to add.
- **Commit contention.** Paimon's lock factory is HMS/JDBC/ZK-mediated
  and conflicts surface as snapshot-file races — the same genus as
  DuckLake's PK-collision OCC. Hoglake's typed-change anti-join +
  advisory-lock tail is strictly more civilized, and appends never
  conflict with appends by construction.
- **One implementation, deployed once.** Paimon's catalog backends
  (FS/Hive/JDBC/REST) plus per-engine connectors is exactly the
  "N client libraries, one fleet" shape hoglake exists to kill.
- **Stats are indexed rows, not manifest walks.** Paimon's
  snapshot → manifest-list → manifest chain is the Iceberg object-store
  metadata model — fine until 59K tables, which is precisely the
  DuckLake failure hoglake's schema answers.

## The honest verdict

These are not really competitors; they're the two coherent answers to
"lakehouse for streaming writers," optimized for different centers of
gravity:

- **Paimon** is the better answer when the workload is *update-heavy
  streaming upserts* and the org is Flink-shaped and doesn't want to
  run a catalog service. Its 2.0 direction (multimodal, vector indexes)
  is a bet that the lake is also an AI store — hoglake has no opinion
  there.
- **Hoglake** is the better answer when the workload is *append-heavy
  CDC fan-out with correctness requirements* (viaduck/hedgerow's exact
  shape), when many readers of different engine genera must share one
  catalog, and when the operating experience of the *catalog itself*
  (one owner, one version, observable, consumer-aware retention) is the
  thing that's been hurting.

The features worth stealing from Paimon, in order: **(1)** the
changelog-producer spectrum — a "snapshot-diff" changefeed mode would
close hoglake's "consumer must diff manifests" gap; **(2)**
merge-engine semantics as a *table property* if hoglake ever grows
upserts beyond DVs — partial-update and aggregate are the two that
would actually pull weight; **(3)** precommit compaction of changelog
files, which is the same small-files problem hedgerow's
`max_rows_per_append` buffer papers over.

The features hoglake has that Paimon should steal: never-reused row
ids, catalog-resident consumer offsets, and a retention floor that
knows what it would destroy.
