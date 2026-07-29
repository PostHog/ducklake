# PostgreSQL metadata benchmark results

These results compare pristine PostHog `posthog/v1.5.3` with the reviewed feature build at `d20f1d38`. Both use the fork's existing PostgreSQL passthrough manager; the feature improves that path directly rather than introducing a parallel adapter. Measurements were collected on 2026-07-22 against PostgreSQL 18.3 from the protected `ducklake-bench-runner` pod.

The baseline is PostHog commit `49ec0dc880a749e059594ee699c87ba3a9f88524`. Both builds use DuckDB `14eca11bd9d4a0de2ea0f078be588a9c1c5b279c`, extension-ci-tools `795096d04b009c0d087468439ebb526a5460dfac`, and the official DuckDB v1.5.3 PostgreSQL extension. Trials alternate baseline and feature. There are 9 trials at small, 7 at medium, 5 at large, and 3 at xlarge for each variant and operation.

## Xlarge results

| Operation | Fixture metadata rows | Result rows | v1.5.3 baseline | Feature | Change | Speedup |
|---|---:|---:|---:|---:|---:|---:|
| Expire snapshots | 2,000,000 | 500,000 | 14.616 s | 7.155 s | -51.05% | **2.04x** |
| Stats lookup microbenchmark | 1,010,000 | 100 | 1.151 s | 0.405 s | -64.79% | **2.84x** |
| Aggregate table information | 1,101,000 | 1,000 | 0.812 s | 0.557 s | -31.48% | **1.46x** |
| List snapshots | 2,000,000 | 1,000,000 | 4.943 s | 4.881 s | -1.25% | 1.01x |
| Current snapshot | 1,000,000 | 1 | 0.360 s | 0.360 s | -0.09% | 1.00x |
| Cleanup candidates | 1,000,000 | 1,000,000 | 1.896 s | 1.892 s | -0.19% | 1.00x |
| Find snapshots to expire (`dry_run`) | 2,000,000 | 500,000 | 2.213 s | 2.177 s | -1.61% | 1.02x |
| File discovery | 1,100,001 | 1,000,000 | 4.368 s | 4.541 s | +3.96% | 0.96x |

`Fixture metadata rows` is the sum of the actual relevant PostgreSQL table counts captured before timing; each JSON row also contains the per-table breakdown in `fixture_row_counts`. `Result rows` is the logical operation cardinality. These are observed counts, not planner-level “rows processed.”

The expiration fixture contains 1,000,000 snapshots and 1,000,000 snapshot-change rows, with 500,000 snapshots selected. The table-information fixture contains 1,000 tables, 1,000,000 data files, and 100,000 delete files. File discovery has one table plus the same file counts. The stats fixture contains 10,000 table-stat rows and 1,000,000 column-stat rows; the feature fetches the 100 rows for the written table.

Snapshot listing, current-snapshot lookup, cleanup-candidate listing, dry-run selection, and file discovery use the existing passthrough implementations. Their changes are within approximately 4% and are treated as noise, not claimed improvements.

The authoritative artifact contains 384 unique rows across eight operations, four scales, and two variants: [`results/release.jsonl`](results/release.jsonl). The 48 stats-only rows are extracted to [`results/commit_stats.jsonl`](results/commit_stats.jsonl).

`max_rss_kib` remains diagnostic only because Python's `RUSAGE_CHILDREN` high-water mark is cumulative.
