# PostgreSQL metadata benchmark results

These results compare pristine PostHog `posthog/v1.5.3` with the feature build. Both use the fork's existing PostgreSQL passthrough manager; the feature improves that path directly rather than introducing a parallel adapter. Measurements were collected on 2026-07-21 against PostgreSQL 18.3 from the protected `ducklake-bench-runner` pod.

The baseline is PostHog commit `49ec0dc880a749e059594ee699c87ba3a9f88524`. Both builds use DuckDB `14eca11bd9d4a0de2ea0f078be588a9c1c5b279c`, extension-ci-tools `795096d04b009c0d087468439ebb526a5460dfac`, and the official DuckDB v1.5.3 PostgreSQL extension. Trials alternate baseline and feature. There are 9 trials at small, 7 at medium, 5 at large, and 3 at xlarge for each variant and operation.

## Xlarge results

| Operation | Fixture metadata rows | Result rows | v1.5.3 baseline | Feature | Change | Speedup |
|---|---:|---:|---:|---:|---:|---:|
| Expire snapshots | 2,000,000 | 500,000 | 14.160 s | 6.586 s | -53.49% | **2.15x** |
| Stats lookup microbenchmark | 1,010,000 | 100 | 1.182 s | 0.378 s | -68.00% | **3.12x** |
| Aggregate table information | 1,101,000 | 1,000 | 0.799 s | 0.557 s | -30.27% | **1.43x** |
| List snapshots | 2,000,000 | 1,000,000 | 4.997 s | 4.959 s | -0.77% | 1.01x |
| Current snapshot | 1,000,000 | 1 | 0.363 s | 0.359 s | -0.94% | 1.01x |
| Cleanup candidates | 1,000,000 | 1,000,000 | 1.805 s | 1.838 s | +1.86% | 0.98x |
| Find snapshots to expire (`dry_run`) | 2,000,000 | 500,000 | 2.150 s | 2.186 s | +1.66% | 0.98x |
| File discovery | 1,100,001 | 1,000,000 | 4.461 s | 4.500 s | +0.87% | 0.99x |

`Fixture metadata rows` is the sum of the actual relevant PostgreSQL table counts captured before timing; each JSON row also contains the per-table breakdown in `fixture_row_counts`. `Result rows` is the logical operation cardinality. These are observed counts, not planner-level “rows processed.”

The expiration fixture contains 1,000,000 snapshots and 1,000,000 snapshot-change rows, with 500,000 snapshots selected. The table-information fixture contains 1,000 tables, 1,000,000 data files, and 100,000 delete files. File discovery has one table plus the same file counts. The stats fixture contains 10,000 table-stat rows and 1,000,000 column-stat rows; the feature fetches the 100 rows for the written table.

Snapshot listing, current-snapshot lookup, cleanup-candidate listing, dry-run selection, and file discovery use the existing passthrough implementations. Their changes are within approximately 2% and are treated as noise, not claimed improvements.

The authoritative artifact contains 384 unique rows across eight operations, four scales, and two variants: [`results/release.jsonl`](results/release.jsonl). The 48 stats-only rows are extracted to [`results/commit_stats.jsonl`](results/commit_stats.jsonl).

`max_rss_kib` remains diagnostic only because Python's `RUSAGE_CHILDREN` high-water mark is cumulative.
