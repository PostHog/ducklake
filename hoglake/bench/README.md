# hoglake-bench

Stress-test / benchmark harness for the [hoglake](../README.md) control
plane, driven through [pyhoglake](../pyhoglake/README.md). Its job is to
**prove the predecessor's pathological cost classes are gone and catch
them if they come back**.

## The history this guards against

Numbers observed operating DuckLake in production (see
[../README.md](../README.md) and the defect ledger) — the baselines
hoglake must embarrass, forever:

| Pathology | Predecessor | Guarded by |
|---|---|---|
| Commit cost scales with the catalog, not the write set: full-catalog stats loads of 5-7s per attempt, re-paid on every OCC retry | **190-264s** single-table commits | `commit-throughput` (preseed ratio + wide-catalog ratio) |
| Commit-storm convoys under writer contention; superlinear collapse on a busy catalog | 60-180s commits convoying every writer on the shard | `commit-contention` |
| Dropped tables' stats rows taxing every commit forever (99.4% of stats rows on one catalog were for dropped tables; purging bought 30-50x) | DDL churn = permanent commit tax | `ddl-churn` (before/after ratio) |
| Snapshot expiry at **~14ms/snapshot** (~70/s) regardless of chunk size | ~50h to drain a 15M-snapshot backlog | `expiry-throughput` |
| CDC as a read-barrier-bound crawl over the whole catalog | changefeed cost ∝ catalog size | `changefeed-scan` (correlations) |

Every scenario also validates its own correctness invariants after the
load (dense snapshot ids, row-range tiling, no lost DV updates, zero
still-referenced deletions) — a benchmark that corrupts silently is
worse than none, so an invariant failure aborts with exit code 4.

## Running

Toolchain: [flox](https://flox.dev) (python312 + uv). Dependencies:
pyhoglake (editable, from `../pyhoglake`), pyarrow, httpx — no locust,
no k6; latency capture is hand-rolled (`time.perf_counter_ns`, sorted
lists for percentiles). numpy + Faker come along for the `seed` task
only (vocabularies and vectorized row fabrication); no scenario's
measurement path touches them.

```sh
flox activate -- uv sync
just                # list recipes
just quick          # all scenarios, smoke profile (~2-3 min)
just full           # all scenarios, real sizes (tens of minutes)
just commit-throughput --preseed-snapshots 0,100000
flox activate -- uv run hoglake-bench commit-contention --writers 1,2,4,8,16
```

### Target server

Two options:

1. **Already running** (the usual dev setup): server at
   `http://localhost:8080`, MinIO at `:19000`
   (`hoglake`/`hoglake123`, path-style). Overridable via `--url`,
   `--s3-endpoint`, … or `HOGLAKE_URL` / `HOGLAKE_S3_*` env vars.
2. **Bring your own**: in `../server`,
   `docker compose up -d && flox activate -- gradle run` (Postgres 16 +
   MinIO + the server; ports overridable via `HOGLAKE_*_PORT`).

The harness never hammers a dead server: 10 consecutive
transport/5xx failures abort the run (exit 3) with a clear message.

Catalogs are `bench-*` prefixed and unique per run (run id in catalog
name and data path), so runs never collide and need no cleanup — there
is no catalog-delete API; bench catalogs are cheap metadata rows.
Real objects are only written by `expiry-throughput` (small cleanup
probes) and `end-to-end-writer` (actual parquet), under
`s3://hoglake-bench/<run-id>/`.

## Scenarios

All scenarios take `--ops`-style sizing caps plus the shared
`--duration <seconds>` cap and `--warmup <n>` (warmup iterations are
excluded from stats).

| Scenario | What it measures | Headline |
|---|---|---|
| `commit-throughput` | Single-writer sequential **registration-only** commits (fabricated paths + footer-shaped stats — the control plane only, no parquet) at `--files-per-commit 1,10,100,1000`, against catalogs preseeded with `--preseed-snapshots 0,10000` existing snapshots, plus a **wide-catalog** stage with `--tables 500` live tables (the predecessor's 59K-table stats pathology, scaled down; a full-scale run is a manual `--tables 59000`; `--tables 0` disables) | commits/s, files/s, and **the preseed + wide-catalog latency ratios** — p50 must not grow with catalog size in snapshots OR tables (>1.5x flags loudly; this is THE O(catalog) regression) |
| `commit-contention` | K threads (`--writers 1,2,4,8,16`), one catalog, mixed shared-table/private-table appends, `read_snapshot` supplied so OCC is exercised | aggregate commits/s vs K, 409 count (**must be ~0**: appends never conflict with appends), per-writer p99 max. Conflicted attempts are excluded from the success percentiles and reported separately (`conflict_p50_ms`). Caveat: all K writers share one client process, so at high K the reported latencies include a client-side share (GIL, httpx connection pool) — treat cross-K comparisons as client-inclusive, not pure server numbers |
| `delete-contention` | DV registration: K writers on disjoint files (expect 0 conflicts), then all writers racing on ONE file | 409 rate + retry-to-success latency; post-run proof that no DV update was lost. The hotfile retry loop is bounded (50 retries/op + the `--duration` deadline, checked inside the loop); abandoned ops are counted separately (`abandoned`). Same client-side-share caveat as `commit-contention` |
| `changefeed-scan` | `changes()` over windows of 10/100/1000/10000 snapshots, measured while the catalog grows through `--stages`; consumer offset commit rate | latency must correlate with window **rows returned**, not catalog snapshot count (both correlations + fixed-window ratio reported; >1.5x flags) |
| `expiry-throughput` | Seed `--snapshots` (10k default), drop, 1s retention, drain via repeated `/maintenance/expire --batch`; then physical cleanup of `--objects` real MinIO objects | **snapshots expired/s** (vs the predecessor's ~70/s), files queued/s, cleanup removed/s, `still_referenced == 0` |
| `ddl-churn` | create/append/drop cycles (the report-table pattern), with an identical small-commit probe before and after | tables/s and the **before/after commit-p50 ratio** (>1.5x flags — the dropped-table stats tax) |
| `end-to-end-writer` | The realistic path: pyarrow tables through `Table.append` — real parquet encode, MinIO upload, footer stats, commit (`--rows 100000 --batch-rows 10000`) | rows/s end to end, per-append latency |
| `all` | every scenario; `--quick` = smoke profile (~2-3 min, the default), `--full` = real sizes | CI-ish gate |

## Seeding a dev warehouse

Not every use of this harness is a measurement: `seed` fills a catalog
with a **realistic fake data warehouse**, sized by total bytes, so the
console, compaction planning and the changefeed have something with real
shape to look at.

```sh
just bench seed --gb 2.5                      # or: --catalog my-catalog
flox activate -- uv run hoglake-bench seed --gb 0.3 --server http://localhost:8080
```

What you get (`--gb` is fractional; the target is rounded to the nearest
100 MB and echoed before a byte is written — `target 2.5 GB -> plan:
2.5 GB (25 x 100MB units)`; anything under 0.1 GB is refused):

| Table | Share | Shape |
|---|---|---|
| `events.pageviews` | 65% | the event stream: uuid, team_id, distinct_id, session, path, referrer, ts over ~6 months, device/utm/properties-ish columns. **Partitioned `identity(team_id) x month(ts)`** and written through pyhoglake's partitioned-append fanout — one parquet file per (team, month) cell, all cells of a commit registered atomically |
| `warehouse.fact_orders` | 18% | FK-shaped ids into the dims, quantities, amounts, order/ship timestamps |
| `warehouse.fact_sessions` | 12% | sessions per user/team, durations, pageview counts, referrers |
| `warehouse.dim_users` / `dim_products` / `dim_teams` | ~5% | names, emails, countries, cities, SKUs, prices, plans, regions — one small file each |

- **Many files, not a few giants**: facts/events aim for `--file-mb`
  (default 32, clamped to 20-80), so compaction planning has real
  candidates to rank. Small budgets over the 6-team x 6-month grid
  produce smaller partition files by construction.
- **Realism without the cost**: [Faker](https://faker.readthedocs.io)
  mints a few thousand distinct values ONCE per run (names, emails,
  domains, cities, countries, products, campaigns); rows are then
  fabricated with numpy/pyarrow by sampling those vocabularies. Nothing
  is per-row Python, so millions of rows cost tens of milliseconds and
  ~100 MB lands in a few seconds.
- **Size control is server-sourced**: after every commit the writer reads
  the table's own `/tables` info and re-divides the remaining budget over
  the remaining work, so a bad bytes-per-row estimate corrects itself.
  Compression makes an exact target impossible — runs land within ~10%
  and the summary says by how much.
- **`--seed` is fixed by default**, so two runs produce the same shape.
- **`--catalog` reuse appends**, it never fails: an existing catalog
  grows (dims get new id ranges; the fixed team dimension is left alone).

The run ends with the catalog's own numbers, read back from the server:

```
seeded warehouse (per-table figures from the server's /tables info):
  table                        files           rows       bytes    this run
  -------------------------------------------------------------------------
  events.pageviews                36        715,123     66.0 MB     66.0 MB
  ...
wrote 101.0 MB in 43 files across 6 tables in 4.6s (target 100.0 MB, +1.0% — on plan)
```

`seed` is deliberately **not** a scenario: `all` never runs it, it
journals no metrics, and it flags no regressions.

## Reading the output

One line per metric:

```
commit.preseed10000.fpc10   ops=200  wall_s=1.92  rate_s=104  p50=8.91ms p95=11.2ms p99=13.9ms max=17.3ms  files_s=1041
commit.scaling.fpc10        ops=0    wall_s=0     rate_s=0    p50_preseed0_ms=8.71  p50_preseed10000_ms=8.91  ratio=1.02
```

- `rate_s` — ops per second of wall time (warmup excluded).
- `p50/p95/p99/max` — per-op latency percentiles in milliseconds.
- `ratio` lines are the regression guards; anything above **1.5x**
  prints a `!!! REGRESSION` line on stderr and exits 2.

### Guard hygiene

Every ratio-guarded stage (commit-throughput preseed + wide stages,
ddl-churn probes, changefeed fixed-window stages):

- runs an identical fixed warm phase first (40 discarded ops), so both
  sides of a ratio are compared at the same thermal state — a cold
  fresh-catalog baseline vs a seed-warmed preseeded stage would hide
  real O(catalog) regressions behind cold-cache inflation;
- must measure at least **20 ops**, or the run aborts (exit 3) with an
  "insufficient samples for a trustworthy ratio" error instead of
  emitting a vacuous ratio (a zero-sample stage never passes silently).

When `--duration` cuts a phase short, any invariant check that depends
on the phase completing is skipped with a loud `NOTICE:` on stderr.

### Exit codes

| Code | Meaning |
|---|---|
| 0 | everything ran, no flags |
| 2 | at least one `!!! REGRESSION` flag |
| 3 | abort: server unresponsive (transport bail-out) or insufficient samples in a guarded stage |
| 4 | invariant violation — the numbers are not trustworthy |
| 5 | unexpected exception (harness bug) |

Across `all`, per-scenario outcomes are severity-resolved with
precedence **4 > 2 > 5 > 3 > 0** — a later scenario can never demote an
earlier scenario's regression (e.g. scenario 3 flags a regression,
scenario 5 aborts → exit 2, and both are journaled). After an abort,
the runner re-checks `/healthz` and only continues to the remaining
scenarios if the server still answers.

### Results journal

Every run appends one JSON line per **completed-or-failed** scenario to
`bench-results.jsonl` (`--results` to redirect): timestamp, run id,
scenario, `status` (`ok` / `regression` / `aborted` /
`invariant_violation` / `error`), the full effective params (including
`duration`, `warmup`, `url`), the run config (profile), flags, error,
metrics — so runs are diffable over time and a duration-capped run is
distinguishable from a full one
(`jq 'select(.scenario=="commit-throughput")' bench-results.jsonl`).
Correlation fields (`rows_latency_corr`, `catalog_latency_corr`) are
always float-or-null, never a string.

## Tests

```sh
flox activate -- uv run pytest                       # unit + integration
flox activate -- uv run pytest -m "not integration"  # no server needed
```

Unit tests cover the percentile/correlation machinery, the loop
drivers (warmup exclusion, caps, failure bail-out), argument parsing +
profiles, and one full scenario loop against an in-memory fake of the
client surface (including a seeded snapshot-id-gap to prove the
invariant checks actually fire). `tests/test_adversarial.py` seeds
faults against the guards themselves: a modeled O(catalog)
per-snapshot commit tax must trip the preseed and wide-catalog ratio
flags (and a cold-cache-only model must not), zero-sample stages must
abort, the hotfile retry loop must respect its bounds, and exit-code
severity resolution must never demote a regression. The
`-m integration` smoke runs `all --quick` against `HOGLAKE_URL` and
skips cleanly when it's down.

The `seed` task has its own unit tests (budget rounding and the
proportional split, the sub-0.1 GB refusal, vocabulary determinism under
`--seed`, every fabricator against its declared schema, and the
month-window grid checked against pyhoglake's Iceberg `month` transform)
plus a live `-m integration` smoke that seeds 0.1 GB and verifies the
result from the server's `/tables` info — tables exist with plausible
row/byte counts, events carry two partition values per file, dims are
single files, and row-id ranges tile every table.

Caveat on correlations: a 2-stage `--stages` list (the quick profile)
makes `catalog_latency_corr` a two-point Pearson, which is ±1 by
construction — read `fixed_window_ratio` instead there; the
correlation is only informative at 3+ stages.
