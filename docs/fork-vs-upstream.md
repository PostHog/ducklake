# Fork vs. Upstream: PostHog/ducklake vs. duckdb/ducklake

**Generated:** 2026-08-26
**Comparison base:** merge-base `c80b9155` — upstream `ducklabs/main` at `57732417` (2026-08-26)

## Status at a glance

| Metric | Value |
| --- | --- |
| Commits ahead of upstream | 6 |
| Commits behind upstream | 579 |
| Files changed by fork (ahead side) | 15 (+1793 / −27) |
| Files changed by upstream (behind side) | 310 |
| Fork divergence scope | CI/CD, tooling, docs only — **zero source changes** |

All fork-side divergence is in `.github/workflows/`, `.gitignore`, `docs/README.md`, plus new files (`justfile`, `.skills/`). No changes to `src/`, `test/`, or any extension code.

## Commits carried in the fork (not upstream)

Most recent first:

| Commit | Date | PR | Summary |
| --- | --- | --- | --- |
| `fbd98441` | 2026-06-09 | [#22](https://github.com/PostHog/ducklake/pull/22) | Add PostHog changes list, `justfile`, and upstream cherry-pick skill. Also vendors `extension-ci-tools` reusable workflows with SHA-pinned actions, job-level `permissions:` blocks, and env-var hoisting in `run:` bodies. |
| `d0d22ea9` | 2026-05-21 | _direct commit_ | Add `nosemgrep` annotation suppressing the `pull_request_target` Semgrep rule in CI. |
| `42ea11be` | 2026-05-18 | [#17](https://github.com/PostHog/ducklake/pull/17) | Pin all GitHub Actions to full-length commit SHAs instead of mutable tags (org policy). |
| `311534f5` | 2026-05-09 | [#10](https://github.com/PostHog/ducklake/pull/10) | Add `.claude` and `.cache` to `.gitignore`. |
| `4c38a855` / `bb16ebd0` | 2026-05-08 | [#4](https://github.com/PostHog/ducklake/pull/4) | Publish extension binaries (`linux_amd64`, `linux_arm64`, `SHA256SUMS`) to GitHub Releases on tag push, giving Duckgres stable artifact URLs. |

## Fork-side changes by area

### 1. Release binary publishing (`MainDistributionPipeline.yml`)

New `publish-github-release` job (tag pushes only): downloads the linux amd64/arm64 extension artifacts, stages them as `ducklake-linux-{amd64,arm64}.duckdb_extension`, generates `SHA256SUMS`, and creates/updates a GitHub Release.

### 2. Vendored `extension-ci-tools` workflows

- **New files:** `.github/workflows/_extension_distribution.yml` (1330 lines) and `_extension_deploy.yml` (166 lines), vendored from `duckdb/extension-ci-tools` v1.5.2 (source SHA recorded in file headers).
- **Rationale:** PostHog org policy requires full-SHA action pinning transitively, including inside reusable workflows; upstream uses mutable refs (`actions/checkout@v4`, `lukka/run-vcpkg@v11.1`, etc.). GitHub Actions has no mechanism to override `uses:` refs in called workflows, so the files are vendored and `MainDistributionPipeline.yml` calls them via relative paths.
- **Divergence from vendored source:** every action SHA-pinned; job-level `permissions:` blocks added (CodeQL `actions/missing-workflow-permissions`, 7 jobs); `${{ ... }}` interpolations hoisted out of `run:` bodies into `env:` (Semgrep `run-shell-injection`, 37 blocks). These patches must be re-applied on any re-vendor.

### 3. Action SHA pinning in existing workflows

`Catalogs.yml`, `ConfigTests.yml`, `Debug.yml`, `DeletionVectors.yml`, `DuckLakeVersion.yml`, `MinIO.yml`, `NoInline.yml`: tags replaced with full-length SHAs (e.g. `actions/checkout@f43a0e5... # v3.6.0`, `ikalnytskyi/action-setup-postgres@10ab8a5... # v7`).

### 4. Tooling and repo hygiene

- `justfile` (85 lines): fork maintenance task runner.
- `.skills/cherry-pick-upstream-pr/SKILL.md`: agent skill for cherry-picking upstream PRs.
- `.gitignore`: `.claude`, `.cache`.
- `docs/README.md`: "PostHog changes" section tracking fork status.

## Behind upstream (579 commits, not merged)

Upstream activity since the merge-base, by commit subject:

| Category | Count |
| --- | --- |
| Fixes | ~142 |
| Merged PRs | ~119 |
| Features/additions | ~54 |
| Updates (incl. DuckDB version bumps) | ~15 |

Notable structural events upstream:

- 2026-06-09 — PR #1216 "Merge V1.5 -> Main" folded ~6 weeks of `v1.5-variegata` into `main` (~234 commits in one merge).
- 2026-08-11 — PR #1366 bumps to DuckDB v1.5.5.
- 2026-08-19/20 — further `v1.5-variegata` → `main` merges (#1395 and related).

The behind-side delta is large (310 files; net −17k lines on the two-way diff, dominated by upstream test reorganization). Of the 427 non-merge upstream commits, ~87 are bug fixes and ~46 are feature commits, enumerated in the following sections. Pulling upstream is tracked as a separate effort (see `try-merge-upstream-2026-06-09`, `merge-upstream-2026-07-10`, and `upstream-main-2026-07-22` branches).

## Bug fixes in upstream (not in fork)

The fork itself contains **no bug fixes**. The 579 behind-commits include ~87 fix commits (non-merge; trivial CI/format fixes omitted). Grouped by area:

### Schema evolution & DDL in transactions

| Commit | Date | Fix |
| --- | --- | --- |
| `1292c85f` | 2026-07-22 | Preserve alters when a committed table or view is renamed in the same transaction |
| `4eb558b2` / `0c0f9eb7` / `e3041158` | 2026-08-18 | Column rename on migration; migration bug; better migration error messages |
| `f2bf1d84` | 2026-05-09 | Missing column in schema evolution |
| `c1776fe8` | 2026-05-07 | Column rename with case-insensitive names |
| `8891f258` | 2026-04-25 | Drop table after change in the same transaction |
| `da7f813a` / `a036d5c8` / `2e3dcd28` | 2026-04/05 | View rename; rename-then-drop view; view rename with dropped comment |
| `1097fff2` | 2026-05-05 | Table/scalar macro with same name created in same txn |
| `17ca5425` / `1978755b` | 2026-04 | Macro quoting; sort quoting |
| `b294467f` | 2026-04-29 | Set default on empty table |
| `ec19db0e` | 2026-06-07 | Default to ASCENDING when sort direction omitted in `SET SORTED BY` |

### Statistics & file pruning

| Commit | Date | Fix |
| --- | --- | --- |
| `db9cb183` | 2026-05-31 | `table_stats` column stats |
| `e3f6a78b` | 2026-05-15 | Empty cardinality stats |
| `48540ad9` | 2026-05-28 | Don't assert string MIN/MAX folds (truncated-prefix stats) |
| `7377f714` | 2026-04-22 | DuckLake stats for columns with defaults |
| `30ea0f5e` | 2026-05-18 | Mis-pruned data files |
| `8f51c142` | 2026-06-22 | Column stats accuracy when drop-to-empty is followed by same-txn insert |
| `3be1c235` | 2026-06-17 | Decrement stats when deletes drop data files |
| `ed4b5e3a` | 2026-06-09 | Recompute nested-leaf stats from full flattened schema after rewrite |

### Concurrency, retry & commit

| Commit | Date | Fix |
| --- | --- | --- |
| `a924b892` | 2026-06-25 | Invalid reference on concurrent transactions |
| `406491c1` | 2026-05-15 | Retrial conflicts |
| `31c68927` | 2026-05-12 | Off-by-one in retry attempts |
| `626704f0` | 2026-05-13 | Patched code; remove `ducklake_debug_force_retryable_commit_failures` |
| `08e12163` | 2026-07-20 | Thread-unsafe CRC32 table initialization |
| `6d078382` / `4f658a2f` | 2026-06-04/10 | Remap transaction-local partition ids on commit retry; guard against committing files with them |

### Deletes & deletion vectors

| Commit | Date | Fix |
| --- | --- | --- |
| `788c309e` | 2026-04-26 | Deletion filter |
| `439ff3ed` | 2026-05-25 | `SET NOT NULL` with deleted NULL rows |
| `d19cb749` | 2026-05-29 | Delete `snapshot_id` under virtual-column projection reordering |
| `55a10f19` | 2026-05-29 | Remap deletion-scan output columns under filter-column removal (#1201) |
| `941c0e50` | 2026-05-15 | Flush inlined data with non-bare-column sort + deletes (#999) |

### Partitioning & compaction

| Commit | Date | Fix |
| --- | --- | --- |
| `a5a6be4d` | 2026-06-10 | `add_files` partition-key lookup for repeated transforms |
| `c6595c9e` | 2026-04-29 | `merge_adjacent_files` with empty source files |
| `feecc999` | 2026-05-01 | Bucket out of range |
| `98510f5a` | 2026-06-30 | Insert rotation |
| `4e0a87aa` | 2026-07-20 | Rewrites after partition evolution |
| `a3dd97c8` | 2026-05-31 | Supersede issue |

### Type system

| Commit | Date | Fix |
| --- | --- | --- |
| `fb00077c` | 2026-05-06 | All type promotion |
| `8e887d9f` | 2026-05-05 | Type promotion for UINTEGER |
| `81cccee9` | 2026-05-13 | Rounding of `timestamp_ns.parquet` |
| `6e8ffef9` | 2026-06-24 | Floating non-equal |
| `986b737e` | 2026-05-05 | Use ANSI cast instead of PostgreSQL-style cast |

### Metadata stores (Postgres etc.)

| Commit | Date | Fix |
| --- | --- | --- |
| `9cacc9e1` | 2026-05-06 | Use `postgres_query()` for file-column-stats filter pushdown when an index exists |
| `45d6df4f` | 2026-07-20 | Scope Postgres metadata attach to `METADATA_SCHEMA` |
| `1eb7354d` | 2026-07-29 | Search all secret storages when resolving ducklake secrets by name |
| `17ced711` / `0087cb3e` | 2026-05/07 | Metadata fixes (backport) |

### Scan, serialization & misc

| Commit | Date | Fix |
| --- | --- | --- |
| `1ebc8c9b` | 2026-05-29 | DuckLake scan serialization for window partition |
| `152ab9f1` / `5145e4d9` | 2026-07-22 | Scan-serialize and max-retry-count patches |
| `bd7a31c1` | 2026-05-14 | DuckLake option segfault |
| `3eab8314` | 2026-07-11 | Surface errored inlined-data reads as catchable errors |
| `61a0fa1f` | 2026-05-28 | Remote execution of `ducklake_delete_orphaned_files()` for quack |
| `664400c5` | 2026-07-26 | Add `hive_file_pattern` option to flush |

## New features in upstream (not in fork)

The fork itself adds **no features**. Major upstream feature work, grouped:

### Server-side commit (`ducklake_commit`)

Large multi-week effort (~May 18 – Jun 30) to push commit logic into the metadata server:

| Commit | Date | Feature |
| --- | --- | --- |
| `9d1a27a9` | 2026-05-18 | Basics of `ducklake_commit`; snapshot id and schema version directly in commit |
| `b632da07` | 2026-05-19 | Server commit class; SQL builder from the committer |
| `ebfc2ee1` | 2026-05-26 | Server-side commit |
| `b4c4be03` / `314c1d05` | 2026-05-21/22 | Delete-files support and add-files-out-of-transaction on `ducklake_commit` |
| `c9a3e868` / `ecf4e44c` | 2026-05-20/27 | Configurable retries; retries working with temp tables |

### Inline data

| Commit | Date | Feature |
| --- | --- | --- |
| `4cfc3ca8` | 2026-05-22 | Inline data inserts |
| `c54e1186` | 2026-06-15 | Flush inlined data plumbing |
| `451edfa2` / `c812f6e6` | 2026-05-12 / 07-17 | Drop orphaned/unnecessary inlined tables |
| `b849b051` | 2026-06-10 | Inlining for `MERGE INTO ... INSERT` (#1186) |
| `f10eb48b` | 2026-08-17 | Rename inlined metadata columns to the `_ducklake_` prefix |

### Deletion vectors (puffin)

| Commit | Date | Feature |
| --- | --- | --- |
| `4d16f7a1` | 2026-06-12 | Write/read puffin files with multiple deletion vectors |
| `48ea3aa5` | 2026-06-12 | Snapshot filter in deletion-vector files |
| `1e805b2d` | 2026-06-12 | Deletion-vector files hooked up to `ducklake_delete` |
| `abb1cc66` | 2026-07-20 | Runtime bounds checks in deletion-vector parsing (replaces debug-only asserts) |

### Partitioning

| Commit | Date | Feature |
| --- | --- | --- |
| `0999e724` | 2026-05-20 | Bucket-transform-aware partition pruning |
| `c5a662e1` / `a56fafd4` / `d5886b5f` | 2026-08-12 | `epoch_year`/`epoch_month`/`epoch_day`/`epoch_hour` partition transforms, gated on DuckLake 1.1 |
| `59e358c5` | 2026-08-12 | Partition bridge |
| `9ef79528` | 2026-06-10 | Compaction hive paths derived from partition values |

### Compaction, rewrite & file sizing

| Commit | Date | Feature |
| --- | --- | --- |
| `961c9799` | 2026-05-11 | `ducklake_target_file_size` session setting |
| `8a991348` | 2026-06-30 | File rotation in rewrite |
| `8b8e0491` | 2026-06-26 | `max_compacted_files` option for `ducklake_rewrite_data_files` |
| `4ab9b124` | 2026-08-06 | `newer_than` option for `ducklake_merge_adjacent_files` |

### Statistics & metadata query performance

| Commit | Date | Feature |
| --- | --- | --- |
| `ee381b43` | 2026-05-21 | Variant stats |
| `e2992882` / `484b9cfc` / `5cc0dc2e` | 2026-05–07 | Per-table stats cache; negative-result cache; committed-schema begin-snapshot cache |
| `8c1e97ab` / `4de135fd` | 2026-05-27 | Answer MIN/MAX from catalog metadata when stats are exact; exact global stats on `REWRITE_DELETES` |
| `cd0581b9` | 2026-06-10 | Row-group information in DuckLake |
| `442bd668` | 2026-07-14 | Top-N stats reads routed through metadata CTEs |
| `44797b51` | 2026-06-18 | Metadata queries moved to `DuckLakeMetadataManager::Query/Execute` |

### SQL surface

| Commit | Date | Feature |
| --- | --- | --- |
| `2303fed1` | 2026-05-01 | `COMMENT ON COLUMN` support for views |
| `7cefdf4b` | 2026-05-29 | Views in the data change feed |
| `0cda8a3c` | 2026-06-09 | Option to hide the metadata catalog |
| `ff7a11e0` | 2026-05-01 | Disallow dropping sorted columns |

## Trial merge (2026-08-26, non-destructive)

A `git merge-tree --write-tree main ducklabs/main` trial (plus a throwaway-worktree `git merge --no-commit` to inspect hunks) produced:

| Result | Detail |
| --- | --- |
| Conflicted files | **1** — `.github/workflows/MainDistributionPipeline.yml` |
| Conflict hunks | 2 |
| Auto-merged fork-touched files | `Catalogs.yml` (other workflows untouched by upstream) |
| Everything else | Clean — all 579 upstream commits merge with no other conflicts |

Both hunks are trivially resolvable:

1. **`uses:` ref for `_extension_distribution.yml`** — upstream changed `@main` → `@v1.5-variegata` (plus `ci_tools_version` and a new `opt_in_archs: musl` input); our side is the vendored relative path. Resolution: keep the vendored path, adopt upstream's new inputs. **Follow-up work, not a conflict:** re-vendor the vendored workflows against `extension-ci-tools` `v1.5-variegata` (the fork pins v1.5.2) and re-apply the SHA-pin / `permissions:` / env-hoisting patches.
2. **Adjacent job additions at end of file** — both sides appended jobs after `duckdb-next-deploy`: we added `publish-github-release`, upstream added `relassert` (and `gcc12-compatibility-build` mid-file). Resolution: keep both. Note upstream's new jobs use unpinned actions (`actions/checkout@v4`, `hendrikmuhs/ccache-action@main`, `lukka/run-vcpkg@v11.1`), which will fail under the PostHog org SHA-pin policy until pinned.

**Verdict: easy merge.** Effort is not in conflict resolution (minutes) but in post-merge policy work: re-vendoring `extension-ci-tools` (with the three patch re-applications) and SHA-pinning the ~3 unpinned actions in upstream's two new jobs. Also expect the `publish-github-release` job's artifact names to need checking against the new musl arch matrix.
