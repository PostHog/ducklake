"""Scenario 1: single-writer sequential registration-only commits.

Measures the control plane's commit tail — fabricated paths and stats,
no parquet. The headline assertion: commit latency must NOT grow with
catalog size. The predecessor loaded the entire catalog's stats per
commit attempt (5-7s loads, 190-264s commits); hoglake commits are
write-set-scoped, so p50 at preseed=100k must look like p50 at
preseed=0.
"""

from __future__ import annotations

import argparse

from ..context import Bench
from ..fabricate import append_payload
from ..runner import FailureGuard, run_loop
from ..stats import Metric
from .common import (
    ScenarioReport,
    check,
    make_bench_table,
    seed_snapshots,
)

RECORD_COUNT = 100


def run(bench: Bench, args: argparse.Namespace) -> ScenarioReport:
    preseeds = args.preseed_snapshots
    fpcs = args.files_per_commit
    report = ScenarioReport(
        scenario="commit-throughput",
        params={
            "preseed_snapshots": preseeds,
            "files_per_commit": fpcs,
            "ops": args.ops,
            "warmup": args.warmup,
        },
    )
    guard = FailureGuard()
    # p50 by (preseed, fpc) for the ratio check
    p50s: dict[tuple[int, int], float] = {}

    for preseed in preseeds:
        catalog, table = make_bench_table(bench, "ct")
        if preseed:
            report.add(
                seed_snapshots(
                    catalog,
                    table,
                    preseed,
                    record_count=RECORD_COUNT,
                    guard=guard,
                    label=f"preseed{preseed}",
                )
            )
        commits_done = 0
        for fpc in fpcs:

            def op(_: int, fpc: int = fpc) -> None:
                catalog._commit(
                    append_payload(catalog, table, fpc, RECORD_COUNT)
                )

            loop = run_loop(
                op,
                ops=args.ops,
                warmup=args.warmup,
                duration_s=args.duration,
                guard=guard,
            )
            check(loop.errors == 0, f"{loop.errors} failed commits (fpc={fpc})")
            m = report.add(
                Metric.from_recorder(
                    f"commit.preseed{preseed}.fpc{fpc}",
                    loop.recorder,
                    loop.wall_s,
                    files_s=loop.recorder.count * fpc / loop.wall_s
                    if loop.wall_s
                    else 0.0,
                )
            )
            p50s[(preseed, fpc)] = m.p50_ms or 0.0
            commits_done += loop.recorder.count + loop.warmup.count

        # correctness: dense head advance + aggregate row accounting
        info = table.info()
        total_commits = preseed + commits_done
        files = table.files()
        check(
            len(files) >= total_commits,  # every commit added >= 1 file
            f"file count {len(files)} < commit count {total_commits}",
        )
        check(
            info.record_count == sum(f.record_count for f in files),
            "table record_count aggregate disagrees with visible files",
        )
        head = catalog.refresh().head_snapshot_id
        # head = create-namespace + create-table + every commit
        check(
            head == 2 + total_commits,
            f"snapshot head {head} != expected {2 + total_commits} "
            "(snapshot ids are not dense)",
        )

    # the headline: latency ratio across preseed levels, per fpc
    if len(preseeds) > 1:
        lo, hi = min(preseeds), max(preseeds)
        for fpc in fpcs:
            base, big = p50s[(lo, fpc)], p50s[(hi, fpc)]
            ratio = big / base if base > 0 else 0.0
            report.add(
                Metric(
                    name=f"commit.scaling.fpc{fpc}",
                    ops=0,
                    wall_s=0.0,
                    extra={
                        f"p50_preseed{lo}_ms": base,
                        f"p50_preseed{hi}_ms": big,
                        "ratio": ratio,
                    },
                )
            )
            report.flag_ratio(
                f"commit p50 (fpc={fpc}) from preseed={lo} to preseed={hi}",
                ratio,
            )
    return report


def add_args(p: argparse.ArgumentParser) -> None:
    p.add_argument(
        "--preseed-snapshots",
        type=lambda s: [int(x) for x in s.split(",")],
        default=[0, 10_000],
        help="comma list of preexisting-snapshot counts to compare "
        "(default: 0,10000)",
    )
    p.add_argument(
        "--files-per-commit",
        type=lambda s: [int(x) for x in s.split(",")],
        default=[1, 10, 100, 1000],
        help="comma list (default: 1,10,100,1000)",
    )
    p.add_argument("--ops", type=int, default=200, help="measured commits per config")
