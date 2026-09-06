"""Scenario 1: single-writer sequential registration-only commits.

Measures the control plane's commit tail — fabricated paths and stats,
no parquet. The headline assertion: commit latency must NOT grow with
catalog size. The predecessor loaded the entire catalog's stats per
commit attempt (5-7s loads, 190-264s commits); hoglake commits are
write-set-scoped, so p50 at preseed=100k must look like p50 at
preseed=0.

Two guarded axes:

- preseed depth: catalogs preseeded with N snapshots vs the smallest
  preseed level (--preseed-snapshots).
- wide catalog: a catalog with --tables live TABLES (the predecessor's
  59K-table stats pathology, scaled down; full 59K-scale runs are a
  manual ``--tables 59000``) vs the same smallest-preseed baseline.

Every guarded stage runs the same fixed discarded warm phase
(THERMAL_WARM_OPS) so ratios compare warm-vs-warm, and every measured
stage must clear MIN_GUARDED_SAMPLES or the run aborts (exit 3).
"""

from __future__ import annotations

import argparse

from ..context import Bench
from ..fabricate import BENCH_SCHEMA, append_payload
from ..runner import FailureGuard, InsufficientSamples, run_loop
from ..stats import Metric
from .common import (
    THERMAL_WARM_OPS,
    ScenarioReport,
    check,
    make_bench_table,
    notice,
    require_samples,
    seed_snapshots,
)

RECORD_COUNT = 100


def _measured_commit_loop(
    report, catalog, table, fpc, args, guard, label
):
    """One guarded stage: fixed warm phase + measured commits + floor."""

    def op(_: int) -> None:
        catalog._commit(append_payload(catalog, table, fpc, RECORD_COUNT))

    loop = run_loop(
        op,
        ops=args.ops,
        warmup=max(args.warmup, THERMAL_WARM_OPS),
        duration_s=args.duration,
        guard=guard,
    )
    check(loop.errors == 0, f"{loop.errors} failed commits ({label})")
    require_samples(loop.recorder.count, f"commit stage {label}")
    m = report.add(
        Metric.from_recorder(
            label,
            loop.recorder,
            loop.wall_s,
            files_s=loop.recorder.count * fpc / loop.wall_s
            if loop.wall_s
            else 0.0,
        )
    )
    commits = loop.recorder.count + loop.warmup.count
    return m, commits


def _ratio(base: float, big: float, what: str) -> float:
    if base <= 0:
        raise InsufficientSamples(
            f"insufficient samples for a trustworthy ratio: baseline p50 "
            f"for {what} is {base} ms — refusing to divide by it"
        )
    return big / base


def run(bench: Bench, args: argparse.Namespace) -> ScenarioReport:
    preseeds = args.preseed_snapshots
    fpcs = args.files_per_commit
    report = ScenarioReport(
        scenario="commit-throughput",
        params={
            "preseed_snapshots": preseeds,
            "files_per_commit": fpcs,
            "tables": args.tables,
            "ops": args.ops,
            "warmup": max(args.warmup, THERMAL_WARM_OPS),
            "duration": args.duration,
            "url": args.url,
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
            m, commits = _measured_commit_loop(
                report,
                catalog,
                table,
                fpc,
                args,
                guard,
                f"commit.preseed{preseed}.fpc{fpc}",
            )
            p50s[(preseed, fpc)] = m.p50_ms or 0.0
            commits_done += commits

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
    lo = min(preseeds)
    if len(preseeds) > 1:
        hi = max(preseeds)
        for fpc in fpcs:
            base, big = p50s[(lo, fpc)], p50s[(hi, fpc)]
            ratio = _ratio(base, big, f"fpc={fpc} preseed comparison")
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

    if args.tables:
        _wide_catalog(bench, args, report, guard, p50s, lo)
    return report


def _wide_catalog(
    bench: Bench,
    args: argparse.Namespace,
    report: ScenarioReport,
    guard: FailureGuard,
    p50s: dict[tuple[int, int], float],
    lo: int,
) -> None:
    """The wide-catalog stage: N live tables, then the same guarded
    commit loop — closer to the predecessor's 59K-table stats pathology
    (full 59K-scale runs are manual: ``--tables 59000``)."""
    catalog = bench.new_catalog("ctw")
    ns = catalog.create_namespace("bench")

    def create(i: int) -> None:
        ns.create_table(f"w{i}", BENCH_SCHEMA)

    loop = run_loop(
        create,
        ops=args.tables,
        warmup=0,
        duration_s=args.duration,
        guard=guard,
    )
    check(loop.errors == 0, f"{loop.errors} failed table creates (wide)")
    created = loop.recorder.count
    if created < args.tables:
        notice(
            f"wide-catalog: --duration cut table creation at {created}/"
            f"{args.tables}; the wide ratio below is for a {created}-table "
            "catalog"
        )
    report.add(
        Metric.from_recorder(
            f"wide.create(n={created})", loop.recorder, loop.wall_s
        )
    )
    table = ns.create_table("t", BENCH_SCHEMA)

    commits_done = 0
    for fpc in args.files_per_commit:
        m, commits = _measured_commit_loop(
            report,
            catalog,
            table,
            fpc,
            args,
            guard,
            f"commit.wide{created}.fpc{fpc}",
        )
        commits_done += commits
        base = p50s[(lo, fpc)]
        ratio = _ratio(base, m.p50_ms or 0.0, f"fpc={fpc} wide comparison")
        report.add(
            Metric(
                name=f"commit.scaling.wide.fpc{fpc}",
                ops=0,
                wall_s=0.0,
                extra={
                    f"p50_preseed{lo}_ms": base,
                    "p50_wide_ms": m.p50_ms or 0.0,
                    "tables": created,
                    "ratio": ratio,
                },
            )
        )
        report.flag_ratio(
            f"commit p50 (fpc={fpc}) from preseed={lo} to a "
            f"{created}-live-table catalog",
            ratio,
        )

    # head = create-namespace + N wide tables + the probe table + commits
    head = catalog.refresh().head_snapshot_id
    check(
        head == 2 + created + commits_done,
        f"wide catalog snapshot head {head} != expected "
        f"{2 + created + commits_done} (snapshot ids are not dense)",
    )


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
    p.add_argument(
        "--tables",
        type=int,
        default=500,
        help="live tables for the wide-catalog stage (0 disables; the "
        "predecessor pathology was 59K tables — that scale is a manual "
        "--tables 59000 run)",
    )
