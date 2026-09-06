"""Scenario 4: changefeed latency scaling + offset commit rate.

The catalog is grown in stages. At every stage the SAME fixed window is
timed — if changes() cost scales with total catalog size instead of the
window, that ratio grows (loud flag; this is the read-barrier-bound
crawl viaduck suffered). At the final stage windows of increasing size
are timed and correlated with rows returned — latency SHOULD correlate
with window rows (that's honest work), NOT with catalog size.
"""

from __future__ import annotations

import argparse

from ..context import Bench
from ..runner import FailureGuard, InsufficientSamples, run_loop
from ..stats import Metric, pearson
from .common import (
    MIN_GUARDED_SAMPLES,
    THERMAL_WARM_OPS,
    ScenarioReport,
    check,
    make_bench_table,
    require_samples,
    seed_snapshots,
)

FIXED_WINDOW = 100


def run(bench: Bench, args: argparse.Namespace) -> ScenarioReport:
    stages = args.stages
    windows = [w for w in args.windows if w <= stages[-1]]
    report = ScenarioReport(
        scenario="changefeed-scan",
        params={
            "stages": stages,
            "windows": windows,
            "reps": args.reps,
            "offset_commits": args.offset_commits,
            "warmup": THERMAL_WARM_OPS,
            "duration": args.duration,
            "url": args.url,
        },
    )
    catalog, table = make_bench_table(bench, "cf")
    guard = FailureGuard()

    seeded = 0
    fixed_p50: list[tuple[int, float]] = []  # (catalog snapshots, p50 ms)
    for stage in stages:
        report.add(
            seed_snapshots(
                catalog, table, stage - seeded, guard=guard, label=f"seed_to_{stage}"
            )
        )
        seeded = stage
        head = catalog.refresh().head_snapshot_id
        w = min(FIXED_WINDOW, stage)

        def op(_: int, head: int = head, w: int = w) -> None:
            plan = table.changes(from_snapshot=head - w, to_snapshot=head)
            check(
                len(plan.files) == w,
                f"changes window {w} returned {len(plan.files)} files "
                f"(one appended per seeded snapshot)",
            )

        # the ratio flag hangs off this p50: every stage gets the same
        # fixed warm phase and must clear the guarded-sample floor
        loop = run_loop(
            op,
            ops=max(args.reps, MIN_GUARDED_SAMPLES),
            warmup=THERMAL_WARM_OPS,
            guard=guard,
        )
        check(loop.errors == 0, f"{loop.errors} failed changes() calls")
        require_samples(
            loop.recorder.count, f"changefeed fixed-window stage {stage}"
        )
        m = report.add(
            Metric.from_recorder(
                f"changes.fixed_w{w}.catalog{stage}", loop.recorder, loop.wall_s
            )
        )
        fixed_p50.append((stage, m.p50_ms or 0.0))

    # window sweep at final catalog size
    head = catalog.refresh().head_snapshot_id
    window_lat: list[tuple[float, float]] = []  # (rows returned, latency ms)
    for w in windows:

        def op(_: int, w: int = w) -> None:
            plan = table.changes(from_snapshot=head - w, to_snapshot=head)
            check(len(plan.files) == w, f"window {w}: {len(plan.files)} files")

        loop = run_loop(op, ops=args.reps, warmup=1, guard=guard)
        check(loop.errors == 0, f"{loop.errors} failed changes() calls")
        m = report.add(
            Metric.from_recorder(f"changes.window{w}", loop.recorder, loop.wall_s)
        )
        for ns in loop.recorder.samples_ns:
            window_lat.append((float(w), ns / 1e6))

    # correlations: rows-vs-latency should be strong, catalog-vs-latency flat
    rows_corr = pearson([x for x, _ in window_lat], [y for _, y in window_lat])
    if len(fixed_p50) > 1:
        if fixed_p50[0][1] <= 0:
            raise InsufficientSamples(
                "insufficient samples for a trustworthy ratio: first-stage "
                f"fixed-window p50 is {fixed_p50[0][1]} ms — refusing to "
                "divide by it"
            )
        catalog_ratio = fixed_p50[-1][1] / fixed_p50[0][1]
    else:
        catalog_ratio = 1.0
    catalog_corr = pearson(
        [float(s) for s, _ in fixed_p50], [p for _, p in fixed_p50]
    )
    # corr fields are float-or-null in the JSONL, never a string
    report.add(
        Metric(
            name="changes.scaling",
            ops=0,
            wall_s=0.0,
            extra={
                "rows_latency_corr": rows_corr,
                "catalog_latency_corr": catalog_corr,
                "fixed_window_ratio": catalog_ratio,
            },
        )
    )
    report.flag_ratio(
        f"changes() p50 for a fixed {FIXED_WINDOW}-snapshot window from "
        f"catalog={stages[0]} to catalog={stages[-1]} snapshots",
        catalog_ratio,
    )

    # offset commit rate
    consumer = "bench-consumer"
    n = args.offset_commits
    base = head - n
    check(base >= 0, "offset_commits exceeds seeded snapshots")

    def op_offset(i: int) -> None:
        catalog.commit_offset(consumer, table.table_uuid, base + i + 1)

    loop = run_loop(op_offset, ops=n, warmup=0, guard=guard)
    check(loop.errors == 0, f"{loop.errors} failed offset commits")
    report.add(
        Metric.from_recorder("offsets.commit", loop.recorder, loop.wall_s)
    )
    offsets = catalog.offsets(consumer)
    check(
        len(offsets) == 1 and offsets[0].committed_snapshot == head,
        "consumer offset did not land at the head snapshot",
    )
    return report


def add_args(p: argparse.ArgumentParser) -> None:
    p.add_argument(
        "--stages",
        type=lambda s: [int(x) for x in s.split(",")],
        default=[2_000, 5_000, 10_000],
        help="cumulative catalog snapshot counts to grow through",
    )
    p.add_argument(
        "--windows",
        type=lambda s: [int(x) for x in s.split(",")],
        default=[10, 100, 1000, 10_000],
        help="changefeed window sizes (snapshots) at final stage",
    )
    p.add_argument("--reps", type=int, default=10, help="timed calls per window")
    p.add_argument("--offset-commits", type=int, default=200)
