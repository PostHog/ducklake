"""Scenario 2: K concurrent writers against one catalog.

Mixed workload: even-indexed writers append to one shared table,
odd-indexed writers each own a private table. Appends never conflict
with appends, so 409s should be ~0, and aggregate commits/s should
degrade roughly linearly with the (milliseconds) commit-lock hold time
— the predecessor collapsed superlinearly here (190-264s commits under
contention).
"""

from __future__ import annotations

import argparse
import threading
import time

from pyhoglake import CommitConflictError, Table

from ..context import Bench
from ..fabricate import BENCH_SCHEMA, append_payload
from ..runner import FailureGuard, LoopResult, OpDiscarded, run_loop, run_threads
from ..stats import Metric, Recorder
from .common import (
    ScenarioReport,
    assert_dense_snapshots,
    assert_row_tiling,
    check,
)

RECORD_COUNT = 100


def run(bench: Bench, args: argparse.Namespace) -> ScenarioReport:
    report = ScenarioReport(
        scenario="commit-contention",
        params={
            "writers": args.writers,
            "ops": args.ops,
            "files_per_commit": args.files_per_commit,
            "warmup": args.warmup,
            "duration": args.duration,
            "url": args.url,
        },
    )
    for k in args.writers:
        _run_level(bench, args, report, k)
    return report


def _run_level(
    bench: Bench, args: argparse.Namespace, report: ScenarioReport, k: int
) -> None:
    catalog = bench.new_catalog(f"cc{k}")
    ns = catalog.create_namespace("bench")
    shared = ns.create_table("shared", BENCH_SCHEMA)
    tables: list[Table] = []
    for i in range(k):
        if i % 2 == 0:
            tables.append(shared)
        else:
            tables.append(ns.create_table(f"own_{i}", BENCH_SCHEMA))
    start_head = catalog.refresh().head_snapshot_id  # after all DDL

    guard = FailureGuard()
    conflicts = [0] * k
    conflict_lat = [Recorder() for _ in range(k)]  # 409 attempts, separately
    stop = threading.Event()

    def make_worker(i: int):
        table = tables[i]
        last_snapshot = {"v": start_head}

        def op(_: int) -> None:
            t0 = time.perf_counter_ns()
            try:
                result = catalog._commit(
                    append_payload(
                        catalog,
                        table,
                        args.files_per_commit,
                        RECORD_COUNT,
                        read_snapshot=last_snapshot["v"],
                    )
                )
                last_snapshot["v"] = result.snapshot_id
            except CommitConflictError:
                # conflicted attempts are recorded on their own; they
                # must never contaminate the success percentiles
                conflicts[i] += 1
                conflict_lat[i].record_ns(time.perf_counter_ns() - t0)
                last_snapshot["v"] = catalog.refresh().head_snapshot_id
                raise OpDiscarded from None

        def worker() -> LoopResult:
            return run_loop(
                op,
                ops=args.ops,
                warmup=args.warmup,
                duration_s=args.duration,
                guard=guard,
                stop=stop,
            )

        return worker

    try:
        results, wall_s = run_threads(k, make_worker)
    except BaseException:
        stop.set()
        raise

    merged = Recorder()
    worker_p99_max = 0.0
    total_ops = 0
    total_warmup = 0
    for r in results:
        assert r.loop is not None
        check(r.loop.errors == 0, f"writer {r.index}: {r.loop.errors} failures")
        merged.merge(r.loop.recorder)
        total_ops += r.loop.recorder.count
        total_warmup += r.loop.warmup.count
        if r.loop.recorder.count:
            worker_p99_max = max(
                worker_p99_max, r.loop.recorder.percentiles_ms()["p99_ms"]
            )
    n_conflicts = sum(conflicts)
    merged_conflict = Recorder()
    for r in conflict_lat:
        merged_conflict.merge(r)
    extra = {
        "files_s": (total_ops * args.files_per_commit / wall_s)
        if wall_s
        else 0.0,
        "conflicts": n_conflicts,
        "worker_p99_max_ms": worker_p99_max,
    }
    if merged_conflict.count:
        extra["conflict_p50_ms"] = merged_conflict.percentiles_ms()["p50_ms"]
    report.add(Metric.from_recorder(f"contention.k{k}", merged, wall_s, **extra))
    if n_conflicts:
        report.flag(
            f"commit-contention k={k}: {n_conflicts} append/append 409s "
            "— appends must never conflict with appends"
        )

    # correctness: dense snapshot ids for every successful commit, and
    # row-range tiling on the shared (most contended) table.
    # 409s mint no snapshot and are discarded from both recorders.
    commits = total_ops + total_warmup
    assert_dense_snapshots(catalog, start_head, commits)
    assert_row_tiling(shared)
    for t in tables:
        if t is not shared:
            assert_row_tiling(t)


def add_args(p: argparse.ArgumentParser) -> None:
    p.add_argument(
        "--writers",
        type=lambda s: [int(x) for x in s.split(",")],
        default=[1, 2, 4, 8, 16],
        help="comma list of concurrency levels (default: 1,2,4,8,16)",
    )
    p.add_argument("--ops", type=int, default=100, help="measured commits per writer")
    p.add_argument("--files-per-commit", type=int, default=10)
