"""Scenario 3: deletion-vector registration under concurrency.

Two subcases:

- disjoint: K writers register growing DVs against their own private
  slice of data files. Expect ~0 conflicts (the per-file DV check only
  fires on the same file).
- hotfile: K writers deliberately race on ONE data file. The server's
  superseded-DV check turns stale writes into 409s; we measure the 409
  rate and the retry-to-success latency, and afterwards prove no update
  was lost: the final delete_count equals the number of successes,
  because every success must have read the latest vector.
"""

from __future__ import annotations

import argparse
import threading
import time

from pyhoglake import Catalog, CommitConflictError, Table

from ..context import Bench
from ..fabricate import BENCH_SCHEMA, dv_payload, fabricated_files
from ..runner import FailureGuard, InvariantViolation, LoopResult, run_loop, run_threads
from ..stats import Metric, Recorder
from .common import ScenarioReport, check

FILE_RECORD_COUNT = 1_000_000  # headroom: delete_count may never exceed it


def _live_dv_count(table: Table, data_file_id: int) -> int:
    for sf in table.scan_plan():
        if sf.data_file.data_file_id == data_file_id:
            return sf.delete_file.delete_count if sf.delete_file else 0
    raise InvariantViolation(f"data file {data_file_id} vanished from scan")


def run(bench: Bench, args: argparse.Namespace) -> ScenarioReport:
    k = args.writers
    report = ScenarioReport(
        scenario="delete-contention",
        params={
            "writers": k,
            "files_per_writer": args.files_per_writer,
            "rounds": args.rounds,
            "hotfile_ops": args.hotfile_ops,
        },
    )
    catalog = bench.new_catalog("dc")
    ns = catalog.create_namespace("bench")
    table = ns.create_table("t", BENCH_SCHEMA)

    # register all data files up front (DVs cannot target same-commit files)
    n_files = k * args.files_per_writer + 1
    catalog._commit(
        {
            "appends": [
                {
                    "namespace": "bench",
                    "table": "t",
                    "files": fabricated_files(
                        catalog, table, n_files, FILE_RECORD_COUNT
                    ),
                }
            ]
        }
    )
    files = sorted(table.files(), key=lambda f: f.data_file_id)
    file_ids = [f.data_file_id for f in files]
    hot_file_id = file_ids[-1]
    guard = FailureGuard()

    _disjoint(bench, args, report, catalog, table, file_ids, guard)
    _hotfile(bench, args, report, catalog, table, hot_file_id, guard)
    return report


def _disjoint(
    bench: Bench,
    args: argparse.Namespace,
    report: ScenarioReport,
    catalog: Catalog,
    table: Table,
    file_ids: list[int],
    guard: FailureGuard,
) -> None:
    k = args.writers
    conflicts = [0] * k
    stop = threading.Event()

    def make_worker(i: int):
        mine = file_ids[i * args.files_per_writer : (i + 1) * args.files_per_writer]
        state = {"snapshot": catalog.refresh().head_snapshot_id}

        def op(n: int) -> None:
            file_id = mine[n % len(mine)]
            delete_count = n // len(mine) + 1
            try:
                r = catalog._commit(
                    dv_payload(catalog, table, file_id, delete_count, state["snapshot"])
                )
                state["snapshot"] = r.snapshot_id
            except CommitConflictError:
                conflicts[i] += 1
                state["snapshot"] = catalog.refresh().head_snapshot_id

        def worker() -> LoopResult:
            return run_loop(
                op,
                ops=args.rounds * len(mine),
                warmup=0,
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
    for r in results:
        assert r.loop is not None
        check(r.loop.errors == 0, f"disjoint writer {r.index}: {r.loop.errors} failures")
        merged.merge(r.loop.recorder)
    n_conflicts = sum(conflicts)
    report.add(
        Metric.from_recorder(
            f"dv.disjoint.k{k}", merged, wall_s, conflicts=n_conflicts
        )
    )
    if n_conflicts:
        report.flag(
            f"delete-contention disjoint: {n_conflicts} 409s on disjoint "
            "files — per-file DV conflict check is over-broad"
        )
    # every private file must carry a live DV at its final round count
    # (skipped if the duration cap cut the loop short, or if the flagged
    # conflicts above already made the round accounting moot)
    complete = all(
        r.loop and r.loop.recorder.count == args.rounds * args.files_per_writer
        for r in results
    )
    if complete and n_conflicts == 0:
        for i in range(k):
            mine = file_ids[i * args.files_per_writer : (i + 1) * args.files_per_writer]
            for fid in mine:
                got = _live_dv_count(table, fid)
                check(
                    got == args.rounds,
                    f"file {fid}: live DV delete_count={got}, expected "
                    f"{args.rounds}",
                )


def _hotfile(
    bench: Bench,
    args: argparse.Namespace,
    report: ScenarioReport,
    catalog: Catalog,
    table: Table,
    hot_file_id: int,
    guard: FailureGuard,
) -> None:
    k = args.writers
    stop = threading.Event()
    attempts_per_writer = max(1, args.hotfile_ops // k)
    conflict_counts = [0] * k
    retry_latency = [Recorder() for _ in range(k)]  # first attempt -> success

    def make_worker(i: int):
        def op(_: int) -> None:
            t0 = time.perf_counter_ns()
            while True:
                snapshot = catalog.refresh().head_snapshot_id
                current = _live_dv_count(table, hot_file_id)
                try:
                    catalog._commit(
                        dv_payload(
                            catalog, table, hot_file_id, current + 1, snapshot
                        )
                    )
                    break
                except CommitConflictError:
                    conflict_counts[i] += 1
                    if stop.is_set():
                        raise
            retry_latency[i].record_ns(time.perf_counter_ns() - t0)

        def worker() -> LoopResult:
            return run_loop(
                op,
                ops=attempts_per_writer,
                warmup=0,
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

    successes = 0
    merged_retry = Recorder()
    for i, r in enumerate(results):
        assert r.loop is not None
        check(r.loop.errors == 0, f"hotfile writer {r.index}: {r.loop.errors} failures")
        successes += r.loop.recorder.count
        merged_retry.merge(retry_latency[i])
    n_conflicts = sum(conflict_counts)
    attempts = successes + n_conflicts
    report.add(
        Metric.from_recorder(
            f"dv.hotfile.k{k}",
            merged_retry,
            wall_s,
            successes=successes,
            conflicts=n_conflicts,
            conflict_rate=(n_conflicts / attempts) if attempts else 0.0,
        )
    )
    # lost-update check: a success proves its writer saw the latest DV,
    # so the final count is exactly the number of successes
    final = _live_dv_count(table, hot_file_id)
    check(
        final == successes,
        f"hot file DV delete_count={final} but {successes} successful "
        "supersessions — an update was lost or double-counted",
    )


def add_args(p: argparse.ArgumentParser) -> None:
    p.add_argument("--writers", type=int, default=8)
    p.add_argument("--files-per-writer", type=int, default=4)
    p.add_argument(
        "--rounds", type=int, default=15, help="DV growth rounds per private file"
    )
    p.add_argument(
        "--hotfile-ops",
        type=int,
        default=60,
        help="total successful supersessions to force on the shared file",
    )
