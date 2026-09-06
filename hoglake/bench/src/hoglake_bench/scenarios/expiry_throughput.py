"""Scenario 5: snapshot expiry + physical cleanup drain rates.

The predecessor expired snapshots at ~14ms each (~70/s) regardless of
chunk size — ~50 hours against a 15M-snapshot backlog. Hoglake expiry
is range-delete based; snapshots/s here is the headline.

Seeding registers one fabricated file per snapshot on a table that is
then DROPPED, so expiry makes every file row unreachable and queues its
path (files queued/s). A second table holds --objects REAL MinIO
objects so the cleanup drain deletes actual bytes; the fabricated paths
drain as 'missing' (cleanup treats a missing object as done). Any
still_referenced count is an invariant violation.
"""

from __future__ import annotations

import argparse
import time
import uuid

from ..context import Bench
from ..fabricate import BENCH_SCHEMA, fabricated_files
from ..runner import FailureGuard
from ..stats import Metric, Recorder
from .common import ScenarioReport, check, notice, seed_snapshots

REAL_OBJECT_BYTES = b"hoglake-bench cleanup probe\n" * 4
FILES_PER_REAL_COMMIT = 50


def run(bench: Bench, args: argparse.Namespace) -> ScenarioReport:
    report = ScenarioReport(
        scenario="expiry-throughput",
        params={
            "snapshots": args.snapshots,
            "batch": args.batch,
            "objects": args.objects,
            "duration": args.duration,
            "warmup": 0,
            "url": args.url,
        },
    )
    guard = FailureGuard()
    catalog = bench.new_catalog("exp")
    ns = catalog.create_namespace("bench")
    t_fab = ns.create_table("fab", BENCH_SCHEMA)
    t_real = ns.create_table("real", BENCH_SCHEMA)

    # seed: one snapshot per fabricated file
    report.add(
        seed_snapshots(
            catalog, t_fab, args.snapshots, guard=guard, label="seed"
        )
    )

    # real objects, registered in batched commits (failure-guard capped:
    # a dead server/MinIO trips the guard instead of looping forever)
    bench.ensure_bucket()
    uploaded = 0
    while uploaded < args.objects:
        n = min(FILES_PER_REAL_COMMIT, args.objects - uploaded)
        try:
            regs = fabricated_files(catalog, t_real, n, record_count=10)
            for reg in regs:
                reg["path"] = (
                    f"{catalog.data_path}data/bench/real/{uuid.uuid4().hex}.parquet"
                )
                reg["file_size_bytes"] = len(REAL_OBJECT_BYTES)
                bench.put_object(reg["path"], REAL_OBJECT_BYTES)
            catalog._commit(
                {"appends": [{"namespace": "bench", "table": "real", "files": regs}]}
            )
        except Exception as exc:
            if guard.is_server_failure(exc):
                guard.failure(exc)  # trips BenchAbort after the cap
                continue
            raise
        guard.success()
        uploaded += n

    # drop both tables -> every file row becomes unreachable once the
    # floor passes the drop snapshots
    t_fab.drop()
    t_real.drop()
    catalog.set_retention(1)
    time.sleep(1.6)

    # expire drain
    expire_lat = Recorder()
    totals = {"snapshots": 0, "files": 0, "calls": 0}
    deadline = (
        time.monotonic() + args.duration if args.duration is not None else None
    )

    def expire_once() -> bool:
        with expire_lat.measure():
            r = catalog.expire(batch=args.batch)
        totals["snapshots"] += r.snapshots_expired
        totals["files"] += r.data_files_queued + r.delete_files_queued
        totals["calls"] += 1
        return r.snapshots_expired > 0

    capped = False
    t0 = time.perf_counter_ns()
    while True:
        try:
            more = expire_once()
        except Exception as exc:
            if guard.is_server_failure(exc):
                guard.failure(exc)
                continue
            raise
        guard.success()
        if not more:
            break
        if deadline is not None and time.monotonic() > deadline:
            capped = True
            break
    expire_wall = (time.perf_counter_ns() - t0) / 1e9
    report.add(
        Metric.from_recorder(
            "expire.calls",
            expire_lat,
            expire_wall,
            snapshots_expired=totals["snapshots"],
            snapshots_s=totals["snapshots"] / expire_wall if expire_wall else 0.0,
            files_queued=totals["files"],
            files_queued_s=totals["files"] / expire_wall if expire_wall else 0.0,
            ms_per_snapshot=(expire_wall * 1000 / totals["snapshots"])
            if totals["snapshots"]
            else 0.0,
        )
    )
    # +4 non-seed snapshots: 2 creates, 2 drops; head-1 floor keeps >= 1
    if capped:
        notice(
            "expiry-throughput: --duration cut the expire drain short — "
            "drain-completeness invariant checks were SKIPPED (rates above "
            "are for a partial drain)"
        )
    else:
        check(
            totals["snapshots"] >= args.snapshots,
            f"expired {totals['snapshots']} snapshots, expected at least "
            f"{args.snapshots}",
        )
        check(
            totals["files"] >= args.snapshots + args.objects,
            f"queued {totals['files']} files, expected at least "
            f"{args.snapshots + args.objects} (all files were unreachable)",
        )

    # cleanup drain against MinIO
    clean_lat = Recorder()
    removed = missing = still_ref = 0
    t0 = time.perf_counter_ns()
    while True:
        try:
            with clean_lat.measure():
                r = catalog.cleanup(batch=args.batch)
        except Exception as exc:
            if guard.is_server_failure(exc):
                guard.failure(exc)
                continue
            raise
        guard.success()
        removed += r.removed
        missing += r.missing
        still_ref += r.still_referenced
        if r.removed + r.missing + r.still_referenced == 0:
            break
        if deadline is not None and time.monotonic() > deadline:
            capped = True
            break
    clean_wall = (time.perf_counter_ns() - t0) / 1e9
    report.add(
        Metric.from_recorder(
            "cleanup.calls",
            clean_lat,
            clean_wall,
            removed=removed,
            removed_s=removed / clean_wall if clean_wall else 0.0,
            missing=missing,
            still_referenced=still_ref,
            drained_s=(removed + missing) / clean_wall if clean_wall else 0.0,
        )
    )
    check(
        still_ref == 0,
        f"cleanup reported {still_ref} still-referenced paths in the "
        "removal queue — expiry queued a live file (invariant violation)",
    )
    if capped:
        notice(
            "expiry-throughput: --duration cut the cleanup drain short — "
            "the removed-object-count invariant check was SKIPPED"
        )
    else:
        check(
            removed == args.objects,
            f"cleanup removed {removed} real objects, expected {args.objects}",
        )
    return report


def add_args(p: argparse.ArgumentParser) -> None:
    p.add_argument("--snapshots", type=int, default=10_000)
    p.add_argument("--batch", type=int, default=1000, help="expire/cleanup batch size")
    p.add_argument(
        "--objects", type=int, default=200, help="real MinIO objects for cleanup"
    )
