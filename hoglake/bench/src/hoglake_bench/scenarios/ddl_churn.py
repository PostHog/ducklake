"""Scenario 6: create/drop churn (the report-table pattern).

The predecessor kept dropped tables' stats rows until snapshot expiry:
99.4% of stats rows on one production catalog belonged to dropped
tables, and every commit paid for them (purging bought 30-50x). Here we
churn N create/drop pairs and then re-measure a small commit — its p50
must not have degraded relative to the pre-churn probe (ratio, loud
flag).
"""

from __future__ import annotations

import argparse

from pyhoglake import Catalog, Namespace

from ..context import Bench
from ..fabricate import BENCH_SCHEMA, append_payload
from ..runner import FailureGuard, InsufficientSamples, run_loop
from ..stats import Metric
from .common import (
    THERMAL_WARM_OPS,
    ScenarioReport,
    check,
    require_samples,
)

RECORD_COUNT = 100


def _probe_commit_p50(
    report: ScenarioReport,
    catalog: Catalog,
    ns: Namespace,
    name: str,
    ops: int,
    warmup: int,
    guard: FailureGuard,
) -> float:
    table = ns.create_table(name, BENCH_SCHEMA)

    def op(_: int) -> None:
        catalog._commit(append_payload(catalog, table, 1, RECORD_COUNT))

    # both probes get the same fixed warm phase so before/after compare
    # at the same thermal state (a cold "before" probe hides real churn
    # tax behind cold-cache inflation)
    loop = run_loop(
        op, ops=ops, warmup=max(warmup, THERMAL_WARM_OPS), guard=guard
    )
    check(loop.errors == 0, f"probe {name}: {loop.errors} failed commits")
    require_samples(loop.recorder.count, f"ddl-churn probe {name}")
    m = report.add(
        Metric.from_recorder(f"probe.{name}", loop.recorder, loop.wall_s)
    )
    return m.p50_ms or 0.0


def run(bench: Bench, args: argparse.Namespace) -> ScenarioReport:
    report = ScenarioReport(
        scenario="ddl-churn",
        params={
            "tables": args.tables,
            "probe_ops": args.ops,
            "warmup": max(args.warmup, THERMAL_WARM_OPS),
            "duration": args.duration,
            "url": args.url,
        },
    )
    guard = FailureGuard()
    catalog = bench.new_catalog("ddl")
    ns = catalog.create_namespace("bench")

    before_p50 = _probe_commit_p50(
        report, catalog, ns, "before", args.ops, args.warmup, guard
    )

    # churn: each op = create + append + drop (a report table's lifetime)
    def churn(i: int) -> None:
        t = ns.create_table(f"churn_{i}", BENCH_SCHEMA)
        catalog._commit(append_payload(catalog, t, 1, RECORD_COUNT))
        t.drop()

    loop = run_loop(
        churn, ops=args.tables, warmup=0, duration_s=args.duration, guard=guard
    )
    check(loop.errors == 0, f"{loop.errors} failed churn cycles")
    churned = loop.recorder.count
    report.add(
        Metric.from_recorder(
            "churn.create_append_drop",
            loop.recorder,
            loop.wall_s,
            tables_s=churned / loop.wall_s if loop.wall_s else 0.0,
        )
    )

    after_p50 = _probe_commit_p50(
        report, catalog, ns, "after", args.ops, args.warmup, guard
    )
    if before_p50 <= 0:
        raise InsufficientSamples(
            "insufficient samples for a trustworthy ratio: before-churn "
            f"probe p50 is {before_p50} ms — refusing to divide by it"
        )
    ratio = after_p50 / before_p50
    report.add(
        Metric(
            name="probe.ratio",
            ops=0,
            wall_s=0.0,
            extra={
                "p50_before_ms": before_p50,
                "p50_after_ms": after_p50,
                "ratio": ratio,
            },
        )
    )
    report.flag_ratio(
        f"small-commit p50 after {churned} create/append/drop cycles", ratio
    )

    # correctness: churned tables are really gone, probes remain
    live = {t.name for t in ns.list_tables()}
    check(
        live == {"before", "after"},
        f"expected only probe tables to survive churn, found {sorted(live)}",
    )
    return report


def add_args(p: argparse.ArgumentParser) -> None:
    p.add_argument("--tables", type=int, default=500, help="create/append/drop cycles")
    p.add_argument("--ops", type=int, default=50, help="probe commits before/after")
