"""Shared scenario plumbing: reports, invariant checks, seeding."""

from __future__ import annotations

import sys
from dataclasses import dataclass, field
from typing import Any

from pyhoglake import Catalog, Table

from ..context import Bench
from ..fabricate import BENCH_SCHEMA, append_payload
from ..runner import FailureGuard, InsufficientSamples, InvariantViolation, run_loop
from ..stats import Metric

RATIO_FLAG_THRESHOLD = 1.5

# Every stage that feeds a regression ratio must have at least this many
# measured samples, or the run aborts (exit 3) instead of emitting a
# vacuous ratio from thin air.
MIN_GUARDED_SAMPLES = 20

# Every guarded stage runs this many identical, discarded warm ops
# before measurement, so both sides of a ratio are compared at the same
# thermal state (a cold fresh-catalog baseline vs a seed-warmed
# preseeded stage hides real O(catalog) regressions behind cold-cache
# inflation of the baseline).
THERMAL_WARM_OPS = 40


def require_samples(
    count: int, what: str, floor: int = MIN_GUARDED_SAMPLES
) -> None:
    """Abort loudly when a guarded stage measured too few ops for a
    trustworthy ratio — never let a ratio compute from ~0 samples."""
    if count < floor:
        raise InsufficientSamples(
            f"insufficient samples for a trustworthy ratio: {what} "
            f"measured {count} ops, floor is {floor} — raise --ops or "
            "--duration instead of trusting a ratio built on noise"
        )


def notice(message: str) -> None:
    """Loud stderr notice for skipped checks / truncated phases."""
    print(f"NOTICE: {message}", file=sys.stderr, flush=True)


@dataclass
class ScenarioReport:
    scenario: str
    params: dict[str, Any]
    metrics: list[Metric] = field(default_factory=list)
    flags: list[str] = field(default_factory=list)

    def add(self, metric: Metric) -> Metric:
        self.metrics.append(metric)
        print(metric.line(), flush=True)
        return metric

    def flag(self, message: str) -> None:
        self.flags.append(message)
        print(f"!!! {message}", file=sys.stderr, flush=True)

    def flag_ratio(self, what: str, ratio: float) -> None:
        """The headline check: latency must not grow with catalog size."""
        if ratio > RATIO_FLAG_THRESHOLD:
            self.flag(
                f"REGRESSION {self.scenario}: {what} grew {ratio:.2f}x "
                f"(threshold {RATIO_FLAG_THRESHOLD}x) — this is the "
                "O(catalog) commit-cost class hoglake exists to kill"
            )


def check(cond: bool, message: str) -> None:
    if not cond:
        raise InvariantViolation(message)


def assert_row_tiling(table: Table, *, expected_rows: int | None = None) -> None:
    """Row-id ranges must tile [0, total) with no gaps or overlap —
    the row-lineage contract, checked via the public files API."""
    files = sorted(table.files(), key=lambda f: f.row_id_start)
    next_start = 0
    for f in files:
        check(
            f.row_id_start == next_start,
            f"row-range tiling broken on {table.name}: file {f.path} "
            f"starts at {f.row_id_start}, expected {next_start}",
        )
        next_start += f.record_count
    if expected_rows is not None:
        check(
            next_start == expected_rows,
            f"{table.name}: row ranges cover {next_start} rows, "
            f"expected {expected_rows}",
        )


def assert_dense_snapshots(catalog: Catalog, after: int, expected_new: int) -> None:
    """Snapshot ids after ``after`` must be exactly the consecutive run
    (after, after + expected_new] — dense allocation is the contract."""
    ids = [s.snapshot_id for s in catalog.snapshots(after=after)]
    check(
        ids == list(range(after + 1, after + expected_new + 1)),
        f"snapshot ids not dense after {after}: got {len(ids)} ids "
        f"{ids[:3]}..{ids[-3:] if ids else []}, expected "
        f"{expected_new} consecutive",
    )


def make_bench_table(
    bench: Bench, slug: str
) -> tuple[Catalog, Table]:
    catalog = bench.new_catalog(slug)
    ns = catalog.create_namespace("bench")
    table = ns.create_table("t", BENCH_SCHEMA)
    return catalog, table


def seed_snapshots(
    catalog: Catalog,
    table: Table,
    count: int,
    *,
    record_count: int = 10,
    guard: FailureGuard | None = None,
    label: str = "seed",
) -> Metric:
    """Mint ``count`` snapshots via rapid single-file registration-only
    commits. Returns the seeding metric (itself a commit-rate number)."""
    guard = guard or FailureGuard()

    def op(_: int) -> None:
        catalog._commit(append_payload(catalog, table, 1, record_count))

    result = run_loop(op, ops=count, warmup=0, guard=guard)
    check(
        result.errors == 0,
        f"{label}: {result.errors} failed commits while seeding",
    )
    return Metric.from_recorder(f"{label}(n={count})", result.recorder, result.wall_s)
