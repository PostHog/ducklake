"""``hoglake-bench seed``: fill a catalog with a realistic fake warehouse.

Star schema (three dims, two facts) plus a partitioned event stream,
sized by a total byte budget rather than a row count. Sizing works
because the writer measures itself against the **server's own** file
accounting after every commit: the per-table byte budget is re-divided
across the work that is left, so a bad bytes-per-row estimate corrects
itself instead of compounding. Compression makes an exact target
impossible; landing inside ~10% is the contract.
"""

from __future__ import annotations

import argparse
import io
import math
import sys
import time
from collections.abc import Callable, Iterator
from dataclasses import dataclass
from datetime import UTC, datetime

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
from pyhoglake import AlreadyExistsError, Catalog, Namespace, Table, ops

from ..context import Bench
from ..runner import BenchAbort
from . import tables as T
from .budget import (
    MIN_GB,
    TOLERANCE,
    WAREHOUSE_SHAPE,
    SeedPlan,
    fmt_bytes,
    plan_budget,
    rebalance,
)
from .vocab import Vocabulary, build_vocabulary

#: Fixed so two runs produce the same shape (the catalog-lock magic
#: number from the server, for no better reason than it is memorable).
DEFAULT_SEED = 4740871
DEFAULT_CATALOG = "seed-warehouse"

#: Facts/events aim for files in this band — many mid-sized files, not a
#: few giants, so compaction planning has something real to rank.
MIN_FILE_MB = 20.0
MAX_FILE_MB = 80.0
DEFAULT_FILE_MB = 32.0

#: Rows used to calibrate bytes-per-row locally (no catalog writes).
PROBE_ROWS = 20_000
#: No file smaller than this, even when a partition's budget is tiny.
MIN_ROWS_PER_FILE = 2_000
#: Dimension tables are cardinality-bounded: a dim does not grow to
#: 500 MB just because the budget did.
DIM_ROW_FLOOR = {"dim_users": 5_000, "dim_products": 500}
DIM_ROW_CAP = {"dim_users": 500_000, "dim_products": 50_000}
#: Belt and braces against a pathological loop (a table whose files
#: somehow never grow) — abort rather than write forever.
MAX_FILES_PER_TABLE = 20_000


@dataclass
class SeedContext:
    rng: np.random.Generator
    vocab: Vocabulary
    anchor: datetime
    file_bytes: int
    fanout: int
    user_count: int = 0
    product_count: int = 0


@dataclass
class Written:
    """Per-table result, every number read back from the server."""

    key: str
    files: int = 0
    rows: int = 0
    bytes: int = 0
    total_files: int = 0
    total_rows: int = 0
    total_bytes: int = 0


class TableMeter:
    """Server-sourced byte/row accounting for one table's seeding.

    ``TableInfo`` aggregates are the catalog's truth (files visible at
    head), so the write loop steers on them rather than on what the
    client believes it uploaded.
    """

    def __init__(self, key: str, table: Table) -> None:
        self.key = key
        self.table = table
        info = table.info()
        self.base_files = info.file_count
        self.base_rows = info.record_count
        self.base_bytes = info.file_size_bytes
        self.files = 0
        self.rows = 0
        self.bytes = 0

    def sync(self) -> None:
        info = self.table.info()
        self.files = info.file_count - self.base_files
        self.rows = info.record_count - self.base_rows
        self.bytes = info.file_size_bytes - self.base_bytes

    @property
    def bytes_per_row(self) -> float | None:
        return self.bytes / self.rows if self.rows > 0 else None

    def written(self) -> Written:
        info = self.table.info()
        return Written(
            key=self.key,
            files=self.files,
            rows=self.rows,
            bytes=self.bytes,
            total_files=info.file_count,
            total_rows=info.record_count,
            total_bytes=info.file_size_bytes,
        )


def _progress(key: str, message: str) -> None:
    print(f"  {key:<26} {message}", flush=True)


def _calibrate(make: Callable[[int], pa.Table]) -> float:
    """Bytes per row, measured by writing a probe batch to memory with
    the same parquet settings the client uses — no catalog writes, no
    guessing from arrow's in-memory size."""
    sink = io.BytesIO()
    pq.write_table(make(PROBE_ROWS), sink)
    return max(1.0, len(sink.getvalue()) / PROBE_ROWS)


def _rows_per_file(file_bytes: int, bytes_per_row: float) -> int:
    return max(MIN_ROWS_PER_FILE, int(file_bytes / bytes_per_row))


def _chunks(values: list[int], size: int) -> Iterator[list[int]]:
    for i in range(0, len(values), size):
        yield values[i : i + size]


# -- catalog / DDL -----------------------------------------------------------


def _ensure_catalog(bench: Bench, name: str) -> Catalog:
    """Create the catalog, or adopt the existing one (``--catalog`` reuse
    appends more data — it never fails)."""
    data_path = f"s3://{bench.cfg.bucket}/seed/{name}/"
    try:
        catalog = bench.client.create_catalog(name, data_path)
        print(f"created catalog {name!r} at {data_path}", flush=True)
    except AlreadyExistsError:
        catalog = bench.client.catalog(name)
        print(
            f"appending to existing catalog {name!r} at {catalog.data_path}",
            flush=True,
        )
    return catalog


def _ensure_namespace(catalog: Catalog, name: str) -> Namespace:
    try:
        return catalog.create_namespace(name)
    except AlreadyExistsError:
        return catalog.namespace(name)


def _ensure_table(
    ns: Namespace,
    name: str,
    schema: pa.Schema,
    partition: tuple[tuple[str, str], ...] = (),
) -> Table:
    try:
        table = ns.create_table(name, schema)
    except AlreadyExistsError:
        table = ns.table(name)
        live = {c.name for c in table.columns}
        if live != set(schema.names):
            raise BenchAbort(
                f"table {ns.name}.{name} already exists with a different "
                f"schema (live columns {sorted(live)}); seed into a fresh "
                "--catalog instead of appending incompatible rows"
            ) from None
        return table
    if partition:
        field_id = {c.name: c.field_id for c in table.columns}
        table.alter(
            [
                ops.set_partition_spec(
                    [
                        ops.partition_field(field_id[col], transform)
                        for col, transform in partition
                    ]
                )
            ]
        )
    return table


# -- write loops -------------------------------------------------------------


def _seed_single_file(
    key: str,
    table: Table,
    budget: int,
    make: Callable[[int], pa.Table],
    *,
    floor: int,
    cap: int,
) -> Written:
    """A dimension: one small file per run, row count bounded by the
    dimension's realistic cardinality (not by the byte budget)."""
    meter = TableMeter(key, table)
    bytes_per_row = _calibrate(make)
    rows = min(cap, max(floor, int(budget / bytes_per_row)))
    table.append(make(rows), author="hoglake-bench", message=f"seed {key}")
    meter.sync()
    _progress(key, f"1 file  rows={rows:,}  {fmt_bytes(meter.bytes)}")
    return meter.written()


def _seed_flat(
    key: str,
    table: Table,
    budget: int,
    make: Callable[[int, int], pa.Table],
    ctx: SeedContext,
) -> Written:
    """An unpartitioned fact table: one file per commit, each sized to
    ``--file-mb``, until the byte budget is met."""
    meter = TableMeter(key, table)
    bytes_per_row = _calibrate(lambda n: make(n, meter.base_rows))
    commits = 0
    while meter.bytes < budget:
        if commits >= MAX_FILES_PER_TABLE:
            raise BenchAbort(
                f"{key}: {commits} files written and still short of the "
                f"{fmt_bytes(budget)} budget — the seeder is not making "
                "progress"
            )
        remaining = budget - meter.bytes
        rows = min(
            _rows_per_file(ctx.file_bytes, bytes_per_row),
            max(MIN_ROWS_PER_FILE, int(remaining / bytes_per_row)),
        )
        table.append(
            make(rows, meter.base_rows + meter.rows),
            author="hoglake-bench",
            message=f"seed {key} #{commits + 1}",
        )
        commits += 1
        meter.sync()
        bytes_per_row = meter.bytes_per_row or bytes_per_row
        _progress(
            key,
            f"commit {commits:<4} files={meter.files:<4} rows={meter.rows:,}"
            f"  {fmt_bytes(meter.bytes)} / {fmt_bytes(budget)}",
        )
    return meter.written()


def _seed_events(key: str, table: Table, budget: int, ctx: SeedContext) -> Written:
    """The partitioned event stream.

    Work is planned as ``identity(team_id) x month(ts)`` cells, weighted
    by both (skewed tenants, growing history). Each commit carries
    ``--fanout`` cells of the same month, so pyhoglake's partitioned
    append fans one batch out to one parquet file per partition tuple —
    all registered in a single atomic commit. Cell budgets are recomputed
    from the server's byte count as the run proceeds, so estimation error
    never accumulates.
    """
    meter = TableMeter(key, table)
    windows = T.month_windows(ctx.anchor, T.EVENT_MONTHS)
    bytes_per_row = _calibrate(
        lambda n: T.pageviews(
            ctx.rng,
            ctx.vocab,
            n,
            team_id=T.TEAM_IDS[0],
            lo_us=windows[-1][0],
            hi_us=windows[-1][1],
        )
    )
    team_slots = list(range(len(T.TEAM_IDS)))
    weight_left = 1.0
    commits = 0
    for month_idx, (lo_us, hi_us) in enumerate(windows):
        for chunk in _chunks(team_slots, ctx.fanout):
            if meter.bytes >= budget:
                return meter.written()
            chunk_weight = sum(T.TEAM_WEIGHTS[t] for t in chunk)
            weight = T.MONTH_WEIGHTS[month_idx] * chunk_weight
            remaining = budget - meter.bytes
            share = weight / weight_left if weight_left > 1e-9 else 1.0
            group_budget = remaining * min(1.0, share)
            weight_left = max(0.0, weight_left - weight)
            rows_left = {
                t: max(
                    MIN_ROWS_PER_FILE,
                    int(
                        group_budget * T.TEAM_WEIGHTS[t] / chunk_weight / bytes_per_row
                    ),
                )
                for t in chunk
            }
            while any(v > 0 for v in rows_left.values()):
                if commits >= MAX_FILES_PER_TABLE:
                    raise BenchAbort(
                        f"{key}: {commits} commits and still short of the "
                        f"{fmt_bytes(budget)} budget — not making progress"
                    )
                rows_per_file = _rows_per_file(ctx.file_bytes, bytes_per_row)
                parts = []
                for slot, left in rows_left.items():
                    if left <= 0:
                        continue
                    rows = min(rows_per_file, left)
                    rows_left[slot] = left - rows
                    parts.append(
                        T.pageviews(
                            ctx.rng,
                            ctx.vocab,
                            rows,
                            team_id=T.TEAM_IDS[slot],
                            lo_us=lo_us,
                            hi_us=hi_us,
                        )
                    )
                result = table.append(
                    pa.concat_tables(parts),
                    author="hoglake-bench",
                    message=f"seed {key} month {month_idx + 1}",
                )
                commits += 1
                meter.sync()
                bytes_per_row = meter.bytes_per_row or bytes_per_row
                _progress(
                    key,
                    f"commit {commits:<4} +{len(result.files)} files "
                    f"(month {month_idx + 1}/{len(windows)})  "
                    f"rows={meter.rows:,}  "
                    f"{fmt_bytes(meter.bytes)} / {fmt_bytes(budget)}",
                )
                if meter.bytes >= budget:
                    return meter.written()
    return meter.written()


# -- orchestration -----------------------------------------------------------


def _summary(plan: SeedPlan, results: list[Written], elapsed: float) -> None:
    """The catalog's own numbers, straight from /tables info."""
    print("\nseeded warehouse (per-table figures from the server's /tables info):")
    header = f"  {'table':<26} {'files':>7} {'rows':>14} {'bytes':>11} {'this run':>11}"
    print(header)
    print("  " + "-" * (len(header) - 2))
    for r in results:
        print(
            f"  {r.key:<26} {r.total_files:>7,} {r.total_rows:>14,} "
            f"{fmt_bytes(r.total_bytes):>11} {fmt_bytes(r.bytes):>11}"
        )
    files = sum(r.files for r in results)
    written = sum(r.bytes for r in results)
    total_files = sum(r.total_files for r in results)
    total_bytes = sum(r.total_bytes for r in results)
    print("  " + "-" * (len(header) - 2))
    print(
        f"  {'TOTAL':<26} {total_files:>7,} "
        f"{sum(r.total_rows for r in results):>14,} "
        f"{fmt_bytes(total_bytes):>11} {fmt_bytes(written):>11}"
    )
    drift = (written - plan.target_bytes) / plan.target_bytes
    verdict = "on plan" if abs(drift) <= TOLERANCE else "OUTSIDE the ~10% band"
    print(
        f"\nwrote {fmt_bytes(written)} in {files} files across "
        f"{len(results)} tables in {elapsed:.1f}s "
        f"(target {fmt_bytes(plan.target_bytes)}, {drift * 100:+.1f}% — {verdict}; "
        "parquet compression makes exact byte targets impossible)"
    )
    if abs(drift) > TOLERANCE:
        print(
            f"NOTICE: seeded volume is {drift * 100:+.1f}% off the rounded "
            "target — a very small --gb over a 6-month x 6-team partition "
            "grid hits the minimum-file-size floor",
            file=sys.stderr,
            flush=True,
        )


def run(bench: Bench, args: argparse.Namespace) -> None:
    """Seed ``--catalog`` with ``--gb`` gigabytes of fake warehouse."""
    try:
        plan = plan_budget(args.gb)
    except ValueError as exc:
        raise BenchAbort(str(exc)) from exc
    print(plan.describe(), flush=True)
    print("plan:", flush=True)
    for line in plan.lines():
        print(line, flush=True)

    file_mb = min(MAX_FILE_MB, max(MIN_FILE_MB, args.file_mb))
    ctx = SeedContext(
        rng=np.random.default_rng(args.seed),
        vocab=build_vocabulary(args.seed),
        anchor=datetime.now(UTC),
        file_bytes=int(file_mb * 1_000_000),
        fanout=max(1, args.fanout),
    )
    print(
        f"seed={args.seed}  file target={file_mb:g} MB  "
        f"partitions per commit={ctx.fanout}  teams={len(T.TEAM_IDS)}  "
        f"months={T.EVENT_MONTHS}",
        flush=True,
    )

    bench.ensure_bucket()
    catalog = _ensure_catalog(bench, args.catalog)
    events_ns = _ensure_namespace(catalog, "events")
    warehouse_ns = _ensure_namespace(catalog, "warehouse")

    pageviews = _ensure_table(
        events_ns,
        "pageviews",
        T.PAGEVIEWS_SCHEMA,
        partition=(("team_id", "identity"), ("ts", "month")),
    )
    dim_teams = _ensure_table(warehouse_ns, "dim_teams", T.DIM_TEAMS_SCHEMA)
    dim_products = _ensure_table(warehouse_ns, "dim_products", T.DIM_PRODUCTS_SCHEMA)
    dim_users = _ensure_table(warehouse_ns, "dim_users", T.DIM_USERS_SCHEMA)
    fact_orders = _ensure_table(warehouse_ns, "fact_orders", T.FACT_ORDERS_SCHEMA)
    fact_sessions = _ensure_table(warehouse_ns, "fact_sessions", T.FACT_SESSIONS_SCHEMA)

    t0 = time.monotonic()
    print("\nwriting:", flush=True)
    results: dict[str, Written] = {}

    # -- dims first: the facts need their id ranges ------------------------
    teams_meter = TableMeter("warehouse.dim_teams", dim_teams)
    if teams_meter.base_rows == 0:
        dim_teams.append(
            T.dim_teams(ctx.rng, ctx.vocab, anchor=ctx.anchor),
            author="hoglake-bench",
            message="seed warehouse.dim_teams",
        )
        teams_meter.sync()
        _progress("warehouse.dim_teams", f"1 file  rows={len(T.TEAM_IDS)}")
    else:
        _progress("warehouse.dim_teams", "already seeded (fixed team set) — skipped")
    results["warehouse.dim_teams"] = teams_meter.written()

    results["warehouse.dim_products"] = _seed_single_file(
        "warehouse.dim_products",
        dim_products,
        plan.budgets["warehouse.dim_products"],
        lambda n: T.dim_products(
            ctx.rng,
            ctx.vocab,
            n,
            id_offset=dim_products.info().record_count,
            anchor=ctx.anchor,
        ),
        floor=DIM_ROW_FLOOR["dim_products"],
        cap=DIM_ROW_CAP["dim_products"],
    )
    results["warehouse.dim_users"] = _seed_single_file(
        "warehouse.dim_users",
        dim_users,
        plan.budgets["warehouse.dim_users"],
        lambda n: T.dim_users(
            ctx.rng,
            ctx.vocab,
            n,
            id_offset=dim_users.info().record_count,
            anchor=ctx.anchor,
        ),
        floor=DIM_ROW_FLOOR["dim_users"],
        cap=DIM_ROW_CAP["dim_users"],
    )
    ctx.user_count = max(1, results["warehouse.dim_users"].total_rows)
    ctx.product_count = max(1, results["warehouse.dim_products"].total_rows)

    # -- the dims spend far less than their nominal share: give the rest
    #    to the tables that are supposed to be big ------------------------
    dim_spend = sum(results[s.key].bytes for s in WAREHOUSE_SHAPE if s.kind == "dim")
    big = [s.key for s in WAREHOUSE_SHAPE if s.kind != "dim"]
    budgets = rebalance(max(0, plan.target_bytes - dim_spend), big)
    _progress(
        "(rebalanced)",
        f"dims spent {fmt_bytes(dim_spend)}; "
        + ", ".join(f"{k}={fmt_bytes(v)}" for k, v in budgets.items()),
    )

    results["events.pageviews"] = _seed_events(
        "events.pageviews", pageviews, budgets["events.pageviews"], ctx
    )
    lo_us, hi_us = T.history_window(ctx.anchor)
    results["warehouse.fact_orders"] = _seed_flat(
        "warehouse.fact_orders",
        fact_orders,
        budgets["warehouse.fact_orders"],
        lambda n, offset: T.fact_orders(
            ctx.rng,
            n,
            id_offset=offset,
            user_count=ctx.user_count,
            product_count=ctx.product_count,
            lo_us=lo_us,
            hi_us=hi_us,
        ),
        ctx,
    )
    results["warehouse.fact_sessions"] = _seed_flat(
        "warehouse.fact_sessions",
        fact_sessions,
        budgets["warehouse.fact_sessions"],
        lambda n, _offset: T.fact_sessions(
            ctx.rng,
            ctx.vocab,
            n,
            user_count=ctx.user_count,
            lo_us=lo_us,
            hi_us=hi_us,
        ),
        ctx,
    )

    _summary(
        plan,
        [results[s.key] for s in WAREHOUSE_SHAPE],
        time.monotonic() - t0,
    )
    print(f"\ncatalog: {catalog.name} ({catalog.data_path})", flush=True)


def _gb(value: str) -> float:
    """``--gb`` accepts fractions; below the floor it is a usage error."""
    try:
        gb = float(value)
    except ValueError:
        raise argparse.ArgumentTypeError(
            f"--gb must be a number of gigabytes, got {value!r}"
        ) from None
    if not math.isfinite(gb) or gb < MIN_GB - 1e-9:
        raise argparse.ArgumentTypeError(
            f"--gb must be at least {MIN_GB:g} (100 MB), got {value}"
        )
    return gb


def add_args(p: argparse.ArgumentParser) -> None:
    p.add_argument(
        "--gb",
        type=_gb,
        default=1.0,
        help="total volume to write, in GB (fractional ok; rounded to the "
        f"nearest 100 MB; minimum {MIN_GB:g})",
    )
    p.add_argument(
        "--catalog",
        default=DEFAULT_CATALOG,
        help=f"catalog to create or append to (default {DEFAULT_CATALOG})",
    )
    p.add_argument(
        "--seed",
        type=int,
        default=DEFAULT_SEED,
        help="RNG/vocabulary seed — fixed by default, so two runs produce "
        "the same shape",
    )
    p.add_argument(
        "--file-mb",
        type=float,
        default=DEFAULT_FILE_MB,
        help=f"target parquet file size for events/facts, MB (clamped to "
        f"{MIN_FILE_MB:g}-{MAX_FILE_MB:g})",
    )
    p.add_argument(
        "--fanout",
        type=int,
        default=2,
        help="partitions (team_id x month cells) per event commit",
    )
    p.add_argument(
        "--server",
        dest="url",
        default=argparse.SUPPRESS,
        help="alias for --url (the hoglake server)",
    )
