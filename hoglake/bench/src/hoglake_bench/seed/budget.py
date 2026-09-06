"""Byte-budget planning for ``hoglake-bench seed``.

Sizes are decimal (1 GB = 1000 MB = 1e9 bytes) and the planning quantum
is **100 MB**, so ``--gb 2.5`` is exactly 25 units. The requested size is
rounded to the nearest unit (half up) and echoed before a byte is
written; the per-table budgets are a largest-remainder split of the
rounded total, so they sum to it exactly.

Nothing here talks to a server: this is the pure arithmetic the seeder
(and its unit tests) plan from.
"""

from __future__ import annotations

import math
from dataclasses import dataclass

#: The planning quantum: 100 MB, decimal.
UNIT_BYTES = 100_000_000
BYTES_PER_GB = 1_000_000_000
UNITS_PER_GB = BYTES_PER_GB // UNIT_BYTES

#: Below this the "warehouse" is not a warehouse — refuse rather than
#: fabricate a shape that cannot hold six tables' worth of files.
MIN_GB = 0.1

#: Anything inside this band of the rounded target counts as on plan:
#: parquet compression makes an exact byte target impossible.
TOLERANCE = 0.10


@dataclass(frozen=True)
class TableSpec:
    """One table of the seeded warehouse and its share of the budget."""

    namespace: str
    table: str
    share: float
    kind: str  # "events" | "fact" | "dim"

    @property
    def key(self) -> str:
        return f"{self.namespace}.{self.table}"


#: The warehouse shape: one big partitioned event stream, two facts, three
#: dims. Shares sum to 1.0. Dims are capped by row count when they seed
#: (a dimension does not grow to 500 MB just because the budget did), and
#: whatever they leave unspent is rebalanced onto the events/fact tables.
WAREHOUSE_SHAPE: tuple[TableSpec, ...] = (
    TableSpec("events", "pageviews", 0.65, "events"),
    TableSpec("warehouse", "fact_orders", 0.18, "fact"),
    TableSpec("warehouse", "fact_sessions", 0.12, "fact"),
    TableSpec("warehouse", "dim_users", 0.030, "dim"),
    TableSpec("warehouse", "dim_products", 0.012, "dim"),
    TableSpec("warehouse", "dim_teams", 0.008, "dim"),
)

SPEC_BY_KEY = {spec.key: spec for spec in WAREHOUSE_SHAPE}


def fmt_bytes(n: float) -> str:
    """Decimal-unit byte formatting, matching the GB the CLI plans in."""
    n = float(n)
    for unit, scale in (("GB", 1e9), ("MB", 1e6), ("KB", 1e3)):
        if abs(n) >= scale:
            return f"{n / scale:.1f} {unit}"
    return f"{n:.0f} B"


def largest_remainder(total: int, weights: dict[str, float]) -> dict[str, int]:
    """Split ``total`` across ``weights`` so the parts sum to it exactly.

    Floor every exact share, then hand the leftover bytes out by
    descending fractional part (ties by key order, so the split is
    deterministic).
    """
    if not weights:
        return {}
    total = max(0, int(total))
    scale = sum(weights.values())
    if scale <= 0:
        return dict.fromkeys(weights, 0)
    exact = {k: total * w / scale for k, w in weights.items()}
    out = {k: math.floor(v) for k, v in exact.items()}
    leftover = total - sum(out.values())
    order = sorted(exact, key=lambda k: (-(exact[k] - math.floor(exact[k])), k))
    for i in range(leftover):
        out[order[i % len(order)]] += 1
    return out


@dataclass(frozen=True)
class SeedPlan:
    """The rounded, per-table byte budget for one seed run."""

    requested_gb: float
    units: int
    budgets: dict[str, int]

    @property
    def target_bytes(self) -> int:
        return self.units * UNIT_BYTES

    @property
    def target_gb(self) -> float:
        return self.target_bytes / BYTES_PER_GB

    def describe(self) -> str:
        return (
            f"target {self.requested_gb:g} GB -> plan: {self.target_gb:g} GB "
            f"({self.units} x 100MB units)"
        )

    def lines(self) -> list[str]:
        """One aligned line per table, in warehouse-shape order."""
        return [
            f"  {spec.key:<26} {spec.share * 100:5.1f}%  "
            f"{fmt_bytes(self.budgets[spec.key]):>9}"
            for spec in WAREHOUSE_SHAPE
        ]


def plan_budget(gb: float) -> SeedPlan:
    """Round ``gb`` to the nearest 100 MB and split it across the shape.

    Raises ``ValueError`` below :data:`MIN_GB` — a sub-100MB "warehouse"
    is not worth writing, and the CLI turns this into a usage error.
    """
    gb = float(gb)
    if not math.isfinite(gb):
        raise ValueError(f"--gb must be a finite number of gigabytes, got {gb!r}")
    if gb < MIN_GB - 1e-9:
        raise ValueError(
            f"--gb {gb:g} is below the {MIN_GB:g} GB (100 MB) floor: a "
            "smaller target cannot fill a six-table warehouse with "
            "realistically-sized files"
        )
    # Half-up, not banker's: 0.25 GB is 3 units, never 2. The epsilon is
    # not decoration — 0.15 * 10 is 1.4999999999999998 in binary floating
    # point, which would round DOWN to one unit without it.
    units = max(1, math.floor(gb * UNITS_PER_GB + 0.5 + 1e-9))
    budgets = largest_remainder(
        units * UNIT_BYTES, {s.key: s.share for s in WAREHOUSE_SHAPE}
    )
    return SeedPlan(requested_gb=gb, units=units, budgets=budgets)


def rebalance(
    remaining_bytes: int, keys: list[str] | tuple[str, ...]
) -> dict[str, int]:
    """Re-split what the dims left over across ``keys``, by their shares.

    Dimension tables are row-capped, so on any sizeable budget they spend
    far less than their nominal share; without this the run would land
    well under target.
    """
    return largest_remainder(remaining_bytes, {k: SPEC_BY_KEY[k].share for k in keys})
