"""The ``seed`` task: populate a hoglake with a realistic fake warehouse.

Not a benchmark scenario — it writes no metrics and never flags a
regression. It exists so the console, compaction planning and the
changefeed have a catalog with real shape to look at.
"""

from __future__ import annotations

from .budget import SeedPlan, plan_budget
from .seeder import DEFAULT_CATALOG, DEFAULT_SEED, add_args, run

__all__ = [
    "DEFAULT_CATALOG",
    "DEFAULT_SEED",
    "SeedPlan",
    "add_args",
    "plan_budget",
    "run",
]
