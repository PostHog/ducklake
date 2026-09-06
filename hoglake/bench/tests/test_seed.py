"""Unit tests for the `seed` task: budget arithmetic, vocabulary
determinism, and the fabricators' shape (no server involved)."""

from __future__ import annotations

import itertools
from datetime import UTC, datetime

import numpy as np
import pytest
from pyhoglake.transforms import month as iceberg_month

from hoglake_bench.cli import SCENARIOS, build_parser
from hoglake_bench.seed import tables as T
from hoglake_bench.seed.budget import (
    MIN_GB,
    UNIT_BYTES,
    WAREHOUSE_SHAPE,
    largest_remainder,
    plan_budget,
    rebalance,
)
from hoglake_bench.seed.vocab import build_vocabulary

ANCHOR = datetime(2026, 9, 6, 12, 0, tzinfo=UTC)


class TestBudgetRounding:
    @pytest.mark.parametrize(
        ("gb", "units"),
        [
            (0.1, 1),
            (0.14, 1),
            # 0.15 * 10 is 1.4999999999999998 in binary floating point:
            # the half-up rounding must survive that, not floor to 1
            (0.15, 2),
            (0.25, 3),
            (1.0, 10),
            (2.5, 25),
            (3.14, 31),
            (3.16, 32),
            (10.0, 100),
        ],
    )
    def test_rounds_to_nearest_100mb(self, gb, units):
        plan = plan_budget(gb)
        assert plan.units == units
        assert plan.target_bytes == units * UNIT_BYTES

    def test_describe_echoes_the_rounded_plan(self):
        assert plan_budget(2.5).describe() == (
            "target 2.5 GB -> plan: 2.5 GB (25 x 100MB units)"
        )
        # a rounded request says so: 0.34 GB of ask, 0.3 GB of plan
        assert plan_budget(0.34).describe() == (
            "target 0.34 GB -> plan: 0.3 GB (3 x 100MB units)"
        )

    @pytest.mark.parametrize("gb", [0.0, 0.05, 0.099, -1.0])
    def test_refuses_below_the_floor(self, gb):
        with pytest.raises(ValueError, match="below the 0.1 GB"):
            plan_budget(gb)

    def test_floor_itself_is_allowed(self):
        assert plan_budget(MIN_GB).units == 1

    def test_refuses_non_finite(self):
        with pytest.raises(ValueError, match="finite"):
            plan_budget(float("inf"))
        with pytest.raises(ValueError, match="finite"):
            plan_budget(float("nan"))


class TestBudgetSplit:
    def test_shape_shares_sum_to_one(self):
        assert sum(s.share for s in WAREHOUSE_SHAPE) == pytest.approx(1.0)

    @pytest.mark.parametrize("gb", [0.1, 0.3, 2.5, 10.0])
    def test_split_is_exact_and_proportional(self, gb):
        plan = plan_budget(gb)
        assert sum(plan.budgets.values()) == plan.target_bytes
        for spec in WAREHOUSE_SHAPE:
            assert plan.budgets[spec.key] == pytest.approx(
                plan.target_bytes * spec.share, rel=1e-6, abs=1
            )

    def test_events_is_the_big_one(self):
        budgets = plan_budget(2.5).budgets
        assert budgets["events.pageviews"] > sum(
            v for k, v in budgets.items() if k != "events.pageviews"
        )

    def test_largest_remainder_never_loses_a_byte(self):
        out = largest_remainder(1000, {"a": 1 / 3, "b": 1 / 3, "c": 1 / 3})
        assert sum(out.values()) == 1000
        assert set(out) == {"a", "b", "c"}

    def test_largest_remainder_is_deterministic(self):
        weights = {"a": 0.5, "b": 0.3, "c": 0.2}
        assert largest_remainder(7, weights) == largest_remainder(7, weights)

    def test_rebalance_gives_dim_leftovers_to_the_big_tables(self):
        keys = ["events.pageviews", "warehouse.fact_orders", "warehouse.fact_sessions"]
        out = rebalance(950_000_000, keys)
        assert sum(out.values()) == 950_000_000
        # shares stay in 65:18:12 proportion after renormalizing
        assert out["events.pageviews"] / out["warehouse.fact_orders"] == pytest.approx(
            0.65 / 0.18, rel=1e-3
        )

    def test_rebalance_clamps_an_overspent_budget(self):
        assert sum(rebalance(-5, ["events.pageviews"]).values()) == 0


class TestVocabularyDeterminism:
    def _fresh(self, seed: int):
        build_vocabulary.cache_clear()
        return build_vocabulary(seed)

    def test_same_seed_same_vocabulary(self):
        assert self._fresh(1234) == self._fresh(1234)

    def test_different_seed_different_vocabulary(self):
        assert self._fresh(1234).names != self._fresh(5678).names

    def test_values_are_distinct(self):
        vocab = self._fresh(99)
        for field in ("names", "domains", "paths", "products", "distinct_ids"):
            values = getattr(vocab, field)
            assert len(set(values)) == len(values), field
            assert values, field


class TestFabricators:
    def test_pageviews_match_the_declared_schema(self):
        rng = np.random.default_rng(7)
        vocab = build_vocabulary(7)
        lo, hi = T.month_windows(ANCHOR, T.EVENT_MONTHS)[0]
        batch = T.pageviews(rng, vocab, 500, team_id=42, lo_us=lo, hi_us=hi)
        assert batch.schema.equals(T.PAGEVIEWS_SCHEMA)
        assert batch.num_rows == 500
        assert batch.column("team_id").null_count == 0
        assert batch.column("ts").null_count == 0
        # nullable-by-design columns actually carry nulls (direct traffic)
        assert batch.column("referrer_domain").null_count > 0

    def test_same_seed_same_rows(self):
        def make():
            rng = np.random.default_rng(11)
            lo, hi = T.month_windows(ANCHOR, T.EVENT_MONTHS)[2]
            return T.pageviews(
                rng, build_vocabulary(11), 200, team_id=17, lo_us=lo, hi_us=hi
            )

        assert make().equals(make())

    def test_every_table_generator_matches_its_schema(self):
        rng = np.random.default_rng(3)
        vocab = build_vocabulary(3)
        lo, hi = T.history_window(ANCHOR)
        cases = [
            (
                T.dim_users(rng, vocab, 50, id_offset=10, anchor=ANCHOR),
                T.DIM_USERS_SCHEMA,
            ),
            (
                T.dim_products(rng, vocab, 50, id_offset=0, anchor=ANCHOR),
                T.DIM_PRODUCTS_SCHEMA,
            ),
            (T.dim_teams(rng, vocab, anchor=ANCHOR), T.DIM_TEAMS_SCHEMA),
            (
                T.fact_orders(
                    rng,
                    50,
                    id_offset=0,
                    user_count=50,
                    product_count=50,
                    lo_us=lo,
                    hi_us=hi,
                ),
                T.FACT_ORDERS_SCHEMA,
            ),
            (
                T.fact_sessions(rng, vocab, 50, user_count=50, lo_us=lo, hi_us=hi),
                T.FACT_SESSIONS_SCHEMA,
            ),
        ]
        for batch, schema in cases:
            assert batch.schema.equals(schema), schema

    def test_dim_ids_continue_from_the_offset(self):
        rng = np.random.default_rng(5)
        vocab = build_vocabulary(5)
        batch = T.dim_users(rng, vocab, 10, id_offset=100, anchor=ANCHOR)
        assert batch.column("user_id").to_pylist() == list(range(101, 111))

    def test_facts_reference_live_dim_id_ranges(self):
        rng = np.random.default_rng(5)
        lo, hi = T.history_window(ANCHOR)
        batch = T.fact_orders(
            rng, 2000, id_offset=0, user_count=7, product_count=3, lo_us=lo, hi_us=hi
        )
        assert max(batch.column("user_id").to_pylist()) <= 7
        assert max(batch.column("product_id").to_pylist()) <= 3
        assert min(batch.column("product_id").to_pylist()) >= 1


class TestPartitionWindows:
    def test_windows_are_contiguous_calendar_months(self):
        windows = T.month_windows(ANCHOR, T.EVENT_MONTHS)
        assert len(windows) == T.EVENT_MONTHS
        for (_, end), (start, _) in itertools.pairwise(windows):
            assert end == start
        assert windows[-1][1] == int(ANCHOR.timestamp() * T.MICROS)

    def test_each_window_is_one_iceberg_month_partition(self):
        """The whole point of the grid: every row of a window transforms
        to the same month() partition value, so a batch fans out to one
        file per (team, month) cell."""
        for lo, hi in T.month_windows(ANCHOR, T.EVENT_MONTHS):
            first = datetime.fromtimestamp(lo / T.MICROS, UTC)
            last = datetime.fromtimestamp((hi - 1) / T.MICROS, UTC)
            assert iceberg_month(first) == iceberg_month(last)

    def test_weights_line_up_with_the_grid(self):
        assert len(T.TEAM_WEIGHTS) == len(T.TEAM_IDS)
        assert len(T.MONTH_WEIGHTS) == T.EVENT_MONTHS
        assert sum(T.TEAM_WEIGHTS) == pytest.approx(1.0)
        assert sum(T.MONTH_WEIGHTS) == pytest.approx(1.0)


class TestSeedCli:
    def test_seed_is_not_a_scenario(self):
        # `all` must never seed a warehouse as a side effect
        assert "seed" not in SCENARIOS

    def test_defaults_parse(self):
        args = build_parser().parse_args(["seed"])
        assert args.scenario == "seed"
        assert args.gb == 1.0
        assert args.catalog.startswith("seed-")
        assert args.url.startswith("http")

    def test_server_is_an_alias_for_url(self):
        args = build_parser().parse_args(["seed", "--server", "http://elsewhere:9"])
        assert args.url == "http://elsewhere:9"

    def test_fractional_gb_accepted(self):
        assert build_parser().parse_args(["seed", "--gb", "2.5"]).gb == 2.5

    def test_below_floor_is_a_usage_error(self):
        with pytest.raises(SystemExit):
            build_parser().parse_args(["seed", "--gb", "0.05"])
