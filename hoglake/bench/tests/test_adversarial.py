"""Adversarial coverage: ways the harness could report false confidence.

Two kinds of tests live here:

- Plain fault-injection coverage for invariant checkers: a checker that
  has never seen a seeded fault is itself unverified.
- Regression tests for the bugs found by the adversarial review (all
  fixed): vacuous zero-sample ratios, warm-cache asymmetry hiding
  seeded O(catalog) regressions, warmup bytes inflating arrow_mb_s,
  the unbounded hotfile retry loop, journal param gaps, and exit-code
  masking across `all`.
"""

from __future__ import annotations

import argparse
import json
import threading
import time
from types import SimpleNamespace

import pytest

from hoglake_bench.cli import _namespace_for, build_parser
from hoglake_bench.runner import FailureGuard, InsufficientSamples, InvariantViolation
from hoglake_bench.scenarios import commit_throughput, delete_contention, end_to_end
from hoglake_bench.scenarios.common import (
    ScenarioReport,
    assert_dense_snapshots,
    assert_row_tiling,
)
from hoglake_bench.stats import pearson

# ---------------------------------------------------------------------------
# Fakes (same surface contract as tests/test_scenario_loop.py)
# ---------------------------------------------------------------------------


class FakeColumn:
    def __init__(self, name, field_id):
        self.name = name
        self.field_id = field_id


class FakeFile:
    def __init__(self, record_count, row_id_start):
        self.record_count = record_count
        self.row_id_start = row_id_start


class FakeTable:
    def __init__(self, catalog, namespace, name):
        self._catalog = catalog
        self.namespace = namespace
        self.name = name
        self.columns = [FakeColumn("id", 1), FakeColumn("v", 2)]
        self._files = []

    def info(self):
        return SimpleNamespace(
            record_count=sum(f.record_count for f in self._files)
        )

    def files(self):
        return list(self._files)


class FakeNamespace:
    def __init__(self, catalog, name):
        self._catalog = catalog
        self.name = name

    def create_table(self, name, schema):
        self._catalog.head += 1
        t = FakeTable(self._catalog, self.name, name)
        self._catalog.tables[(self.name, name)] = t
        return t


class FakeCatalog:
    """In-memory commit surface with optional latency modelling.

    ``cold_commits`` commits pay ``cold_penalty_s`` extra (cold-cache
    model); every commit pays ``per_snapshot_tax_s * head`` (a seeded
    O(catalog) regression).
    """

    def __init__(
        self,
        name,
        *,
        base_latency_s=0.0,
        cold_commits=0,
        cold_penalty_s=0.0,
        per_snapshot_tax_s=0.0,
    ):
        self.name = name
        self.data_path = f"s3://fake/{name}/"
        self.head = 0
        self.tables = {}
        self._commits = 0
        self.base_latency_s = base_latency_s
        self.cold_commits = cold_commits
        self.cold_penalty_s = cold_penalty_s
        self.per_snapshot_tax_s = per_snapshot_tax_s

    def create_namespace(self, name):
        self.head += 1
        return FakeNamespace(self, name)

    def _commit(self, payload):
        delay = self.base_latency_s + self.per_snapshot_tax_s * self.head
        if self._commits < self.cold_commits:
            delay += self.cold_penalty_s
        self._commits += 1
        if delay:
            time.sleep(delay)
        self.head += 1
        for appended in payload.get("appends", []):
            table = self.tables[(appended["namespace"], appended["table"])]
            total = sum(f.record_count for f in table._files)
            for f in appended["files"]:
                table._files.append(FakeFile(f["record_count"], total))
                total += f["record_count"]
        return SimpleNamespace(snapshot_id=self.head)

    def refresh(self):
        return SimpleNamespace(head_snapshot_id=self.head)


class FakeBench:
    def __init__(self, tmp_path, **catalog_kwargs):
        from hoglake_bench.context import BenchConfig

        self.cfg = BenchConfig(results_path=str(tmp_path / "r.jsonl"))
        self._catalog_kwargs = catalog_kwargs

    def new_catalog(self, slug):
        return FakeCatalog(f"bench-{slug}", **self._catalog_kwargs)

    def ensure_bucket(self):
        pass


def _ct_args(**overrides):
    return _namespace_for(
        "commit-throughput",
        build_parser().parse_args(["commit-throughput"]),
        {
            "preseed_snapshots": [0, 50],
            "files_per_commit": [1],
            "ops": 25,  # >= MIN_GUARDED_SAMPLES
            "warmup": 1,
            "tables": 0,  # wide stage exercised by its own tests below
            **overrides,
        },
    )


# ---------------------------------------------------------------------------
# Fault-injection coverage for invariant checkers
# ---------------------------------------------------------------------------


def _tiling_table(specs):
    t = SimpleNamespace(name="t")
    t.files = lambda: [
        SimpleNamespace(row_id_start=s, record_count=c, path=f"f{i}")
        for i, (s, c) in enumerate(specs)
    ]
    return t


class TestRowTilingChecker:
    def test_clean_tiling_passes(self):
        assert_row_tiling(_tiling_table([(0, 10), (10, 5), (15, 1)]), expected_rows=16)

    def test_gap_detected(self):
        with pytest.raises(InvariantViolation, match="tiling"):
            assert_row_tiling(_tiling_table([(0, 10), (15, 5)]))

    def test_overlap_detected(self):
        with pytest.raises(InvariantViolation, match="tiling"):
            assert_row_tiling(_tiling_table([(0, 10), (5, 10)]))

    def test_total_mismatch_detected(self):
        with pytest.raises(InvariantViolation, match="cover"):
            assert_row_tiling(_tiling_table([(0, 10)]), expected_rows=11)

    def test_lost_first_file_detected(self):
        # tiling must anchor at 0, not just be contiguous
        with pytest.raises(InvariantViolation, match="tiling"):
            assert_row_tiling(_tiling_table([(5, 10), (15, 5)]))


class _SnapCatalog:
    def __init__(self, ids):
        self._ids = ids

    def snapshots(self, after=0):
        for i in self._ids:
            if i > after:
                yield SimpleNamespace(snapshot_id=i)


class TestDenseSnapshotsChecker:
    def test_dense_passes(self):
        assert_dense_snapshots(_SnapCatalog([1, 2, 3, 4, 5]), after=2, expected_new=3)

    def test_gap_detected(self):
        with pytest.raises(InvariantViolation, match="dense"):
            assert_dense_snapshots(_SnapCatalog([3, 4, 6]), after=2, expected_new=3)

    def test_surplus_snapshot_detected(self):
        with pytest.raises(InvariantViolation, match="dense"):
            assert_dense_snapshots(_SnapCatalog([3, 4, 5, 6]), after=2, expected_new=3)

    def test_missing_tail_detected(self):
        with pytest.raises(InvariantViolation, match="dense"):
            assert_dense_snapshots(_SnapCatalog([3, 4]), after=2, expected_new=3)


class TestPearsonDegeneracy:
    def test_two_point_series_is_always_unit_correlation(self):
        """n=2 Pearson is ±1 by construction — any 'correlation' computed
        from two stages carries zero information. Documents why a 2-stage
        catalog_latency_corr field reads 1.0."""
        assert pearson([500.0, 2000.0], [1.5, 2.0]) == pytest.approx(1.0)
        assert pearson([500.0, 2000.0], [2.0, 1.5]) == pytest.approx(-1.0)


# ---------------------------------------------------------------------------
# Regressions for the review's confirmed bugs (all fixed)
# ---------------------------------------------------------------------------


class TestVacuousRatioGuard:
    """A guarded ratio must never compute from zero/insufficient samples."""

    def test_zero_sample_stage_aborts_loudly(self, tmp_path):
        # --duration too small to measure a single op: the old harness
        # emitted ratio=0.0 and exited 0; now it must abort (exit 3)
        with pytest.raises(InsufficientSamples, match="insufficient samples"):
            commit_throughput.run(
                FakeBench(tmp_path), _ct_args(warmup=5, ops=5, duration=1e-9)
            )

    def test_sub_floor_ops_abort(self, tmp_path):
        # even with no duration cap, fewer measured ops than the floor
        # cannot produce a trustworthy ratio
        with pytest.raises(InsufficientSamples, match="insufficient samples"):
            commit_throughput.run(FakeBench(tmp_path), _ct_args(ops=5))


class TestWarmCacheThermalEqualization:
    """Both sides of the preseed ratio get the same fixed warm phase, so
    cold-cache inflation of the baseline can no longer hide a real
    O(catalog) regression."""

    def test_seeded_ocatalog_regression_flags(self, tmp_path):
        bench = FakeBench(
            tmp_path,
            base_latency_s=0.002,
            cold_commits=40,           # cold-cache inflation of a fresh catalog
            cold_penalty_s=0.0015,     # ~1.75x on cold commits
            per_snapshot_tax_s=10e-6,  # seeded regression: +1ms per 100 snaps
        )
        # true warm regression at preseed=300: (2ms + 3ms) / 2ms = 2.5x
        report = commit_throughput.run(
            bench,
            _ct_args(preseed_snapshots=[0, 300], ops=20, warmup=2),
        )
        assert report.flags, (
            "a seeded 2.5x O(catalog) commit-latency regression was not "
            "flagged"
        )
        scaling = next(
            m for m in report.metrics if m.name == "commit.scaling.fpc1"
        )
        assert scaling.extra["ratio"] > 1.5

    def test_cold_cache_alone_does_not_flag(self, tmp_path):
        # same cold-cache model, NO seeded regression: the equalized warm
        # phase must absorb the thermal asymmetry on both sides
        bench = FakeBench(
            tmp_path,
            base_latency_s=0.002,
            cold_commits=40,
            cold_penalty_s=0.0015,
        )
        report = commit_throughput.run(
            bench,
            _ct_args(preseed_snapshots=[0, 300], ops=20, warmup=2),
        )
        assert not report.flags, f"unseeded run flagged: {report.flags}"


class TestWideCatalogGuard:
    """The wide-catalog (N live tables) stage is wired into the same
    ratio guard as the preseed stages."""

    def test_seeded_wide_table_tax_flags(self, tmp_path):
        bench = FakeBench(
            tmp_path, base_latency_s=0.002, per_snapshot_tax_s=15e-6
        )
        report = commit_throughput.run(
            bench,
            _ct_args(preseed_snapshots=[0], tables=200, ops=20, warmup=2),
        )
        wide = [
            m for m in report.metrics if m.name.startswith("commit.scaling.wide")
        ]
        assert wide and wide[0].extra["ratio"] > 1.5
        assert report.flags

    def test_clean_wide_catalog_does_not_flag(self, tmp_path):
        report = commit_throughput.run(
            FakeBench(tmp_path, base_latency_s=0.002),
            _ct_args(preseed_snapshots=[0], tables=200, ops=20, warmup=2),
        )
        assert not report.flags, f"clean wide run flagged: {report.flags}"
        assert any(m.name.startswith("wide.create") for m in report.metrics)


class TestE2EThroughputAccounting:
    """arrow_mb_s numerator and denominator cover the SAME measured
    window — warmup bytes stay out of both."""

    def test_arrow_mb_s_counts_only_measured_bytes(self, tmp_path):
        class T:
            def __init__(self):
                self.rows = 0
                self._files = []
                self.name = "events"
                self.namespace = "bench"

            def append(self, data, author=None, message=None):
                time.sleep(0.002)
                self._files.append(
                    SimpleNamespace(
                        stats_state="provided",
                        row_id_start=self.rows,
                        record_count=data.num_rows,
                        path=f"f{len(self._files)}",
                    )
                )
                self.rows += data.num_rows

            def info(self):
                return SimpleNamespace(
                    record_count=self.rows, file_count=len(self._files)
                )

            def files(self):
                return list(self._files)

        table = T()
        catalog = SimpleNamespace(
            create_namespace=lambda name: SimpleNamespace(
                create_table=lambda n, schema: table
            )
        )
        bench = SimpleNamespace(
            ensure_bucket=lambda: None, new_catalog=lambda slug: catalog
        )
        args = argparse.Namespace(
            rows=20_000, batch_rows=5_000, duration=None, url="http://x"
        )
        report = end_to_end.run(bench, args)
        m = next(m for m in report.metrics if m.name == "append.real_parquet")
        counted_bytes = m.extra["arrow_mb_s"] * 1e6 * m.wall_s
        measured_bytes = sum(
            end_to_end._batch(i * 5_000, 5_000).nbytes for i in range(1, 5)
        )
        assert counted_bytes == pytest.approx(measured_bytes, rel=0.05), (
            "arrow_mb_s numerator includes the warmup batch"
        )
        assert report.params["batches"] == 4
        assert report.params["warmup_batches"] == 1


class TestHotfileRetryLoop:
    """The hotfile retry loop is bounded: --duration deadline and a
    per-op retry cap are consulted INSIDE op(); abandoned ops are
    counted separately and never contaminate the latency stats."""

    def test_hotfile_retry_respects_duration_cap(self, tmp_path):
        from pyhoglake import CommitConflictError

        lock = threading.Lock()
        state = {"head": 1, "count": 0}
        unlock_at = time.monotonic() + 1.0  # conflicts for a full second

        class Cat:
            def refresh(self):
                return SimpleNamespace(head_snapshot_id=state["head"])

            def _commit(self, payload):
                with lock:
                    if time.monotonic() < unlock_at:
                        time.sleep(0.0005)
                        raise CommitConflictError("hot", status_code=409)
                    want = payload["deletes"][0]["files"][0]["delete_count"]
                    if (
                        payload["read_snapshot"] != state["head"]
                        or want != state["count"] + 1
                    ):
                        raise CommitConflictError("superseded", status_code=409)
                    state["count"] += 1
                    state["head"] += 1
                    return SimpleNamespace(snapshot_id=state["head"])

        class Tab:
            namespace = "bench"
            name = "t"

            def scan_plan(self):
                df = SimpleNamespace(data_file_id=1)
                dv = (
                    SimpleNamespace(delete_count=state["count"])
                    if state["count"]
                    else None
                )
                return [SimpleNamespace(data_file=df, delete_file=dv)]

        cat = Cat()
        cat.data_path = "s3://fake/dc/"
        args = argparse.Namespace(writers=2, hotfile_ops=2, duration=0.1)
        report = ScenarioReport(scenario="delete-contention", params={})
        t0 = time.monotonic()
        delete_contention._hotfile(
            None, args, report, cat, Tab(), 1, FailureGuard()
        )
        elapsed = time.monotonic() - t0
        assert elapsed < 0.5, (
            f"duration cap 0.1s, but the retry loop hammered for {elapsed:.2f}s"
        )
        m = next(m for m in report.metrics if m.name == "dv.hotfile.k2")
        assert m.extra["abandoned"] >= 1
        assert m.extra["successes"] == 0


class TestJournalParamCapture:
    """Scenario params journal the full effective config: a
    duration-capped run must be distinguishable in the JSONL."""

    def test_effective_config_is_journaled(self, tmp_path):
        report = commit_throughput.run(
            FakeBench(tmp_path), _ct_args(duration=30.0)
        )
        assert report.params["duration"] == 30.0
        assert "warmup" in report.params
        assert "url" in report.params


class TestExitCodeMasking:
    """In `all` mode a regression flag survives a later scenario's abort
    (severity precedence 4 > 2 > 5 > 3), and the aborted scenario still
    reaches the JSONL journal with a status field."""

    def test_regression_exit_survives_later_abort(self, tmp_path, monkeypatch):
        from hoglake_bench import cli
        from hoglake_bench.runner import BenchAbort

        def _flagging_run(bench, args):
            rep = ScenarioReport(scenario="a", params={})
            rep.flags.append("REGRESSION a: ratio grew 9.99x")
            return rep

        def _aborting_run(bench, args):
            raise BenchAbort("10 consecutive failures")

        mod_a = SimpleNamespace(
            __doc__="fake scenario a", add_args=lambda p: None, run=_flagging_run
        )
        mod_b = SimpleNamespace(
            __doc__="fake scenario b", add_args=lambda p: None, run=_aborting_run
        )
        monkeypatch.setattr(cli, "SCENARIOS", {"a": mod_a, "b": mod_b})
        monkeypatch.setattr(cli, "QUICK_PROFILE", {"a": {}, "b": {}})
        monkeypatch.setattr(cli, "FULL_PROFILE", {"a": {}, "b": {}})
        monkeypatch.setattr(cli, "check_server", lambda url: None)
        results = tmp_path / "r.jsonl"
        rc = cli.main(["all", "--quick", "--results", str(results)])
        assert rc == 2, f"regression flag masked: exit {rc}, expected 2"

        recs = {
            r["scenario"]: r
            for r in map(json.loads, results.read_text().splitlines())
        }
        assert recs["a"]["status"] == "regression"
        assert recs["a"]["flags"]
        assert recs["b"]["status"] == "aborted"
        assert "consecutive failures" in recs["b"]["error"]

    def test_invariant_violation_outranks_regression(self, tmp_path, monkeypatch):
        from hoglake_bench import cli

        def _flagging_run(bench, args):
            rep = ScenarioReport(scenario="a", params={})
            rep.flags.append("REGRESSION a")
            return rep

        def _violating_run(bench, args):
            raise InvariantViolation("rows lost")

        mod_a = SimpleNamespace(
            __doc__="fake a", add_args=lambda p: None, run=_flagging_run
        )
        mod_b = SimpleNamespace(
            __doc__="fake b", add_args=lambda p: None, run=_violating_run
        )
        monkeypatch.setattr(cli, "SCENARIOS", {"a": mod_a, "b": mod_b})
        monkeypatch.setattr(cli, "QUICK_PROFILE", {"a": {}, "b": {}})
        monkeypatch.setattr(cli, "FULL_PROFILE", {"a": {}, "b": {}})
        monkeypatch.setattr(cli, "check_server", lambda url: None)
        rc = cli.main(["all", "--quick", "--results", str(tmp_path / "r.jsonl")])
        assert rc == 4

    def test_unexpected_exception_gets_own_code(self, tmp_path, monkeypatch):
        from hoglake_bench import cli

        def _crashing_run(bench, args):
            raise KeyError("harness bug")

        mod = SimpleNamespace(
            __doc__="fake", add_args=lambda p: None, run=_crashing_run
        )
        monkeypatch.setattr(cli, "SCENARIOS", {"a": mod})
        monkeypatch.setattr(cli, "QUICK_PROFILE", {"a": {}})
        monkeypatch.setattr(cli, "FULL_PROFILE", {"a": {}})
        monkeypatch.setattr(cli, "check_server", lambda url: None)
        results = tmp_path / "r.jsonl"
        rc = cli.main(["all", "--quick", "--results", str(results)])
        assert rc == 5
        rec = json.loads(results.read_text().splitlines()[0])
        assert rec["status"] == "error"
        assert "KeyError" in rec["error"]
