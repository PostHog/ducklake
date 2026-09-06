"""Drive the commit-throughput scenario end to end against an in-memory
fake of the pyhoglake surface it uses — proves the measurement loop,
invariant checks, ratio math, and results plumbing without a server."""

import json

import pytest

from hoglake_bench.cli import _namespace_for, build_parser
from hoglake_bench.context import Bench, BenchConfig
from hoglake_bench.runner import InvariantViolation
from hoglake_bench.scenarios import commit_throughput


class FakeInfo:
    def __init__(self, head):
        self.head_snapshot_id = head


class FakeColumn:
    def __init__(self, name, field_id):
        self.name = name
        self.field_id = field_id


class FakeTableInfo:
    def __init__(self, record_count):
        self.record_count = record_count


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
        return FakeTableInfo(sum(f.record_count for f in self._files))

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
    def __init__(self, name, *, lose_a_commit=False):
        self.name = name
        self.data_path = f"s3://fake/{name}/"
        self.head = 0
        self.tables = {}
        self.lose_a_commit = lose_a_commit
        self._commits = 0

    def create_namespace(self, name):
        self.head += 1
        return FakeNamespace(self, name)

    def _commit(self, payload):
        self._commits += 1
        if self.lose_a_commit and self._commits == 5:
            # simulate a snapshot-id gap: commit accepted, no snapshot row
            class R:
                snapshot_id = self.head

            return R()
        self.head += 1
        for appended in payload.get("appends", []):
            table = self.tables[(appended["namespace"], appended["table"])]
            total = sum(f.record_count for f in table._files)
            for f in appended["files"]:
                table._files.append(FakeFile(f["record_count"], total))
                total += f["record_count"]

        class R:
            snapshot_id = self.head

        return R()

    def refresh(self):
        return FakeInfo(self.head)


class FakeBench:
    def __init__(self, tmp_path, lose_a_commit=False):
        self.cfg = BenchConfig(results_path=str(tmp_path / "r.jsonl"))
        self.lose_a_commit = lose_a_commit

    def new_catalog(self, slug):
        return FakeCatalog(f"bench-{slug}", lose_a_commit=self.lose_a_commit)


def _args(**overrides):
    ns = _namespace_for(
        "commit-throughput",
        build_parser().parse_args(["commit-throughput"]),
        {
            "preseed_snapshots": [0, 50],
            "files_per_commit": [1, 10],
            "ops": 25,  # >= MIN_GUARDED_SAMPLES
            "warmup": 1,
            "tables": 0,  # the wide stage has its own tests
            **overrides,
        },
    )
    return ns


class TestCommitThroughputLoop:
    def test_runs_and_reports(self, tmp_path):
        bench = FakeBench(tmp_path)
        report = commit_throughput.run(bench, _args())
        names = [m.name for m in report.metrics]
        assert "commit.preseed0.fpc1" in names
        assert "commit.preseed50.fpc10" in names
        assert "preseed50(n=50)" in names
        assert any(n.startswith("commit.scaling.fpc") for n in names)
        for m in report.metrics:
            if m.name.startswith("commit.preseed"):
                assert m.ops == 25  # warmup excluded
                assert m.p50_ms is not None

    def test_ratio_metric_present_and_sane(self, tmp_path):
        report = commit_throughput.run(FakeBench(tmp_path), _args())
        scaling = [m for m in report.metrics if m.name == "commit.scaling.fpc1"]
        assert len(scaling) == 1
        assert scaling[0].extra["ratio"] >= 0.0

    def test_dense_snapshot_invariant_catches_gaps(self, tmp_path):
        bench = FakeBench(tmp_path, lose_a_commit=True)
        with pytest.raises(InvariantViolation, match="dense"):
            commit_throughput.run(bench, _args())


class TestResultsJsonl:
    def test_append_result_writes_record(self, tmp_path):
        cfg = BenchConfig(results_path=str(tmp_path / "out.jsonl"))
        bench = Bench(cfg)
        try:
            report_metrics = []
            from hoglake_bench.stats import Metric

            report_metrics.append(Metric(name="m1", ops=3, wall_s=1.5))
            bench.append_result("unit-test", {"x": 1}, report_metrics)
            bench.append_result("unit-test", {"x": 2}, report_metrics)
        finally:
            bench.close()
        lines = (tmp_path / "out.jsonl").read_text().strip().splitlines()
        assert len(lines) == 2
        rec = json.loads(lines[0])
        assert rec["scenario"] == "unit-test"
        assert rec["params"] == {"x": 1}
        assert rec["metrics"][0]["name"] == "m1"
        assert rec["metrics"][0]["rate_s"] == 2.0
        assert rec["ts"] > 1_700_000_000
        assert rec["run_id"] == cfg.run_id
