import math

import pytest

from hoglake_bench.stats import Metric, Recorder, pearson, percentile


class TestPercentile:
    def test_single_value(self):
        assert percentile([42], 50) == 42.0
        assert percentile([42], 99) == 42.0

    def test_empty_raises(self):
        with pytest.raises(ValueError):
            percentile([], 50)

    def test_median_of_even_list_interpolates(self):
        assert percentile([10, 20], 50) == 15.0

    def test_endpoints(self):
        vals = list(range(1, 101))
        assert percentile(vals, 0) == 1.0
        assert percentile(vals, 100) == 100.0

    def test_p99_of_100(self):
        vals = list(range(1, 101))  # 1..100
        assert percentile(vals, 99) == pytest.approx(99.01)

    def test_p50_odd(self):
        assert percentile([1, 2, 3, 4, 5], 50) == 3.0


class TestPearson:
    def test_perfect_positive(self):
        assert pearson([1, 2, 3], [10, 20, 30]) == pytest.approx(1.0)

    def test_perfect_negative(self):
        assert pearson([1, 2, 3], [3, 2, 1]) == pytest.approx(-1.0)

    def test_zero_variance_is_none(self):
        assert pearson([1, 1, 1], [1, 2, 3]) is None

    def test_too_short_is_none(self):
        assert pearson([1], [1]) is None

    def test_uncorrelated(self):
        r = pearson([1, 2, 3, 4], [1, -1, 1, -1])
        assert r is not None and abs(r) < 0.5

    def test_length_mismatch(self):
        with pytest.raises(ValueError):
            pearson([1, 2], [1])


class TestRecorder:
    def test_measure_records_positive_ns(self):
        r = Recorder()
        with r.measure():
            math.sqrt(2.0)
        assert r.count == 1
        assert r.samples_ns[0] > 0

    def test_merge(self):
        a, b = Recorder(), Recorder()
        a.record_ns(1)
        b.record_ns(2)
        a.merge(b)
        assert sorted(a.samples_ns) == [1, 2]

    def test_percentiles_ms(self):
        r = Recorder()
        for ms in (1, 2, 3, 4, 100):
            r.record_ns(ms * 1_000_000)
        p = r.percentiles_ms()
        assert p["p50_ms"] == 3.0
        assert p["max_ms"] == 100.0
        assert p["p95_ms"] <= p["p99_ms"] <= p["max_ms"]


class TestMetric:
    def test_rate(self):
        m = Metric(name="x", ops=100, wall_s=2.0)
        assert m.rate_s == 50.0

    def test_zero_wall_rate(self):
        assert Metric(name="x", ops=5, wall_s=0.0).rate_s == 0.0

    def test_line_contains_all_fields(self):
        r = Recorder()
        for ms in (10, 20, 30):
            r.record_ns(ms * 1_000_000)
        m = Metric.from_recorder("commit.fpc1", r, wall_s=0.06, files_s=50.0)
        line = m.line()
        for token in ("commit.fpc1", "ops=3", "rate_s=50", "p50=20", "p99=", "max=30", "files_s=50"):
            assert token in line, (token, line)

    def test_to_json_roundtrippable(self):
        r = Recorder()
        r.record_ns(5_000_000)
        m = Metric.from_recorder("m", r, wall_s=1.0, conflicts=0)
        d = m.to_json()
        assert d["name"] == "m"
        assert d["ops"] == 1
        assert d["p50_ms"] == 5.0
        assert d["conflicts"] == 0

    def test_metric_without_latency(self):
        m = Metric(name="ratio", ops=0, wall_s=0.0, extra={"ratio": 1.2})
        assert "p50" not in m.line()
        assert "ratio=1.20" in m.line()
