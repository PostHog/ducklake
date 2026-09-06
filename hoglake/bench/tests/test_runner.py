import time

import httpx
import pytest

from hoglake_bench.runner import (
    BenchAbort,
    FailureGuard,
    OpDiscarded,
    run_loop,
    run_threads,
)


class TestFailureGuard:
    def test_trips_after_limit(self):
        g = FailureGuard(limit=3)
        exc = httpx.ConnectError("boom")
        g.failure(exc)
        g.failure(exc)
        with pytest.raises(BenchAbort, match="unresponsive"):
            g.failure(exc)

    def test_success_resets(self):
        g = FailureGuard(limit=2)
        exc = httpx.ConnectError("boom")
        for _ in range(5):
            g.failure(exc)
            g.success()

    def test_classification(self):
        from pyhoglake import HoglakeError, NotFoundError

        assert FailureGuard.is_server_failure(httpx.ConnectTimeout("t"))
        assert FailureGuard.is_server_failure(
            HoglakeError("ise", status_code=500)
        )
        assert not FailureGuard.is_server_failure(
            NotFoundError("nope", status_code=404)
        )
        assert not FailureGuard.is_server_failure(ValueError("bug"))


class TestRunLoop:
    def test_warmup_excluded_from_stats(self):
        seen = []
        result = run_loop(seen.append, ops=5, warmup=2)
        assert seen == [0, 1, 2, 3, 4, 5, 6]
        assert result.recorder.count == 5
        assert result.warmup.count == 2

    def test_duration_cap(self):
        def slow(_):
            time.sleep(0.02)

        result = run_loop(slow, ops=1000, warmup=0, duration_s=0.1)
        assert result.recorder.count < 1000

    def test_server_failures_counted_and_bail(self):
        def always_down(_):
            raise httpx.ConnectError("down")

        with pytest.raises(BenchAbort):
            run_loop(always_down, ops=100, warmup=0)

    def test_non_server_error_propagates(self):
        def bug(_):
            raise KeyError("bench bug")

        with pytest.raises(KeyError):
            run_loop(bug, ops=3, warmup=0)

    def test_discarded_ops_excluded_from_stats(self):
        def op(i):
            if i % 2:
                raise OpDiscarded

        result = run_loop(op, ops=6, warmup=0)
        assert result.recorder.count == 3
        assert result.discarded == 3
        assert result.errors == 0

    def test_discarded_op_resets_failure_streak(self):
        g = FailureGuard(limit=3)
        calls = {"n": 0}

        def op(_):
            calls["n"] += 1
            if calls["n"] % 3 == 0:
                raise OpDiscarded  # server answered; streak resets
            raise httpx.ReadTimeout("blip")

        result = run_loop(op, ops=30, warmup=0, guard=g)
        assert result.discarded == 10
        assert result.errors == 20  # never 3 consecutive -> no BenchAbort

    def test_intermittent_failures_recovered(self):
        calls = {"n": 0}

        def flaky(_):
            calls["n"] += 1
            if calls["n"] % 3 == 0:
                raise httpx.ReadTimeout("blip")

        result = run_loop(flaky, ops=9, warmup=0)
        assert result.errors == 3
        assert result.recorder.count == 6


class TestRunThreads:
    def test_all_workers_run_and_merge(self):
        def make_worker(i):
            def worker():
                return run_loop(lambda _: None, ops=10, warmup=1)

            return worker

        results, wall_s = run_threads(4, make_worker)
        assert len(results) == 4
        assert all(r.loop and r.loop.recorder.count == 10 for r in results)
        assert wall_s > 0

    def test_worker_exception_reraised(self):
        def make_worker(i):
            def worker():
                if i == 2:
                    raise RuntimeError("worker 2 died")
                return run_loop(lambda _: None, ops=1, warmup=0)

            return worker

        with pytest.raises(RuntimeError, match="worker 2"):
            run_threads(3, make_worker)
