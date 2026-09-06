"""Loop drivers: sequential and threaded op loops with warmup exclusion,
ops/duration caps, and an unresponsive-server bail-out."""

from __future__ import annotations

import threading
import time
from collections.abc import Callable
from dataclasses import dataclass

import httpx
from pyhoglake import HoglakeError

from .stats import Recorder

MAX_CONSECUTIVE_FAILURES = 10


class BenchAbort(RuntimeError):
    """The run cannot produce trustworthy numbers and refuses to go on
    (unresponsive server, insufficient samples, ...). Exit code 3."""


class InsufficientSamples(BenchAbort):
    """A guarded stage measured too few ops to compute a trustworthy
    ratio/headline. Aborting loudly beats emitting a vacuous 0.0."""


class InvariantViolation(AssertionError):
    """A correctness check failed after a load phase. The numbers above
    the raise are not trustworthy."""


class OpDiscarded(Exception):
    """Raised by an op to drop the current iteration from the latency
    recorder — e.g. a conflicted or abandoned attempt whose latency is
    tracked in a separate, clearly-labeled recorder. The op index still
    advances; the sample never contaminates the success percentiles."""


class FailureGuard:
    """Counts consecutive transport/server failures across a scenario;
    trips after MAX_CONSECUTIVE_FAILURES. Thread-safe."""

    def __init__(self, limit: int = MAX_CONSECUTIVE_FAILURES) -> None:
        self.limit = limit
        self._consecutive = 0
        self._lock = threading.Lock()

    def success(self) -> None:
        with self._lock:
            self._consecutive = 0

    def failure(self, exc: BaseException) -> None:
        with self._lock:
            self._consecutive += 1
            n = self._consecutive
        if n >= self.limit:
            raise BenchAbort(
                f"{n} consecutive failures talking to the server "
                f"(last: {type(exc).__name__}: {exc}); the server looks "
                "unresponsive — aborting instead of hammering it"
            ) from exc

    @staticmethod
    def is_server_failure(exc: BaseException) -> bool:
        """Transport errors and 5xx are 'server down' signals; 4xx are
        bench bugs or expected conflicts and must be handled by the
        scenario, not swallowed here."""
        if isinstance(exc, httpx.HTTPError):
            return True
        if isinstance(exc, HoglakeError):
            return exc.status_code is None or exc.status_code >= 500
        return False


@dataclass
class LoopResult:
    recorder: Recorder
    warmup: Recorder
    wall_s: float
    errors: int = 0
    discarded: int = 0


def run_loop(
    op: Callable[[int], None],
    *,
    ops: int,
    warmup: int = 0,
    duration_s: float | None = None,
    guard: FailureGuard | None = None,
    stop: threading.Event | None = None,
) -> LoopResult:
    """Run ``op(i)`` up to ``ops`` measured times (after ``warmup``
    unmeasured-for-stats iterations), stopping early when ``duration_s``
    elapses or ``stop`` is set. Failures the guard classifies as
    server-side are counted and retried-forward (the op index still
    advances); anything else propagates."""
    guard = guard or FailureGuard()
    measured = Recorder()
    warm = Recorder()
    errors = 0
    discarded = 0
    deadline = None
    if duration_s is not None:
        deadline = time.monotonic() + duration_s
    wall_t0 = None
    i = 0
    total = warmup + ops
    while i < total:
        if stop is not None and stop.is_set():
            break
        if deadline is not None and time.monotonic() > deadline:
            break
        recorder = warm if i < warmup else measured
        if i == warmup:
            wall_t0 = time.perf_counter_ns()
        t0 = time.perf_counter_ns()
        try:
            op(i)
        except OpDiscarded:
            # the op handled (and separately recorded) this attempt;
            # the server answered, so the failure streak resets
            guard.success()
            discarded += 1
            i += 1
            continue
        except BaseException as exc:
            if guard.is_server_failure(exc):
                guard.failure(exc)
                errors += 1
                i += 1
                continue
            raise
        guard.success()
        recorder.record_ns(time.perf_counter_ns() - t0)
        i += 1
    wall_s = 0.0
    if wall_t0 is not None:
        wall_s = (time.perf_counter_ns() - wall_t0) / 1e9
    return LoopResult(
        recorder=measured,
        warmup=warm,
        wall_s=wall_s,
        errors=errors,
        discarded=discarded,
    )


@dataclass
class WorkerResult:
    index: int
    loop: LoopResult | None = None
    error: BaseException | None = None


def run_threads(
    n: int,
    make_worker: Callable[[int], Callable[[], LoopResult]],
) -> tuple[list[WorkerResult], float]:
    """Start ``n`` workers (each produced by ``make_worker(i)``) on a
    barrier, join them, and return per-worker results plus the wall time
    of the whole race. First worker exception is re-raised after all
    threads join, unless it is a BenchAbort (then workers were told to
    stop and the abort propagates)."""
    results = [WorkerResult(index=i) for i in range(n)]
    barrier = threading.Barrier(n + 1)

    def _run(i: int, fn: Callable[[], LoopResult]) -> None:
        try:
            barrier.wait()
            results[i].loop = fn()
        except BaseException as exc:  # noqa: BLE001 - reported to caller
            results[i].error = exc

    threads = []
    for i in range(n):
        t = threading.Thread(target=_run, args=(i, make_worker(i)), daemon=True)
        t.start()
        threads.append(t)
    barrier.wait()
    t0 = time.perf_counter_ns()
    for t in threads:
        t.join()
    wall_s = (time.perf_counter_ns() - t0) / 1e9
    for r in results:
        if r.error is not None:
            raise r.error
    return results, wall_s
