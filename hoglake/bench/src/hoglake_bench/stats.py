"""Latency capture and percentile summaries.

Hand-rolled on purpose: ``time.perf_counter_ns`` per operation, sorted
lists for percentiles. No HDR histogram dependency — at bench op counts
(<= a few hundred thousand) exact sorted-list percentiles are cheaper
than the machinery to approximate them.
"""

from __future__ import annotations

import math
import time
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass, field
from typing import Any

NS_PER_MS = 1_000_000


def percentile(sorted_ns: list[int], q: float) -> float:
    """Linear-interpolated percentile (q in [0, 100]) of a pre-sorted list."""
    if not sorted_ns:
        raise ValueError("percentile of empty list")
    if len(sorted_ns) == 1:
        return float(sorted_ns[0])
    rank = (q / 100.0) * (len(sorted_ns) - 1)
    lo = math.floor(rank)
    hi = math.ceil(rank)
    if lo == hi:
        return float(sorted_ns[lo])
    frac = rank - lo
    return sorted_ns[lo] * (1.0 - frac) + sorted_ns[hi] * frac


def pearson(xs: list[float], ys: list[float]) -> float | None:
    """Pearson correlation coefficient; None when undefined (n < 2 or a
    zero-variance series)."""
    n = len(xs)
    if n != len(ys):
        raise ValueError("length mismatch")
    if n < 2:
        return None
    mx = sum(xs) / n
    my = sum(ys) / n
    sxx = sum((x - mx) ** 2 for x in xs)
    syy = sum((y - my) ** 2 for y in ys)
    if sxx == 0.0 or syy == 0.0:
        return None
    sxy = sum((x - mx) * (y - my) for x, y in zip(xs, ys))
    return sxy / math.sqrt(sxx * syy)


class Recorder:
    """Collects per-op latencies in nanoseconds."""

    def __init__(self) -> None:
        self.samples_ns: list[int] = []

    def record_ns(self, ns: int) -> None:
        self.samples_ns.append(ns)

    @contextmanager
    def measure(self) -> Iterator[None]:
        t0 = time.perf_counter_ns()
        yield
        self.samples_ns.append(time.perf_counter_ns() - t0)

    @property
    def count(self) -> int:
        return len(self.samples_ns)

    def merge(self, other: Recorder) -> None:
        self.samples_ns.extend(other.samples_ns)

    def percentiles_ms(self) -> dict[str, float]:
        s = sorted(self.samples_ns)
        return {
            "p50_ms": percentile(s, 50) / NS_PER_MS,
            "p95_ms": percentile(s, 95) / NS_PER_MS,
            "p99_ms": percentile(s, 99) / NS_PER_MS,
            "max_ms": s[-1] / NS_PER_MS,
        }


def _fmt(v: Any) -> str:
    if v is None:
        return "n/a"  # display only; the JSONL keeps a real null
    if isinstance(v, bool):
        return str(v).lower()
    if isinstance(v, float):
        if v == 0:
            return "0"
        if abs(v) >= 100:
            return f"{v:.0f}"
        if abs(v) >= 1:
            return f"{v:.2f}"
        return f"{v:.4f}"
    return str(v)


@dataclass
class Metric:
    """One benchmark measurement: a named op loop with rate + latency
    percentiles, plus scenario-specific extras (counts, ratios, flags)."""

    name: str
    ops: int
    wall_s: float
    p50_ms: float | None = None
    p95_ms: float | None = None
    p99_ms: float | None = None
    max_ms: float | None = None
    extra: dict[str, Any] = field(default_factory=dict)

    @property
    def rate_s(self) -> float:
        return self.ops / self.wall_s if self.wall_s > 0 else 0.0

    @classmethod
    def from_recorder(
        cls,
        name: str,
        recorder: Recorder,
        wall_s: float,
        **extra: Any,
    ) -> Metric:
        p = recorder.percentiles_ms() if recorder.count else {}
        return cls(
            name=name,
            ops=recorder.count,
            wall_s=wall_s,
            p50_ms=p.get("p50_ms"),
            p95_ms=p.get("p95_ms"),
            p99_ms=p.get("p99_ms"),
            max_ms=p.get("max_ms"),
            extra=dict(extra),
        )

    def line(self) -> str:
        parts = [
            f"{self.name:<42}",
            f"ops={self.ops}",
            f"wall_s={_fmt(self.wall_s)}",
            f"rate_s={_fmt(self.rate_s)}",
        ]
        for k in ("p50_ms", "p95_ms", "p99_ms", "max_ms"):
            v = getattr(self, k)
            if v is not None:
                parts.append(f"{k[:-3]}={_fmt(v)}ms")
        for k, v in self.extra.items():
            parts.append(f"{k}={_fmt(v)}")
        return "  ".join(parts)

    def to_json(self) -> dict[str, Any]:
        out: dict[str, Any] = {
            "name": self.name,
            "ops": self.ops,
            "wall_s": self.wall_s,
            "rate_s": self.rate_s,
        }
        for k in ("p50_ms", "p95_ms", "p99_ms", "max_ms"):
            v = getattr(self, k)
            if v is not None:
                out[k] = v
        out.update(self.extra)
        return out
