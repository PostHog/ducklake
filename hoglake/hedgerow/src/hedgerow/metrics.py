"""Optional Prometheus metrics — passive reporting ONLY (viaduck lesson
#7): nothing in here ever feeds back into replication decisions, and a
metrics failure never takes the write loop down with it.

Disabled (``metrics.port: 0``) by default; when enabled, requires
``prometheus-client`` (the ``hedgerow[metrics]`` extra) and fails fast at
startup if it is missing.
"""

from __future__ import annotations

import logging

from .config import ConfigError, MetricsConfig

log = logging.getLogger("hedgerow.metrics")


class NullMetrics:
    """Metrics disabled: every hook is a no-op."""

    def observe_cycle(self, result) -> None:
        pass

    def observe_error(self) -> None:
        pass


class PrometheusMetrics:
    def __init__(self, port: int, *, registry=None, start_server: bool = True) -> None:
        try:
            import prometheus_client as prom
        except ImportError:
            raise ConfigError(
                f"metrics.port={port} requires prometheus-client "
                "(install the hedgerow[metrics] extra)"
            ) from None
        self._registry = registry if registry is not None else prom.CollectorRegistry()
        self.rows_replicated_total = prom.Counter(
            "hedgerow_rows_replicated_total",
            "Rows appended to the destination table",
            registry=self._registry,
        )
        self.cycles_total = prom.Counter(
            "hedgerow_cycles_total",
            "Replication cycles completed (idle cycles included)",
            registry=self._registry,
        )
        self.errors_total = prom.Counter(
            "hedgerow_errors_total",
            "Transient (retried) cycle errors",
            registry=self._registry,
        )
        self.last_committed_snapshot = prom.Gauge(
            "hedgerow_last_committed_snapshot",
            "Consumer offset last committed to the source catalog",
            registry=self._registry,
        )
        self.lag_snapshots = prom.Gauge(
            "hedgerow_lag_snapshots",
            "Source catalog head minus the committed consumer offset",
            registry=self._registry,
        )
        if start_server:
            prom.start_http_server(port, registry=self._registry)
            log.info("metrics server listening on :%d", port)

    def observe_cycle(self, result) -> None:
        try:
            self.cycles_total.inc()
            self.rows_replicated_total.inc(result.rows_appended)
            self.last_committed_snapshot.set(result.committed_offset)
            self.lag_snapshots.set(result.lag_snapshots)
        except Exception:  # passive: never let metrics kill replication
            log.exception("metrics observation failed (ignored)")

    def observe_error(self) -> None:
        try:
            self.errors_total.inc()
        except Exception:
            log.exception("metrics observation failed (ignored)")


def build_metrics(cfg: MetricsConfig):
    if cfg.port and cfg.port > 0:
        return PrometheusMetrics(cfg.port)
    return NullMetrics()
