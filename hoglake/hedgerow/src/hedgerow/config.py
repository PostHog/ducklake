"""hedgerow configuration: YAML file + optional env-var overrides.

Parsing is strict (unknown keys are errors) and shape validation happens
here; *live* validation (tables exist, schemas project) happens at
:meth:`hedgerow.daemon.Hedgerow.start` — together they implement the
fail-fast-config viaduck lesson (#6): a misconfigured replicator refuses
to start instead of limping into a wrong state.

Env overrides use double-underscore paths with a ``HEDGEROW__`` prefix,
values parsed as YAML scalars::

    HEDGEROW__SOURCE__S3__SECRET_KEY=...   -> source.s3.secret_key
    HEDGEROW__REPLICATION__POLL_INTERVAL_S=1
"""

from __future__ import annotations

import math
import os
from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Any

import yaml

ENV_PREFIX = "HEDGEROW__"


class ConfigError(Exception):
    """Invalid configuration. The daemon must not start."""


def _require(d: Mapping[str, Any], key: str, where: str) -> Any:
    if key not in d or d[key] is None:
        raise ConfigError(f"missing required key {where}.{key}")
    return d[key]


def _check_keys(d: Mapping[str, Any], allowed: set[str], where: str) -> None:
    unknown = sorted(set(d) - allowed)
    if unknown:
        raise ConfigError(f"unknown key(s) under {where}: {', '.join(unknown)}")


_SENSITIVE_KEY_TOKENS = ("secret", "key", "password")


def _str(
    d: Mapping[str, Any], key: str, where: str, required: bool = True
) -> str | None:
    if key not in d or d[key] is None:
        if required:
            raise ConfigError(f"missing required key {where}.{key}")
        return None
    v = d[key]
    if not isinstance(v, str) or not v:
        # Never interpolate the value of a credential-shaped key into an
        # error message (it ends up in logs); report only its type.
        if any(tok in key.lower() for tok in _SENSITIVE_KEY_TOKENS):
            shown = f"a value of type {type(v).__name__}"
        else:
            shown = repr(v)
        raise ConfigError(f"{where}.{key} must be a non-empty string, got {shown}")
    return v


def _int(
    d: Mapping[str, Any], key: str, where: str, default: int, minimum: int = 0
) -> int:
    v = d.get(key, default)
    if isinstance(v, bool) or not isinstance(v, int):
        raise ConfigError(f"{where}.{key} must be an integer, got {v!r}")
    if v < minimum:
        raise ConfigError(f"{where}.{key} must be >= {minimum}, got {v}")
    return v


@dataclass(frozen=True)
class S3Settings:
    endpoint: str | None = None
    access_key: str | None = None
    secret_key: str | None = None
    path_style: bool = True

    @classmethod
    def parse(cls, d: Mapping[str, Any] | None, where: str) -> S3Settings:
        if d is None:
            return cls()
        if not isinstance(d, Mapping):
            raise ConfigError(f"{where} must be a mapping")
        _check_keys(d, {"endpoint", "access_key", "secret_key", "path_style"}, where)
        path_style = d.get("path_style", True)
        if not isinstance(path_style, bool):
            raise ConfigError(f"{where}.path_style must be a boolean")
        return cls(
            endpoint=_str(d, "endpoint", where, required=False),
            access_key=_str(d, "access_key", where, required=False),
            secret_key=_str(d, "secret_key", where, required=False),
            path_style=path_style,
        )

    def to_pyhoglake(self):
        """Build a pyhoglake S3Config. pyarrow's S3FileSystem uses
        path-style addressing whenever an endpoint override is set, which
        is the only case ``path_style`` matters for; the flag is kept for
        config symmetry and future backends."""
        from pyhoglake import S3Config

        return S3Config(
            access_key=self.access_key,
            secret_key=self.secret_key,
            endpoint_override=self.endpoint,
            region="us-east-1" if self.endpoint else None,
            allow_bucket_creation=False,
        )


@dataclass(frozen=True)
class SourceConfig:
    url: str
    catalog: str
    namespace: str
    table: str
    consumer_id: str
    start_snapshot: int = 0
    s3: S3Settings = field(default_factory=S3Settings)

    @classmethod
    def parse(cls, d: Any, where: str = "source") -> SourceConfig:
        if not isinstance(d, Mapping):
            raise ConfigError(f"{where} must be a mapping")
        _check_keys(
            d,
            {
                "url",
                "catalog",
                "namespace",
                "table",
                "s3",
                "consumer_id",
                "start_snapshot",
            },
            where,
        )
        return cls(
            url=_str(d, "url", where),
            catalog=_str(d, "catalog", where),
            namespace=_str(d, "namespace", where),
            table=_str(d, "table", where),
            consumer_id=_str(d, "consumer_id", where),
            start_snapshot=_int(d, "start_snapshot", where, default=0),
            s3=S3Settings.parse(d.get("s3"), f"{where}.s3"),
        )


@dataclass(frozen=True)
class DestinationConfig:
    url: str
    catalog: str
    namespace: str
    table: str
    s3: S3Settings = field(default_factory=S3Settings)

    @classmethod
    def parse(cls, d: Any, where: str = "destination") -> DestinationConfig:
        if not isinstance(d, Mapping):
            raise ConfigError(f"{where} must be a mapping")
        _check_keys(d, {"url", "catalog", "namespace", "table", "s3"}, where)
        return cls(
            url=_str(d, "url", where),
            catalog=_str(d, "catalog", where),
            namespace=_str(d, "namespace", where),
            table=_str(d, "table", where),
            s3=S3Settings.parse(d.get("s3"), f"{where}.s3"),
        )


@dataclass(frozen=True)
class FilterConfig:
    """Client-side row filter (the routing_value analog): keep rows where
    ``column == equals``. NULLs never match."""

    column: str
    equals: Any

    @classmethod
    def parse(cls, d: Any, where: str = "filter") -> FilterConfig | None:
        if d is None:
            return None
        if not isinstance(d, Mapping):
            raise ConfigError(f"{where} must be a mapping")
        _check_keys(d, {"column", "equals"}, where)
        column = _str(d, "column", where)
        if "equals" not in d:
            raise ConfigError(f"missing required key {where}.equals")
        if d["equals"] is None:
            # `equals:` / `equals: null` is a one-character YAML trap:
            # NULL == x is NULL for every row, so it would silently drop
            # 100% of rows while the offset advances (BUG-5).
            raise ConfigError(
                f"{where}.equals filter value must be non-null; a null "
                "filter matches nothing (NULLs never match an equality "
                "filter — see README)"
            )
        v = d["equals"]
        if isinstance(v, float) and not math.isfinite(v):
            # YAML `.nan`/`.inf` parse to floats through safe_load. NaN
            # never compares equal to anything (IEEE 754: NaN != NaN), so
            # the filter would be vacuous: every cycle reads the window,
            # appends ZERO rows, and still advances the offset — the
            # dropped rows are silently, permanently skipped (same trap
            # as the null filter). Inf is refused with it: non-finite
            # equality filters are always a config mistake.
            raise ConfigError(
                f"{where}.equals filter value must be finite, got {v!r}; "
                "a NaN filter matches no row (NaN never equals anything), "
                "so it would drop 100% of rows while the offset advances"
            )
        return cls(column=column, equals=v)


@dataclass(frozen=True)
class ReplicationConfig:
    poll_interval_s: float = 5.0
    max_snapshot_window: int = 1000
    max_rows_per_append: int = 100_000
    # Transient-failure retry budget PER WINDOW: each retry replays the
    # whole window (duplicates possible per replay); after this many
    # consecutive failed cycles hedgerow halts as persistent (BUG-3).
    max_window_replays: int = 3
    # Retryable-conflict budget PER APPEND: a retryable
    # CommitConflictError from the destination commit gets this many
    # single-append retries (duplicate-free) before the error escalates
    # to the window-replay path above (bugs.md #13).
    max_append_retries: int = 3

    @classmethod
    def parse(cls, d: Any, where: str = "replication") -> ReplicationConfig:
        if d is None:
            return cls()
        if not isinstance(d, Mapping):
            raise ConfigError(f"{where} must be a mapping")
        _check_keys(
            d,
            {
                "poll_interval_s",
                "max_snapshot_window",
                "max_rows_per_append",
                "max_window_replays",
                "max_append_retries",
            },
            where,
        )
        poll = d.get("poll_interval_s", 5.0)
        if isinstance(poll, bool) or not isinstance(poll, (int, float)):
            raise ConfigError(f"{where}.poll_interval_s must be a number, got {poll!r}")
        if not math.isfinite(poll):
            # YAML `.nan`/`.inf` are floats, isinstance passes, and
            # `nan < 0.1` is False — without this check the daemon would
            # start and only explode at the first `time.sleep(nan)`
            # mid-loop. Fail at startup instead (bugs.md #21).
            raise ConfigError(
                f"{where}.poll_interval_s must be a finite number, got {poll!r}"
            )
        if poll < 0.1:
            # 0 would busy-spin the loop (and hot-loop error retries)
            raise ConfigError(f"{where}.poll_interval_s must be >= 0.1, got {poll}")
        return cls(
            poll_interval_s=float(poll),
            max_snapshot_window=_int(
                d, "max_snapshot_window", where, default=1000, minimum=1
            ),
            max_rows_per_append=_int(
                d, "max_rows_per_append", where, default=100_000, minimum=1
            ),
            max_window_replays=_int(
                d, "max_window_replays", where, default=3, minimum=0
            ),
            max_append_retries=_int(
                d, "max_append_retries", where, default=3, minimum=0
            ),
        )


@dataclass(frozen=True)
class MetricsConfig:
    port: int = 0  # 0 = disabled

    @classmethod
    def parse(cls, d: Any, where: str = "metrics") -> MetricsConfig:
        if d is None:
            return cls()
        if not isinstance(d, Mapping):
            raise ConfigError(f"{where} must be a mapping")
        _check_keys(d, {"port"}, where)
        return cls(port=_int(d, "port", where, default=0))


@dataclass(frozen=True)
class HedgerowConfig:
    source: SourceConfig
    destination: DestinationConfig
    replication: ReplicationConfig = field(default_factory=ReplicationConfig)
    metrics: MetricsConfig = field(default_factory=MetricsConfig)
    filter: FilterConfig | None = None

    @classmethod
    def parse(cls, d: Any) -> HedgerowConfig:
        if not isinstance(d, Mapping):
            raise ConfigError("config root must be a mapping")
        _check_keys(
            d, {"source", "destination", "filter", "replication", "metrics"}, "config"
        )
        return cls(
            source=SourceConfig.parse(_require(d, "source", "config")),
            destination=DestinationConfig.parse(_require(d, "destination", "config")),
            filter=FilterConfig.parse(d.get("filter")),
            replication=ReplicationConfig.parse(d.get("replication")),
            metrics=MetricsConfig.parse(d.get("metrics")),
        )


def apply_env_overrides(
    raw: dict[str, Any], environ: Mapping[str, str] | None = None
) -> dict[str, Any]:
    """Overlay ``HEDGEROW__A__B__C=value`` env vars onto the raw config
    dict (lowercased path, YAML-parsed scalar values)."""
    environ = os.environ if environ is None else environ
    for key, value in environ.items():
        if not key.startswith(ENV_PREFIX):
            continue
        path = [p.lower() for p in key[len(ENV_PREFIX) :].split("__") if p]
        if not path:
            continue
        node = raw
        for part in path[:-1]:
            nxt = node.get(part)
            if not isinstance(nxt, dict):
                nxt = {}
                node[part] = nxt
            node = nxt
        node[path[-1]] = yaml.safe_load(value)
    return raw


def load_config(path: str, environ: Mapping[str, str] | None = None) -> HedgerowConfig:
    try:
        with open(path) as f:
            raw = yaml.safe_load(f)
    except FileNotFoundError:
        raise ConfigError(f"config file not found: {path}") from None
    except yaml.YAMLError as e:
        raise ConfigError(f"config file {path} is not valid YAML: {e}") from None
    if raw is None:
        raw = {}
    if not isinstance(raw, dict):
        raise ConfigError("config root must be a mapping")
    raw = apply_env_overrides(raw, environ)
    return HedgerowConfig.parse(raw)
