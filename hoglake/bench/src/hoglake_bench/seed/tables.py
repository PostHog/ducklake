"""Schemas and vectorized row fabrication for the seeded warehouse.

Every generator builds a whole pyarrow table at once from numpy: random
integers, index draws into the :mod:`~hoglake_bench.seed.vocab`
vocabularies (arrow ``take``, never a Python loop), and int64 epoch
micros cast to timestamps. Nothing here is per-row Python, so millions
of rows cost tens of milliseconds.
"""

from __future__ import annotations

from collections.abc import Sequence
from datetime import UTC, datetime, timedelta

import numpy as np
import pyarrow as pa

from .vocab import (
    BROWSERS,
    CURRENCIES,
    DEVICE_TYPES,
    EVENT_NAMES,
    OPERATING_SYSTEMS,
    ORDER_STATUSES,
    PLANS,
    PRODUCT_CATEGORIES,
    REGIONS,
    UTM_SOURCES,
    Vocabulary,
)

#: The coarse partition dimension: a handful of teams, deliberately
#: skewed (the ops model is months x team_id, and real tenants are not
#: uniform).
TEAM_IDS: tuple[int, ...] = (17, 42, 137, 1042, 4740, 88231)
TEAM_WEIGHTS: tuple[float, ...] = (0.36, 0.24, 0.16, 0.11, 0.08, 0.05)

#: The other partition dimension: ~6 months of history, growing.
MONTH_WEIGHTS: tuple[float, ...] = (0.11, 0.13, 0.15, 0.17, 0.21, 0.23)
EVENT_MONTHS = len(MONTH_WEIGHTS)

MICROS = 1_000_000

TS = pa.timestamp("us", tz="UTC")

PAGEVIEWS_SCHEMA = pa.schema(
    [
        pa.field("event_id", pa.string(), nullable=False),
        pa.field("team_id", pa.int64(), nullable=False),
        pa.field("distinct_id", pa.string(), nullable=False),
        pa.field("session_id", pa.string()),
        pa.field("ts", TS, nullable=False),
        pa.field("event", pa.string()),
        pa.field("path", pa.string()),
        pa.field("referrer_domain", pa.string()),
        pa.field("country", pa.string()),
        pa.field("browser", pa.string()),
        pa.field("os", pa.string()),
        pa.field("device_type", pa.string()),
        pa.field("utm_source", pa.string()),
        pa.field("utm_campaign", pa.string()),
        pa.field("viewport_width", pa.int32()),
        pa.field("duration_ms", pa.int64()),
        pa.field("is_bounce", pa.bool_()),
    ]
)

DIM_USERS_SCHEMA = pa.schema(
    [
        pa.field("user_id", pa.int64(), nullable=False),
        pa.field("distinct_id", pa.string(), nullable=False),
        pa.field("name", pa.string()),
        pa.field("email", pa.string()),
        pa.field("country", pa.string()),
        pa.field("city", pa.string()),
        pa.field("team_id", pa.int64(), nullable=False),
        pa.field("plan", pa.string()),
        pa.field("signup_ts", TS),
        pa.field("is_active", pa.bool_()),
    ]
)

DIM_PRODUCTS_SCHEMA = pa.schema(
    [
        pa.field("product_id", pa.int64(), nullable=False),
        pa.field("sku", pa.string(), nullable=False),
        pa.field("name", pa.string()),
        pa.field("category", pa.string()),
        pa.field("price", pa.decimal128(10, 2)),
        pa.field("in_stock", pa.bool_()),
        pa.field("created_ts", TS),
    ]
)

DIM_TEAMS_SCHEMA = pa.schema(
    [
        pa.field("team_id", pa.int64(), nullable=False),
        pa.field("name", pa.string()),
        pa.field("plan", pa.string()),
        pa.field("region", pa.string()),
        pa.field("seat_count", pa.int32()),
        pa.field("created_ts", TS),
    ]
)

FACT_ORDERS_SCHEMA = pa.schema(
    [
        pa.field("order_id", pa.int64(), nullable=False),
        pa.field("team_id", pa.int64(), nullable=False),
        pa.field("user_id", pa.int64(), nullable=False),
        pa.field("product_id", pa.int64(), nullable=False),
        pa.field("quantity", pa.int32()),
        pa.field("unit_price", pa.float64()),
        pa.field("amount", pa.float64()),
        pa.field("currency", pa.string()),
        pa.field("status", pa.string()),
        pa.field("ordered_at", TS, nullable=False),
        pa.field("shipped_at", TS),
    ]
)

FACT_SESSIONS_SCHEMA = pa.schema(
    [
        pa.field("session_id", pa.string(), nullable=False),
        pa.field("team_id", pa.int64(), nullable=False),
        pa.field("user_id", pa.int64(), nullable=False),
        pa.field("started_at", TS, nullable=False),
        pa.field("ended_at", TS),
        pa.field("duration_s", pa.int32()),
        pa.field("pageview_count", pa.int32()),
        pa.field("device_type", pa.string()),
        pa.field("country", pa.string()),
        pa.field("referrer_domain", pa.string()),
        pa.field("is_converted", pa.bool_()),
    ]
)


# -- vectorized primitives --------------------------------------------------

_HEX = np.array(list("0123456789abcdef"), dtype="<U1")


def _hex_chars(rng: np.random.Generator, n: int, nbytes: int) -> np.ndarray:
    """(n, 2*nbytes) array of hex characters from ``nbytes`` random bytes."""
    raw = np.frombuffer(rng.bytes(n * nbytes), dtype=np.uint8).reshape(n, nbytes)
    out = np.empty((n, nbytes * 2), dtype="<U1")
    out[:, 0::2] = _HEX[raw >> 4]
    out[:, 1::2] = _HEX[raw & 0x0F]
    return out


def uuid_strings(rng: np.random.Generator, n: int) -> np.ndarray:
    """``n`` distinct-looking uuid4-shaped strings, ~4M/s."""
    chars = _hex_chars(rng, n, 16)
    out = np.full((n, 36), "-", dtype="<U1")
    # (destination offset, source offset, width) — 8-4-4-4-12 with the
    # dashes already in place
    for dst, src, width in (
        (0, 0, 8),
        (9, 8, 4),
        (14, 12, 4),
        (19, 16, 4),
        (24, 20, 12),
    ):
        out[:, dst : dst + width] = chars[:, src : src + width]
    return np.ascontiguousarray(out).view("<U36").reshape(n)


def hex_strings(rng: np.random.Generator, n: int, nbytes: int) -> np.ndarray:
    chars = _hex_chars(rng, n, nbytes)
    return np.ascontiguousarray(chars).view(f"<U{nbytes * 2}").reshape(n)


def take(
    values: Sequence[str], idx: np.ndarray, null_mask: np.ndarray | None = None
) -> pa.Array:
    """Sample a vocabulary by index (arrow ``take``); ``null_mask`` rows
    come back null."""
    indices = pa.array(idx.astype(np.int32), mask=null_mask)
    return pa.array(values, pa.string()).take(indices)


def uniform_idx(rng: np.random.Generator, size: int, n: int) -> np.ndarray:
    return rng.integers(0, size, n, dtype=np.int64)


def weighted_idx(
    rng: np.random.Generator, weights: Sequence[float], n: int
) -> np.ndarray:
    """Weighted draws over ``range(len(weights))`` — searchsorted over the
    cumulative distribution, which is far faster than ``choice(p=...)``."""
    cdf = np.cumsum(np.asarray(weights, dtype=np.float64))
    cdf /= cdf[-1]
    return np.searchsorted(cdf, rng.random(n)).astype(np.int64)


def timestamps(
    rng: np.random.Generator, n: int, lo_us: int, hi_us: int, sort: bool = True
) -> pa.Array:
    """Epoch-micro timestamps uniformly in ``[lo_us, hi_us)``, sorted by
    default (event streams arrive roughly in time order, and sorted
    columns give the footer stats something to prune on)."""
    hi_us = max(hi_us, lo_us + 1)
    micros = rng.integers(lo_us, hi_us, n, dtype=np.int64)
    if sort:
        micros.sort()
    return pa.array(micros, pa.int64()).cast(TS)


def month_windows(anchor: datetime, count: int) -> list[tuple[int, int]]:
    """``count`` calendar-month windows ending with the (partial) month
    containing ``anchor``, as [start, end) epoch-micro pairs."""
    year, month = anchor.year, anchor.month
    starts: list[datetime] = []
    for back in range(count - 1, -1, -1):
        total = (year * 12 + (month - 1)) - back
        starts.append(datetime(total // 12, total % 12 + 1, 1, tzinfo=UTC))
    windows = []
    for i, start in enumerate(starts):
        if i + 1 < len(starts):
            end = starts[i + 1]
        else:
            end = anchor
        windows.append((int(start.timestamp() * MICROS), int(end.timestamp() * MICROS)))
    return windows


def history_window(anchor: datetime, count: int = EVENT_MONTHS) -> tuple[int, int]:
    """The full [oldest, now) epoch-micro span the seeded history covers."""
    windows = month_windows(anchor, count)
    return windows[0][0], windows[-1][1]


# -- row fabricators ---------------------------------------------------------


def pageviews(
    rng: np.random.Generator,
    vocab: Vocabulary,
    n: int,
    *,
    team_id: int,
    lo_us: int,
    hi_us: int,
) -> pa.Table:
    """One partition's worth of pageviews: a single team, a single month
    (so the partitioned append fans this out to exactly one file)."""
    referrer_idx = uniform_idx(rng, len(vocab.domains), n)
    # ~35% of pageviews are direct traffic: no referrer at all
    direct = rng.random(n) < 0.35
    campaign_idx = uniform_idx(rng, len(vocab.campaigns), n)
    no_campaign = rng.random(n) < 0.6
    return pa.table(
        {
            "event_id": pa.array(uuid_strings(rng, n)),
            "team_id": pa.array(np.full(n, team_id, dtype=np.int64)),
            "distinct_id": take(
                vocab.distinct_ids, uniform_idx(rng, len(vocab.distinct_ids), n)
            ),
            "session_id": pa.array(hex_strings(rng, n, 8)),
            "ts": timestamps(rng, n, lo_us, hi_us),
            "event": take(EVENT_NAMES, weighted_idx(rng, (70, 12, 12, 3, 3), n)),
            "path": take(vocab.paths, uniform_idx(rng, len(vocab.paths), n)),
            "referrer_domain": take(vocab.domains, referrer_idx, direct),
            "country": take(vocab.countries, uniform_idx(rng, len(vocab.countries), n)),
            "browser": take(BROWSERS, weighted_idx(rng, (52, 18, 9, 7, 3, 7, 4), n)),
            "os": take(OPERATING_SYSTEMS, weighted_idx(rng, (30, 34, 8, 14, 12, 2), n)),
            "device_type": take(DEVICE_TYPES, weighted_idx(rng, (58, 36, 6), n)),
            "utm_source": take(UTM_SOURCES, uniform_idx(rng, len(UTM_SOURCES), n)),
            "utm_campaign": take(vocab.campaigns, campaign_idx, no_campaign),
            "viewport_width": pa.array(
                rng.integers(320, 3840, n, dtype=np.int64), pa.int64()
            ).cast(pa.int32()),
            "duration_ms": pa.array(rng.integers(80, 900_000, n, dtype=np.int64)),
            "is_bounce": pa.array(rng.random(n) < 0.42),
        },
        schema=PAGEVIEWS_SCHEMA,
    )


def dim_users(
    rng: np.random.Generator,
    vocab: Vocabulary,
    n: int,
    *,
    id_offset: int,
    anchor: datetime,
) -> pa.Table:
    signup_lo = int((anchor - timedelta(days=730)).timestamp() * MICROS)
    signup_hi = int(anchor.timestamp() * MICROS)
    return pa.table(
        {
            "user_id": pa.array(
                np.arange(id_offset + 1, id_offset + n + 1, dtype=np.int64)
            ),
            "distinct_id": take(
                vocab.distinct_ids, uniform_idx(rng, len(vocab.distinct_ids), n)
            ),
            "name": take(vocab.names, uniform_idx(rng, len(vocab.names), n)),
            "email": take(vocab.emails, uniform_idx(rng, len(vocab.emails), n)),
            "country": take(vocab.countries, uniform_idx(rng, len(vocab.countries), n)),
            "city": take(vocab.cities, uniform_idx(rng, len(vocab.cities), n)),
            "team_id": pa.array(
                np.asarray(TEAM_IDS, dtype=np.int64)[weighted_idx(rng, TEAM_WEIGHTS, n)]
            ),
            "plan": take(PLANS, weighted_idx(rng, (55, 25, 15, 5), n)),
            "signup_ts": timestamps(rng, n, signup_lo, signup_hi, sort=False),
            "is_active": pa.array(rng.random(n) < 0.72),
        },
        schema=DIM_USERS_SCHEMA,
    )


def dim_products(
    rng: np.random.Generator,
    vocab: Vocabulary,
    n: int,
    *,
    id_offset: int,
    anchor: datetime,
) -> pa.Table:
    created_lo = int((anchor - timedelta(days=1460)).timestamp() * MICROS)
    created_hi = int(anchor.timestamp() * MICROS)
    ids = np.arange(id_offset + 1, id_offset + n + 1, dtype=np.int64)
    prices = np.round(rng.gamma(2.2, 40.0, n) + 4.99, 2)
    return pa.table(
        {
            "product_id": pa.array(ids),
            "sku": pa.array(np.char.add("SKU-", hex_strings(rng, n, 4))),
            "name": take(vocab.products, uniform_idx(rng, len(vocab.products), n)),
            "category": take(
                PRODUCT_CATEGORIES, uniform_idx(rng, len(PRODUCT_CATEGORIES), n)
            ),
            "price": pa.array(prices).cast(pa.decimal128(10, 2)),
            "in_stock": pa.array(rng.random(n) < 0.85),
            "created_ts": timestamps(rng, n, created_lo, created_hi, sort=False),
        },
        schema=DIM_PRODUCTS_SCHEMA,
    )


def dim_teams(
    rng: np.random.Generator, vocab: Vocabulary, *, anchor: datetime
) -> pa.Table:
    n = len(TEAM_IDS)
    created_lo = int((anchor - timedelta(days=1825)).timestamp() * MICROS)
    created_hi = int((anchor - timedelta(days=200)).timestamp() * MICROS)
    return pa.table(
        {
            "team_id": pa.array(np.asarray(TEAM_IDS, dtype=np.int64)),
            "name": take(vocab.team_names, np.arange(n, dtype=np.int64)),
            "plan": take(PLANS, weighted_idx(rng, (10, 25, 35, 30), n)),
            "region": take(REGIONS, uniform_idx(rng, len(REGIONS), n)),
            "seat_count": pa.array(
                rng.integers(3, 900, n, dtype=np.int64), pa.int64()
            ).cast(pa.int32()),
            "created_ts": timestamps(rng, n, created_lo, created_hi, sort=False),
        },
        schema=DIM_TEAMS_SCHEMA,
    )


def fact_orders(
    rng: np.random.Generator,
    n: int,
    *,
    id_offset: int,
    user_count: int,
    product_count: int,
    lo_us: int,
    hi_us: int,
) -> pa.Table:
    quantity = rng.integers(1, 9, n, dtype=np.int64)
    unit_price = np.round(rng.gamma(2.0, 45.0, n) + 5.0, 2)
    shipped_gap = rng.integers(3_600, 5 * 86_400, n, dtype=np.int64) * MICROS
    ordered = rng.integers(lo_us, max(hi_us, lo_us + 1), n, dtype=np.int64)
    ordered.sort()
    unshipped = rng.random(n) < 0.18
    return pa.table(
        {
            "order_id": pa.array(
                np.arange(id_offset + 1, id_offset + n + 1, dtype=np.int64)
            ),
            "team_id": pa.array(
                np.asarray(TEAM_IDS, dtype=np.int64)[weighted_idx(rng, TEAM_WEIGHTS, n)]
            ),
            "user_id": pa.array(rng.integers(1, user_count + 1, n, dtype=np.int64)),
            "product_id": pa.array(
                rng.integers(1, product_count + 1, n, dtype=np.int64)
            ),
            "quantity": pa.array(quantity, pa.int64()).cast(pa.int32()),
            "unit_price": pa.array(unit_price),
            "amount": pa.array(np.round(unit_price * quantity, 2)),
            "currency": take(CURRENCIES, weighted_idx(rng, (62, 18, 10, 6, 4), n)),
            "status": take(ORDER_STATUSES, weighted_idx(rng, (8, 20, 18, 45, 5, 4), n)),
            "ordered_at": pa.array(ordered, pa.int64()).cast(TS),
            "shipped_at": pa.array(
                ordered + shipped_gap, pa.int64(), mask=unshipped
            ).cast(TS),
        },
        schema=FACT_ORDERS_SCHEMA,
    )


def fact_sessions(
    rng: np.random.Generator,
    vocab: Vocabulary,
    n: int,
    *,
    user_count: int,
    lo_us: int,
    hi_us: int,
) -> pa.Table:
    started = rng.integers(lo_us, max(hi_us, lo_us + 1), n, dtype=np.int64)
    started.sort()
    duration_s = rng.integers(2, 7_200, n, dtype=np.int64)
    open_session = rng.random(n) < 0.04
    referrer_idx = uniform_idx(rng, len(vocab.domains), n)
    direct = rng.random(n) < 0.4
    return pa.table(
        {
            "session_id": pa.array(uuid_strings(rng, n)),
            "team_id": pa.array(
                np.asarray(TEAM_IDS, dtype=np.int64)[weighted_idx(rng, TEAM_WEIGHTS, n)]
            ),
            "user_id": pa.array(rng.integers(1, user_count + 1, n, dtype=np.int64)),
            "started_at": pa.array(started, pa.int64()).cast(TS),
            "ended_at": pa.array(
                started + duration_s * MICROS, pa.int64(), mask=open_session
            ).cast(TS),
            "duration_s": pa.array(duration_s, pa.int64()).cast(pa.int32()),
            "pageview_count": pa.array(
                rng.integers(1, 60, n, dtype=np.int64), pa.int64()
            ).cast(pa.int32()),
            "device_type": take(DEVICE_TYPES, weighted_idx(rng, (58, 36, 6), n)),
            "country": take(vocab.countries, uniform_idx(rng, len(vocab.countries), n)),
            "referrer_domain": take(vocab.domains, referrer_idx, direct),
            "is_converted": pa.array(rng.random(n) < 0.09),
        },
        schema=FACT_SESSIONS_SCHEMA,
    )
