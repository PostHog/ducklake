"""Faker-built vocabularies for the seeded warehouse.

Faker is called **once per run**, for a few thousand distinct values;
row fabrication then samples these tuples with numpy/arrow. A per-row
Faker call would turn a 10 GB seed into an overnight job — the whole
point of the split is that realism costs a few thousand calls, not a few
hundred million.

Everything here is a pure function of the seed: same ``--seed``, same
vocabulary, byte for byte.
"""

from __future__ import annotations

from dataclasses import dataclass
from functools import lru_cache

import numpy as np
from faker import Faker

# Static vocabularies: small closed sets where Faker adds nothing.
BROWSERS = (
    "Chrome",
    "Safari",
    "Firefox",
    "Edge",
    "Opera",
    "Chrome Mobile",
    "Safari Mobile",
)
OPERATING_SYSTEMS = ("macOS", "Windows", "Linux", "iOS", "Android", "ChromeOS")
DEVICE_TYPES = ("Desktop", "Mobile", "Tablet")
PLANS = ("free", "starter", "growth", "enterprise")
ORDER_STATUSES = ("placed", "paid", "shipped", "delivered", "refunded", "cancelled")
CURRENCIES = ("USD", "EUR", "GBP", "CAD", "AUD")
REGIONS = ("us-east-1", "us-west-2", "eu-west-1", "eu-central-1", "ap-southeast-2")
UTM_SOURCES = (
    "google",
    "bing",
    "newsletter",
    "twitter",
    "linkedin",
    "hackernews",
    "reddit",
    "direct",
    "partner",
)
EVENT_NAMES = (
    "$pageview",
    "$pageleave",
    "$autocapture",
    "$identify",
    "feature_flag_called",
)
PRODUCT_CATEGORIES = (
    "Analytics",
    "Storage",
    "Compute",
    "Networking",
    "Observability",
    "Security",
    "Support",
    "Training",
)

# How many distinct values to mint per vocabulary.
N_NAMES = 4000
N_CITIES = 400
N_COUNTRIES = 60
N_DOMAINS = 1500
N_PATHS = 240
N_PRODUCTS = 600
N_CAMPAIGNS = 120
N_TEAM_NAMES = 16
N_DISTINCT_IDS = 50_000


@dataclass(frozen=True)
class Vocabulary:
    """Distinct values the row fabricators sample from."""

    seed: int
    names: tuple[str, ...]
    emails: tuple[str, ...]
    cities: tuple[str, ...]
    countries: tuple[str, ...]
    domains: tuple[str, ...]
    paths: tuple[str, ...]
    products: tuple[str, ...]
    campaigns: tuple[str, ...]
    team_names: tuple[str, ...]
    distinct_ids: tuple[str, ...]


def _unique(make, count: int, attempts: int = 6) -> tuple[str, ...]:
    """``count`` distinct values from ``make()``, in generation order.

    Faker repeats itself on small pools (countries especially); dedupe by
    first occurrence and give up after a bounded number of passes rather
    than spinning forever on an exhausted generator.
    """
    seen: dict[str, None] = {}
    for _ in range(attempts):
        for _ in range(count * 2):
            if len(seen) >= count:
                break
            seen[str(make())] = None
        if len(seen) >= count:
            break
    return tuple(seen)[:count]


def _paths(fake: Faker, count: int) -> tuple[str, ...]:
    """Realistic-looking site paths built from Faker words."""
    fixed = ("/", "/pricing", "/blog", "/docs", "/login", "/signup", "/dashboard")
    out: dict[str, None] = dict.fromkeys(fixed)
    templates = ("/blog/{0}", "/docs/{0}/{1}", "/{0}", "/product/{0}", "/help/{0}")
    while len(out) < count:
        template = templates[len(out) % len(templates)]
        out[template.format(fake.word(), fake.word())] = None
    return tuple(out)[:count]


def _distinct_ids(seed: int, count: int) -> tuple[str, ...]:
    """Stable, opaque person ids (``u_`` + 12 hex) — numpy, not Faker:
    50k of these are pure volume, not realism."""
    rng = np.random.default_rng(seed ^ 0x5EED)
    raw = rng.integers(0, 1 << 48, size=count, dtype=np.uint64)
    return tuple(f"u_{int(v):012x}" for v in raw)


@lru_cache(maxsize=4)
def build_vocabulary(seed: int) -> Vocabulary:
    """Mint (and memoize) every vocabulary for ``seed``."""
    fake = Faker()
    fake.seed_instance(seed)
    names = _unique(fake.name, N_NAMES)
    return Vocabulary(
        seed=seed,
        names=names,
        emails=_unique(fake.email, N_NAMES),
        cities=_unique(fake.city, N_CITIES),
        countries=_unique(fake.country, N_COUNTRIES),
        domains=_unique(fake.domain_name, N_DOMAINS),
        paths=_paths(fake, N_PATHS),
        products=_unique(
            lambda: f"{fake.word().capitalize()} {fake.word().capitalize()}",
            N_PRODUCTS,
        ),
        campaigns=_unique(lambda: f"{fake.word()}-{fake.word()}", N_CAMPAIGNS),
        team_names=_unique(fake.company, N_TEAM_NAMES),
        distinct_ids=_distinct_ids(seed, N_DISTINCT_IDS),
    )
