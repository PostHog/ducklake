"""Client-side Iceberg partition transforms for the writer path.

Implements the server's Transform vocabulary (``identity``, ``bucket``,
``year``, ``month``, ``day``, ``hour`` — Model.kt, iceberg-federation.md
§3) plus ``truncate`` with Iceberg-spec semantics, ready for the day the
server vocabulary grows it. Semantics follow the Apache Iceberg Table
Spec exactly:

- ``year``/``month``/``day``/``hour`` produce **epoch-relative ints**
  (years/months since 1970, days/hours since the epoch — floored, so
  pre-1970 values are negative), never calendar components.
- ``bucket[N]`` is ``(murmur3_x86_32(encode(v)) & Integer.MAX_VALUE) % N``
  over the spec's Appendix-B hash encoding. NOTE: that encoding is NOT
  the bounds codec for every type — ``int``/``date`` hash as an 8-byte
  little-endian **long** (the bounds codec stores them as 4-byte ints).
- ``truncate[W]`` floors ints/longs (and decimal unscaled values) to the
  nearest lower multiple of W (negative-correct) and takes the first W
  **codepoints** of a string / W bytes of a binary.
- A null source value transforms to a null partition value (its own
  partition group), per Iceberg's null handling.

The wire shape for the commit's ``partition_values`` is a list of
strings by key_index (nullable — see openapi FileRegistration); the
server treats them as opaque and groups by equality, so
:func:`wire_string` defines this client's canonical stringification.

References: Apache Iceberg Table Spec — "Partition Transforms" and
Appendix B "32-bit Hash Requirements"
(https://iceberg.apache.org/spec/#partition-transforms,
https://iceberg.apache.org/spec/#appendix-b-32-bit-hash-requirements).
"""

from __future__ import annotations

import base64
import struct
import uuid as _uuid
from datetime import date, datetime, time
from decimal import Decimal, localcontext
from typing import Any

import pyarrow as pa
import pyarrow.compute as pc

from .bounds import _micros_since_epoch, _minimal_twos_complement, _unscaled
from .errors import ValidationError

_EPOCH_DATE = date(1970, 1, 1)
_MICROS_PER_HOUR = 3_600_000_000
_MICROS_PER_DAY = 86_400_000_000

#: Column types bucket() accepts (Iceberg spec: no boolean/float/double).
_BUCKETABLE = frozenset(
    {
        "int",
        "long",
        "date",
        "time",
        "timestamp",
        "timestamptz",
        "string",
        "uuid",
        "binary",
        "decimal",
    }
)
_TRUNCATABLE = frozenset({"int", "long", "string", "binary", "decimal"})
_YEAR_MONTH_DAY_TYPES = frozenset({"date", "timestamp", "timestamptz"})
_HOUR_TYPES = frozenset({"timestamp", "timestamptz"})

_INT32_MASK = 0xFFFFFFFF
_INT31_MASK = 0x7FFFFFFF  # Java Integer.MAX_VALUE


def murmur3_32(data: bytes, seed: int = 0) -> int:
    """32-bit Murmur3 (x86 variant), signed result — bit-identical to
    Iceberg's ``BucketUtil`` (Guava ``Hashing.murmur3_32``)."""
    c1 = 0xCC9E2D51
    c2 = 0x1B873593
    h = seed & _INT32_MASK
    length = len(data)
    nblocks = length // 4
    for i in range(nblocks):
        k = int.from_bytes(data[4 * i : 4 * i + 4], "little")
        k = (k * c1) & _INT32_MASK
        k = ((k << 15) | (k >> 17)) & _INT32_MASK
        k = (k * c2) & _INT32_MASK
        h ^= k
        h = ((h << 13) | (h >> 19)) & _INT32_MASK
        h = (h * 5 + 0xE6546B64) & _INT32_MASK
    tail = data[nblocks * 4 :]
    k = 0
    if len(tail) >= 3:
        k ^= tail[2] << 16
    if len(tail) >= 2:
        k ^= tail[1] << 8
    if len(tail) >= 1:
        k ^= tail[0]
        k = (k * c1) & _INT32_MASK
        k = ((k << 15) | (k >> 17)) & _INT32_MASK
        k = (k * c2) & _INT32_MASK
        h ^= k
    h ^= length
    h ^= h >> 16
    h = (h * 0x85EBCA6B) & _INT32_MASK
    h ^= h >> 13
    h = (h * 0xC2B2AE35) & _INT32_MASK
    h ^= h >> 16
    return h - (1 << 32) if h & 0x80000000 else h


def _days_since_epoch(value: Any) -> int:
    if isinstance(value, datetime):
        raise TypeError("expected a date, got a datetime")
    if isinstance(value, date):
        return (value - _EPOCH_DATE).days
    return int(value)


def _time_micros(value: Any) -> int:
    if isinstance(value, time):
        return (
            value.hour * 3600 + value.minute * 60 + value.second
        ) * 1_000_000 + value.microsecond
    return int(value)


def bucket_encode(
    col_type: str, value: Any, type_params: dict[str, Any] | None = None
) -> bytes:
    """Iceberg Appendix-B hash input encoding for ``value``.

    Diverges from the bounds codec (:mod:`pyhoglake.bounds`) where the
    spec says so: ``int`` and ``date`` hash as 8-byte LE **longs**
    (``hashInt(v) = hashLong(long(v))``), not the 4-byte forms the
    bounds codec stores. ``decimal``/``uuid``/``string``/``binary``
    reuse the bounds encodings, which already match the spec.
    """
    if col_type in ("int", "long"):
        return struct.pack("<q", int(value))
    if col_type == "date":
        return struct.pack("<q", _days_since_epoch(value))
    if col_type == "time":
        return struct.pack("<q", _time_micros(value))
    if col_type in ("timestamp", "timestamptz"):
        return struct.pack("<q", _micros_since_epoch(value))
    if col_type == "string":
        if isinstance(value, bytes):
            return value
        return str(value).encode("utf-8")
    if col_type == "uuid":
        if isinstance(value, _uuid.UUID):
            return value.bytes
        if isinstance(value, str):
            return _uuid.UUID(value).bytes
        return bytes(value)
    if col_type == "binary":
        return bytes(value)
    if col_type == "decimal":
        scale = int((type_params or {}).get("scale", 0))
        return _minimal_twos_complement(_unscaled(value, scale))
    raise ValidationError(
        f"bucket cannot be applied to column type {col_type!r} "
        f"(Iceberg spec: bucketable types are {sorted(_BUCKETABLE)})"
    )


def bucket_hash(
    col_type: str, value: Any, type_params: dict[str, Any] | None = None
) -> int:
    """The signed 32-bit Iceberg hash of ``value`` (spec Appendix B)."""
    return murmur3_32(bucket_encode(col_type, value, type_params))


def bucket(
    col_type: str,
    value: Any,
    n: int,
    type_params: dict[str, Any] | None = None,
) -> int | None:
    """``bucket[n]``: ``(hash & Integer.MAX_VALUE) % n``; null -> null."""
    if not isinstance(n, int) or n <= 0:
        raise ValidationError(f"bucket requires a positive bucket count, got {n!r}")
    if value is None:
        return None
    return (bucket_hash(col_type, value, type_params) & _INT31_MASK) % n


def truncate(
    col_type: str,
    value: Any,
    width: int,
    type_params: dict[str, Any] | None = None,
) -> Any:
    """``truncate[width]`` with Iceberg semantics; null -> null."""
    if not isinstance(width, int) or width <= 0:
        raise ValidationError(f"truncate requires a positive width, got {width!r}")
    if value is None:
        return None
    if col_type in ("int", "long"):
        v = int(value)
        return v - (v % width)  # Python % is floor-mod: negative-correct
    if col_type == "string":
        return str(value)[:width]  # codepoints, not bytes
    if col_type == "binary":
        return bytes(value)[:width]
    if col_type == "decimal":
        scale = int((type_params or {}).get("scale", 0))
        unscaled = _unscaled(value, scale)
        floored = unscaled - (unscaled % width)
        with localcontext() as ctx:
            ctx.prec = 60
            return Decimal(floored).scaleb(-scale)
    raise ValidationError(
        f"truncate cannot be applied to column type {col_type!r} "
        f"(truncatable types are {sorted(_TRUNCATABLE)})"
    )


def year(value: date | datetime | None) -> int | None:
    """Years since 1970 (calendar year - 1970); null -> null."""
    if value is None:
        return None
    return value.year - 1970


def month(value: date | datetime | None) -> int | None:
    """Months since 1970-01 (floored, negative before the epoch)."""
    if value is None:
        return None
    return (value.year - 1970) * 12 + (value.month - 1)


def day(value: date | datetime | None) -> int | None:
    """Days since the epoch (floored for timestamps)."""
    if value is None:
        return None
    if isinstance(value, datetime):
        return _micros_since_epoch(value) // _MICROS_PER_DAY
    return (value - _EPOCH_DATE).days


def hour(value: datetime | None) -> int | None:
    """Hours since the epoch (floored; timestamps only)."""
    if value is None:
        return None
    return _micros_since_epoch(value) // _MICROS_PER_HOUR


def _check_temporal(transform: str, col_type: str) -> None:
    allowed = _HOUR_TYPES if transform == "hour" else _YEAR_MONTH_DAY_TYPES
    if col_type not in allowed:
        raise ValidationError(
            f"{transform} transform cannot be applied to column type "
            f"{col_type!r} (allowed: {sorted(allowed)})"
        )


def transform_value(
    transform: str,
    param: int | None,
    col_type: str,
    value: Any,
    type_params: dict[str, Any] | None = None,
) -> Any:
    """Apply one partition transform to one Python-native value."""
    if transform == "identity":
        return value
    if transform == "bucket":
        if param is None:
            raise ValidationError("bucket transform requires transform_param (N)")
        return bucket(col_type, value, param, type_params)
    if transform == "truncate":
        if param is None:
            raise ValidationError("truncate transform requires transform_param (W)")
        return truncate(col_type, value, param, type_params)
    if transform in ("year", "month", "day", "hour"):
        _check_temporal(transform, col_type)
        fn = {"year": year, "month": month, "day": day, "hour": hour}[transform]
        return fn(value)
    raise ValidationError(f"unknown partition transform {transform!r}")


def wire_string(col_type: str, transform: str, transformed: Any) -> str | None:
    """Canonical partition-value string for the commit wire (opaque to the
    server, which groups by string equality — see FileRegistration in the
    openapi spec). Null stays null (JSON null on the wire)."""
    v = transformed
    if v is None:
        return None
    if transform in ("bucket", "year", "month", "day", "hour"):
        return str(int(v))
    # identity / truncate: stringify by the SOURCE column type
    if col_type == "boolean":
        return "true" if v else "false"
    if col_type in ("int", "long"):
        return str(int(v))
    if col_type in ("float", "double"):
        return repr(float(v))
    if col_type == "string":
        return str(v)
    if col_type in ("date", "time", "timestamp", "timestamptz"):
        return v.isoformat()
    if col_type == "decimal":
        return str(v)
    if col_type == "uuid":
        if isinstance(v, _uuid.UUID):
            return str(v)
        return str(_uuid.UUID(bytes=bytes(v)))
    if col_type == "binary":
        return base64.b64encode(bytes(v)).decode("ascii")
    raise ValidationError(f"cannot stringify partition value for type {col_type!r}")


# -- array-level driver (the fanout path) -----------------------------------


def _floordiv(arr: pa.Array, divisor: int) -> pa.Array:
    """Floor division on an int64 array (arrow's ``divide`` truncates
    toward zero; Iceberg's day/hour need flooring for pre-epoch values)."""
    q = pc.divide(arr, divisor)
    r = pc.subtract(arr, pc.multiply(q, divisor))
    adjust = pc.and_(pc.less(arr, 0), pc.not_equal(r, 0))
    return pc.subtract(q, pc.cast(adjust, pa.int64()))


def _temporal_ints(transform: str, arr: pa.Array, col_type: str) -> pa.Array:
    """Arrow-native year/month/day/hour over a date/timestamp column."""
    _check_temporal(transform, col_type)
    if transform == "year":
        return pc.subtract(pc.year(arr), 1970)
    if transform == "month":
        years = pc.subtract(pc.year(arr), 1970)
        return pc.add(pc.multiply(years, 12), pc.subtract(pc.month(arr), 1))
    if col_type == "date":  # day; hour is rejected for date by _check_temporal
        return pc.cast(pc.cast(arr, pa.int32()), pa.int64())
    micros = pc.cast(arr, pa.int64())
    divisor = _MICROS_PER_DAY if transform == "day" else _MICROS_PER_HOUR
    return _floordiv(micros, divisor)


def transform_strings(
    transform: str,
    param: int | None,
    column: pa.ChunkedArray | pa.Array,
    col_type: str,
    type_params: dict[str, Any] | None = None,
) -> pa.Array:
    """Per-row wire partition-value strings for one spec field.

    Arrow-native for the temporal transforms; ``identity``/``bucket``/
    ``truncate`` go through Python once **per unique value** (dictionary
    encoding), never per row. Nulls stay null throughout.
    """
    arr = column.combine_chunks() if isinstance(column, pa.ChunkedArray) else column
    if transform in ("year", "month", "day", "hour"):
        return pc.cast(_temporal_ints(transform, arr, col_type), pa.string())
    if transform not in ("identity", "bucket", "truncate"):
        raise ValidationError(f"unknown partition transform {transform!r}")

    def one(value: Any) -> str | None:
        return wire_string(
            col_type,
            transform,
            transform_value(transform, param, col_type, value, type_params),
        )

    try:
        encoded = arr.dictionary_encode()
    except (pa.ArrowNotImplementedError, pa.ArrowInvalid):
        # types without dictionary support: per-value python fallback
        return pa.array([one(v) for v in arr.to_pylist()], pa.string())
    uniques = [one(v) for v in encoded.dictionary.to_pylist()]
    if not uniques:
        return pa.array([None] * len(arr), pa.string())
    # null indices (null source rows) take() to null outputs
    return pc.take(pa.array(uniques, pa.string()), encoded.indices)
