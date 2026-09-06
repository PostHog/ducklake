"""Iceberg single-value binary serialization for column-stat bounds.

Encodings (little-endian unless stated):

    boolean      1 byte, 0x00 / 0x01
    int          4-byte LE signed
    long         8-byte LE signed
    float        4-byte LE IEEE-754
    double       8-byte LE IEEE-754
    date         days since 1970-01-01, 4-byte LE signed
    time         microseconds since midnight, 8-byte LE signed
    timestamp    microseconds since epoch, 8-byte LE signed
    timestamptz  microseconds since epoch UTC, 8-byte LE signed
    string       UTF-8 bytes
    uuid         16 bytes, big-endian
    binary       raw bytes
    decimal      minimal two's-complement big-endian unscaled value
"""

from __future__ import annotations

import struct
import uuid as _uuid
from datetime import UTC, date, datetime, time
from decimal import Decimal, localcontext
from typing import Any

_EPOCH_DATE = date(1970, 1, 1)
_EPOCH_UTC = datetime(1970, 1, 1, tzinfo=UTC)
_EPOCH_NAIVE = datetime(1970, 1, 1)  # noqa: DTZ001  # naive epoch is the codec's intent for timestamp-without-tz


def _minimal_twos_complement(n: int) -> bytes:
    """Minimal-length big-endian two's-complement encoding of ``n``."""
    if n >= 0:
        length = n.bit_length() // 8 + 1
    else:
        length = (n + 1).bit_length() // 8 + 1
    return n.to_bytes(length, "big", signed=True)


def _unscaled(value: Any, scale: int) -> int:
    if isinstance(value, int) and not isinstance(value, bool):
        return value  # already unscaled
    if not isinstance(value, Decimal):
        value = Decimal(str(value))
    # Ambient decimal context (prec=28) silently ROUNDS wide values in
    # scaleb; precision-38 unscaled values need up to 39 digits, plus
    # headroom for the shift (QE find, 2026-09-05).
    with localcontext() as ctx:
        ctx.prec = 60
        shifted = value.scaleb(scale)
    if shifted != shifted.to_integral_value():
        raise ValueError(f"decimal value {value} does not fit scale {scale} exactly")
    return int(shifted)


def _micros_since_epoch(value: Any) -> int:
    if isinstance(value, int):
        return value
    if isinstance(value, datetime):
        if value.tzinfo is not None:
            delta = value - _EPOCH_UTC
        else:
            delta = value - _EPOCH_NAIVE
        return (delta.days * 86400 + delta.seconds) * 1_000_000 + delta.microseconds
    raise TypeError(f"cannot encode {type(value).__name__} as timestamp micros")


def encode_bound(
    col_type: str, value: Any, type_params: dict[str, Any] | None = None
) -> bytes:
    """Encode a single value in Iceberg single-value binary for ``col_type``."""
    if col_type == "boolean":
        return b"\x01" if value else b"\x00"
    if col_type == "int":
        return struct.pack("<i", int(value))
    if col_type == "long":
        return struct.pack("<q", int(value))
    if col_type == "float":
        return struct.pack("<f", float(value))
    if col_type == "double":
        return struct.pack("<d", float(value))
    if col_type == "date":
        if isinstance(value, date) and not isinstance(value, datetime):
            days = (value - _EPOCH_DATE).days
        else:
            days = int(value)
        return struct.pack("<i", days)
    if col_type == "time":
        if isinstance(value, time):
            micros = (
                value.hour * 3600 + value.minute * 60 + value.second
            ) * 1_000_000 + value.microsecond
        else:
            micros = int(value)
        return struct.pack("<q", micros)
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
        b = bytes(value)
        if len(b) != 16:
            raise ValueError(f"uuid bound must be 16 bytes, got {len(b)}")
        return b
    if col_type == "binary":
        return bytes(value)
    if col_type == "decimal":
        scale = int((type_params or {}).get("scale", 0))
        return _minimal_twos_complement(_unscaled(value, scale))
    raise ValueError(f"cannot encode bound for column type {col_type!r}")


def decode_bound(
    col_type: str, data: bytes, type_params: dict[str, Any] | None = None
) -> Any:
    """Inverse of :func:`encode_bound` (returns naive-Python values)."""
    if col_type == "boolean":
        return data != b"\x00"
    if col_type == "int":
        return struct.unpack("<i", data)[0]
    if col_type == "long":
        return struct.unpack("<q", data)[0]
    if col_type == "float":
        return struct.unpack("<f", data)[0]
    if col_type == "double":
        return struct.unpack("<d", data)[0]
    if col_type == "date":
        from datetime import timedelta

        return _EPOCH_DATE + timedelta(days=struct.unpack("<i", data)[0])
    if col_type == "time":
        micros = struct.unpack("<q", data)[0]
        return time(
            micros // 3_600_000_000,
            micros % 3_600_000_000 // 60_000_000,
            micros % 60_000_000 // 1_000_000,
            micros % 1_000_000,
        )
    if col_type in ("timestamp", "timestamptz"):
        from datetime import timedelta

        micros = struct.unpack("<q", data)[0]
        base = _EPOCH_UTC if col_type == "timestamptz" else _EPOCH_NAIVE
        return base + timedelta(microseconds=micros)
    if col_type == "string":
        return data.decode("utf-8")
    if col_type == "uuid":
        return _uuid.UUID(bytes=data)
    if col_type == "binary":
        return data
    if col_type == "decimal":
        scale = int((type_params or {}).get("scale", 0))
        unscaled = int.from_bytes(data, "big", signed=True)
        # Same context hazard as the encode path: prec=28 rounds >28-digit
        # unscaled values silently.
        with localcontext() as ctx:
            ctx.prec = 60
            return Decimal(unscaled).scaleb(-scale)
    raise ValueError(f"cannot decode bound for column type {col_type!r}")
