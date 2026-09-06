"""Cross-language differential seed: verify the Python bounds codec
against tests/vectors/bounds_vectors.json.

The vector file is the language-neutral contract (format documented in
its own header keys): (type, type_params, value, hex) triples where
`hex` is the exact Iceberg single-value binary encoding. A JVM (or any
other) implementation consumes the SAME file: parse `value` per the
header's value_conventions, then assert encode(value) == unhex(hex)
and — unless verify == "encode_only" — decode(unhex(hex)) == value and
re-encode(decode(...)) == unhex(hex).

The two decimal "encode_only" vectors document the pyhoglake
decode_bound context-precision bug (see qe_prop_bounds.py); the JVM
side has no such context and SHOULD run them as full round-trips.

Regeneration (only when the codec intentionally changes): the vectors
were produced by feeding these exact values through
pyhoglake.bounds.encode_bound — see the file's generated_by key.
"""

import base64
import json
import math
import struct
import uuid as _uuid
from datetime import UTC, date, datetime, time, timedelta
from decimal import Decimal, localcontext
from pathlib import Path

import pytest

from pyhoglake import decode_bound, encode_bound

VECTOR_PATH = Path(__file__).parent / "vectors" / "bounds_vectors.json"

with VECTOR_PATH.open(encoding="utf-8") as f:
    DOC = json.load(f)

VECTORS = DOC["vectors"]

_EPOCH_DATE = date(1970, 1, 1)
_EPOCH_NAIVE = datetime(1970, 1, 1)
_EPOCH_UTC = datetime(1970, 1, 1, tzinfo=UTC)


def _parse_value(vec):
    """value string -> the Python value fed to encode_bound (per the
    value_conventions header)."""
    t, v = vec["type"], vec["value"]
    if t == "boolean":
        return {"true": True, "false": False}[v]
    if t in ("int", "long", "date", "time", "timestamp", "timestamptz"):
        return int(v)  # integer-domain conventions; codec accepts ints
    if t in ("float", "double"):
        if v == "NaN":
            # hex is authoritative for NaN (payload bits not expressible
            # as a portable decimal string)
            fmt = "<f" if t == "float" else "<d"
            return struct.unpack(fmt, bytes.fromhex(vec["hex"]))[0]
        if v == "Infinity":
            return math.inf
        if v == "-Infinity":
            return -math.inf
        return float(v)
    if t == "string":
        return v
    if t == "uuid":
        return _uuid.UUID(v)
    if t == "binary":
        return base64.b64decode(v)
    if t == "decimal":
        return int(v)  # unscaled integer convention
    raise AssertionError(f"unknown vector type {t!r}")


def _decoded_matches(vec, decoded):
    """decode_bound output -> equality with the vector's value."""
    t, v = vec["type"], vec["value"]
    if t == "boolean":
        return decoded is ({"true": True, "false": False}[v])
    if t in ("int", "long"):
        return decoded == int(v)
    if t in ("float", "double"):
        fmt = "<f" if t == "float" else "<d"
        return struct.pack(fmt, decoded) == bytes.fromhex(vec["hex"])
    if t == "date":
        return decoded == _EPOCH_DATE + timedelta(days=int(v))
    if t == "time":
        micros = int(v)
        return decoded == time(
            micros // 3_600_000_000,
            micros % 3_600_000_000 // 60_000_000,
            micros % 60_000_000 // 1_000_000,
            micros % 1_000_000,
        )
    if t == "timestamp":
        return decoded == _EPOCH_NAIVE + timedelta(microseconds=int(v))
    if t == "timestamptz":
        return decoded == _EPOCH_UTC + timedelta(microseconds=int(v))
    if t == "string":
        return decoded == v
    if t == "uuid":
        return decoded == _uuid.UUID(v)
    if t == "binary":
        return decoded == base64.b64decode(v)
    if t == "decimal":
        scale = (vec["type_params"] or {}).get("scale", 0)
        with localcontext() as ctx:
            ctx.prec = 60
            expected = Decimal(int(v)).scaleb(-scale)
        return decoded == expected
    raise AssertionError(f"unknown vector type {t!r}")


def _vector_id(vec):
    val = vec["value"]
    tag = val if len(val) <= 24 else val[:21] + "..."
    return f"{vec['type']}:{tag}"


def test_vector_file_header_contract():
    assert DOC["format"] == "hoglake-bounds-vectors"
    assert DOC["version"] == 1
    assert set(DOC["value_conventions"]) >= {
        "boolean",
        "int",
        "long",
        "float",
        "double",
        "date",
        "time",
        "timestamp",
        "timestamptz",
        "string",
        "uuid",
        "binary",
        "decimal",
    }
    assert len(VECTORS) >= 40
    for vec in VECTORS:
        assert set(vec) >= {"type", "type_params", "value", "hex", "note"}
        # hex must be lowercase and byte-aligned
        assert vec["hex"] == vec["hex"].lower()
        assert len(vec["hex"]) % 2 == 0


@pytest.mark.parametrize("vec", VECTORS, ids=_vector_id)
def test_python_codec_matches_vector(vec):
    raw = bytes.fromhex(vec["hex"])
    params = vec["type_params"]

    # encode: always verified
    assert encode_bound(vec["type"], _parse_value(vec), params) == raw

    if vec.get("verify") == "encode_only":
        return  # decode is knowingly broken for this case (BUG documented
        # in the vector's note and in qe_prop_bounds.py)

    # decode: value equality
    decoded = decode_bound(vec["type"], raw, params)
    assert _decoded_matches(vec, decoded), (
        f"decode mismatch for {vec['type']} {vec['value']!r}: got {decoded!r}"
    )
    # canonical re-encode (skip int-convention temporal types where
    # decode returns the rich Python object; re-encode those too — the
    # codec accepts them and must reproduce identical bytes)
    assert encode_bound(vec["type"], decoded, params) == raw


def test_all_coltypes_are_covered():
    covered = {v["type"] for v in VECTORS}
    assert covered == {
        "boolean",
        "int",
        "long",
        "float",
        "double",
        "date",
        "time",
        "timestamp",
        "timestamptz",
        "string",
        "uuid",
        "binary",
        "decimal",
    }


def test_fixed_width_vectors_have_fixed_width_hex():
    widths = {
        "boolean": 1,
        "int": 4,
        "long": 8,
        "float": 4,
        "double": 8,
        "date": 4,
        "time": 8,
        "timestamp": 8,
        "timestamptz": 8,
        "uuid": 16,
    }
    for vec in VECTORS:
        w = widths.get(vec["type"])
        if w is not None:
            assert len(vec["hex"]) == 2 * w, vec
