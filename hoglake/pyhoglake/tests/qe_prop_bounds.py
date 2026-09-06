"""Property-based fuzzing of the Iceberg single-value bounds codec.

Pinned policies (verified here, and load-bearing for the JVM port):

* NaN: the codec neither rejects nor canonicalizes NaN. float/double
  encode whatever IEEE-754 bits the value carries (payload preserved,
  including for float32) and decode returns a NaN with those bits.
  NaN-exclusion is the STATS layer's job (parquet-cpp already excludes
  NaN from min/max) — see qe_prop_stats.py.
* Negative zero: sign bit is preserved bit-exactly in both widths.
* float32: encode rounds the Python double to the nearest float32
  (struct semantics); decode returns exactly that float32 value.
* Strings: UTF-8; lone surrogates are rejected at encode time with
  UnicodeEncodeError (a ValueError subclass); decode of invalid UTF-8
  raises UnicodeDecodeError. Astral-plane text round-trips.
* Fixed widths: boolean=1, int/float/date=4, long/double/time/
  timestamp/timestamptz=8, uuid=16 bytes — always.
* Decimal: minimal-length big-endian two's-complement unscaled value.
  Encoding is exact and canonical (no redundant sign-extension byte).
  Decode of unscaled values wider than the default decimal-context
  precision (28 digits) once silently rounded; encode and decode now
  both run under a widened localcontext, pinned by regressions below.
* Out-of-range ints (int/long) raise struct.error at encode.
"""

import math
import struct
import uuid as _uuid
from datetime import UTC, date, datetime, time, timedelta, timezone
from decimal import Decimal, localcontext

import pytest
from hypothesis import given
from hypothesis import strategies as st

from pyhoglake import decode_bound, encode_bound

# The "qe" hypothesis profile (deadline=None) is loaded in conftest.py.

INT32_MIN, INT32_MAX = -(2**31), 2**31 - 1
INT64_MIN, INT64_MAX = -(2**63), 2**63 - 1


def f32(x: float) -> float:
    return struct.unpack("<f", struct.pack("<f", x))[0]


def bits64(x: float) -> bytes:
    return struct.pack("<d", x)


# -- fixed-width integer types ---------------------------------------------


@given(st.integers(INT32_MIN, INT32_MAX))
def test_int_roundtrip_and_width(v):
    enc = encode_bound("int", v)
    assert len(enc) == 4
    assert decode_bound("int", enc) == v


@given(st.integers(INT64_MIN, INT64_MAX))
def test_long_roundtrip_and_width(v):
    enc = encode_bound("long", v)
    assert len(enc) == 8
    assert decode_bound("long", enc) == v


@given(
    st.one_of(
        st.integers(max_value=INT32_MIN - 1), st.integers(min_value=INT32_MAX + 1)
    )
)
def test_int_out_of_range_raises(v):
    with pytest.raises(struct.error):
        encode_bound("int", v)


@given(
    st.one_of(
        st.integers(max_value=INT64_MIN - 1), st.integers(min_value=INT64_MAX + 1)
    )
)
def test_long_out_of_range_raises(v):
    with pytest.raises((struct.error, OverflowError)):
        encode_bound("long", v)


# -- floats: full domain incl. nan / inf / subnormal / -0.0 ----------------


@given(st.floats(width=32, allow_nan=True, allow_infinity=True, allow_subnormal=True))
def test_float_roundtrip_bit_exact(v):
    enc = encode_bound("float", v)
    assert len(enc) == 4
    dec = decode_bound("float", enc)
    # decode must yield exactly the float32 rounding of v (bitwise:
    # covers nan payloads and -0.0, where == is the wrong question)
    assert struct.pack("<f", dec) == struct.pack("<f", f32(v))


@given(st.floats(allow_nan=True, allow_infinity=True, allow_subnormal=True))
def test_double_roundtrip_bit_exact(v):
    enc = encode_bound("double", v)
    assert len(enc) == 8
    dec = decode_bound("double", enc)
    assert bits64(dec) == bits64(v)


def test_nan_policy_passthrough():
    """NaN policy: encode accepts NaN, bits (incl. payload) preserved."""
    payload_nan = struct.unpack("<d", b"\x39\x05\x00\x00\x00\x00\xf8\x7f")[0]
    assert math.isnan(payload_nan)
    enc = encode_bound("double", payload_nan)
    assert enc == b"\x39\x05\x00\x00\x00\x00\xf8\x7f"
    assert math.isnan(decode_bound("double", enc))

    f_nan = struct.unpack("<f", b"\x01\x00\xc0\x7f")[0]
    assert encode_bound("float", f_nan) == b"\x01\x00\xc0\x7f"


def test_negative_zero_sign_preserved():
    assert encode_bound("double", -0.0) == b"\x00" * 7 + b"\x80"
    assert encode_bound("float", -0.0) == b"\x00\x00\x00\x80"
    assert (
        math.copysign(1.0, decode_bound("double", encode_bound("double", -0.0))) == -1.0
    )


def test_infinities():
    assert decode_bound("float", encode_bound("float", math.inf)) == math.inf
    assert decode_bound("double", encode_bound("double", -math.inf)) == -math.inf


def test_min_subnormals():
    tiny64 = 5e-324
    assert decode_bound("double", encode_bound("double", tiny64)) == tiny64
    tiny32 = struct.unpack("<f", b"\x01\x00\x00\x00")[0]
    assert decode_bound("float", encode_bound("float", tiny32)) == tiny32


# -- boolean ----------------------------------------------------------------


@given(st.booleans())
def test_boolean_roundtrip(v):
    enc = encode_bound("boolean", v)
    assert enc == (b"\x01" if v else b"\x00")
    assert decode_bound("boolean", enc) is v


def test_boolean_decode_of_garbage_is_lenient():
    # Pinned wart: decode treats anything != b"\x00" as True, including
    # the empty payload and multi-byte garbage. Encode never emits these,
    # so this is decode-of-invalid-input leniency, not a roundtrip hole.
    assert decode_bound("boolean", b"") is True
    assert decode_bound("boolean", b"\x02") is True
    assert decode_bound("boolean", b"\x00\x00") is True


# -- date / time / timestamp -----------------------------------------------


@given(st.dates())
def test_date_roundtrip_full_domain(v):
    enc = encode_bound("date", v)
    assert len(enc) == 4
    assert decode_bound("date", enc) == v


@given(
    st.integers((date.min - date(1970, 1, 1)).days, (date.max - date(1970, 1, 1)).days)
)
def test_date_int_passthrough(days):
    assert decode_bound("date", encode_bound("date", days)) == date(
        1970, 1, 1
    ) + timedelta(days=days)


def test_date_decode_beyond_pydate_overflows():
    # Pinned: 4-byte payloads beyond datetime.date's range raise
    # OverflowError at decode (encode of an int passes through).
    with pytest.raises(OverflowError):
        decode_bound("date", struct.pack("<i", INT32_MAX))


@given(st.times())
def test_time_roundtrip_full_domain(v):
    v = v.replace(tzinfo=None)
    enc = encode_bound("time", v)
    assert len(enc) == 8
    assert decode_bound("time", enc) == v


def test_time_decode_out_of_day_raises():
    with pytest.raises(ValueError):
        decode_bound("time", struct.pack("<q", 24 * 3600 * 1_000_000))
    with pytest.raises(ValueError):
        decode_bound("time", encode_bound("time", -1))


@given(
    st.datetimes(
        min_value=datetime(1, 1, 1),
        max_value=datetime(9999, 12, 31, 23, 59, 59, 999999),
    )
)
def test_timestamp_roundtrip_full_domain(v):
    enc = encode_bound("timestamp", v)
    assert len(enc) == 8
    assert decode_bound("timestamp", enc) == v


@given(
    st.datetimes(
        min_value=datetime(2, 1, 1),
        max_value=datetime(9998, 12, 31),
        timezones=st.sampled_from(
            [
                UTC,
                timezone(timedelta(hours=-12)),
                timezone(timedelta(hours=14)),
                timezone(timedelta(minutes=331)),  # weird +05:31 offset
            ]
        ),
    )
)
def test_timestamptz_roundtrip_normalizes_to_utc(v):
    enc = encode_bound("timestamptz", v)
    assert len(enc) == 8
    dec = decode_bound("timestamptz", enc)
    assert dec.tzinfo == UTC
    assert dec == v  # aware comparison: same instant


@given(st.integers(-(2**60), 2**60))
def test_timestamp_micros_passthrough_encoding(micros):
    assert encode_bound("timestamp", micros) == struct.pack("<q", micros)
    assert encode_bound("timestamptz", micros) == struct.pack("<q", micros)


def test_timestamp_decode_2_pow_62_overflows():
    # Pinned: encode accepts any int64 micros; decode of instants beyond
    # datetime.max raises OverflowError.
    with pytest.raises(OverflowError):
        decode_bound("timestamp", struct.pack("<q", 2**62))


# -- string -----------------------------------------------------------------


@given(st.text())  # hypothesis text() excludes surrogates; includes astral
def test_string_roundtrip(v):
    enc = encode_bound("string", v)
    assert enc == v.encode("utf-8")
    assert decode_bound("string", enc) == v


@given(
    st.text(
        alphabet=st.characters(min_codepoint=0x10000, max_codepoint=0x10FFFF),
        min_size=1,
    )
)
def test_string_astral_planes_roundtrip(v):
    assert decode_bound("string", encode_bound("string", v)) == v


@given(st.integers(0xD800, 0xDFFF))
def test_string_lone_surrogates_rejected(cp):
    with pytest.raises(UnicodeEncodeError):
        encode_bound("string", chr(cp))


def test_string_decode_invalid_utf8_raises():
    with pytest.raises(UnicodeDecodeError):
        decode_bound("string", b"\xff\xfe")


def test_string_empty_is_empty_payload():
    assert encode_bound("string", "") == b""
    assert decode_bound("string", b"") == ""


def test_string_bytes_input_passes_through_unvalidated():
    # Pinned wart: encode accepts bytes verbatim without UTF-8
    # validation, so encode(b"\xff") produces a payload decode rejects.
    assert encode_bound("string", b"\xff") == b"\xff"
    with pytest.raises(UnicodeDecodeError):
        decode_bound("string", encode_bound("string", b"\xff"))


# -- uuid -------------------------------------------------------------------


@given(st.binary(min_size=16, max_size=16))
def test_uuid_roundtrip_all_bit_patterns(raw):
    u = _uuid.UUID(bytes=raw)
    enc = encode_bound("uuid", u)
    assert enc == raw and len(enc) == 16
    assert decode_bound("uuid", enc) == u
    # str and raw-bytes inputs agree with the UUID input
    assert encode_bound("uuid", str(u)) == enc
    assert encode_bound("uuid", raw) == enc


@given(st.binary(max_size=32).filter(lambda b: len(b) != 16))
def test_uuid_wrong_length_rejected(raw):
    with pytest.raises(ValueError):
        encode_bound("uuid", raw)


def test_uuid_edge_patterns():
    for raw in (b"\x00" * 16, b"\xff" * 16, b"\x80" + b"\x00" * 15):
        assert encode_bound("uuid", _uuid.UUID(bytes=raw)) == raw


# -- binary -----------------------------------------------------------------


@given(st.binary(max_size=256))
def test_binary_roundtrip(v):
    assert decode_bound("binary", encode_bound("binary", v)) == v


def test_binary_empty():
    assert encode_bound("binary", b"") == b""
    assert decode_bound("binary", b"") == b""


# -- decimal ----------------------------------------------------------------


def _minimal(b: bytes) -> bool:
    if len(b) <= 1:
        return True
    if b[0] == 0x00 and b[1] < 0x80:
        return False  # redundant leading zero
    # not a redundant sign extension
    return not (b[0] == 0xFF and b[1] >= 0x80)


@given(st.integers(-(10**38) + 1, 10**38 - 1), st.integers(0, 38))
def test_decimal_encode_exact_and_minimal(unscaled, scale):
    enc = encode_bound("decimal", unscaled, {"scale": scale})
    assert int.from_bytes(enc, "big", signed=True) == unscaled
    assert _minimal(enc)
    assert len(enc) <= 17  # precision-38 unscaled fits 16 bytes + sign slack


@given(st.integers(-(10**27), 10**27), st.integers(0, 10))
def test_decimal_roundtrip_within_default_context(unscaled, scale):
    enc = encode_bound("decimal", unscaled, {"scale": scale})
    with localcontext() as ctx:
        ctx.prec = 60
        expected = Decimal(unscaled).scaleb(-scale)
    assert decode_bound("decimal", enc, {"scale": scale}) == expected


# Regression (fixed 2026-09-05): >28-digit unscaled values were silently
# rounded by the ambient decimal context on decode.
@given(st.integers(10**28, 10**38 - 1), st.integers(0, 38))
def test_decimal_decode_wide_unscaled_is_exact_regression(unscaled, scale):
    enc = encode_bound("decimal", unscaled, {"scale": scale})
    with localcontext() as ctx:
        ctx.prec = 60
        expected = Decimal(unscaled).scaleb(-scale)
    assert decode_bound("decimal", enc, {"scale": scale}) == expected
    assert decode_bound("decimal", enc, {"scale": 0}) == unscaled


def test_decimal_precision38_boundary_values_encode():
    hi = 10**38 - 1
    assert len(encode_bound("decimal", hi, {"scale": 0})) == 16
    assert len(encode_bound("decimal", -hi, {"scale": 0})) == 16
    # canonical sign-byte boundary
    assert encode_bound("decimal", 128, {"scale": 0}) == b"\x00\x80"
    assert encode_bound("decimal", -128, {"scale": 0}) == b"\x80"
    assert encode_bound("decimal", -129, {"scale": 0}) == b"\xff\x7f"


@given(
    st.decimals(
        allow_nan=False,
        allow_infinity=False,
        places=2,
        min_value=Decimal("-1e20"),
        max_value=Decimal("1e20"),
    )
)
def test_decimal_from_decimal_value_roundtrip(v):
    enc = encode_bound("decimal", v, {"precision": 25, "scale": 2})
    assert decode_bound("decimal", enc, {"scale": 2}) == v


def test_decimal_scale_overflow_rejected():
    with pytest.raises(ValueError):
        encode_bound("decimal", Decimal("0.001"), {"scale": 2})


# -- canonical byte-level second-preimage sanity ---------------------------


@given(
    st.sampled_from(
        ["int", "long", "float", "double", "date", "time", "timestamp", "timestamptz"]
    )
)
def test_fixed_width_table(col_type):
    widths = {
        "int": 4,
        "long": 8,
        "float": 4,
        "double": 8,
        "date": 4,
        "time": 8,
        "timestamp": 8,
        "timestamptz": 8,
    }
    sample = {
        "int": 1,
        "long": 1,
        "float": 1.0,
        "double": 1.0,
        "date": 1,
        "time": time(1, 2, 3),
        "timestamp": 1,
        "timestamptz": 1,
    }[col_type]
    assert len(encode_bound(col_type, sample)) == widths[col_type]


def test_unknown_coltype_rejected_both_directions():
    with pytest.raises(ValueError):
        encode_bound("varchar", "x")
    with pytest.raises(ValueError):
        decode_bound("varchar", b"x")
