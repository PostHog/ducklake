"""Byte-pattern tests for the Iceberg single-value binary codec."""

import struct
import uuid
from datetime import UTC, date, datetime, time, timedelta, timezone
from decimal import Decimal

import pytest

from pyhoglake import decode_bound, encode_bound


def test_boolean():
    assert encode_bound("boolean", True) == b"\x01"
    assert encode_bound("boolean", False) == b"\x00"
    assert decode_bound("boolean", b"\x01") is True
    assert decode_bound("boolean", b"\x00") is False


def test_int_le():
    assert encode_bound("int", 1) == b"\x01\x00\x00\x00"
    assert encode_bound("int", -1) == b"\xff\xff\xff\xff"
    assert encode_bound("int", 2**31 - 1) == b"\xff\xff\xff\x7f"
    assert decode_bound("int", b"\x01\x00\x00\x00") == 1
    assert decode_bound("int", b"\xff\xff\xff\xff") == -1


def test_long_le():
    assert encode_bound("long", 1) == b"\x01" + b"\x00" * 7
    assert encode_bound("long", -1) == b"\xff" * 8
    assert decode_bound("long", encode_bound("long", -(2**62))) == -(2**62)


def test_float_double():
    assert encode_bound("float", 1.0) == struct.pack("<f", 1.0)
    assert encode_bound("double", -2.5) == struct.pack("<d", -2.5)
    assert decode_bound("float", struct.pack("<f", 1.5)) == 1.5
    assert decode_bound("double", struct.pack("<d", -2.5)) == -2.5


def test_date_days_since_epoch():
    assert encode_bound("date", date(1970, 1, 1)) == b"\x00\x00\x00\x00"
    assert encode_bound("date", date(1970, 1, 2)) == b"\x01\x00\x00\x00"
    assert encode_bound("date", date(1969, 12, 31)) == b"\xff\xff\xff\xff"
    assert encode_bound("date", 17532) == struct.pack("<i", 17532)
    assert decode_bound("date", struct.pack("<i", 17532)) == date(2018, 1, 1)


def test_time_micros():
    assert encode_bound("time", time(0, 0, 0)) == b"\x00" * 8
    one_hour = 3600 * 1_000_000
    assert encode_bound("time", time(1, 0, 0)) == struct.pack("<q", one_hour)
    t = time(13, 37, 42, 123456)
    assert decode_bound("time", encode_bound("time", t)) == t


def test_timestamp_micros():
    dt = datetime(2026, 9, 4, 12, 0, 0, 500)
    micros = int((dt - datetime(1970, 1, 1)) / timedelta(microseconds=1))
    assert encode_bound("timestamp", dt) == struct.pack("<q", micros)
    assert encode_bound("timestamp", micros) == struct.pack("<q", micros)
    assert decode_bound("timestamp", struct.pack("<q", micros)) == dt


def test_timestamptz_converts_to_utc():
    tz = timezone(timedelta(hours=-5))
    dt = datetime(2026, 9, 4, 7, 0, 0, tzinfo=tz)  # == 12:00 UTC
    utc = datetime(2026, 9, 4, 12, 0, 0, tzinfo=UTC)
    assert encode_bound("timestamptz", dt) == encode_bound("timestamptz", utc)
    assert decode_bound("timestamptz", encode_bound("timestamptz", dt)) == utc


def test_string_utf8():
    assert encode_bound("string", "ab") == b"\x61\x62"
    assert encode_bound("string", "héllo") == "héllo".encode()
    assert decode_bound("string", b"\x61\x62") == "ab"


def test_uuid_big_endian():
    u = uuid.UUID("f79c3e09-677c-4bbd-a479-3f349cb785e7")
    enc = encode_bound("uuid", u)
    assert enc == u.bytes
    assert enc[0] == 0xF7  # big-endian: MSB of the hex form first
    assert encode_bound("uuid", str(u)) == u.bytes
    assert decode_bound("uuid", enc) == u


def test_binary_raw():
    assert encode_bound("binary", b"\x00\x01\xff") == b"\x00\x01\xff"
    assert decode_bound("binary", b"\x00\x01\xff") == b"\x00\x01\xff"


@pytest.mark.parametrize(
    "unscaled,expected",
    [
        (0, b"\x00"),
        (1, b"\x01"),
        (127, b"\x7f"),
        (128, b"\x00\x80"),  # needs the sign byte
        (255, b"\x00\xff"),
        (256, b"\x01\x00"),
        (-1, b"\xff"),
        (-128, b"\x80"),  # fits in one byte
        (-129, b"\xff\x7f"),
        (1234, b"\x04\xd2"),
        (-1234, b"\xfb\x2e"),
    ],
)
def test_decimal_minimal_twos_complement(unscaled, expected):
    assert encode_bound("decimal", unscaled, {"scale": 2}) == expected


def test_decimal_from_decimal_value():
    assert (
        encode_bound("decimal", Decimal("12.34"), {"precision": 10, "scale": 2})
        == b"\x04\xd2"
    )
    assert (
        encode_bound("decimal", Decimal("-1.00"), {"precision": 10, "scale": 2})
        == b"\x9c"
    )
    assert decode_bound("decimal", b"\x04\xd2", {"scale": 2}) == Decimal("12.34")


def test_decimal_scale_mismatch_raises():
    with pytest.raises(ValueError):
        encode_bound("decimal", Decimal("1.234"), {"scale": 2})
