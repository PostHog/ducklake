"""Stats extraction from an in-memory parquet footer with known values."""

import io
import struct
from datetime import date, datetime
from decimal import Decimal

import pyarrow as pa
import pyarrow.parquet as pq

from pyhoglake import encode_bound
from pyhoglake.models import Column
from pyhoglake.stats import extract_column_stats
from pyhoglake.types import columns_to_arrow_schema

COLUMNS = (
    Column(name="id", type="long", field_id=1, ordinal=0, nullable=False),
    Column(name="name", type="string", field_id=2, ordinal=1),
    Column(name="score", type="double", field_id=3, ordinal=2),
    Column(name="day", type="date", field_id=4, ordinal=3),
    Column(name="ts", type="timestamp", field_id=5, ordinal=4),
    Column(
        name="amount",
        type="decimal",
        field_id=6,
        ordinal=5,
        type_params={"precision": 10, "scale": 2},
    ),
)


def _write(table: pa.Table, row_group_size: int) -> pq.FileMetaData:
    sink = io.BytesIO()
    pq.write_table(table, sink, row_group_size=row_group_size)
    return pq.read_metadata(io.BytesIO(sink.getvalue()))


def _make_table(n=100):
    ids = list(range(n))
    names = [f"name-{i:04d}" if i % 10 != 0 else None for i in ids]
    scores = [float(i) / 2 if i % 7 != 0 else None for i in ids]
    days = [date(2026, 1, 1 + (i % 28)) for i in ids]
    ts = [datetime(2026, 1, 1, 0, 0, i % 60) for i in ids]
    amounts = [Decimal(i).scaleb(-2) * 100 for i in ids]  # i.00 -> unscaled i*100
    schema = columns_to_arrow_schema(COLUMNS)
    return pa.table(
        {
            "id": ids,
            "name": names,
            "score": scores,
            "day": days,
            "ts": ts,
            "amount": amounts,
        },
        schema=schema,
    )


def test_stats_across_multiple_row_groups():
    n = 100
    table = _make_table(n)
    meta = _write(table, row_group_size=30)
    assert meta.num_row_groups == 4

    stats = {s.field_id: s for s in extract_column_stats(meta, COLUMNS)}
    assert set(stats) == {1, 2, 3, 4, 5, 6}

    s_id = stats[1]
    assert s_id.value_count == n
    assert s_id.null_count == 0
    assert s_id.lower_bound == struct.pack("<q", 0)
    assert s_id.upper_bound == struct.pack("<q", n - 1)
    assert s_id.size_bytes > 0

    s_name = stats[2]
    assert s_name.value_count == n
    assert s_name.null_count == 10  # every 10th
    assert s_name.lower_bound == b"name-0001"
    assert s_name.upper_bound == b"name-0099"

    s_score = stats[3]
    assert s_score.null_count == 15  # multiples of 7 in [0, 100)
    assert s_score.lower_bound == struct.pack("<d", 0.5)
    assert s_score.upper_bound == struct.pack("<d", 49.5)

    s_day = stats[4]
    assert s_day.lower_bound == encode_bound("date", date(2026, 1, 1))
    assert s_day.upper_bound == encode_bound("date", date(2026, 1, 28))

    s_ts = stats[5]
    assert s_ts.lower_bound == encode_bound("timestamp", datetime(2026, 1, 1, 0, 0, 0))
    assert s_ts.upper_bound == encode_bound("timestamp", datetime(2026, 1, 1, 0, 0, 59))

    s_amount = stats[6]
    assert s_amount.lower_bound == b"\x00"  # unscaled 0
    # unscaled 99*100 = 9900 -> 0x26AC
    assert s_amount.upper_bound == (9900).to_bytes(2, "big")


def test_all_null_column_has_no_bounds():
    columns = (Column(name="x", type="long", field_id=1, ordinal=0),)
    schema = columns_to_arrow_schema(columns)
    table = pa.table({"x": pa.array([None, None, None], pa.int64())}, schema=schema)
    meta = _write(table, row_group_size=2)
    (s,) = extract_column_stats(meta, columns)
    assert s.value_count == 3
    assert s.null_count == 3
    assert s.lower_bound is None
    assert s.upper_bound is None


def test_all_null_row_group_does_not_suppress_bounds():
    columns = (Column(name="x", type="long", field_id=1, ordinal=0),)
    schema = columns_to_arrow_schema(columns)
    # first row group all null, second has values
    table = pa.table({"x": pa.array([None, None, 5, 9], pa.int64())}, schema=schema)
    meta = _write(table, row_group_size=2)
    assert meta.num_row_groups == 2
    (s,) = extract_column_stats(meta, columns)
    assert s.value_count == 4
    assert s.null_count == 2
    assert s.lower_bound == struct.pack("<q", 5)
    assert s.upper_bound == struct.pack("<q", 9)


def test_unknown_file_column_ignored():
    columns = (Column(name="x", type="long", field_id=1, ordinal=0),)
    table = pa.table({"x": [1, 2], "y": ["a", "b"]})
    meta = _write(table, row_group_size=10)
    stats = extract_column_stats(meta, columns)
    assert [s.field_id for s in stats] == [1]
