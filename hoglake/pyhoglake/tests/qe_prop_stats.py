"""Property-based fuzzing of footer stats extraction.

Generated tables (random types, random null placement, random row-group
splits) -> extract_column_stats must agree with ground truth computed
independently from the raw Python values.

Pinned policies (verified here):

* value_count is the row count (nulls included), null_count is exact,
  for every row-group split.
* Bounds decode back to exactly the true min/max over non-null values.
* NaN: parquet-cpp excludes NaN from min/max, so bounds never contain
  NaN; a column whose only non-null values are NaN gets NO bounds.
  nan_count is NEVER populated by pyhoglake (always absent from the
  wire) — the server can only ever see nan_count from other clients.
* All-null column -> null_count honest, no bounds. All-null row group
  does not suppress bounds from other groups.
* 0-row file -> no column_stats at all (pyarrow writes one empty row
  group without per-chunk statistics objects).
* Strings longer than parquet-cpp's 4096-byte statistics cap -> that
  row group has no min/max -> bounds omitted entirely (never a
  truncated — i.e. WRONG — upper bound).
* A file column absent from the catalog schema is skipped.
"""

import io
import struct
from datetime import date, datetime
from decimal import Decimal

import pyarrow as pa
import pyarrow.parquet as pq
from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st

from pyhoglake import decode_bound
from pyhoglake.models import Column
from pyhoglake.stats import extract_column_stats
from pyhoglake.types import columns_to_arrow_schema

# The "qe" hypothesis profile (deadline=None) is loaded in conftest.py.
# Parquet round-trips are expensive; cap the main property explicitly.
STATS_SETTINGS = settings(
    max_examples=30,
    suppress_health_check=[HealthCheck.too_slow, HealthCheck.data_too_large],
)


# (coltype, type_params, value strategy) — value domains deliberately
# include the type boundaries and nasty values.
COLUMN_KINDS = {
    "long": (None, st.integers(-(2**63), 2**63 - 1)),
    "int": (None, st.integers(-(2**31), 2**31 - 1)),
    "double": (
        None,
        st.floats(allow_nan=False, allow_infinity=True, allow_subnormal=True),
    ),
    "float": (
        None,
        st.floats(allow_nan=False, allow_infinity=True, width=32),
    ),
    "boolean": (None, st.booleans()),
    "string": (None, st.text(max_size=40)),  # includes "" mins
    "binary": (None, st.binary(max_size=40)),
    "date": (None, st.dates(date(1, 1, 1), date(9999, 12, 31))),
    "timestamp": (
        None,
        st.datetimes(datetime(1677, 9, 22), datetime(2262, 4, 11)),
    ),
    "decimal": (
        {"precision": 18, "scale": 3},
        st.integers(-(10**18) + 1, 10**18 - 1).map(lambda n: Decimal(n).scaleb(-3)),
    ),
    "uuid": (None, st.binary(min_size=16, max_size=16)),
}


@st.composite
def stats_case(draw):
    kinds = draw(
        st.lists(st.sampled_from(sorted(COLUMN_KINDS)), min_size=1, max_size=4)
    )
    n_rows = draw(st.integers(0, 40))
    columns = []
    values = []
    for i, kind in enumerate(kinds):
        params, value_st = COLUMN_KINDS[kind]
        columns.append(
            Column(
                name=f"c{i}",
                type=kind,
                field_id=i + 1,
                ordinal=i,
                nullable=True,
                type_params=params,
            )
        )
        values.append(
            draw(
                st.lists(
                    st.one_of(st.none(), value_st),
                    min_size=n_rows,
                    max_size=n_rows,
                )
            )
        )
    row_group_size = draw(st.integers(1, 41))
    return tuple(columns), values, n_rows, row_group_size


def _ground_truth_minmax(kind, vals):
    nn = [v for v in vals if v is not None]
    if not nn:
        return None, None
    return min(nn), max(nn)


def _normalize(kind, decoded):
    if kind == "uuid":
        return decoded.bytes  # ground truth is raw 16-byte strings
    return decoded


def _write_meta(table, row_group_size):
    sink = io.BytesIO()
    pq.write_table(table, sink, row_group_size=row_group_size)
    return pq.read_metadata(io.BytesIO(sink.getvalue()))


@STATS_SETTINGS
@given(stats_case())
def test_extracted_stats_match_ground_truth(case):
    columns, values, n_rows, row_group_size = case
    schema = columns_to_arrow_schema(columns)
    table = pa.table(
        {
            c.name: pa.array(v, schema.field(c.name).type)
            for c, v in zip(columns, values)
        },
        schema=schema,
    )
    meta = _write_meta(table, row_group_size)
    stats = {s.field_id: s for s in extract_column_stats(meta, columns)}

    if n_rows == 0:
        # pinned: empty file ships no column stats at all
        assert stats == {}
        return

    for c, vals in zip(columns, values):
        s = stats[c.field_id]
        assert s.value_count == n_rows
        assert s.null_count == sum(1 for v in vals if v is None)
        assert s.nan_count is None  # pinned: pyhoglake never reports it
        assert s.size_bytes is not None and s.size_bytes > 0

        lo, hi = _ground_truth_minmax(c.type, vals)
        if lo is None:
            assert s.lower_bound is None and s.upper_bound is None
            continue
        assert s.lower_bound is not None and s.upper_bound is not None
        got_lo = _normalize(c.type, decode_bound(c.type, s.lower_bound, c.type_params))
        got_hi = _normalize(c.type, decode_bound(c.type, s.upper_bound, c.type_params))
        if c.type in ("double", "float"):
            # -0.0 == 0.0 makes plain == fine, but keep inf exact
            assert got_lo == lo and got_hi == hi
        else:
            assert got_lo == lo
            assert got_hi == hi


# -- NaN policy (targeted; property above excludes NaN by construction) ----


def _double_col():
    return (Column(name="f", type="double", field_id=1, ordinal=0),)


def test_nan_values_never_reach_bounds():
    cols = _double_col()
    t = pa.table(
        {"f": pa.array([1.0, float("nan"), 3.0], pa.float64())},
        schema=columns_to_arrow_schema(cols),
    )
    (s,) = extract_column_stats(_write_meta(t, 10), cols)
    assert s.null_count == 0  # NaN is not null
    assert decode_bound("double", s.lower_bound) == 1.0
    assert decode_bound("double", s.upper_bound) == 3.0
    assert s.nan_count is None  # pinned: never computed client-side


def test_all_nan_column_has_no_bounds():
    cols = _double_col()
    t = pa.table(
        {"f": pa.array([float("nan"), float("nan")], pa.float64())},
        schema=columns_to_arrow_schema(cols),
    )
    (s,) = extract_column_stats(_write_meta(t, 10), cols)
    assert s.value_count == 2
    assert s.null_count == 0
    assert s.lower_bound is None and s.upper_bound is None


def test_negative_zero_bounds_bitpattern():
    cols = _double_col()
    t = pa.table(
        {"f": pa.array([-0.0, 0.0], pa.float64())},
        schema=columns_to_arrow_schema(cols),
    )
    (s,) = extract_column_stats(_write_meta(t, 10), cols)
    # parquet-cpp orders -0.0 < 0.0 for stats; the encoded bounds must
    # carry the sign bit
    assert s.lower_bound == struct.pack("<d", -0.0)
    assert s.lower_bound[-1] == 0x80
    assert s.upper_bound == struct.pack("<d", 0.0)


# -- oversized string stats -------------------------------------------------


def test_string_beyond_4096_stat_cap_omits_bounds_not_truncates():
    cols = (Column(name="s", type="string", field_id=1, ordinal=0),)
    big = "z" * 5000
    t = pa.table({"s": pa.array(["a", big])}, schema=columns_to_arrow_schema(cols))
    (s,) = extract_column_stats(_write_meta(t, 10), cols)
    assert s.value_count == 2 and s.null_count == 0
    # a truncated max ("zzz...z"[:4096]) would be an INVALID upper bound
    # (< the actual max); the honest answer is no bounds at all
    assert s.lower_bound is None and s.upper_bound is None


def test_string_at_4096_cap_keeps_exact_bounds():
    cols = (Column(name="s", type="string", field_id=1, ordinal=0),)
    lo, hi = "a" * 4096, "z" * 4096
    t = pa.table({"s": pa.array([lo, hi])}, schema=columns_to_arrow_schema(cols))
    (s,) = extract_column_stats(_write_meta(t, 10), cols)
    assert s.lower_bound == lo.encode()
    assert s.upper_bound == hi.encode()


# -- stats disabled entirely ------------------------------------------------


def test_write_statistics_false_yields_no_stats_rows():
    cols = (Column(name="x", type="long", field_id=1, ordinal=0),)
    t = pa.table({"x": [1, 2]}, schema=columns_to_arrow_schema(cols))
    sink = io.BytesIO()
    pq.write_table(t, sink, write_statistics=False)
    meta = pq.read_metadata(io.BytesIO(sink.getvalue()))
    # no null_count available -> the honest move is to omit the column
    assert extract_column_stats(meta, cols) == []


# -- the dot-name collision bug --------------------------------------------


def test_dot_named_column_not_misattributed_regression():
    cols = (
        Column(name="a", type="long", field_id=1, ordinal=0),
        Column(name="a.b", type="string", field_id=2, ordinal=1),
    )
    t = pa.table(
        {
            "a": pa.array([1, 2], pa.int64()),
            "a.b": pa.array(["x", "y"], pa.string()),
        }
    )
    stats = {s.field_id: s for s in extract_column_stats(_write_meta(t, 10), cols)}
    assert decode_bound("long", stats[1].lower_bound) == 1
    assert stats[2].lower_bound == b"x"


# -- misc edges -------------------------------------------------------------


def test_file_column_missing_from_catalog_is_skipped():
    cols = (Column(name="x", type="long", field_id=1, ordinal=0),)
    t = pa.table({"x": [3], "ghost": ["boo"]})
    stats = extract_column_stats(_write_meta(t, 10), cols)
    assert [s.field_id for s in stats] == [1]


def test_int64_extremes_roundtrip_through_footer():
    cols = (Column(name="x", type="long", field_id=1, ordinal=0),)
    t = pa.table(
        {"x": pa.array([-(2**63), 2**63 - 1], pa.int64())},
        schema=columns_to_arrow_schema(cols),
    )
    (s,) = extract_column_stats(_write_meta(t, 1), cols)  # 2 row groups
    assert decode_bound("long", s.lower_bound) == -(2**63)
    assert decode_bound("long", s.upper_bound) == 2**63 - 1


def test_every_row_group_all_null_still_reports_counts():
    cols = (Column(name="x", type="long", field_id=1, ordinal=0),)
    t = pa.table(
        {"x": pa.array([None] * 5, pa.int64())},
        schema=columns_to_arrow_schema(cols),
    )
    (s,) = extract_column_stats(_write_meta(t, 2), cols)
    assert s.value_count == 5
    assert s.null_count == 5
    assert s.lower_bound is None and s.upper_bound is None
