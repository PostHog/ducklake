"""Row-filter construction and application."""

import pyarrow as pa
import pytest
from fakes import col

from hedgerow.config import ConfigError, FilterConfig
from hedgerow.filtering import build_filter

SRC = (
    col("id", "long", 1, 0, nullable=False),
    col("team_id", "long", 2, 1),
    col("name", "string", 3, 2),
    col("active", "boolean", 4, 3),
)


def _batch(**cols) -> pa.RecordBatch:
    return pa.record_batch(cols)


def test_no_filter_config_is_none():
    assert build_filter(None, SRC) is None


def test_equals_int_filter():
    f = build_filter(FilterConfig(column="team_id", equals=42), SRC)
    batch = _batch(
        id=pa.array([1, 2, 3, 4], pa.int64()),
        team_id=pa.array([42, 7, 42, None], pa.int64()),
    )
    out = f.apply(batch)
    assert out.column("id").to_pylist() == [1, 3]


def test_nulls_never_match():
    f = build_filter(FilterConfig(column="team_id", equals=42), SRC)
    batch = _batch(
        id=pa.array([1, 2], pa.int64()),
        team_id=pa.array([None, None], pa.int64()),
    )
    assert f.apply(batch).num_rows == 0


def test_string_filter():
    f = build_filter(FilterConfig(column="name", equals="a"), SRC)
    batch = _batch(
        id=pa.array([1, 2, 3], pa.int64()),
        name=pa.array(["a", "b", None], pa.string()),
    )
    assert f.apply(batch).column("id").to_pylist() == [1]


def test_boolean_filter():
    f = build_filter(FilterConfig(column="active", equals=False), SRC)
    batch = _batch(
        id=pa.array([1, 2], pa.int64()),
        active=pa.array([True, False]),
    )
    assert f.apply(batch).column("id").to_pylist() == [2]


def test_value_cast_to_column_type():
    # YAML "42" for a long column: int scalar carries the column type
    f = build_filter(FilterConfig(column="team_id", equals=42), SRC)
    assert f.value.type == pa.int64()


def test_unknown_filter_column_rejected():
    with pytest.raises(ConfigError, match="'ghost' is not a source column"):
        build_filter(FilterConfig(column="ghost", equals=1), SRC)


def test_uncastable_filter_value_rejected():
    with pytest.raises(ConfigError, match="not valid for column 'team_id'"):
        build_filter(FilterConfig(column="team_id", equals="not-a-number"), SRC)


@pytest.mark.parametrize("value", [float("nan"), float("inf"), float("-inf")])
def test_nan_inf_filter_value_rejected_on_direct_construction(value):
    # bugs.md #4 regression, direct-construction path: pc.equal(col, nan)
    # is False for every row, so the filter silently drops 100% of rows
    # while the offset advances. Refused next to the null guard.
    with pytest.raises(ConfigError, match="finite"):
        build_filter(FilterConfig(column="team_id", equals=value), SRC)


def test_filter_preserves_schema():
    f = build_filter(FilterConfig(column="team_id", equals=1), SRC)
    batch = _batch(
        id=pa.array([1], pa.int64()),
        team_id=pa.array([1], pa.int64()),
    )
    assert f.apply(batch).schema == batch.schema
