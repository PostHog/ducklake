"""Projection / schema-diff logic (fail-fast, lesson #6)."""

import pytest
from fakes import col

from hedgerow.halts import SchemaMismatchError
from hedgerow.projection import validate_projection

SRC = (
    col("id", "long", 1, 0, nullable=False),
    col("name", "string", 2, 1),
    col("team_id", "long", 3, 2),
    col("amount", "decimal", 4, 3, type_params={"precision": 10, "scale": 2}),
)


def test_identity_projection():
    plan = validate_projection(SRC, SRC)
    assert plan.dest_columns == ("id", "name", "team_id", "amount")
    assert plan.read_columns == plan.dest_columns


def test_subset_drops_extra_source_columns():
    dest = (col("id", "long", 1, 0, nullable=False), col("name", "string", 2, 1))
    plan = validate_projection(SRC, dest)
    assert plan.dest_columns == ("id", "name")
    assert plan.read_columns == ("id", "name")


def test_dest_ordinal_order_defines_output_order():
    dest = (col("name", "string", 9, 1), col("id", "long", 8, 0, nullable=False))
    plan = validate_projection(SRC, dest)
    assert plan.dest_columns == ("id", "name")  # by dest ordinal, not input order


def test_filter_column_added_to_read_set_when_dropped():
    dest = (col("id", "long", 1, 0, nullable=False),)
    plan = validate_projection(SRC, dest, filter_column="team_id")
    assert plan.read_columns == ("id", "team_id")
    assert plan.dest_columns == ("id",)


def test_filter_column_not_duplicated_when_projected():
    plan = validate_projection(SRC, SRC, filter_column="team_id")
    assert plan.read_columns == ("id", "name", "team_id", "amount")


def test_missing_column_diff():
    dest = (col("id", "long", 1, 0, nullable=False), col("ghost", "string", 2, 1))
    with pytest.raises(SchemaMismatchError) as ei:
        validate_projection(SRC, dest)
    msg = str(ei.value)
    assert "ghost: missing from source" in msg
    assert "1 problem(s)" in msg


def test_type_mismatch_diff_names_both_types():
    dest = (col("id", "long", 1, 0, nullable=False), col("name", "long", 2, 1))
    with pytest.raises(SchemaMismatchError) as ei:
        validate_projection(SRC, dest)
    assert "name: type mismatch (source: string, destination: long)" in str(ei.value)


def test_type_params_participate_in_type_identity():
    dest = (col("amount", "decimal", 4, 0, type_params={"precision": 12, "scale": 2}),)
    with pytest.raises(SchemaMismatchError) as ei:
        validate_projection(SRC, dest)
    msg = str(ei.value)
    assert "amount: type mismatch" in msg
    assert "precision=10" in msg and "precision=12" in msg


def test_nullability_narrowing_rejected():
    dest = (col("name", "string", 2, 0, nullable=False),)
    with pytest.raises(
        SchemaMismatchError, match="nullable but destination is NOT NULL"
    ):
        validate_projection(SRC, dest)


def test_nullability_widening_ok():
    dest = (col("id", "long", 1, 0, nullable=True),)
    plan = validate_projection(SRC, dest)
    assert plan.dest_columns == ("id",)


def test_missing_filter_column_reported():
    dest = (col("id", "long", 1, 0, nullable=False),)
    with pytest.raises(SchemaMismatchError, match="filter column 'nope': missing"):
        validate_projection(SRC, dest, filter_column="nope")


def test_all_problems_reported_at_once():
    dest = (
        col("ghost", "string", 1, 0),
        col("name", "long", 2, 1),
        col("id", "long", 3, 2, nullable=False),
    )
    with pytest.raises(SchemaMismatchError) as ei:
        validate_projection(SRC, dest, filter_column="also_missing")
    msg = str(ei.value)
    assert "3 problem(s)" in msg
    assert "ghost" in msg and "name" in msg and "also_missing" in msg
