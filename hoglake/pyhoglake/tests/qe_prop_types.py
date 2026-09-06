"""Property-based fuzzing of the arrow <-> hoglake ColType mapping.

Two totality claims under test:

1. Round-trip: every supported (coltype, type_params) maps to an arrow
   type and back to itself; every canonical arrow type is a fixed point
   after one round-trip (non-canonical spellings — large_string,
   tz-of-any-name, uuid extension — converge to a fixed point in one
   hop and never drift further).
2. Rejection completeness: every generated exotic arrow type (other
   int widths, other temporal units, decimal256, and ANY nested or
   parameterized combinator, even over supported inner types) raises
   UnsupportedTypeError — never a silent wrong mapping, never a
   different exception type.
"""

import pyarrow as pa
import pytest
from hypothesis import given
from hypothesis import strategies as st

from pyhoglake import UnsupportedTypeError, arrow_type_to_coltype, coltype_to_arrow
from pyhoglake.models import Column
from pyhoglake.types import (
    PARQUET_FIELD_ID_KEY,
    columns_to_arrow_schema,
    schema_to_column_defs,
)

# The "qe" hypothesis profile (deadline=None) is loaded in conftest.py.

# -- generators -------------------------------------------------------------

SIMPLE_COLTYPES = [
    "boolean",
    "int",
    "long",
    "float",
    "double",
    "string",
    "binary",
    "date",
    "time",
    "timestamp",
    "timestamptz",
    "uuid",
]

coltype_with_params = st.one_of(
    st.tuples(st.sampled_from(SIMPLE_COLTYPES), st.none()),
    st.tuples(
        st.just("decimal"),
        st.integers(1, 38).flatmap(
            lambda p: st.integers(0, p).map(lambda s: {"precision": p, "scale": s})
        ),
    ),
)

canonical_arrow = st.one_of(
    st.sampled_from(
        [
            pa.bool_(),
            pa.int32(),
            pa.int64(),
            pa.float32(),
            pa.float64(),
            pa.string(),
            pa.binary(),
            pa.date32(),
            pa.time64("us"),
            pa.timestamp("us"),
            pa.timestamp("us", tz="UTC"),
            pa.binary(16),
        ]
    ),
    st.integers(1, 38).flatmap(
        lambda p: st.integers(0, p).map(lambda s: pa.decimal128(p, s))
    ),
)

noncanonical_arrow = st.one_of(
    st.sampled_from([pa.large_string(), pa.large_binary()]),
    st.sampled_from(["America/New_York", "Asia/Tokyo", "+05:30", "Europe/Berlin"]).map(
        lambda tz: pa.timestamp("us", tz=tz)
    ),
)

_unsupported_scalars = [
    pa.null(),
    pa.int8(),
    pa.int16(),
    pa.uint8(),
    pa.uint16(),
    pa.uint32(),
    pa.uint64(),
    pa.float16(),
    pa.date64(),
    pa.time32("s"),
    pa.time32("ms"),
    pa.time64("ns"),
    pa.timestamp("s"),
    pa.timestamp("ms"),
    pa.timestamp("ns"),
    pa.timestamp("ns", tz="UTC"),
    pa.timestamp("s", tz="America/New_York"),
    pa.duration("s"),
    pa.duration("us"),
    pa.duration("ns"),
    pa.month_day_nano_interval(),
    pa.decimal256(40, 2),
    pa.decimal256(10, 2),
]

unsupported_scalar = st.one_of(
    st.sampled_from(_unsupported_scalars),
    # fixed_size_binary of any width except the uuid-blessed 16
    st.integers(1, 64).filter(lambda w: w != 16).map(pa.binary),
)

any_inner = st.one_of(canonical_arrow, unsupported_scalar)


def _nest(inner: st.SearchStrategy) -> st.SearchStrategy:
    return st.one_of(
        inner.map(pa.list_),
        inner.map(pa.large_list),
        st.tuples(inner, st.integers(1, 4)).map(lambda t: pa.list_(t[0], t[1])),
        inner.map(lambda t: pa.struct([("a", t)])),
        st.tuples(inner, inner).map(lambda t: pa.struct([("a", t[0]), ("b", t[1])])),
        st.tuples(canonical_arrow, inner).map(lambda t: pa.map_(pa.string(), t[1])),
        # dictionary-encoded: even dictionary<string> must be rejected
        st.sampled_from([pa.string(), pa.int64()]).map(
            lambda v: pa.dictionary(pa.int32(), v)
        ),
    )


exotic_arrow = st.recursive(
    st.one_of(unsupported_scalar, _nest(any_inner)),
    _nest,
    max_leaves=6,
)


# -- round-trip properties --------------------------------------------------


@given(coltype_with_params)
def test_coltype_to_arrow_to_coltype_is_identity(tp):
    coltype, params = tp
    arrow = coltype_to_arrow(coltype, params)
    back, back_params = arrow_type_to_coltype(arrow)
    assert back == coltype
    if coltype == "decimal":
        assert back_params == params
    else:
        assert back_params is None


@given(canonical_arrow)
def test_canonical_arrow_is_roundtrip_fixed_point(t):
    coltype, params = arrow_type_to_coltype(t)
    back = coltype_to_arrow(coltype, params)
    # uuid is the one canonical mapping that is not type-identical
    # (fixed_size_binary(16) -> "uuid" -> fixed_size_binary(16)); all
    # others must be exactly equal.
    assert back.equals(t)


@given(noncanonical_arrow)
def test_noncanonical_arrow_converges_in_one_hop(t):
    c1, p1 = arrow_type_to_coltype(t)
    a1 = coltype_to_arrow(c1, p1)
    c2, p2 = arrow_type_to_coltype(a1)
    assert (c1, p1) == (c2, p2)
    assert coltype_to_arrow(c2, p2).equals(a1)  # stable thereafter


# -- rejection completeness -------------------------------------------------


@given(exotic_arrow)
def test_exotic_types_always_raise_unsupported(t):
    with pytest.raises(UnsupportedTypeError):
        arrow_type_to_coltype(t)


@given(exotic_arrow.map(lambda t: pa.schema([pa.field("x", t)])))
def test_exotic_types_rejected_at_schema_level_too(schema):
    with pytest.raises(UnsupportedTypeError):
        schema_to_column_defs(schema)


def test_unsupported_type_error_is_typeerror_and_hoglake_error():
    from pyhoglake import HoglakeError

    try:
        arrow_type_to_coltype(pa.int8())
    except UnsupportedTypeError as e:
        assert isinstance(e, TypeError)
        assert isinstance(e, HoglakeError)
    else:
        pytest.fail("no raise")


def test_decimal_missing_type_params_raise_unsupported_not_keyerror():
    with pytest.raises(UnsupportedTypeError):
        coltype_to_arrow("decimal", None)
    with pytest.raises(UnsupportedTypeError):
        coltype_to_arrow("decimal", {"precision": 10})  # scale missing
    with pytest.raises(UnsupportedTypeError):
        coltype_to_arrow("decimal", {"scale": 2})  # precision missing


# -- schema-level properties ------------------------------------------------

col_names = st.text(
    alphabet=st.characters(codec="utf-8", exclude_characters="\x00"),
    min_size=1,
    max_size=24,
)


@given(
    st.lists(
        st.tuples(col_names, coltype_with_params, st.booleans()),
        min_size=1,
        max_size=12,
        unique_by=lambda t: t[0],
    )
)
def test_schema_to_column_defs_exact_shape(cols):
    schema = pa.schema(
        [
            pa.field(name, coltype_to_arrow(ct, params), nullable=nullable)
            for name, (ct, params), nullable in cols
        ]
    )
    defs = schema_to_column_defs(schema)
    assert len(defs) == len(cols)
    for d, (name, (ct, params), nullable) in zip(defs, cols):
        expected_keys = {"name", "type", "nullable"} | (
            {"type_params"} if ct == "decimal" else set()
        )
        assert set(d) == expected_keys  # exact wire keys, nothing more
        assert d["name"] == name
        assert d["type"] == ct
        assert d["nullable"] is nullable
        if ct == "decimal":
            assert d["type_params"] == params


@given(
    st.lists(
        st.tuples(col_names, coltype_with_params, st.booleans()),
        min_size=1,
        max_size=12,
        unique_by=lambda t: t[0],
    ),
    st.randoms(use_true_random=False),
)
def test_columns_to_arrow_schema_sorts_by_ordinal_and_embeds_field_ids(cols, rnd):
    columns = [
        Column(
            name=name,
            type=ct,
            field_id=100 + i,
            ordinal=i,
            nullable=nullable,
            type_params=params,
        )
        for i, (name, (ct, params), nullable) in enumerate(cols)
    ]
    shuffled = list(columns)
    rnd.shuffle(shuffled)
    schema = columns_to_arrow_schema(shuffled)
    assert schema.names == [c.name for c in columns]  # ordinal order restored
    for c in columns:
        f = schema.field(c.name)
        assert f.nullable is c.nullable
        assert f.metadata[PARQUET_FIELD_ID_KEY] == str(c.field_id).encode()
        assert f.type.equals(coltype_to_arrow(c.type, c.type_params))
