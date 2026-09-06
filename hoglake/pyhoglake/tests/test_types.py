"""Arrow <-> hoglake ColType mapping, both directions, plus rejection."""

import pyarrow as pa
import pytest

from pyhoglake import UnsupportedTypeError, arrow_type_to_coltype, coltype_to_arrow
from pyhoglake.models import Column
from pyhoglake.types import (
    PARQUET_FIELD_ID_KEY,
    columns_to_arrow_schema,
    schema_to_column_defs,
)

FORWARD = [
    (pa.bool_(), "boolean", None),
    (pa.int32(), "int", None),
    (pa.int64(), "long", None),
    (pa.float32(), "float", None),
    (pa.float64(), "double", None),
    (pa.string(), "string", None),
    (pa.large_string(), "string", None),
    (pa.binary(), "binary", None),
    (pa.large_binary(), "binary", None),
    (pa.date32(), "date", None),
    (pa.time64("us"), "time", None),
    (pa.timestamp("us"), "timestamp", None),
    (pa.timestamp("us", tz="UTC"), "timestamptz", None),
    (pa.timestamp("us", tz="America/New_York"), "timestamptz", None),
    (pa.decimal128(10, 2), "decimal", {"precision": 10, "scale": 2}),
    (pa.binary(16), "uuid", None),
]


@pytest.mark.parametrize("arrow_type,coltype,params", FORWARD)
def test_arrow_to_coltype(arrow_type, coltype, params):
    assert arrow_type_to_coltype(arrow_type) == (coltype, params)


REVERSE = [
    ("boolean", None, pa.bool_()),
    ("int", None, pa.int32()),
    ("long", None, pa.int64()),
    ("float", None, pa.float32()),
    ("double", None, pa.float64()),
    ("string", None, pa.string()),
    ("binary", None, pa.binary()),
    ("date", None, pa.date32()),
    ("time", None, pa.time64("us")),
    ("timestamp", None, pa.timestamp("us")),
    ("timestamptz", None, pa.timestamp("us", tz="UTC")),
    ("decimal", {"precision": 10, "scale": 2}, pa.decimal128(10, 2)),
    ("uuid", None, pa.binary(16)),
]


@pytest.mark.parametrize("coltype,params,arrow_type", REVERSE)
def test_coltype_to_arrow(coltype, params, arrow_type):
    assert coltype_to_arrow(coltype, params) == arrow_type


def test_uuid_extension_type_maps_if_available():
    if not hasattr(pa, "uuid"):
        pytest.skip("pyarrow without pa.uuid()")
    assert arrow_type_to_coltype(pa.uuid()) == ("uuid", None)


REJECTED = [
    pa.int8(),
    pa.int16(),
    pa.uint32(),
    pa.uint64(),
    pa.float16(),
    pa.date64(),
    pa.time32("s"),
    pa.time64("ns"),
    pa.timestamp("ns"),
    pa.timestamp("s", tz="UTC"),
    pa.binary(8),  # fixed-size but not 16
    pa.list_(pa.int64()),
    pa.struct([("a", pa.int64())]),
    pa.map_(pa.string(), pa.int64()),
    pa.duration("us"),
]


@pytest.mark.parametrize("arrow_type", REJECTED, ids=str)
def test_rejects_unsupported(arrow_type):
    with pytest.raises(UnsupportedTypeError) as ei:
        arrow_type_to_coltype(arrow_type)
    assert "supported types" in str(ei.value)


def test_unknown_coltype_rejected():
    with pytest.raises(UnsupportedTypeError):
        coltype_to_arrow("varchar", None)


def test_schema_to_column_defs():
    schema = pa.schema(
        [
            pa.field("id", pa.int64(), nullable=False),
            pa.field("name", pa.string()),
            pa.field("amount", pa.decimal128(18, 4)),
        ]
    )
    assert schema_to_column_defs(schema) == [
        {"name": "id", "type": "long", "nullable": False},
        {"name": "name", "type": "string", "nullable": True},
        {
            "name": "amount",
            "type": "decimal",
            "nullable": True,
            "type_params": {"precision": 18, "scale": 4},
        },
    ]


def test_columns_to_arrow_schema_embeds_field_ids_and_orders_by_ordinal():
    cols = (
        Column(name="b", type="string", field_id=12, ordinal=1),
        Column(name="a", type="long", field_id=11, ordinal=0, nullable=False),
    )
    schema = columns_to_arrow_schema(cols)
    assert schema.names == ["a", "b"]
    assert schema.field("a").type == pa.int64()
    assert not schema.field("a").nullable
    assert schema.field("a").metadata[PARQUET_FIELD_ID_KEY] == b"11"
    assert schema.field("b").metadata[PARQUET_FIELD_ID_KEY] == b"12"
