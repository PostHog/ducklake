"""pyarrow <-> hoglake column-type mapping.

Supported mappings (both directions):

    pa.bool_()               <-> boolean
    pa.int32()               <-> int
    pa.int64()               <-> long
    pa.float32()             <-> float
    pa.float64()             <-> double
    pa.string()/large_string <-> string
    pa.binary()/large_binary <-> binary
    pa.date32()              <-> date
    pa.time64("us")          <-> time
    pa.timestamp("us")       <-> timestamp
    pa.timestamp("us", tz)   <-> timestamptz
    pa.decimal128(p, s)      <-> decimal  (type_params: {"precision": p, "scale": s})
    pa.binary(16) (fixed)    <-> uuid

Note on uuid: hoglake's ``uuid`` column type maps from Arrow
``fixed_size_binary(16)`` (``pa.binary(16)``) holding the UUID's 16
big-endian bytes (``uuid.UUID(...).bytes``). pyarrow's canonical uuid
extension type (``pa.uuid()``, pyarrow >= 18) is accepted on input and
treated identically.
"""

from __future__ import annotations

from typing import Any

import pyarrow as pa

from .errors import UnsupportedTypeError
from .models import Column

PARQUET_FIELD_ID_KEY = b"PARQUET:field_id"

_SUPPORTED = (
    "bool, int32, int64, float32, float64, string, large_string, binary, "
    "large_binary, fixed_size_binary(16) [uuid], date32, time64(us), "
    "timestamp(us[, tz]), decimal128"
)


def _is_uuid_extension(t: pa.DataType) -> bool:
    try:
        return t.equals(pa.uuid())  # pyarrow >= 18
    except AttributeError:  # pragma: no cover - old pyarrow
        return False


def arrow_type_to_coltype(t: pa.DataType) -> tuple[str, dict[str, Any] | None]:
    """Map an Arrow type to (hoglake column type, type_params)."""
    if pa.types.is_boolean(t):
        return "boolean", None
    if pa.types.is_int32(t):
        return "int", None
    if pa.types.is_int64(t):
        return "long", None
    if pa.types.is_float32(t):
        return "float", None
    if pa.types.is_float64(t):
        return "double", None
    if pa.types.is_string(t) or pa.types.is_large_string(t):
        return "string", None
    if _is_uuid_extension(t):
        return "uuid", None
    if pa.types.is_fixed_size_binary(t):
        if t.byte_width == 16:
            return "uuid", None
        raise UnsupportedTypeError(
            f"unsupported Arrow type {t!r}: only fixed_size_binary(16) (uuid) "
            f"is supported; supported types: {_SUPPORTED}"
        )
    if pa.types.is_binary(t) or pa.types.is_large_binary(t):
        return "binary", None
    if pa.types.is_date32(t):
        return "date", None
    if pa.types.is_time64(t):
        if t.unit == "us":
            return "time", None
        raise UnsupportedTypeError(
            f"unsupported Arrow type {t!r}: time must be time64('us'); "
            f"supported types: {_SUPPORTED}"
        )
    if pa.types.is_timestamp(t):
        if t.unit != "us":
            raise UnsupportedTypeError(
                f"unsupported Arrow type {t!r}: timestamps must have "
                f"microsecond unit; supported types: {_SUPPORTED}"
            )
        return ("timestamptz", None) if t.tz else ("timestamp", None)
    if pa.types.is_decimal128(t):
        return "decimal", {"precision": t.precision, "scale": t.scale}
    raise UnsupportedTypeError(
        f"unsupported Arrow type {t!r}; supported types: {_SUPPORTED}"
    )


def coltype_to_arrow(
    type_: str, type_params: dict[str, Any] | None = None
) -> pa.DataType:
    """Map a hoglake column type back to an Arrow type."""
    if type_ == "boolean":
        return pa.bool_()
    if type_ == "int":
        return pa.int32()
    if type_ == "long":
        return pa.int64()
    if type_ == "float":
        return pa.float32()
    if type_ == "double":
        return pa.float64()
    if type_ == "string":
        return pa.string()
    if type_ == "binary":
        return pa.binary()
    if type_ == "date":
        return pa.date32()
    if type_ == "time":
        return pa.time64("us")
    if type_ == "timestamp":
        return pa.timestamp("us")
    if type_ == "timestamptz":
        return pa.timestamp("us", tz="UTC")
    if type_ == "uuid":
        return pa.binary(16)
    if type_ == "decimal":
        params = type_params or {}
        try:
            return pa.decimal128(int(params["precision"]), int(params["scale"]))
        except KeyError as e:
            raise UnsupportedTypeError(
                f"decimal column missing type_params key {e}"
            ) from None
    raise UnsupportedTypeError(f"unknown hoglake column type {type_!r}")


def schema_to_column_defs(schema: pa.Schema) -> list[dict[str, Any]]:
    """Convert a pyarrow schema into CreateTableRequest column defs."""
    out: list[dict[str, Any]] = []
    for f in schema:
        type_, params = arrow_type_to_coltype(f.type)
        col: dict[str, Any] = {"name": f.name, "type": type_, "nullable": f.nullable}
        if params:
            col["type_params"] = params
        out.append(col)
    return out


def columns_to_arrow_schema(columns: list[Column] | tuple[Column, ...]) -> pa.Schema:
    """Build the target Arrow schema for a table's columns, with parquet
    field ids embedded as ``PARQUET:field_id`` field metadata (pyarrow
    writes these into the parquet SchemaElement field_id slots)."""
    fields = []
    for c in sorted(columns, key=lambda c: c.ordinal):
        fields.append(
            pa.field(
                c.name,
                coltype_to_arrow(c.type, c.type_params),
                nullable=c.nullable,
                metadata={PARQUET_FIELD_ID_KEY: str(c.field_id).encode("ascii")},
            )
        )
    return pa.schema(fields)
