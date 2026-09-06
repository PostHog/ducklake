"""Typed alter-op helpers for :meth:`pyhoglake.Table.alter`.

Usage::

    from pyhoglake import ops
    table.alter([
        ops.add_column("score", pa.float64()),
        ops.rename_column("old", "new"),
    ])
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

import pyarrow as pa

from .models import PartitionField
from .types import arrow_type_to_coltype


@dataclass(frozen=True)
class AlterOp:
    op: str
    body: dict[str, Any] = field(default_factory=dict)

    def to_wire(self) -> dict[str, Any]:
        return {"op": self.op, **self.body}


def _column_def(
    name: str, type_: pa.DataType | str, nullable: bool = True
) -> dict[str, Any]:
    if isinstance(type_, pa.DataType):
        col_type, params = arrow_type_to_coltype(type_)
    else:
        col_type, params = type_, None
    col: dict[str, Any] = {"name": name, "type": col_type, "nullable": nullable}
    if params:
        col["type_params"] = params
    return col


def add_column(name: str, type_: pa.DataType | str, nullable: bool = True) -> AlterOp:
    """Add a column. ``type_`` is a pyarrow DataType or a hoglake type name."""
    return AlterOp("add_column", {"column": _column_def(name, type_, nullable)})


def drop_column(name: str) -> AlterOp:
    return AlterOp("drop_column", {"name": name})


def rename_column(from_: str, to: str) -> AlterOp:
    return AlterOp("rename_column", {"from": from_, "to": to})


def promote_column(name: str, to: str) -> AlterOp:
    """Widen a column's type (e.g. int -> long). ``to`` is a hoglake type name."""
    return AlterOp("promote_column", {"name": name, "to": to})


def rename_table(new_name: str) -> AlterOp:
    return AlterOp("rename_table", {"new_name": new_name})


def partition_field(
    source_field_id: int, transform: str, transform_param: int | None = None
) -> PartitionField:
    return PartitionField(
        source_field_id=source_field_id,
        transform=transform,
        transform_param=transform_param,
    )


def set_partition_spec(
    fields: list[PartitionField | dict[str, Any]],
) -> AlterOp:
    """Set the partition spec. An empty list makes the table unpartitioned."""
    wire = [f.to_wire() if isinstance(f, PartitionField) else dict(f) for f in fields]
    return AlterOp("set_partition_spec", {"fields": wire})
