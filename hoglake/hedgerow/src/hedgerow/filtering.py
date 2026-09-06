"""Client-side row filter: keep rows where ``column == equals``.

The routing_value analog from the viaduck world, minus everything else:
one column, one equality, evaluated per record batch with Arrow compute.
NULLs never match. The filter value is validated (castable to the source
column's Arrow type) at startup — fail-fast, lesson #6.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass

import pyarrow as pa
import pyarrow.compute as pc
from pyhoglake import Column, coltype_to_arrow

from .config import ConfigError, FilterConfig


@dataclass(frozen=True)
class RowFilter:
    column: str
    value: pa.Scalar

    def apply(self, batch: pa.RecordBatch) -> pa.RecordBatch:
        mask = pc.equal(batch.column(self.column), self.value)
        mask = pc.fill_null(mask, False)  # NULL == x is NULL: drop, don't keep
        return batch.filter(mask)


def build_filter(
    cfg: FilterConfig | None, source_columns: Sequence[Column]
) -> RowFilter | None:
    if cfg is None:
        return None
    if cfg.equals is None:
        # BUG-5 regression: pa.scalar(None, t) validates and pc.equal
        # against a null scalar is null for EVERY row -> fill_null(False)
        # drops 100% of rows while the offset advances. Refuse at
        # startup; FilterConfig.parse refuses the same way for YAML/env
        # configs, this covers direct construction.
        raise ConfigError(
            "filter.equals filter value must be non-null; a null filter "
            "matches nothing (NULLs never match an equality filter)"
        )
    col = next((c for c in source_columns if c.name == cfg.column), None)
    if col is None:
        # validate_projection reports this too; belt and braces for
        # direct construction paths.
        raise ConfigError(f"filter.column {cfg.column!r} is not a source column")
    arrow_type = coltype_to_arrow(col.type, col.type_params)
    try:
        value = pa.scalar(cfg.equals, type=arrow_type)
    except (pa.ArrowInvalid, pa.ArrowTypeError, OverflowError, TypeError) as e:
        raise ConfigError(
            f"filter.equals value {cfg.equals!r} is not valid for column "
            f"{cfg.column!r} of type {col.type} ({arrow_type}): {e}"
        ) from None
    return RowFilter(column=cfg.column, value=value)
