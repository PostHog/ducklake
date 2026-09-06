"""Column projection: the destination's columns define the projected set.

Rules (validated at startup — viaduck lesson #6, fail-fast config):

- every destination column must exist in the source by NAME and TYPE
  (hoglake type name + type_params must match exactly);
- a nullable source column cannot feed a non-nullable destination column;
- extra source columns are dropped;
- the filter column may be one of the dropped columns, but must exist in
  the source.

Violations raise :class:`~hedgerow.halts.SchemaMismatchError` with a
precise per-column diff, and the daemon refuses to start.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass

from pyhoglake import Column

from .halts import SchemaMismatchError


def _type_repr(c: Column) -> str:
    if c.type_params:
        params = ", ".join(f"{k}={v}" for k, v in sorted(c.type_params.items()))
        return f"{c.type}({params})"
    return c.type


def _types_match(src: Column, dst: Column) -> bool:
    return src.type == dst.type and (src.type_params or {}) == (dst.type_params or {})


@dataclass(frozen=True)
class ProjectionPlan:
    """What to read from source parquet and what to hand to append().

    ``read_columns``: parquet columns to read (destination set plus the
    filter column when it is otherwise dropped).
    ``dest_columns``: destination column names in destination ordinal
    order — the exact shape ``Table.append`` expects.
    """

    read_columns: tuple[str, ...]
    dest_columns: tuple[str, ...]


def validate_projection(
    source_columns: Sequence[Column],
    dest_columns: Sequence[Column],
    filter_column: str | None = None,
) -> ProjectionPlan:
    src_by_name = {c.name: c for c in source_columns}
    problems: list[str] = []

    for dst in sorted(dest_columns, key=lambda c: c.ordinal):
        src = src_by_name.get(dst.name)
        if src is None:
            problems.append(
                f"  - {dst.name}: missing from source (destination: {_type_repr(dst)})"
            )
            continue
        if not _types_match(src, dst):
            problems.append(
                f"  - {dst.name}: type mismatch "
                f"(source: {_type_repr(src)}, destination: {_type_repr(dst)})"
            )
        if src.nullable and not dst.nullable:
            problems.append(
                f"  - {dst.name}: source is nullable but destination is NOT NULL"
            )

    if filter_column is not None and filter_column not in src_by_name:
        problems.append(f"  - filter column {filter_column!r}: missing from source")

    if problems:
        raise SchemaMismatchError(
            "destination schema does not project from source "
            f"({len(problems)} problem(s)):\n" + "\n".join(problems)
        )

    dest_names = tuple(c.name for c in sorted(dest_columns, key=lambda c: c.ordinal))
    read = list(dest_names)
    if filter_column is not None and filter_column not in read:
        read.append(filter_column)
    return ProjectionPlan(read_columns=tuple(read), dest_columns=dest_names)
