"""Per-column stats extraction from an in-memory parquet footer.

Operates on the ``pyarrow.parquet.FileMetaData`` the writer already holds
(never re-reads the file from object storage): value/null counts summed
across row groups; min/max only when every row group carries min/max
statistics for the column; bounds encoded to Iceberg single-value binary
by the catalog column type.
"""

from __future__ import annotations

import struct

import pyarrow.parquet as pq

from .bounds import encode_bound
from .models import Column, ColumnStats

# Column types whose bounds are IEEE floats: min/max over row-group
# bounds must use total-order semantics (see _float_total_order_key).
_FLOAT_TYPES = frozenset({"float", "double"})


def _float_total_order_key(v: float) -> int:
    """IEEE-754 total-order sort key: the semantics of Kotlin/Java's
    ``Double.compare``, under which ``-0.0 < 0.0``.

    Python's ``min``/``max`` treat ``-0.0 == 0.0`` and keep the FIRST of
    equal values, so the winning zero's sign — and therefore the encoded
    bound bytes — would depend on row-group order (bugs.md #20). Mapping
    the float's bit pattern (sign-magnitude) to a monotone integer makes
    the choice deterministic: a min bound prefers -0.0 over +0.0, a max
    bound prefers +0.0 over -0.0, regardless of order.
    """
    (bits,) = struct.unpack("<q", struct.pack("<d", v))
    return bits if bits >= 0 else -(bits & 0x7FFFFFFFFFFFFFFF) - 1


def extract_column_stats(
    metadata: pq.FileMetaData, columns: list[Column] | tuple[Column, ...]
) -> list[ColumnStats]:
    by_name = {c.name: c for c in columns}

    # Map parquet leaf index -> top-level column name (flat schemas only;
    # nested types are not in the supported type set).
    n_cols = metadata.num_columns
    leaf_names: list[str] = []
    if metadata.num_row_groups > 0:
        rg0 = metadata.row_group(0)
        for j in range(n_cols):
            # Full path, verbatim: for the flat schemas we support the leaf
            # path IS the column name — splitting on "." misattributed a
            # top-level column literally named "a.b" (QE find, 2026-09-05).
            leaf_names.append(rg0.column(j).path_in_schema)

    out: list[ColumnStats] = []
    for j, name in enumerate(leaf_names):
        col = by_name.get(name)
        if col is None:
            continue  # file column not in the catalog schema; nothing to report

        value_count = 0
        null_count = 0
        size_bytes = 0
        have_null_counts = True
        have_min_max = True
        mins: list = []
        maxs: list = []

        for r in range(metadata.num_row_groups):
            rg = metadata.row_group(r)
            chunk = rg.column(j)
            value_count += rg.num_rows
            size_bytes += chunk.total_compressed_size
            st = chunk.statistics
            if st is None or not st.has_null_count:
                have_null_counts = False
                have_min_max = False
                continue
            null_count += st.null_count
            if st.has_min_max and rg.num_rows > st.null_count:
                mins.append(st.min)
                maxs.append(st.max)
            elif rg.num_rows > st.null_count:
                have_min_max = False
            # an all-null row group legitimately has no min/max; skip it

        if not have_null_counts:
            # Cannot honestly report this column; omit it entirely rather
            # than ship a fabricated null_count.
            continue

        lower = upper = None
        if have_min_max and mins:
            if col.type in _FLOAT_TYPES:
                lo = min(mins, key=_float_total_order_key)
                hi = max(maxs, key=_float_total_order_key)
            else:
                lo = min(mins)
                hi = max(maxs)
            lower = encode_bound(col.type, lo, col.type_params)
            upper = encode_bound(col.type, hi, col.type_params)

        out.append(
            ColumnStats(
                field_id=col.field_id,
                value_count=value_count,
                null_count=null_count,
                size_bytes=size_bytes,
                lower_bound=lower,
                upper_bound=upper,
            )
        )
    return out
