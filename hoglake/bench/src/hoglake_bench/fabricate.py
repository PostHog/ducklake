"""Fabricated file registrations for control-plane-only load.

The commit endpoint is footer-shipping: the server registers file paths
and writer-supplied stats without ever opening the file. That means the
control plane can be driven at full speed with parquet that does not
exist — paths are unique URIs under the catalog's data_path, stats are
well-formed so the file registers as ``provided`` (never ``pending``,
which would send the hydrator chasing ghosts in S3).
"""

from __future__ import annotations

import uuid
from typing import Any

import pyarrow as pa
from pyhoglake import Catalog, Table
from pyhoglake.bounds import encode_bound
from pyhoglake.models import ColumnStats

BENCH_SCHEMA = pa.schema(
    [
        pa.field("id", pa.int64(), nullable=False),
        pa.field("v", pa.float64()),
    ]
)


def fabricated_files(
    catalog: Catalog,
    table: Table,
    count: int,
    record_count: int,
    file_size_bytes: int = 64 * 1024,
) -> list[dict[str, Any]]:
    """Build ``count`` FileRegistration dicts (fake paths, real-shaped
    stats) for a table created with BENCH_SCHEMA."""
    cols = {c.name: c for c in table.columns}
    id_fid = cols["id"].field_id
    v_fid = cols["v"].field_id
    files = []
    for _ in range(count):
        stats = [
            ColumnStats(
                field_id=id_fid,
                value_count=record_count,
                null_count=0,
                lower_bound=encode_bound("long", 0),
                upper_bound=encode_bound("long", record_count - 1),
            ),
            ColumnStats(
                field_id=v_fid,
                value_count=record_count,
                null_count=0,
                nan_count=0,
                lower_bound=encode_bound("double", 0.0),
                upper_bound=encode_bound("double", 1.0),
            ),
        ]
        files.append(
            {
                "path": f"{catalog.data_path}data/{table.namespace}/"
                f"{table.name}/{uuid.uuid4().hex}.parquet",
                "record_count": record_count,
                "file_size_bytes": file_size_bytes,
                "footer_size": 512,
                "column_stats": [s.to_wire() for s in stats],
            }
        )
    return files


def append_payload(
    catalog: Catalog,
    table: Table,
    files_per_commit: int,
    record_count: int,
    *,
    read_snapshot: int | None = None,
) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "appends": [
            {
                "namespace": table.namespace,
                "table": table.name,
                "files": fabricated_files(
                    catalog, table, files_per_commit, record_count
                ),
            }
        ]
    }
    if read_snapshot is not None:
        payload["read_snapshot"] = read_snapshot
    return payload


def dv_payload(
    catalog: Catalog,
    table: Table,
    data_file_id: int,
    delete_count: int,
    read_snapshot: int,
) -> dict[str, Any]:
    return {
        "read_snapshot": read_snapshot,
        "deletes": [
            {
                "namespace": table.namespace,
                "table": table.name,
                "files": [
                    {
                        "data_file_id": data_file_id,
                        "path": f"{catalog.data_path}dv/"
                        f"{uuid.uuid4().hex}.puffin",
                        "delete_count": delete_count,
                        "file_size_bytes": 4096,
                    }
                ],
            }
        ],
    }
