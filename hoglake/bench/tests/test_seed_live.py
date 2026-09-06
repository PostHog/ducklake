"""Live smoke for `hoglake-bench seed`: a 0.1 GB warehouse against a
real server, verified from the server's own /tables info.

Skips cleanly when nothing answers at HOGLAKE_URL (same convention as
the rest of the suite).
"""

from __future__ import annotations

import os
import uuid

import pytest
from pyhoglake import HoglakeClient, S3Config

from hoglake_bench.cli import main
from hoglake_bench.seed.budget import UNIT_BYTES, WAREHOUSE_SHAPE

pytestmark = pytest.mark.integration

SMOKE_GB = 0.1


@pytest.fixture(scope="module")
def seeded(live_server_url):
    """Seed one throwaway catalog for the whole module."""
    catalog = f"seed-smoke-{uuid.uuid4().hex[:8]}"
    rc = main(
        [
            "seed",
            "--gb",
            str(SMOKE_GB),
            "--server",
            live_server_url,
            "--catalog",
            catalog,
        ]
    )
    assert rc == 0, f"hoglake-bench seed exited {rc}"
    client = HoglakeClient(
        live_server_url,
        s3=S3Config(
            access_key=os.environ.get("HOGLAKE_S3_ACCESS_KEY", "hoglake"),
            secret_key=os.environ.get("HOGLAKE_S3_SECRET_KEY", "hoglake123"),
            endpoint_override=os.environ.get(
                "HOGLAKE_S3_ENDPOINT", "http://localhost:19000"
            ),
            region="us-east-1",
        ),
    )
    try:
        yield client.catalog(catalog)
    finally:
        client.close()


def _info(catalog, key):
    namespace, table = key.split(".", 1)
    return catalog.namespace(namespace).table(table).info()


def test_every_table_exists_with_plausible_counts(seeded):
    for spec in WAREHOUSE_SHAPE:
        info = _info(seeded, spec.key)
        assert info.record_count > 0, spec.key
        assert info.file_count >= 1, spec.key
        assert info.file_size_bytes > 0, spec.key
        # bytes per row must be sane for parquet — a broken writer that
        # registers empty or absurd files would show up here
        assert 1 < info.file_size_bytes / info.record_count < 5_000, spec.key


def test_total_volume_lands_near_the_target(seeded):
    total = sum(_info(seeded, spec.key).file_size_bytes for spec in WAREHOUSE_SHAPE)
    target = SMOKE_GB * 10 * UNIT_BYTES
    assert 0.75 * target < total < 1.25 * target, total


def test_events_are_partitioned_and_written_as_many_files(seeded):
    table = seeded.namespace("events").table("pageviews")
    info = table.info()
    spec = info.partition_spec
    assert spec is not None
    assert [f.transform for f in spec.fields] == ["identity", "month"]

    files = table.files()
    assert len(files) > 6, "the event stream should be many files, not a few"
    assert all(f.partition_values is not None for f in files)
    assert all(len(f.partition_values) == 2 for f in files)
    # a coarse months x team_id grid: several distinct partitions, and
    # every file belongs to exactly one of them
    assert len({tuple(f.partition_values) for f in files}) > 6
    assert all(f.stats_state == "provided" for f in files)


def test_dims_are_single_small_files(seeded):
    for key in ("warehouse.dim_users", "warehouse.dim_products", "warehouse.dim_teams"):
        info = _info(seeded, key)
        assert info.file_count == 1, key
        assert info.file_size_bytes < 20_000_000, key


def test_row_ids_tile_every_table(seeded):
    """Seeded data must satisfy the row-lineage contract like any other
    writer: contiguous ranges tiling [0, total)."""
    for spec in WAREHOUSE_SHAPE:
        namespace, name = spec.key.split(".", 1)
        table = seeded.namespace(namespace).table(name)
        next_start = 0
        for f in sorted(table.files(), key=lambda f: f.row_id_start):
            assert f.row_id_start == next_start, spec.key
            next_start += f.record_count
        assert next_start == table.info().record_count, spec.key
