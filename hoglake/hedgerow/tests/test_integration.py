"""Live-server integration tests (skip cleanly when no server is up).

Everything created here is ``hedgerow-``-prefixed and disposable:
catalogs ``hedgerow-<runid>-src`` / ``hedgerow-<runid>-dst`` with data
under ``s3://hedgerow-itest/<runid>/``. Source and destination are
separate catalogs on the same server, so destination appends never move
the source head.

Tests drive ``Hedgerow.run_once()`` synchronously (no background loop).
"""

import uuid

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from conftest import S3_ACCESS_KEY, S3_ENDPOINT, S3_SECRET_KEY
from pyhoglake import HoglakeClient, S3Config

from hedgerow import (
    DeletesPresentError,
    DestinationConfig,
    FilterConfig,
    Hedgerow,
    HedgerowConfig,
    IncarnationChangedError,
    MetricsConfig,
    ReplicationConfig,
    SourceConfig,
)
from hedgerow.config import S3Settings

pytestmark = pytest.mark.integration

RUN_ID = uuid.uuid4().hex[:8]
BUCKET = "hedgerow-itest"


def _s3_settings() -> S3Settings:
    return S3Settings(
        endpoint=S3_ENDPOINT,
        access_key=S3_ACCESS_KEY,
        secret_key=S3_SECRET_KEY,
        path_style=True,
    )


@pytest.fixture(scope="module")
def s3config() -> S3Config:
    return S3Config(
        access_key=S3_ACCESS_KEY,
        secret_key=S3_SECRET_KEY,
        endpoint_override=S3_ENDPOINT,
        region="us-east-1",
        allow_bucket_creation=True,
    )


@pytest.fixture(scope="module")
def client(live_server_url, s3config):
    with HoglakeClient(live_server_url, s3=s3config) as c:
        fs = s3config.filesystem()
        fs.create_dir(BUCKET)  # idempotent
        yield c


@pytest.fixture(scope="module")
def src_catalog(client):
    return client.create_catalog(
        f"hedgerow-{RUN_ID}-src", f"s3://{BUCKET}/{RUN_ID}/src/"
    )


@pytest.fixture(scope="module")
def dst_catalog(client):
    return client.create_catalog(
        f"hedgerow-{RUN_ID}-dst", f"s3://{BUCKET}/{RUN_ID}/dst/"
    )


@pytest.fixture(scope="module")
def src_ns(src_catalog):
    return src_catalog.create_namespace("ns1")


@pytest.fixture(scope="module")
def dst_ns(dst_catalog):
    return dst_catalog.create_namespace("ns1")


def _schema() -> pa.Schema:
    return pa.schema(
        [
            pa.field("id", pa.int64(), nullable=False),
            pa.field("team_id", pa.int64()),
            pa.field("name", pa.string()),
        ]
    )


def _dest_schema() -> pa.Schema:  # drops team_id: projection under test
    return pa.schema(
        [
            pa.field("id", pa.int64(), nullable=False),
            pa.field("name", pa.string()),
        ]
    )


def _data(n: int, start: int = 0, team: int | None = None) -> pa.Table:
    ids = list(range(start, start + n))
    return pa.table(
        {
            "id": ids,
            "team_id": [team if team is not None else i % 3 for i in ids],
            "name": [f"name-{i:05d}" if i % 7 else None for i in ids],
        },
        schema=_schema(),
    )


def _make_config(
    live_server_url: str,
    src_catalog,
    dst_catalog,
    src_table: str,
    dst_table: str,
    consumer_id: str,
    *,
    start_snapshot: int | None = None,
    max_window: int = 1000,
    max_rows: int = 100_000,
    filter_cfg: FilterConfig | None = None,
) -> HedgerowConfig:
    if start_snapshot is None:
        start_snapshot = src_catalog.refresh().head_snapshot_id
    return HedgerowConfig(
        source=SourceConfig(
            url=live_server_url,
            catalog=src_catalog.name,
            namespace="ns1",
            table=src_table,
            consumer_id=consumer_id,
            start_snapshot=start_snapshot,
            s3=_s3_settings(),
        ),
        destination=DestinationConfig(
            url=live_server_url,
            catalog=dst_catalog.name,
            namespace="ns1",
            table=dst_table,
            s3=_s3_settings(),
        ),
        replication=ReplicationConfig(
            poll_interval_s=0.0,
            max_snapshot_window=max_window,
            max_rows_per_append=max_rows,
        ),
        metrics=MetricsConfig(port=0),
        filter=filter_cfg,
    )


def _read_table_back(table, s3config: S3Config, columns: list[str]) -> pa.Table:
    fs = s3config.filesystem()
    parts = []
    for f in table.files():
        parts.append(pq.read_table(f.path[len("s3://") :], filesystem=fs))
    if not parts:
        return pa.table({c: [] for c in columns})
    merged = pa.concat_tables(parts).select(columns)
    return merged.sort_by("id")


# ---------------------------------------------------------------------------


def test_end_to_end_three_windows(
    live_server_url, client, src_catalog, dst_catalog, src_ns, dst_ns, s3config
):
    src = src_ns.create_table("e2e", _schema())
    dst = dst_ns.create_table("e2e", _schema())
    cfg = _make_config(
        live_server_url, src_catalog, dst_catalog, "e2e", "e2e", f"hedge-e2e-{RUN_ID}"
    )
    daemon = Hedgerow(cfg)
    daemon.start()

    batches = [_data(40, 0), _data(35, 40), _data(25, 75)]
    results = []
    for b in batches:
        src.append(b, author="itest", message="source batch")
        results.append(daemon.run_once())

    # three real windows, each committed through its plan.to_snapshot
    assert all(not r.idle for r in results)
    assert [r.files for r in results] == [1, 1, 1]
    assert [r.rows_appended for r in results] == [40, 35, 25]
    assert results[0].to_snapshot == results[1].from_snapshot
    assert results[1].to_snapshot == results[2].from_snapshot

    # offset in the SOURCE catalog is at the last window's end
    offs = src_catalog.offsets(f"hedge-e2e-{RUN_ID}")
    assert [o.committed_snapshot for o in offs if o.table_uuid == src.table_uuid] == [
        results[2].to_snapshot
    ]

    # row-count + content equality, read back from the dest's own parquet
    expect = pa.concat_tables(batches).sort_by("id")
    got = _read_table_back(dst, s3config, ["id", "team_id", "name"])
    assert got.num_rows == 100
    assert got.equals(expect)

    # caught up: next cycle idles
    assert daemon.run_once().idle
    daemon.close()


def test_filter_and_projection(
    live_server_url, client, src_catalog, dst_catalog, src_ns, dst_ns, s3config
):
    src = src_ns.create_table("filt", _schema())
    dst = dst_ns.create_table("filt", _dest_schema())  # team_id dropped
    cfg = _make_config(
        live_server_url,
        src_catalog,
        dst_catalog,
        "filt",
        "filt",
        f"hedge-filt-{RUN_ID}",
        filter_cfg=FilterConfig(column="team_id", equals=1),
    )
    daemon = Hedgerow(cfg)

    data = _data(60, 0)  # team_id = i % 3 -> 20 rows with team 1
    src.append(data)
    result = daemon.run_once()
    assert result.rows_read == 60
    assert result.rows_appended == 20

    got = _read_table_back(dst, s3config, ["id", "name"])
    expect_ids = [i for i in range(60) if i % 3 == 1]
    assert got.column("id").to_pylist() == expect_ids
    # content equality on the projected columns
    expect = data.filter(pa.compute.equal(data["team_id"], 1)).select(["id", "name"])
    assert got.equals(expect.sort_by("id"))
    daemon.close()


def test_resume_from_offset_across_restart(
    live_server_url, client, src_catalog, dst_catalog, src_ns, dst_ns, s3config
):
    src = src_ns.create_table("resume", _schema())
    dst = dst_ns.create_table("resume", _schema())
    consumer = f"hedge-resume-{RUN_ID}"
    cfg = _make_config(
        live_server_url, src_catalog, dst_catalog, "resume", "resume", consumer
    )

    daemon1 = Hedgerow(cfg)
    src.append(_data(10, 0))
    r1 = daemon1.run_once()
    assert r1.rows_appended == 10
    daemon1.close()  # "process exits"

    src.append(_data(10, 10))

    # fresh process: a new Hedgerow must resume from the COMMITTED offset,
    # not from start_snapshot, and must not duplicate window 1
    daemon2 = Hedgerow(cfg)
    r2 = daemon2.run_once()
    assert r2.from_snapshot == r1.to_snapshot
    assert r2.rows_appended == 10

    got = _read_table_back(dst, s3config, ["id", "team_id", "name"])
    assert got.num_rows == 20  # no duplicates on clean handoff
    assert got.column("id").to_pylist() == list(range(20))
    assert daemon2.run_once().idle
    daemon2.close()


def test_lag_metric_sanity(
    live_server_url, client, src_catalog, dst_catalog, src_ns, dst_ns
):
    prom = pytest.importorskip("prometheus_client")
    from hedgerow.metrics import PrometheusMetrics

    src = src_ns.create_table("lag", _schema())
    dst_ns.create_table("lag", _schema())
    cfg = _make_config(
        live_server_url,
        src_catalog,
        dst_catalog,
        "lag",
        "lag",
        f"hedge-lag-{RUN_ID}",
        max_window=1,
    )
    registry = prom.CollectorRegistry()
    daemon = Hedgerow(
        cfg, metrics=PrometheusMetrics(0, registry=registry, start_server=False)
    )
    daemon.start()

    src.append(_data(5, 0))
    src.append(_data(5, 5))  # two snapshots of backlog

    r1 = daemon.run_once()  # window of 1: lag must remain
    assert r1.lag_snapshots >= 1
    assert registry.get_sample_value("hedgerow_lag_snapshots") == r1.lag_snapshots
    assert (
        registry.get_sample_value("hedgerow_last_committed_snapshot")
        == r1.committed_offset
    )

    # drain: lag reaches 0 and the gauge follows
    while daemon.run_once().lag_snapshots > 0:
        pass
    assert registry.get_sample_value("hedgerow_lag_snapshots") == 0
    assert registry.get_sample_value("hedgerow_rows_replicated_total") == 10
    daemon.close()


def test_halt_on_delete(
    live_server_url, client, src_catalog, dst_catalog, src_ns, dst_ns
):
    src = src_ns.create_table("deltest", _schema())
    dst_ns.create_table("deltest", _schema())
    cfg = _make_config(
        live_server_url,
        src_catalog,
        dst_catalog,
        "deltest",
        "deltest",
        f"hedge-del-{RUN_ID}",
    )
    daemon = Hedgerow(cfg)

    src.append(_data(10, 0))
    assert daemon.run_once().rows_appended == 10

    # Register a deletion vector on the source data file. pyhoglake has no
    # public DV-write API in 0.1, so ship the raw commit payload.
    (data_file,) = src.files()
    read_snapshot = src_catalog.refresh().head_snapshot_id
    src_catalog._commit(
        {
            "read_snapshot": read_snapshot,
            "deletes": [
                {
                    "namespace": "ns1",
                    "table": "deltest",
                    "files": [
                        {
                            "data_file_id": data_file.data_file_id,
                            "path": f"s3://{BUCKET}/{RUN_ID}/src/dv/deltest-1.puffin",
                            "delete_count": 1,
                            "file_size_bytes": 64,
                        }
                    ],
                }
            ],
        }
    )

    with pytest.raises(DeletesPresentError, match="append-only"):
        daemon.run_once()

    # nothing consumed: the offset did not move past the delete
    offs = src_catalog.offsets(f"hedge-del-{RUN_ID}")
    (committed,) = [
        o.committed_snapshot for o in offs if o.table_uuid == src.table_uuid
    ]
    assert committed < src_catalog.refresh().head_snapshot_id
    daemon.close()


def test_halt_on_source_recreate(
    live_server_url, client, src_catalog, dst_catalog, src_ns, dst_ns
):
    src = src_ns.create_table("reborn", _schema())
    dst_ns.create_table("reborn", _schema())
    cfg = _make_config(
        live_server_url,
        src_catalog,
        dst_catalog,
        "reborn",
        "reborn",
        f"hedge-reborn-{RUN_ID}",
    )
    daemon = Hedgerow(cfg)

    src.append(_data(5, 0))
    assert daemon.run_once().rows_appended == 5
    old_uuid = src.table_uuid

    src.drop()
    reborn = src_ns.create_table("reborn", _schema())  # same name, new identity
    assert reborn.table_uuid != old_uuid
    reborn.append(_data(5, 100))

    with pytest.raises(IncarnationChangedError, match="recreated"):
        daemon.run_once()
    daemon.close()
