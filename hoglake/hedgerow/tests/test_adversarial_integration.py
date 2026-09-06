"""Live-server regression for BUG-1 (see test_adversarial.py) — the
destination name-rebind race, exercised against the real pyhoglake write
path and now FIXED.

pyhoglake Table.append ships a name-addressed commit payload; the fix
puts ``expected_table_uuid`` ON the commit body, where the server
enforces it atomically at commit time (409, zero writes, on mismatch),
with one client-side pre-flight re-resolve kept as an upload-saving
fast-fail. hedgerow passes its pinned destination uuid on every append,
so a destination drop+recreate mid-window HALTS the cycle
(IncarnationChangedError, nonzero exit code) with the offset untouched —
instead of splitting appends across incarnations and silently committing
the offset over lost rows (the pre-fix behavior, quantified 2026-09-05:
150-row window, 50 rows stranded in the dropped incarnation, offset
covering all 150).

The old residual race (a recreate landing between the client's final
re-resolve and the server processing the commit) is CLOSED by the
server-side guard; the unit-level pin for exactly that gap is
test_adversarial.py::test_recreate_after_preflight_before_commit_halts_with_zero_rows.

Everything created here is ``hedgerow-qe-`` prefixed and disposable.
"""

import uuid

import pyarrow as pa
import pytest
from conftest import S3_ACCESS_KEY, S3_ENDPOINT, S3_SECRET_KEY
from pyhoglake import HoglakeClient, S3Config

from hedgerow import (
    DestinationConfig,
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
BUCKET = "hedgerow-qe-itest"


@pytest.fixture(scope="module")
def client(live_server_url):
    s3c = S3Config(
        access_key=S3_ACCESS_KEY,
        secret_key=S3_SECRET_KEY,
        endpoint_override=S3_ENDPOINT,
        region="us-east-1",
        allow_bucket_creation=True,
    )
    with HoglakeClient(live_server_url, s3=s3c) as c:
        s3c.filesystem().create_dir(BUCKET)  # idempotent
        yield c


def test_dest_recreate_mid_window_live(live_server_url, client):
    schema = pa.schema([pa.field("id", pa.int64(), nullable=False)])
    src_cat = client.create_catalog(
        f"hedgerow-qe-{RUN_ID}-rebind-src", f"s3://{BUCKET}/{RUN_ID}/src/"
    )
    dst_cat = client.create_catalog(
        f"hedgerow-qe-{RUN_ID}-rebind-dst", f"s3://{BUCKET}/{RUN_ID}/dst/"
    )
    src_ns = src_cat.create_namespace("ns1")
    dst_ns = dst_cat.create_namespace("ns1")
    src = src_ns.create_table("t", schema)
    dst_ns.create_table("t", schema)

    start = src_cat.refresh().head_snapshot_id
    src.append(pa.table({"id": list(range(150))}, schema=schema))

    s3s = S3Settings(
        endpoint=S3_ENDPOINT, access_key=S3_ACCESS_KEY, secret_key=S3_SECRET_KEY
    )
    cfg = HedgerowConfig(
        source=SourceConfig(
            url=live_server_url,
            catalog=src_cat.name,
            namespace="ns1",
            table="t",
            consumer_id=f"hedgerow-qe-rebind-{RUN_ID}",
            start_snapshot=start,
            s3=s3s,
        ),
        destination=DestinationConfig(
            url=live_server_url,
            catalog=dst_cat.name,
            namespace="ns1",
            table="t",
            s3=s3s,
        ),
        replication=ReplicationConfig(
            poll_interval_s=0.0,
            max_snapshot_window=1000,
            max_rows_per_append=50,  # 150 rows -> 3 appends
        ),
        metrics=MetricsConfig(port=0),
    )
    daemon = Hedgerow(cfg)
    daemon.start()

    real_reader = daemon._batch_reader
    state = {"yields": 0}

    def hostile_reader(path, columns, batch_size):
        for b in real_reader(path, columns, batch_size):
            state["yields"] += 1
            if state["yields"] == 3:
                # operator drops + recreates the destination while the
                # window is mid-flight (between append 1 and append 2)
                dst_ns.table("t").drop()
                dst_ns.create_table("t", schema)
            yield b

    daemon._batch_reader = hostile_reader

    try:
        # The write path enforces the pinned dest uuid: the pre-flight
        # fast-fails or the server's atomic expected_table_uuid guard
        # 409s the commit — either way the cycle HALTS with a nonzero
        # exit code and the offset never moves.
        with pytest.raises(IncarnationChangedError) as ei:
            daemon.run_once()
        assert ei.value.exit_code not in (0, None)  # nonzero-exit halt
        offs = src_cat.offsets(f"hedgerow-qe-rebind-{RUN_ID}")
        assert [o for o in offs if o.committed_snapshot > start] == []
        # nothing was ever committed into the NEW incarnation: the guard
        # refused before the commit landed
        assert dst_ns.table("t").info().record_count == 0
    finally:
        daemon.close()
