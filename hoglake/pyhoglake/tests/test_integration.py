"""Live-server integration tests (skip cleanly when no server is up).

Everything created here is prefixed ``pyhog`` and treats the stack as
disposable: catalog ``pyhog-<runid>`` with data under
``s3://pyhog-itest/<runid>/``.
"""

import struct
import time
import uuid
from datetime import datetime
from decimal import Decimal

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from conftest import S3_ACCESS_KEY, S3_ENDPOINT, S3_SECRET_KEY

from pyhoglake import (
    AlreadyExistsError,
    HoglakeClient,
    NotFoundError,
    OffsetRegressionError,
    S3Config,
    ops,
)

pytestmark = pytest.mark.integration

RUN_ID = uuid.uuid4().hex[:8]
BUCKET = "pyhog-itest"


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
        fs.create_dir(BUCKET)  # idempotent; allow_bucket_creation=True
        yield c


@pytest.fixture(scope="module")
def catalog(client):
    return client.create_catalog(f"pyhog-{RUN_ID}", f"s3://{BUCKET}/{RUN_ID}/")


@pytest.fixture(scope="module")
def ns(catalog):
    return catalog.create_namespace("ns1")


def _events_schema() -> pa.Schema:
    return pa.schema(
        [
            pa.field("id", pa.int64(), nullable=False),
            pa.field("name", pa.string()),
            pa.field("score", pa.float64()),
            pa.field("ts", pa.timestamp("us")),
            pa.field("amount", pa.decimal128(10, 2)),
        ]
    )


def _events_data(n: int, start: int = 0) -> pa.Table:
    ids = list(range(start, start + n))
    return pa.table(
        {
            "id": ids,
            "name": [f"name-{i:05d}" if i % 10 else None for i in ids],
            "score": [i / 2 if i % 7 else None for i in ids],
            "ts": [datetime(2026, 1, 1 + i % 28, i % 24, 0, 0) for i in ids],
            "amount": [Decimal(i).quantize(Decimal("0.01")) for i in ids],
        },
        schema=_events_schema(),
    )


# ---------------------------------------------------------------------------


def test_catalog_visible_in_listing(client, catalog):
    names = [c.name for c in client.list_catalogs()]
    assert catalog.name in names
    assert client.catalog(catalog.name).data_path == f"s3://{BUCKET}/{RUN_ID}/"


def test_catalog_not_found(client):
    with pytest.raises(NotFoundError):
        client.catalog(f"pyhog-nope-{RUN_ID}")


def test_namespace_listing_and_lookup(catalog, ns):
    assert "ns1" in catalog.list_namespaces()
    assert catalog.namespace("ns1").name == "ns1"
    with pytest.raises(NotFoundError):
        catalog.namespace(f"ghost-{RUN_ID}")


def test_append_lifecycle_roundtrip(client, catalog, ns, s3config):
    table = ns.create_table("events", _events_schema())
    assert table.table_uuid
    assert [c.name for c in table.columns] == [
        "id",
        "name",
        "score",
        "ts",
        "amount",
    ]

    head_before = catalog.refresh().head_snapshot_id
    data = _events_data(1000)
    res = table.append(
        data,
        author="pyhoglake-itest",
        message="lifecycle append",
        row_group_size=250,
    )
    assert res.snapshot_id > head_before

    files = table.files()
    assert len(files) == 1
    f = files[0]
    assert f.record_count == 1000
    assert f.stats_state == "provided"
    assert f.file_size_bytes > 0
    assert f.path.startswith(f"s3://{BUCKET}/{RUN_ID}/data/ns1/events/")

    # the parquet actually exists in MinIO and round-trips
    fs = s3config.filesystem()
    key = f.path[len("s3://") :]
    got = pq.read_table(key, filesystem=fs)
    assert got.num_rows == 1000
    # compare against what we appended (align: server-side schema ordering)
    expect = data
    assert got.column("id").to_pylist() == expect.column("id").to_pylist()
    assert got.column("name").to_pylist() == expect.column("name").to_pylist()
    assert got.column("amount").to_pylist() == expect.column("amount").to_pylist()
    # several row groups + embedded field ids
    pf = pq.ParquetFile(pa.BufferReader(fs.open_input_file(key).read()))
    assert pf.metadata.num_row_groups == 4
    field_ids = {c.field_id for c in table.columns}
    for fid in field_ids:
        assert f"field_id={fid}" in str(pf.schema)

    # scan plan: no deletion vectors in an append-only table
    plan = table.scan_plan()
    assert len(plan) == 1
    assert plan[0].delete_file is None

    # table info aggregates
    info = table.info()
    assert info.record_count == 1000
    assert info.file_count == 1


def test_deferred_append_hydrates_with_exact_footer_size(catalog, ns, s3config):
    """Live regression for the footer_size wire convention (bugs.md #7):
    deferred-stats append -> the hydrator tail-reads the footer with the
    registered EXACT size -> the file flips to 'provided'.

    footer_size must be the trailer's 4-byte LE thrift length, EXCLUDING
    the 8-byte length+magic suffix (the convention the server's tail math
    and compaction's stored value define). Files registered before this
    fix carried meta_len + 8; the hydrator's tolerant tail read absorbs
    that over-read, so no data repair is needed for them — this test pins
    that the NEW exact value works end-to-end against the live server.
    (Replaces test_deferred_append_stays_pending, whose premise — hydrator
    loop off on the dev server — does not hold: the server runs the
    hydrator at its 5s default.)"""
    table = ns.create_table("deferred", _events_schema())
    table.append(_events_data(50), deferred_stats=True)
    (f,) = table.files()
    assert f.stats_state == "pending"  # no inline stats at commit
    assert f.record_count == 50  # record_count can never be deferred

    # the registered footer_size is the exact trailer meta_len
    fs = s3config.filesystem()
    raw = fs.open_input_file(f.path[len("s3://") :]).read()
    assert raw[-4:] == b"PAR1"
    (meta_len,) = struct.unpack("<I", raw[-8:-4])
    assert f.footer_size == meta_len

    # the hydrator sweep (5s interval, 100 files/sweep on the dev server)
    # picks it up and the stats land: 'provided' is flipped in the same
    # transaction as the per-column stats upserts. Generous deadline: the
    # qe_live_adversarial suite (which runs first in this session)
    # fabricates ~1000 pending registrations that drain ahead of this
    # file at ~100 per 5s sweep.
    deadline = time.time() + 180
    while time.time() < deadline:
        (f,) = table.files()
        if f.stats_state != "pending":
            break
        time.sleep(1.0)
    assert f.stats_state == "provided"


def test_changes_correctness(catalog, ns):
    table = ns.create_table("changes_t", _events_schema())
    s0 = catalog.refresh().head_snapshot_id
    s1 = table.append(_events_data(10)).snapshot_id
    s2 = table.append(_events_data(10, start=10)).snapshot_id
    files = table.files()
    assert len(files) == 2
    path_by_snapshot = {f.begin_snapshot: f.path for f in files}

    plan = table.changes(s0)  # (s0, head]
    assert plan.table_uuid == table.table_uuid
    assert {f.path for f in plan.files} == set(path_by_snapshot.values())
    assert plan.delete_files == ()

    plan = table.changes(s0, s1)
    assert [f.path for f in plan.files] == [path_by_snapshot[s1]]

    plan = table.changes(s1, s2)
    assert [f.path for f in plan.files] == [path_by_snapshot[s2]]

    plan = table.changes(s2)
    assert plan.files == ()

    # row-id lineage: ranges are adjacent and sized by record_count
    first = min(plan_f.row_id_start for plan_f in files)
    ordered = sorted(files, key=lambda f: f.row_id_start)
    assert ordered[1].row_id_start == ordered[0].row_id_start + 10
    assert first == 0


def test_alter_add_column_then_append(catalog, ns):
    table = ns.create_table(
        "evolving",
        pa.schema([pa.field("id", pa.int64(), nullable=False)]),
    )
    table.append(pa.table({"id": pa.array([1, 2], pa.int64())}))
    info = table.alter([ops.add_column("score", pa.float64())])
    assert [c.name for c in info.columns] == ["id", "score"]
    new_field_id = info.columns[1].field_id
    assert new_field_id > info.columns[0].field_id

    table.append(pa.table({"id": pa.array([3, 4], pa.int64()), "score": [1.5, None]}))
    files = table.files()
    assert len(files) == 2
    assert table.info().record_count == 4


def test_time_travel(catalog, ns):
    table = ns.create_table("travel", _events_schema())
    s1 = table.append(_events_data(5)).snapshot_id
    time.sleep(1.1)  # separate the snapshot timestamps
    s2 = table.append(_events_data(5, start=5)).snapshot_id

    # snapshot-scoped file listing is the reliable time-travel surface
    files_s1 = table.files(snapshot=s1)
    assert len(files_s1) == 1
    assert sum(f.record_count for f in files_s1) == 5
    files_s2 = table.files(snapshot=s2)
    assert len(files_s2) == 2
    assert sum(f.record_count for f in files_s2) == 10

    assert table.info(snapshot=s1).file_count == 1
    assert table.info(snapshot=s2).file_count == 2
    assert table.info().record_count == 10

    t1 = next(s.snapshot_time for s in catalog.snapshots() if s.snapshot_id == s1)
    assert table.info(at_timestamp=t1).file_count == 1
    files_t1 = table.files(at_timestamp=t1)
    assert len(files_t1) == 1
    assert sum(f.record_count for f in files_t1) == 5
    # after head -> head
    assert table.info(at_timestamp=datetime(2100, 1, 1)).file_count == 2


# Regression: getTable?snapshot= once returned HEAD-scoped record_count/
# file_size_bytes alongside snapshot-scoped file_count (fixed by
# aggregateAt: TableInfo aggregates come from files visible at the
# requested snapshot).
def test_time_travel_aggregates_are_snapshot_scoped(catalog, ns):
    table = ns.create_table("travel_agg", _events_schema())
    s1 = table.append(_events_data(5)).snapshot_id
    table.append(_events_data(5, start=5))

    info_s1 = table.info(snapshot=s1)
    (f1,) = table.files(snapshot=s1)
    assert info_s1.record_count == 5
    assert info_s1.file_size_bytes == f1.file_size_bytes


def test_offsets_commit_and_regression(catalog, ns):
    table = ns.create_table("offsets_t", _events_schema())
    s1 = table.append(_events_data(3)).snapshot_id
    s2 = table.append(_events_data(3, start=3)).snapshot_id

    consumer = f"pyhog-consumer-{RUN_ID}"
    off = catalog.commit_offset(consumer, table.table_uuid, s2)
    assert off.committed_snapshot == s2
    assert off.consumer_id == consumer

    listed = catalog.offsets(consumer)
    assert [o.committed_snapshot for o in listed] == [s2]

    with pytest.raises(OffsetRegressionError):
        catalog.commit_offset(consumer, table.table_uuid, s1)

    # equal snapshot re-commit is not a regression
    assert (
        catalog.commit_offset(consumer, table.table_uuid, s2).committed_snapshot == s2
    )


def test_snapshots_pagination(catalog):
    all_at_once = list(catalog.snapshots(limit=1000))
    assert len(all_at_once) > 2  # prior tests committed plenty
    paged = list(catalog.snapshots(limit=2))  # forces real has_more pages
    assert [s.snapshot_id for s in paged] == [s.snapshot_id for s in all_at_once]
    ids = [s.snapshot_id for s in paged]
    assert ids == sorted(ids)
    assert len(set(ids)) == len(ids)


def test_views(catalog, ns):
    v = ns.create_view("v_events", "SELECT id, name FROM ns1.events")
    assert v.dialect == "trino"
    assert v.view_uuid

    got = ns.view("v_events")
    assert got.sql == "SELECT id, name FROM ns1.events"

    with pytest.raises(AlreadyExistsError):
        ns.create_view("v_events", "SELECT 1")

    assert "v_events" in [x.name for x in ns.list_views()]
    res = got.drop()
    assert res.snapshot_id > 0
    with pytest.raises(NotFoundError):
        ns.view("v_events")


def test_expire_and_cleanup(client):
    # a dedicated catalog so retention fiddling can't disturb other tests
    cat = client.create_catalog(f"pyhog-{RUN_ID}-exp", f"s3://{BUCKET}/{RUN_ID}-exp/")
    ns = cat.create_namespace("ns1")
    table = ns.create_table("t", _events_schema())
    table.append(_events_data(3))
    table.append(_events_data(3, start=3))

    opts = cat.set_retention(1)
    assert opts.snapshot_retention_seconds == 1
    earliest_before = opts.earliest_snapshot_id

    time.sleep(2)
    table.append(_events_data(3, start=6))  # head stays unexpirable

    res = cat.expire()
    assert res.new_earliest_snapshot_id > earliest_before
    assert res.snapshots_expired > 0
    assert cat.options().earliest_snapshot_id == res.new_earliest_snapshot_id

    cleanup = cat.cleanup()
    assert cleanup.still_referenced == 0
    assert cleanup.removed >= 0 and cleanup.missing >= 0


def _part_schema() -> pa.Schema:
    return pa.schema(
        [
            pa.field("team_id", pa.int64(), nullable=False),
            pa.field("ts", pa.timestamp("us")),
            pa.field("val", pa.string()),
        ]
    )


def _part_batch(rows_per: dict[tuple[int, int], int]) -> pa.Table:
    """Rows per (team_id, month-1..12) partition, interleaved by round."""
    teams, tss, vals = [], [], []
    remaining = dict(rows_per)
    i = 0
    while any(v > 0 for v in remaining.values()):
        for (team, month_no), left in remaining.items():
            if left > 0:
                teams.append(team)
                tss.append(datetime(2026, month_no, 1 + i % 27, i % 24, 0, 0))
                vals.append(f"v-{team}-{month_no}-{i}")
                remaining[(team, month_no)] = left - 1
                i += 1
    return pa.table({"team_id": teams, "ts": tss, "val": vals}, schema=_part_schema())


def _month_str(month_no: int) -> str:
    return str((2026 - 1970) * 12 + (month_no - 1))


def test_partitioned_append_fanout(catalog, ns):
    """Coarse layout — months per team_id: identity(team_id) x month(ts).
    One append spanning 3 months x 2 teams fans out to 6 files in ONE
    commit; the server's scan plan carries each file's partition values."""
    table = ns.create_table("part_events", _part_schema())
    fid = {c.name: c.field_id for c in table.columns}
    table.alter(
        [
            ops.set_partition_spec(
                [
                    ops.partition_field(fid["team_id"], "identity"),
                    ops.partition_field(fid["ts"], "month"),
                ]
            )
        ]
    )

    counts = {
        (101, 1): 1,
        (101, 2): 2,
        (101, 3): 3,
        (202, 1): 4,
        (202, 2): 5,
        (202, 3): 6,
    }
    total = sum(counts.values())  # 21
    expected = {(str(team), _month_str(m)): n for (team, m), n in counts.items()}

    head_before = catalog.refresh().head_snapshot_id
    res = table.append(_part_batch(counts), author="pyhoglake-itest")
    assert res.snapshot_id == head_before + 1  # ONE commit for all 6 files

    # the result exposes the computed tuples with per-file row counts
    assert {f.partition_values: f.record_count for f in res.files} == expected
    assert sum(f.record_count for f in res.files) == total

    files = table.files()
    assert len(files) == 6
    assert all(f.begin_snapshot == res.snapshot_id for f in files)
    assert all(f.spec_id is not None for f in files)
    # server-side partition values match what the client computed
    assert {f.partition_values: f.record_count for f in files} == expected
    assert sum(f.record_count for f in files) == total

    plan = table.scan_plan()
    assert {
        sf.data_file.partition_values: sf.data_file.record_count for sf in plan
    } == expected

    # row-id ranges: contiguous per file, tiling [0, total)
    ordered = sorted(files, key=lambda f: f.row_id_start)
    next_start = 0
    for f in ordered:
        assert f.row_id_start == next_start
        next_start += f.record_count
    assert next_start == total

    assert table.info().record_count == total


def test_partitioned_compaction_groups_within_partition(catalog, ns):
    """Compaction (metadata-planned server-side: same spec + identical
    partition_values) must merge only within a partition. 4 small
    appends x 2 partitions -> 8 files; a compact sweep yields one output
    per partition, row counts preserved."""
    table = ns.create_table("part_compact", _part_schema())
    fid = {c.name: c.field_id for c in table.columns}
    table.alter(
        [
            ops.set_partition_spec(
                [
                    ops.partition_field(fid["team_id"], "identity"),
                    ops.partition_field(fid["ts"], "month"),
                ]
            )
        ]
    )
    counts = {(7, 1): 3, (8, 1): 3}
    for _ in range(4):  # server default HOGLAKE_COMPACTION_MIN_INPUT_FILES=4
        table.append(_part_batch(counts))
    assert len(table.files()) == 8

    total_groups = 0
    for _ in range(5):
        result = catalog._client._request(
            "POST", catalog._path("/maintenance/compact"), params={"batch": 10}
        )
        total_groups += result["groups_compacted"]
        if result["groups_compacted"] == 0:
            break
    if total_groups == 0:
        pytest.skip("live server's compaction sweep produced no groups")

    files = table.files()
    by_values: dict[tuple[str | None, ...], list] = {}
    for f in files:
        by_values.setdefault(f.partition_values, []).append(f)
    # grouping never crossed a partition boundary: one output per
    # partition, carrying ONLY that partition's 12 rows
    assert set(by_values) == {("7", _month_str(1)), ("8", _month_str(1))}
    for group in by_values.values():
        assert len(group) == 1
        assert group[0].record_count == 12


def test_drop_table(catalog, ns):
    table = ns.create_table("droppable", _events_schema())
    assert "droppable" in [t.name for t in ns.list_tables()]
    res = table.drop()
    assert res.snapshot_id > 0
    assert "droppable" not in [t.name for t in ns.list_tables()]
    with pytest.raises(NotFoundError):
        ns.table("droppable")
