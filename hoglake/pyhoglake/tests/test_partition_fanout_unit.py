"""Unit tests for partition-fanout appends: a batch against a table with
a live partition spec splits into one parquet file per distinct partition
tuple, all registered in ONE commit, each file carrying its
``partition_values`` (wire strings by key_index — opaque to the server,
which groups by equality). Writer conventions (field ids, footer stats,
exact footer_size) are unchanged from the single-file path."""

import io
import json
import struct
from datetime import datetime

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from test_append_unit import FakeS3

from pyhoglake import (
    AppendResult,
    CommitConflictError,
    HoglakeClient,
    ValidationError,
)
from pyhoglake.client import Namespace

BASE = "http://hog.test"

CATALOG_WIRE = {
    "name": "cat",
    "data_path": "s3://bkt/lake/",
    "head_snapshot_id": 5,
    "schema_version": 2,
}

# months per team_id: the near-term consumer's coarse layout —
# identity(team_id) x month(ts)
PART_TABLE_WIRE = {
    "name": "events",
    "namespace": "ns1",
    "table_uuid": "0b8ee9ba-79a1-4f3e-b7e5-6a0b6ab6f012",
    "columns": [
        {
            "name": "team_id",
            "type": "long",
            "field_id": 1,
            "ordinal": 0,
            "nullable": True,
        },
        {
            "name": "ts",
            "type": "timestamp",
            "field_id": 2,
            "ordinal": 1,
            "nullable": True,
        },
        {
            "name": "val",
            "type": "string",
            "field_id": 3,
            "ordinal": 2,
            "nullable": True,
        },
    ],
    "record_count": 0,
    "file_count": 0,
    "file_size_bytes": 0,
    "partition_spec": {
        "spec_id": 1,
        "fields": [
            {"source_field_id": 1, "transform": "identity"},
            {"source_field_id": 2, "transform": "month"},
        ],
    },
}

_TABLES_URL = f"{BASE}/v1/catalogs/cat/namespaces/ns1/tables/events"
_COMMIT_URL = f"{BASE}/v1/catalogs/cat/commit"

# months since 1970 for 2026-01/02/03
M_JAN, M_FEB, M_MAR = "672", "673", "674"


def _posted_body(httpx_mock):
    return json.loads(
        next(r for r in httpx_mock.get_requests() if r.method == "POST").content
    )


@pytest.fixture
def fake_s3():
    return FakeS3()


def _make_table(httpx_mock, fake_s3, wire=None):
    client = HoglakeClient(BASE)
    client.s3 = fake_s3
    httpx_mock.add_response(
        method="GET", url=f"{BASE}/v1/catalogs/cat", json=CATALOG_WIRE
    )
    cat = client.catalog("cat")
    httpx_mock.add_response(method="GET", url=_TABLES_URL, json=wire or PART_TABLE_WIRE)
    return client, Namespace(cat, "ns1").table("events")


@pytest.fixture
def table(httpx_mock, fake_s3):
    client, t = _make_table(httpx_mock, fake_s3)
    yield t
    client.close()


def _mock_refresh_and_commit(httpx_mock, wire=None):
    httpx_mock.add_response(method="GET", url=_TABLES_URL, json=wire or PART_TABLE_WIRE)
    httpx_mock.add_response(
        method="POST",
        url=_COMMIT_URL,
        json={"snapshot_id": 6, "schema_version": 2},
    )


def _batch():
    """3 months x 2 teams, interleaved, with uneven per-partition counts:
    (7,jan)x1, (8,jan)x2, (7,feb)x1, (8,feb)x1, (7,mar)x2, (8,mar)x1."""
    rows = [
        (7, datetime(2026, 1, 5, 1), "a"),
        (8, datetime(2026, 1, 6, 2), "b"),
        (7, datetime(2026, 2, 7, 3), "c"),
        (8, datetime(2026, 2, 8, 4), "d"),
        (7, datetime(2026, 3, 9, 5), "e"),
        (8, datetime(2026, 1, 10, 6), "f"),
        (7, datetime(2026, 3, 11, 7), "g"),
        (8, datetime(2026, 3, 12, 8), "h"),
    ]
    return pa.table(
        {
            "team_id": pa.array([r[0] for r in rows], pa.int64()),
            "ts": pa.array([r[1] for r in rows], pa.timestamp("us")),
            "val": pa.array([r[2] for r in rows], pa.string()),
        }
    )


EXPECTED_COUNTS = {
    ("7", M_JAN): 1,
    ("8", M_JAN): 2,
    ("7", M_FEB): 1,
    ("8", M_FEB): 1,
    ("7", M_MAR): 2,
    ("8", M_MAR): 1,
}


def test_fanout_six_partitions_one_commit(table, httpx_mock, fake_s3):
    _mock_refresh_and_commit(httpx_mock)
    res = table.append(_batch(), read_snapshot=5)
    assert isinstance(res, AppendResult)
    assert res.snapshot_id == 6

    posts = [r for r in httpx_mock.get_requests() if r.method == "POST"]
    assert len(posts) == 1  # ONE commit registers every file
    body = json.loads(posts[0].content)
    (append,) = body["appends"]
    assert append["expected_table_uuid"] == PART_TABLE_WIRE["table_uuid"]
    files = append["files"]
    assert len(files) == 6  # one file per distinct partition tuple
    assert len(fake_s3.files) == 6

    by_values = {tuple(f["partition_values"]): f for f in files}
    assert {k: v["record_count"] for k, v in by_values.items()} == EXPECTED_COUNTS
    assert sum(f["record_count"] for f in files) == 8

    # groups are ordered by first occurrence in the batch
    assert [tuple(f["partition_values"]) for f in files] == [
        ("7", M_JAN),
        ("8", M_JAN),
        ("7", M_FEB),
        ("8", M_FEB),
        ("7", M_MAR),
        ("8", M_MAR),
    ]

    # each parquet holds exactly its partition's rows, conventions intact
    for f in files:
        raw = fake_s3.files[f["path"][len("s3://") :]]
        assert f["file_size_bytes"] == len(raw)
        assert f["footer_size"] == struct.unpack("<I", raw[-8:-4])[0]
        pf = pq.ParquetFile(io.BytesIO(raw))
        assert "field_id=1" in str(pf.schema)  # field ids unchanged
        got = pf.read()
        assert got.num_rows == f["record_count"]
        team, month_str = f["partition_values"]
        assert set(got.column("team_id").to_pylist()) == {int(team)}
        months = {
            (ts.year - 1970) * 12 + ts.month - 1 for ts in got.column("ts").to_pylist()
        }
        assert months == {int(month_str)}
        # per-file inline stats still shipped
        stats = {s["field_id"]: s for s in f["column_stats"]}
        assert stats[1]["value_count"] == f["record_count"]


def test_fanout_result_exposes_partition_tuples(table, httpx_mock, fake_s3):
    _mock_refresh_and_commit(httpx_mock)
    res = table.append(_batch())
    assert {f.partition_values: f.record_count for f in res.files} == EXPECTED_COUNTS
    assert all(f.path.startswith("s3://bkt/lake/data/ns1/events/") for f in res.files)
    # result mirrors what went on the wire, in the same order
    body = _posted_body(httpx_mock)
    wire_files = body["appends"][0]["files"]
    assert [f.path for f in res.files] == [f["path"] for f in wire_files]
    assert [list(f.partition_values) for f in res.files] == [
        f["partition_values"] for f in wire_files
    ]


def test_unpartitioned_result_exposes_single_file(httpx_mock, fake_s3):
    wire = {k: v for k, v in PART_TABLE_WIRE.items() if k != "partition_spec"}
    client, t = _make_table(httpx_mock, fake_s3, wire=wire)
    httpx_mock.add_response(method="GET", url=_TABLES_URL, json=wire)
    httpx_mock.add_response(method="POST", url=_COMMIT_URL, json={"snapshot_id": 6})
    res = t.append(_batch())
    (f,) = res.files
    assert f.partition_values is None
    assert f.record_count == 8
    body = _posted_body(httpx_mock)
    (file_reg,) = body["appends"][0]["files"]
    assert "partition_values" not in file_reg
    client.close()


def test_fanout_null_source_value_gets_null_partition_group(table, httpx_mock, fake_s3):
    _mock_refresh_and_commit(httpx_mock)
    data = pa.table(
        {
            "team_id": pa.array([7, None, 7], pa.int64()),
            "ts": pa.array(
                [datetime(2026, 1, 5), datetime(2026, 1, 6), None],
                pa.timestamp("us"),
            ),
            "val": pa.array(["a", "b", "c"], pa.string()),
        }
    )
    res = table.append(data)
    assert {f.partition_values: f.record_count for f in res.files} == {
        ("7", M_JAN): 1,
        (None, M_JAN): 1,
        ("7", None): 1,
    }
    body = _posted_body(httpx_mock)
    wire_values = [f["partition_values"] for f in body["appends"][0]["files"]]
    assert [None, M_JAN] in wire_values  # JSON null on the wire, not "None"


def test_fanout_deferred_stats_still_ships_partition_values(table, httpx_mock, fake_s3):
    _mock_refresh_and_commit(httpx_mock)
    table.append(_batch(), deferred_stats=True)
    body = _posted_body(httpx_mock)
    for f in body["appends"][0]["files"]:
        assert "column_stats" not in f
        assert len(f["partition_values"]) == 2


def test_fanout_empty_batch_rejected(table, httpx_mock, fake_s3):
    httpx_mock.add_response(method="GET", url=_TABLES_URL, json=PART_TABLE_WIRE)
    empty = _batch().slice(0, 0)
    with pytest.raises(ValidationError, match="0 rows to partitioned table"):
        table.append(empty)
    assert fake_s3.files == {}  # nothing uploaded


def test_fanout_stale_spec_conflict_surfaces_taxonomy(table, httpx_mock, fake_s3):
    # the client computes tuples under the pre-flight spec; if the spec
    # moves before the commit lands, the server refuses and the EXISTING
    # taxonomy surfaces it (retryable CommitConflictError) — the client
    # never silently recomputes under a new spec
    httpx_mock.add_response(method="GET", url=_TABLES_URL, json=PART_TABLE_WIRE)
    httpx_mock.add_response(
        method="POST",
        url=_COMMIT_URL,
        json={"error": "commit_conflict", "detail": "concurrent DDL: alter_table"},
        status_code=409,
    )
    with pytest.raises(CommitConflictError) as ei:
        table.append(_batch())
    assert ei.value.retryable
    assert len(fake_s3.files) == 6  # uploads orphaned, cleanup's problem


def test_fanout_unknown_transform_fails_before_upload(httpx_mock, fake_s3):
    wire = dict(PART_TABLE_WIRE)
    wire["partition_spec"] = {
        "spec_id": 2,
        "fields": [{"source_field_id": 1, "transform": "squiggle"}],
    }
    client, t = _make_table(httpx_mock, fake_s3, wire=wire)
    httpx_mock.add_response(method="GET", url=_TABLES_URL, json=wire)
    with pytest.raises(ValidationError, match="unknown partition transform"):
        t.append(_batch())
    assert fake_s3.files == {}
    client.close()


def test_fanout_bucket_without_param_fails_before_upload(httpx_mock, fake_s3):
    wire = dict(PART_TABLE_WIRE)
    wire["partition_spec"] = {
        "spec_id": 2,
        "fields": [{"source_field_id": 1, "transform": "bucket"}],
    }
    client, t = _make_table(httpx_mock, fake_s3, wire=wire)
    httpx_mock.add_response(method="GET", url=_TABLES_URL, json=wire)
    with pytest.raises(ValidationError, match="requires transform_param"):
        t.append(_batch())
    assert fake_s3.files == {}
    client.close()


def test_fanout_spec_referencing_dead_column_fails(httpx_mock, fake_s3):
    wire = dict(PART_TABLE_WIRE)
    wire["partition_spec"] = {
        "spec_id": 2,
        "fields": [{"source_field_id": 99, "transform": "identity"}],
    }
    client, t = _make_table(httpx_mock, fake_s3, wire=wire)
    httpx_mock.add_response(method="GET", url=_TABLES_URL, json=wire)
    with pytest.raises(ValidationError, match="field_id 99"):
        t.append(_batch())
    assert fake_s3.files == {}
    client.close()


def test_fanout_bucket_spec(httpx_mock, fake_s3):
    # bucket(4) on team_id x month(ts): rows with the same team land in
    # the same bucket, tuples match the scalar transform exactly
    from pyhoglake.transforms import bucket

    wire = dict(PART_TABLE_WIRE)
    wire["partition_spec"] = {
        "spec_id": 3,
        "fields": [
            {"source_field_id": 1, "transform": "bucket", "transform_param": 4},
            {"source_field_id": 2, "transform": "month"},
        ],
    }
    client, t = _make_table(httpx_mock, fake_s3, wire=wire)
    httpx_mock.add_response(method="GET", url=_TABLES_URL, json=wire)
    httpx_mock.add_response(method="POST", url=_COMMIT_URL, json={"snapshot_id": 6})
    res = t.append(_batch())
    expected: dict[tuple[str, str], int] = {}
    for (team_s, m), c in EXPECTED_COUNTS.items():
        key = (str(bucket("long", int(team_s), 4)), m)
        expected[key] = expected.get(key, 0) + c  # buckets may collide
    assert {f.partition_values: f.record_count for f in res.files} == expected
    client.close()
