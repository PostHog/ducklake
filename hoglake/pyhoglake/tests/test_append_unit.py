"""Unit tests for the append writer path: parquet bytes written through a
fake filesystem, commit body shape, field ids, deferred stats, and the
name-rebind incarnation guard (BUG-1, 2026-09-05 adversarial review; now
atomic server-side — the commit ships ``expected_table_uuid`` and the
server 409s a mismatch with zero writes, the client keeping only one
cheap pre-flight re-resolve as an upload-saving fast-fail)."""

import base64
import io
import json
import struct
import uuid

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from pyhoglake import (
    HoglakeClient,
    HoglakeError,
    IncarnationChangedError,
    ValidationError,
)
from pyhoglake.client import Namespace

BASE = "http://hog.test"

CATALOG_WIRE = {
    "name": "cat",
    "data_path": "s3://bkt/lake",  # note: no trailing slash; client must add it
    "head_snapshot_id": 5,
    "schema_version": 2,
}

TABLE_WIRE = {
    "name": "events",
    "namespace": "ns1",
    "table_uuid": "0b8ee9ba-79a1-4f3e-b7e5-6a0b6ab6f012",
    "columns": [
        {"name": "id", "type": "long", "field_id": 1, "ordinal": 0, "nullable": False},
        {
            "name": "name",
            "type": "string",
            "field_id": 2,
            "ordinal": 1,
            "nullable": True,
        },
    ],
    "record_count": 0,
    "file_count": 0,
    "file_size_bytes": 0,
}


class _FakeStream(io.BytesIO):
    def __init__(self, store: dict, key: str):
        super().__init__()
        self._store = store
        self._key = key

    def close(self):
        if not self.closed:
            self._store[self._key] = self.getvalue()
        super().close()


class FakeS3:
    """Duck-typed stand-in for S3Config + pyarrow S3FileSystem."""

    def __init__(self):
        self.files: dict[str, bytes] = {}

    def filesystem(self):
        return self

    def open_output_stream(self, key: str):
        return _FakeStream(self.files, key)


@pytest.fixture
def fake_s3():
    return FakeS3()


@pytest.fixture
def table(httpx_mock, fake_s3):
    client = HoglakeClient(BASE)
    client.s3 = fake_s3
    httpx_mock.add_response(
        method="GET", url=f"{BASE}/v1/catalogs/cat", json=CATALOG_WIRE
    )
    cat = client.catalog("cat")
    httpx_mock.add_response(
        method="GET",
        url=f"{BASE}/v1/catalogs/cat/namespaces/ns1/tables/events",
        json=TABLE_WIRE,
    )
    t = Namespace(cat, "ns1").table("events")
    yield t
    client.close()


def _mock_refresh_and_commit(httpx_mock):
    # append re-resolves the table by name exactly ONCE (the pre-flight
    # fast-fail before the upload; the server-side expected_table_uuid
    # commit guard replaced the old second, post-upload re-resolve).
    # Non-reusable on purpose: a second GET would fail the mock.
    httpx_mock.add_response(
        method="GET",
        url=f"{BASE}/v1/catalogs/cat/namespaces/ns1/tables/events",
        json=TABLE_WIRE,
    )
    httpx_mock.add_response(
        method="POST",
        url=f"{BASE}/v1/catalogs/cat/commit",
        json={"snapshot_id": 6, "schema_version": 2},
    )


def test_append_full(table, httpx_mock, fake_s3):
    _mock_refresh_and_commit(httpx_mock)
    data = pa.table({"id": [1, 2, 3], "name": ["a", "b", None]})
    res = table.append(data, read_snapshot=5, author="tester", message="first")
    assert res.snapshot_id == 6

    body = json.loads(httpx_mock.get_requests()[-1].content)
    assert body["read_snapshot"] == 5
    assert body["author"] == "tester"
    assert body["message"] == "first"
    (append,) = body["appends"]
    assert append["namespace"] == "ns1"
    assert append["table"] == "events"
    # the atomic guard rides the wire: default = the pinned table_uuid
    assert append["expected_table_uuid"] == TABLE_WIRE["table_uuid"]
    (file_reg,) = append["files"]

    # exactly one file written, under the data path with the missing '/' fixed
    (key,) = fake_s3.files.keys()
    assert key.startswith("bkt/lake/data/ns1/events/")
    assert key.endswith(".parquet")
    assert file_reg["path"] == "s3://" + key

    raw = fake_s3.files[key]
    assert file_reg["record_count"] == 3
    assert file_reg["file_size_bytes"] == len(raw)
    # wire convention (bugs.md #7): footer_size == the trailer's 4-byte LE
    # thrift length EXACTLY — it EXCLUDES the 8-byte length+magic suffix
    expected_footer = struct.unpack("<I", raw[-8:-4])[0]
    assert file_reg["footer_size"] == expected_footer

    # stats: base64 Iceberg single-value bounds
    stats = {s["field_id"]: s for s in file_reg["column_stats"]}
    assert stats[1]["value_count"] == 3
    assert stats[1]["null_count"] == 0
    assert base64.b64decode(stats[1]["lower_bound"]) == struct.pack("<q", 1)
    assert base64.b64decode(stats[1]["upper_bound"]) == struct.pack("<q", 3)
    assert stats[2]["null_count"] == 1
    assert base64.b64decode(stats[2]["lower_bound"]) == b"a"
    assert base64.b64decode(stats[2]["upper_bound"]) == b"b"

    # the written parquet round-trips and carries the catalog field ids
    pf = pq.ParquetFile(io.BytesIO(raw))
    assert "field_id=1" in str(pf.schema)
    assert "field_id=2" in str(pf.schema)
    got = pf.read()
    assert got.column("id").to_pylist() == [1, 2, 3]
    assert got.column("name").to_pylist() == ["a", "b", None]
    meta = pf.schema_arrow
    assert meta.field("id").metadata[b"PARQUET:field_id"] == b"1"

    # single pre-flight shape: exactly ONE re-resolve during the append
    # (plus the fixture's initial resolve) — the old post-upload second
    # re-resolve is gone, superseded by the server-side commit guard
    table_gets = [
        r
        for r in httpx_mock.get_requests()
        if r.method == "GET" and str(r.url).endswith("/tables/events")
    ]
    assert len(table_gets) == 2


def test_append_reserved_hog_column_fast_fails(table, httpx_mock, fake_s3):
    """A user ``_hog*`` field in the append batch fast-fails client-side
    BEFORE the pre-flight resolve and the parquet upload — the server
    reserves the prefix (``_hog_row_id`` is compaction's row-id carrier),
    so shipping the file first would only waste the S3 write."""
    data = pa.table({"id": [1], "name": ["a"], "_hog_row_id": [7]})
    with pytest.raises(ValidationError, match="reserved"):
        table.append(data)
    # No request beyond the fixture's catalog + table resolves, no upload.
    assert len(httpx_mock.get_requests()) == 2
    assert fake_s3.files == {}


def test_footer_size_wire_convention():
    """bugs.md #7 regression: ``footer_size`` is the serialized thrift
    FileMetaData length — the DB convention proven by the server's
    hydrator tail math (``[file_size - footer_size - 8, file_size)``) and
    by compaction's stored value — EXCLUDING the trailing 8-byte suffix
    (4-byte LE length + b"PAR1"). pyhoglake used to ship ``meta_len + 8``;
    the hydrator's tail read absorbed the 8-byte over-read, so files
    registered with the old value need no repair — but any consumer
    treating footer_size as exact would mis-slice every client-written
    file. Pinned against a real pyarrow-written file with the trailer
    parsed by hand."""
    from pyhoglake.client import _footer_size

    sink = io.BytesIO()
    pq.write_table(pa.table({"id": [1, 2, 3], "name": ["a", "b", None]}), sink)
    raw = sink.getvalue()

    # parse the parquet trailer ourselves: ... [metadata][len LE32]["PAR1"]
    assert raw[-4:] == b"PAR1"
    (meta_len,) = struct.unpack("<I", raw[-8:-4])

    assert _footer_size(raw) == meta_len  # NOT meta_len + 8
    # and the value really delimits the serialized footer: the region the
    # server's tail read covers, [file_size - footer_size - 8, file_size),
    # starts exactly at the thrift metadata
    footer_region = raw[len(raw) - meta_len - 8 : len(raw) - 8]
    assert len(footer_region) == meta_len


def test_append_deferred_stats(table, httpx_mock, fake_s3):
    _mock_refresh_and_commit(httpx_mock)
    data = pa.table({"id": [1], "name": ["x"]})
    table.append(data, deferred_stats=True)
    body = json.loads(httpx_mock.get_requests()[-1].content)
    (file_reg,) = body["appends"][0]["files"]
    assert "column_stats" not in file_reg
    assert file_reg["record_count"] == 1
    assert "read_snapshot" not in body  # blind append


def test_append_reorders_and_casts(table, httpx_mock, fake_s3):
    _mock_refresh_and_commit(httpx_mock)
    # columns out of order; ints as int32 needing an upcast to long
    data = pa.table(
        {
            "name": pa.array(["z"], pa.string()),
            "id": pa.array([7], pa.int32()),
        }
    )
    table.append(data)
    (key,) = fake_s3.files.keys()
    pf = pq.ParquetFile(io.BytesIO(fake_s3.files[key]))
    assert pf.schema_arrow.names == ["id", "name"]
    assert pf.schema_arrow.field("id").type == pa.int64()


def test_append_missing_column_rejected(table, httpx_mock):
    httpx_mock.add_response(
        method="GET",
        url=f"{BASE}/v1/catalogs/cat/namespaces/ns1/tables/events",
        json=TABLE_WIRE,
    )
    with pytest.raises(ValidationError, match="missing table columns"):
        table.append(pa.table({"id": [1]}))


def test_append_extra_column_rejected(table, httpx_mock):
    httpx_mock.add_response(
        method="GET",
        url=f"{BASE}/v1/catalogs/cat/namespaces/ns1/tables/events",
        json=TABLE_WIRE,
    )
    with pytest.raises(ValidationError, match="not in the table schema"):
        table.append(pa.table({"id": [1], "name": ["a"], "ghost": [1]}))


# -- incarnation guard (BUG-1 regression: name-rebind race; the guard is
# -- now ATOMIC at commit — expected_table_uuid on the commit body, 409
# -- with zero writes on mismatch — with one pre-flight kept as an
# -- upload-saving fast-fail) ------------------------------------------------

REBOUND_WIRE = dict(TABLE_WIRE, table_uuid="9d1c2f34-0000-4000-8000-000000000bad")
_TABLES_URL = f"{BASE}/v1/catalogs/cat/namespaces/ns1/tables/events"
_COMMIT_URL = f"{BASE}/v1/catalogs/cat/commit"

RECREATED_409 = {
    "error": "commit_conflict",
    "detail": (
        "expected_table_uuid mismatch: the table was recreated "
        f"(expected {TABLE_WIRE['table_uuid']})"
    ),
}


def test_append_rebind_detected_before_upload(table, httpx_mock, fake_s3):
    # the pre-flight fast-fail sees the new incarnation: no S3 write
    httpx_mock.add_response(method="GET", url=_TABLES_URL, json=REBOUND_WIRE)
    with pytest.raises(IncarnationChangedError, match="recreated"):
        table.append(pa.table({"id": [1], "name": ["a"]}))
    assert fake_s3.files == {}  # nothing uploaded
    assert not [r for r in httpx_mock.get_requests() if r.method == "POST"]


def test_append_rebind_after_preflight_is_409d_by_the_server(
    table, httpx_mock, fake_s3
):
    # THE previously-racy half: the pre-flight passes, the table is
    # recreated before the commit lands, and the SERVER's atomic
    # expected_table_uuid guard 409s the commit ("the table was
    # recreated") with zero writes. The client maps it to
    # IncarnationChangedError.
    httpx_mock.add_response(method="GET", url=_TABLES_URL, json=TABLE_WIRE)
    httpx_mock.add_response(
        method="POST", url=_COMMIT_URL, json=RECREATED_409, status_code=409
    )
    with pytest.raises(IncarnationChangedError, match="commit_conflict") as ei:
        table.append(pa.table({"id": [1], "name": ["a"]}))
    assert ei.value.status_code == 409
    assert "the table was recreated" in (ei.value.detail or "")
    assert not ei.value.retryable
    assert len(fake_s3.files) == 1  # parquet orphaned in the bucket


def test_append_ordinary_commit_conflict_stays_retryable(table, httpx_mock, fake_s3):
    # a 409 that does NOT indicate recreation keeps the existing
    # taxonomy: CommitConflictError, retryable
    from pyhoglake import CommitConflictError

    httpx_mock.add_response(method="GET", url=_TABLES_URL, json=TABLE_WIRE)
    httpx_mock.add_response(
        method="POST",
        url=_COMMIT_URL,
        json={"error": "commit conflict", "detail": "concurrent DDL"},
        status_code=409,
    )
    with pytest.raises(CommitConflictError) as ei:
        table.append(pa.table({"id": [1], "name": ["a"]}))
    assert ei.value.retryable


def test_append_server_refusal_does_not_rebase_pinned_identity(
    table, httpx_mock, fake_s3
):
    # a server-side guard refusal must leave the Table pinned to the
    # ORIGINAL uuid (the pre-flight saw the old incarnation and adopted
    # nothing new)
    httpx_mock.add_response(method="GET", url=_TABLES_URL, json=TABLE_WIRE)
    httpx_mock.add_response(
        method="POST", url=_COMMIT_URL, json=RECREATED_409, status_code=409
    )
    with pytest.raises(IncarnationChangedError):
        table.append(pa.table({"id": [1], "name": ["a"]}))
    assert table.table_uuid == TABLE_WIRE["table_uuid"]


def test_append_rebind_does_not_rebase_pinned_identity(table, httpx_mock, fake_s3):
    # after a refused append, the Table object still pins the ORIGINAL
    # uuid — a naive retry must trip the guard again, not silently adopt
    # the new incarnation.
    httpx_mock.add_response(method="GET", url=_TABLES_URL, json=REBOUND_WIRE)
    with pytest.raises(IncarnationChangedError):
        table.append(pa.table({"id": [1], "name": ["a"]}))
    assert table.table_uuid == TABLE_WIRE["table_uuid"]
    httpx_mock.add_response(method="GET", url=_TABLES_URL, json=REBOUND_WIRE)
    with pytest.raises(IncarnationChangedError):
        table.append(pa.table({"id": [1], "name": ["a"]}))


def test_append_explicit_expected_table_uuid(table, httpx_mock, fake_s3):
    # an explicit pin (uuid.UUID accepted) overrides the object's own
    httpx_mock.add_response(method="GET", url=_TABLES_URL, json=TABLE_WIRE)
    with pytest.raises(IncarnationChangedError, match="recreated"):
        table.append(
            pa.table({"id": [1], "name": ["a"]}),
            expected_table_uuid=uuid.UUID("9d1c2f34-0000-4000-8000-000000000bad"),
        )
    assert fake_s3.files == {}


def test_append_explicit_expected_table_uuid_match_commits(table, httpx_mock, fake_s3):
    _mock_refresh_and_commit(httpx_mock)
    res = table.append(
        pa.table({"id": [1], "name": ["a"]}),
        expected_table_uuid=uuid.UUID(TABLE_WIRE["table_uuid"]),
    )
    assert res.snapshot_id == 6
    assert len(fake_s3.files) == 1
    body = json.loads(httpx_mock.get_requests()[-1].content)
    (append,) = body["appends"]
    # an explicit pin is what goes on the wire
    assert append["expected_table_uuid"] == TABLE_WIRE["table_uuid"]


def test_append_unguarded_sends_no_field_and_skips_the_guard(
    table, httpx_mock, fake_s3
):
    # opt-out: name-only resolution. Even a rebound name commits (the
    # caller asked for exactly that), and no expected_table_uuid field
    # rides the commit body.
    from pyhoglake import UNGUARDED

    httpx_mock.add_response(method="GET", url=_TABLES_URL, json=REBOUND_WIRE)
    httpx_mock.add_response(method="POST", url=_COMMIT_URL, json={"snapshot_id": 6})
    res = table.append(
        pa.table({"id": [1], "name": ["a"]}), expected_table_uuid=UNGUARDED
    )
    assert res.snapshot_id == 6
    body = json.loads(httpx_mock.get_requests()[-1].content)
    (append,) = body["appends"]
    assert "expected_table_uuid" not in append


def test_append_without_s3_config(httpx_mock):
    client = HoglakeClient(BASE)  # no s3
    httpx_mock.add_response(
        method="GET", url=f"{BASE}/v1/catalogs/cat", json=CATALOG_WIRE
    )
    cat = client.catalog("cat")
    httpx_mock.add_response(
        method="GET",
        url=f"{BASE}/v1/catalogs/cat/namespaces/ns1/tables/events",
        json=TABLE_WIRE,
    )
    t = Namespace(cat, "ns1").table("events")
    httpx_mock.add_response(
        method="GET",
        url=f"{BASE}/v1/catalogs/cat/namespaces/ns1/tables/events",
        json=TABLE_WIRE,
    )
    with pytest.raises(HoglakeError, match="S3 configuration"):
        t.append(pa.table({"id": [1], "name": ["a"]}))
    client.close()
