"""Unit tests for the append writer path: parquet bytes written through a
fake filesystem, commit body shape, field ids, deferred stats."""

import base64
import io
import json
import struct

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from pyhoglake import HoglakeClient, HoglakeError, ValidationError
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
        {"name": "name", "type": "string", "field_id": 2, "ordinal": 1, "nullable": True},
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
    res = table.append(
        data, read_snapshot=5, author="tester", message="first"
    )
    assert res.snapshot_id == 6

    body = json.loads(httpx_mock.get_requests()[-1].content)
    assert body["read_snapshot"] == 5
    assert body["author"] == "tester"
    assert body["message"] == "first"
    (append,) = body["appends"]
    assert append["namespace"] == "ns1"
    assert append["table"] == "events"
    (file_reg,) = append["files"]

    # exactly one file written, under the data path with the missing '/' fixed
    (key,) = fake_s3.files.keys()
    assert key.startswith("bkt/lake/data/ns1/events/")
    assert key.endswith(".parquet")
    assert file_reg["path"] == "s3://" + key

    raw = fake_s3.files[key]
    assert file_reg["record_count"] == 3
    assert file_reg["file_size_bytes"] == len(raw)
    expected_footer = struct.unpack("<I", raw[-8:-4])[0] + 8
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


def test_append_partitioned_table_rejected(table, httpx_mock):
    wire = dict(TABLE_WIRE)
    wire["partition_spec"] = {
        "spec_id": 1,
        "fields": [{"source_field_id": 1, "transform": "identity"}],
    }
    httpx_mock.add_response(
        method="GET",
        url=f"{BASE}/v1/catalogs/cat/namespaces/ns1/tables/events",
        json=wire,
    )
    with pytest.raises(HoglakeError, match="partitioned"):
        table.append(pa.table({"id": [1], "name": ["a"]}))


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
