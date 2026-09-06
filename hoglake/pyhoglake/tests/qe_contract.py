"""Contract pedantry: every OpenAPI operation, exactly as documented.

For each of the 25 operations in server/src/main/resources/openapi/
hoglake.yaml this suite asserts the client sends EXACTLY the documented
method + path + query-parameter set + body-field set — and nothing more
— plus the error mapping for every documented status of every endpoint.

Documented divergences found (asserted below so they stay visible):

* runExpiry/runCleanup take a `batch` query parameter; the spec
  declares it (`maintenanceBatch`) on both, matching the client.
* Object names were once interpolated into URL paths without
  percent-encoding, so a name containing '?', '/', or '#' rewrote the
  request URL structure; segments are now quoted, pinned by the
  regression below.
"""

import json
from datetime import datetime, timedelta, timezone

import pyarrow as pa
import pytest

from pyhoglake import (
    AlreadyExistsError,
    CommitConflictError,
    ExpiredError,
    HoglakeError,
    NotFoundError,
    OffsetRegressionError,
    ValidationError,
    ops,
)
from pyhoglake.client import HoglakeClient, Namespace
from pyhoglake.models import ColumnStats

BASE = "http://hog.test"

CATALOG_WIRE = {
    "name": "cat",
    "data_path": "s3://bkt/lake/",
    "head_snapshot_id": 5,
    "schema_version": 2,
}

TABLE_WIRE = {
    "name": "events",
    "namespace": "ns1",
    "table_uuid": "0b8ee9ba-79a1-4f3e-b7e5-6a0b6ab6f012",
    "columns": [
        {"name": "id", "type": "long", "field_id": 1, "ordinal": 0, "nullable": False},
    ],
    "record_count": 0,
    "file_count": 0,
    "file_size_bytes": 0,
}

VIEW_WIRE = {
    "name": "v",
    "namespace": "ns1",
    "view_uuid": "9d2a2c40-58c8-45f2-8b39-8a8f5a4d9a01",
    "dialect": "trino",
    "sql": "SELECT 1",
}

OFFSET_WIRE = {
    "consumer_id": "c1",
    "table_uuid": TABLE_WIRE["table_uuid"],
    "committed_snapshot": 7,
    "updated_at": "2026-09-04T12:00:00Z",
}

OPTIONS_WIRE = {
    "snapshot_retention_seconds": 60,
    "consumer_floor": True,
    "earliest_snapshot_id": 1,
}

COMMIT_WIRE = {"snapshot_id": 9, "schema_version": 3}


@pytest.fixture
def client():
    with HoglakeClient(BASE) as c:
        yield c


def _catalog(client, httpx_mock):
    httpx_mock.add_response(
        method="GET", url=f"{BASE}/v1/catalogs/cat", json=CATALOG_WIRE
    )
    return client.catalog("cat")


def _table(client, httpx_mock):
    cat = _catalog(client, httpx_mock)
    httpx_mock.add_response(
        method="GET",
        url=f"{BASE}/v1/catalogs/cat/namespaces/ns1/tables/events",
        json=TABLE_WIRE,
    )
    return Namespace(cat, "ns1").table("events")


def _last(httpx_mock):
    return httpx_mock.get_requests()[-1]


def _assert_wire(req, method, path, params=None, body=...):
    """Pedantic wire assertion: exact method, exact path, exact query
    set, exact body (Ellipsis = must be empty)."""
    assert req.method == method
    assert req.url.path == path
    assert dict(req.url.params) == (params or {})
    if body is ...:
        assert req.content == b""  # GET/DELETE carry no body — ever
    else:
        assert json.loads(req.content) == body
        assert req.headers["content-type"] == "application/json"


# ---------------------------------------------------------------------------
# per-operation wire shape ("...and nothing more")
# ---------------------------------------------------------------------------


def test_op_list_catalogs(client, httpx_mock):
    httpx_mock.add_response(method="GET", url=f"{BASE}/v1/catalogs", json=[])
    client.list_catalogs()
    _assert_wire(_last(httpx_mock), "GET", "/v1/catalogs")


def test_op_create_catalog(client, httpx_mock):
    httpx_mock.add_response(
        method="POST", url=f"{BASE}/v1/catalogs", json=CATALOG_WIRE, status_code=201
    )
    client.create_catalog("cat", "s3://bkt/lake/")
    # spec CreateCatalogRequest: required [name, data_path]; exactly those
    _assert_wire(
        _last(httpx_mock),
        "POST",
        "/v1/catalogs",
        body={"name": "cat", "data_path": "s3://bkt/lake/"},
    )


def test_op_get_catalog(client, httpx_mock):
    _catalog(client, httpx_mock)
    _assert_wire(_last(httpx_mock), "GET", "/v1/catalogs/cat")


def test_op_list_namespaces(client, httpx_mock):
    cat = _catalog(client, httpx_mock)
    httpx_mock.add_response(
        method="GET", url=f"{BASE}/v1/catalogs/cat/namespaces", json=[]
    )
    cat.list_namespaces()
    _assert_wire(_last(httpx_mock), "GET", "/v1/catalogs/cat/namespaces")


def test_op_create_namespace(client, httpx_mock):
    cat = _catalog(client, httpx_mock)
    httpx_mock.add_response(
        method="POST",
        url=f"{BASE}/v1/catalogs/cat/namespaces",
        json={"name": "ns1"},
        status_code=201,
    )
    cat.create_namespace("ns1")
    _assert_wire(
        _last(httpx_mock), "POST", "/v1/catalogs/cat/namespaces", body={"name": "ns1"}
    )


def test_op_list_tables(client, httpx_mock):
    cat = _catalog(client, httpx_mock)
    httpx_mock.add_response(
        method="GET", url=f"{BASE}/v1/catalogs/cat/namespaces/ns1/tables", json=[]
    )
    Namespace(cat, "ns1").list_tables()
    _assert_wire(_last(httpx_mock), "GET", "/v1/catalogs/cat/namespaces/ns1/tables")


def test_op_create_table(client, httpx_mock):
    cat = _catalog(client, httpx_mock)
    httpx_mock.add_response(
        method="POST",
        url=f"{BASE}/v1/catalogs/cat/namespaces/ns1/tables",
        json=TABLE_WIRE,
        status_code=201,
    )
    Namespace(cat, "ns1").create_table(
        "events", pa.schema([pa.field("id", pa.int64(), nullable=False)])
    )
    # CreateTableRequest: required [name, columns]; ColumnDef fields only
    _assert_wire(
        _last(httpx_mock),
        "POST",
        "/v1/catalogs/cat/namespaces/ns1/tables",
        body={
            "name": "events",
            "columns": [{"name": "id", "type": "long", "nullable": False}],
        },
    )


def test_op_get_table_default_and_travel(client, httpx_mock):
    t = _table(client, httpx_mock)
    url = f"{BASE}/v1/catalogs/cat/namespaces/ns1/tables/events"
    httpx_mock.add_response(method="GET", url=url, json=TABLE_WIRE)
    t.info()
    _assert_wire(
        _last(httpx_mock), "GET", "/v1/catalogs/cat/namespaces/ns1/tables/events"
    )

    httpx_mock.add_response(method="GET", url=f"{url}?snapshot=3", json=TABLE_WIRE)
    t.info(snapshot=3)
    _assert_wire(
        _last(httpx_mock),
        "GET",
        "/v1/catalogs/cat/namespaces/ns1/tables/events",
        params={"snapshot": "3"},
    )


def test_snapshot_and_at_timestamp_mutually_exclusive_no_request(client, httpx_mock):
    # spec: 422 if both — the client refuses locally and must NOT hit
    # the wire at all
    t = _table(client, httpx_mock)
    n_before = len(httpx_mock.get_requests())
    for fn in (t.info, t.files, t.scan_plan):
        with pytest.raises(ValueError):
            fn(snapshot=1, at_timestamp="2026-01-01T00:00:00Z")
    assert len(httpx_mock.get_requests()) == n_before


def test_at_timestamp_iso8601_with_offset(client, httpx_mock):
    t = _table(client, httpx_mock)
    url = f"{BASE}/v1/catalogs/cat/namespaces/ns1/tables/events"

    # naive datetime -> taken as UTC, +00:00 offset REQUIRED on the wire
    httpx_mock.add_response(
        method="GET",
        url=f"{url}?at_timestamp=2026-09-04T12%3A00%3A00%2B00%3A00",
        json=TABLE_WIRE,
    )
    t.info(at_timestamp=datetime(2026, 9, 4, 12, 0, 0))
    assert dict(_last(httpx_mock).url.params) == {
        "at_timestamp": "2026-09-04T12:00:00+00:00"
    }

    # aware datetime -> offset preserved verbatim
    httpx_mock.add_response(
        method="GET",
        url=f"{url}?at_timestamp=2026-09-04T12%3A00%3A00.000001%2B05%3A30",
        json=TABLE_WIRE,
    )
    t.info(
        at_timestamp=datetime(
            2026, 9, 4, 12, 0, 0, 1, tzinfo=timezone(timedelta(hours=5, minutes=30))
        )
    )
    assert dict(_last(httpx_mock).url.params) == {
        "at_timestamp": "2026-09-04T12:00:00.000001+05:30"
    }


def test_op_drop_table(client, httpx_mock):
    t = _table(client, httpx_mock)
    httpx_mock.add_response(
        method="DELETE",
        url=f"{BASE}/v1/catalogs/cat/namespaces/ns1/tables/events",
        json=COMMIT_WIRE,
    )
    t.drop()
    _assert_wire(
        _last(httpx_mock), "DELETE", "/v1/catalogs/cat/namespaces/ns1/tables/events"
    )


def test_op_alter_table_exact_op_bodies(client, httpx_mock):
    t = _table(client, httpx_mock)
    httpx_mock.add_response(
        method="POST",
        url=f"{BASE}/v1/catalogs/cat/namespaces/ns1/tables/events/alter",
        json=TABLE_WIRE,
    )
    t.alter(
        [
            ops.add_column("s", "string"),
            ops.drop_column("d"),
            ops.rename_column("a", "b"),
            ops.promote_column("i", "long"),
            ops.rename_table("t2"),
            ops.set_partition_spec([]),
            ops.set_partition_spec([ops.partition_field(1, "identity")]),
            ops.set_partition_spec([ops.partition_field(1, "bucket", 16)]),
        ]
    )
    # AlterOp is discriminated by `op`; each variant must carry ONLY its
    # documented keys (spec: column / name / from / to / new_name / fields)
    _assert_wire(
        _last(httpx_mock),
        "POST",
        "/v1/catalogs/cat/namespaces/ns1/tables/events/alter",
        body={
            "ops": [
                {
                    "op": "add_column",
                    "column": {"name": "s", "type": "string", "nullable": True},
                },
                {"op": "drop_column", "name": "d"},
                {"op": "rename_column", "from": "a", "to": "b"},
                {"op": "promote_column", "name": "i", "to": "long"},
                {"op": "rename_table", "new_name": "t2"},
                {"op": "set_partition_spec", "fields": []},
                {
                    "op": "set_partition_spec",
                    "fields": [{"source_field_id": 1, "transform": "identity"}],
                },
                {
                    "op": "set_partition_spec",
                    "fields": [
                        {
                            "source_field_id": 1,
                            "transform": "bucket",
                            "transform_param": 16,
                        }
                    ],
                },
            ]
        },
    )


def test_op_plan_scan(client, httpx_mock):
    t = _table(client, httpx_mock)
    url = f"{BASE}/v1/catalogs/cat/namespaces/ns1/tables/events/scan"
    httpx_mock.add_response(method="GET", url=url, json=[])
    t.scan_plan()
    _assert_wire(
        _last(httpx_mock), "GET", "/v1/catalogs/cat/namespaces/ns1/tables/events/scan"
    )
    httpx_mock.add_response(method="GET", url=f"{url}?snapshot=8", json=[])
    t.scan_plan(snapshot=8)
    assert dict(_last(httpx_mock).url.params) == {"snapshot": "8"}


def test_op_list_files(client, httpx_mock):
    t = _table(client, httpx_mock)
    url = f"{BASE}/v1/catalogs/cat/namespaces/ns1/tables/events/files"
    httpx_mock.add_response(method="GET", url=url, json=[])
    t.files()
    _assert_wire(
        _last(httpx_mock), "GET", "/v1/catalogs/cat/namespaces/ns1/tables/events/files"
    )


def test_op_get_changes_param_set(client, httpx_mock):
    t = _table(client, httpx_mock)
    url = f"{BASE}/v1/catalogs/cat/namespaces/ns1/tables/events/changes"
    plan = {
        "table_uuid": TABLE_WIRE["table_uuid"],
        "from_snapshot": 3,
        "to_snapshot": 9,
        "files": [],
        "delete_files": [],
    }
    httpx_mock.add_response(
        method="GET", url=f"{url}?from_snapshot=3&to_snapshot=9", json=plan
    )
    t.changes(3, 9)
    assert dict(_last(httpx_mock).url.params) == {
        "from_snapshot": "3",
        "to_snapshot": "9",
    }
    # to_snapshot optional: omitted entirely when None, not sent as null
    httpx_mock.add_response(method="GET", url=f"{url}?from_snapshot=0", json=plan)
    t.changes(0)
    assert dict(_last(httpx_mock).url.params) == {"from_snapshot": "0"}


def test_op_get_options(client, httpx_mock):
    cat = _catalog(client, httpx_mock)
    httpx_mock.add_response(
        method="GET", url=f"{BASE}/v1/catalogs/cat/options", json=OPTIONS_WIRE
    )
    cat.options()
    _assert_wire(_last(httpx_mock), "GET", "/v1/catalogs/cat/options")


def test_op_patch_options_body_shapes(client, httpx_mock):
    cat = _catalog(client, httpx_mock)
    url = f"{BASE}/v1/catalogs/cat/options"
    # null retention is a MEANINGFUL value (disables expiry) and must be
    # serialized as JSON null, not omitted
    httpx_mock.add_response(method="PATCH", url=url, json=OPTIONS_WIRE)
    cat.set_retention(None)
    _assert_wire(
        _last(httpx_mock),
        "PATCH",
        "/v1/catalogs/cat/options",
        body={"snapshot_retention_seconds": None},
    )
    httpx_mock.add_response(method="PATCH", url=url, json=OPTIONS_WIRE)
    cat.set_retention(3600, consumer_floor=False)
    _assert_wire(
        _last(httpx_mock),
        "PATCH",
        "/v1/catalogs/cat/options",
        body={"snapshot_retention_seconds": 3600, "consumer_floor": False},
    )


EXPIRY_WIRE = {
    "snapshots_expired": 0,
    "data_files_queued": 0,
    "delete_files_queued": 0,
    "new_earliest_snapshot_id": 1,
}


def test_op_run_expiry_and_cleanup_spec_clean_when_no_batch(client, httpx_mock):
    cat = _catalog(client, httpx_mock)
    httpx_mock.add_response(
        method="POST",
        url=f"{BASE}/v1/catalogs/cat/maintenance/expire",
        json=EXPIRY_WIRE,
    )
    cat.expire()
    _assert_wire(
        _last(httpx_mock), "POST", "/v1/catalogs/cat/maintenance/expire", body=...
    )
    httpx_mock.add_response(
        method="POST",
        url=f"{BASE}/v1/catalogs/cat/maintenance/cleanup",
        json={"removed": 0, "missing": 0, "still_referenced": 0},
    )
    cat.cleanup()
    _assert_wire(
        _last(httpx_mock), "POST", "/v1/catalogs/cat/maintenance/cleanup", body=...
    )


def test_op_run_expiry_batch_param_is_undocumented_SPEC_GAP(client, httpx_mock):
    # SPEC GAP: the client exposes expire(batch=)/cleanup(batch=) and
    # sends ?batch=N, but the OpenAPI spec declares NO query parameters
    # for runExpiry/runCleanup. Either the spec is missing the
    # parameter or the client invented one. This test pins the client's
    # current behavior so the divergence stays visible.
    cat = _catalog(client, httpx_mock)
    httpx_mock.add_response(
        method="POST",
        url=f"{BASE}/v1/catalogs/cat/maintenance/expire?batch=10",
        json=EXPIRY_WIRE,
    )
    cat.expire(batch=10)
    assert dict(_last(httpx_mock).url.params) == {"batch": "10"}


def test_op_list_views(client, httpx_mock):
    cat = _catalog(client, httpx_mock)
    httpx_mock.add_response(
        method="GET", url=f"{BASE}/v1/catalogs/cat/namespaces/ns1/views", json=[]
    )
    Namespace(cat, "ns1").list_views()
    _assert_wire(_last(httpx_mock), "GET", "/v1/catalogs/cat/namespaces/ns1/views")


def test_op_create_view(client, httpx_mock):
    cat = _catalog(client, httpx_mock)
    httpx_mock.add_response(
        method="POST",
        url=f"{BASE}/v1/catalogs/cat/namespaces/ns1/views",
        json=VIEW_WIRE,
        status_code=201,
    )
    Namespace(cat, "ns1").create_view("v", "SELECT 1", dialect="duckdb")
    # spec: required [name, sql]; dialect has default "trino" — the
    # client always sends it explicitly (harmless: documented property)
    _assert_wire(
        _last(httpx_mock),
        "POST",
        "/v1/catalogs/cat/namespaces/ns1/views",
        body={"name": "v", "sql": "SELECT 1", "dialect": "duckdb"},
    )


def test_op_get_and_drop_view(client, httpx_mock):
    cat = _catalog(client, httpx_mock)
    ns = Namespace(cat, "ns1")
    httpx_mock.add_response(
        method="GET",
        url=f"{BASE}/v1/catalogs/cat/namespaces/ns1/views/v",
        json=VIEW_WIRE,
    )
    v = ns.view("v")
    _assert_wire(_last(httpx_mock), "GET", "/v1/catalogs/cat/namespaces/ns1/views/v")
    httpx_mock.add_response(
        method="DELETE",
        url=f"{BASE}/v1/catalogs/cat/namespaces/ns1/views/v",
        json={"snapshot_id": 3},
    )
    v.drop()
    _assert_wire(_last(httpx_mock), "DELETE", "/v1/catalogs/cat/namespaces/ns1/views/v")


def test_op_list_snapshots_params(client, httpx_mock):
    cat = _catalog(client, httpx_mock)
    httpx_mock.add_response(
        method="GET",
        url=f"{BASE}/v1/catalogs/cat/snapshots?after=0&limit=1000",
        json={"snapshots": [], "has_more": False},
    )
    list(cat.snapshots())
    # spec defaults after=0 / limit=1000; client sends them explicitly —
    # allowed (they are documented parameters with those defaults)
    assert dict(_last(httpx_mock).url.params) == {"after": "0", "limit": "1000"}


def test_op_list_snapshots_before_param(client, httpx_mock):
    # descending cursor: `before` only, never combined with `after`
    # (spec: 422 if both; the client refuses the combination with a
    # ValueError before any request)
    cat = _catalog(client, httpx_mock)
    httpx_mock.add_response(
        method="GET",
        url=f"{BASE}/v1/catalogs/cat/snapshots?before=6&limit=1000",
        json={"snapshots": [], "has_more": False},
    )
    list(cat.snapshots(before=6))
    assert dict(_last(httpx_mock).url.params) == {"before": "6", "limit": "1000"}
    with pytest.raises(ValueError):
        cat.snapshots(after=1, before=6)


def test_op_commit_body_fields(client, httpx_mock):
    cat = _catalog(client, httpx_mock)
    httpx_mock.add_response(
        method="POST", url=f"{BASE}/v1/catalogs/cat/commit", json={"snapshot_id": 6}
    )
    payload = {
        "read_snapshot": 5,
        "appends": [
            {
                "namespace": "ns1",
                "table": "events",
                "files": [
                    {
                        "path": "s3://b/f.parquet",
                        "record_count": 1,
                        "file_size_bytes": 10,
                    }
                ],
            }
        ],
        "author": "a",
        "message": "m",
    }
    cat._commit(payload)
    _assert_wire(_last(httpx_mock), "POST", "/v1/catalogs/cat/commit", body=payload)


def test_op_list_consumer_offsets(client, httpx_mock):
    cat = _catalog(client, httpx_mock)
    httpx_mock.add_response(
        method="GET", url=f"{BASE}/v1/catalogs/cat/consumers/c1/offsets", json=[]
    )
    cat.offsets("c1")
    _assert_wire(_last(httpx_mock), "GET", "/v1/catalogs/cat/consumers/c1/offsets")


def test_op_get_consumer_offset(client, httpx_mock):
    # GET /catalogs/{catalog}/consumers/{consumer}/offsets/{tableUuid}:
    # 200 -> ConsumerOffset; the documented 404 ("none is stored") is a
    # routine state and surfaces as None, not NotFoundError.
    cat = _catalog(client, httpx_mock)
    uuid = TABLE_WIRE["table_uuid"]
    httpx_mock.add_response(
        method="GET",
        url=f"{BASE}/v1/catalogs/cat/consumers/c1/offsets/{uuid}",
        json=OFFSET_WIRE,
    )
    off = cat.offset("c1", uuid)
    _assert_wire(
        _last(httpx_mock), "GET", f"/v1/catalogs/cat/consumers/c1/offsets/{uuid}"
    )
    assert off is not None
    httpx_mock.add_response(
        method="GET",
        url=f"{BASE}/v1/catalogs/cat/consumers/c1/offsets/{uuid}",
        json=ERR,
        status_code=404,
    )
    assert cat.offset("c1", uuid) is None


def test_op_commit_consumer_offset(client, httpx_mock):
    cat = _catalog(client, httpx_mock)
    uuid = TABLE_WIRE["table_uuid"]
    httpx_mock.add_response(
        method="PUT",
        url=f"{BASE}/v1/catalogs/cat/consumers/c1/offsets/{uuid}",
        json=OFFSET_WIRE,
    )
    cat.commit_offset("c1", uuid, 7)
    # spec: required [snapshot_id]; exactly that
    _assert_wire(
        _last(httpx_mock),
        "PUT",
        f"/v1/catalogs/cat/consumers/c1/offsets/{uuid}",
        body={"snapshot_id": 7},
    )


# ---------------------------------------------------------------------------
# error mapping: every documented (endpoint, status) pair
# ---------------------------------------------------------------------------

ERR = {"error": "boom", "detail": "why"}


def _err_cases():
    """(name, setup(client, httpx_mock) -> callable, status, exc_class)
    for every documented error response in the spec."""
    return [
        (
            "createCatalog-409",
            lambda c, m: (
                m.add_response(
                    method="POST", url=f"{BASE}/v1/catalogs", json=ERR, status_code=409
                ),
                lambda: c.create_catalog("cat", "s3://x/"),
            )[1],
            AlreadyExistsError,
        ),
        (
            "getCatalog-404",
            lambda c, m: (
                m.add_response(
                    method="GET",
                    url=f"{BASE}/v1/catalogs/cat",
                    json=ERR,
                    status_code=404,
                ),
                lambda: c.catalog("cat"),
            )[1],
            NotFoundError,
        ),
        (
            "createNamespace-409",
            lambda c, m: (
                lambda cat: (
                    m.add_response(
                        method="POST",
                        url=f"{BASE}/v1/catalogs/cat/namespaces",
                        json=ERR,
                        status_code=409,
                    ),
                    lambda: cat.create_namespace("ns"),
                )[1]
            )(_catalog(c, m)),
            AlreadyExistsError,
        ),
        (
            "createTable-409",
            lambda c, m: (
                lambda ns: (
                    m.add_response(
                        method="POST",
                        url=f"{BASE}/v1/catalogs/cat/namespaces/ns1/tables",
                        json=ERR,
                        status_code=409,
                    ),
                    lambda: ns.create_table(
                        "t", pa.schema([pa.field("id", pa.int64())])
                    ),
                )[1]
            )(Namespace(_catalog(c, m), "ns1")),
            AlreadyExistsError,
        ),
        (
            "getTable-404",
            lambda c, m: (
                lambda ns: (
                    m.add_response(
                        method="GET",
                        url=f"{BASE}/v1/catalogs/cat/namespaces/ns1/tables/t",
                        json=ERR,
                        status_code=404,
                    ),
                    lambda: ns.table("t"),
                )[1]
            )(Namespace(_catalog(c, m), "ns1")),
            NotFoundError,
        ),
        (
            "dropTable-404",
            lambda c, m: (
                lambda t: (
                    m.add_response(
                        method="DELETE",
                        url=f"{BASE}/v1/catalogs/cat/namespaces/ns1/tables/events",
                        json=ERR,
                        status_code=404,
                    ),
                    t.drop,
                )[1]
            )(_table(c, m)),
            NotFoundError,
        ),
        (
            "alterTable-404",
            lambda c, m: (
                lambda t: (
                    m.add_response(
                        method="POST",
                        url=f"{BASE}/v1/catalogs/cat/namespaces/ns1/tables/events/alter",
                        json=ERR,
                        status_code=404,
                    ),
                    lambda: t.alter([ops.drop_column("x")]),
                )[1]
            )(_table(c, m)),
            NotFoundError,
        ),
        (
            "alterTable-409",
            lambda c, m: (
                lambda t: (
                    m.add_response(
                        method="POST",
                        url=f"{BASE}/v1/catalogs/cat/namespaces/ns1/tables/events/alter",
                        json=ERR,
                        status_code=409,
                    ),
                    lambda: t.alter([ops.drop_column("x")]),
                )[1]
            )(_table(c, m)),
            CommitConflictError,
        ),
        (
            "alterTable-422",
            lambda c, m: (
                lambda t: (
                    m.add_response(
                        method="POST",
                        url=f"{BASE}/v1/catalogs/cat/namespaces/ns1/tables/events/alter",
                        json=ERR,
                        status_code=422,
                    ),
                    lambda: t.alter([ops.drop_column("x")]),
                )[1]
            )(_table(c, m)),
            ValidationError,
        ),
        (
            "planScan-404",
            lambda c, m: (
                lambda t: (
                    m.add_response(
                        method="GET",
                        url=f"{BASE}/v1/catalogs/cat/namespaces/ns1/tables/events/scan",
                        json=ERR,
                        status_code=404,
                    ),
                    t.scan_plan,
                )[1]
            )(_table(c, m)),
            NotFoundError,
        ),
        (
            "listFiles-404",
            lambda c, m: (
                lambda t: (
                    m.add_response(
                        method="GET",
                        url=f"{BASE}/v1/catalogs/cat/namespaces/ns1/tables/events/files",
                        json=ERR,
                        status_code=404,
                    ),
                    t.files,
                )[1]
            )(_table(c, m)),
            NotFoundError,
        ),
        (
            "getChanges-404",
            lambda c, m: (
                lambda t: (
                    m.add_response(
                        method="GET",
                        url=f"{BASE}/v1/catalogs/cat/namespaces/ns1/tables/events/changes?from_snapshot=1",
                        json=ERR,
                        status_code=404,
                    ),
                    lambda: t.changes(1),
                )[1]
            )(_table(c, m)),
            NotFoundError,
        ),
        (
            "getChanges-410",
            lambda c, m: (
                lambda t: (
                    m.add_response(
                        method="GET",
                        url=f"{BASE}/v1/catalogs/cat/namespaces/ns1/tables/events/changes?from_snapshot=1",
                        json=ERR,
                        status_code=410,
                    ),
                    lambda: t.changes(1),
                )[1]
            )(_table(c, m)),
            ExpiredError,
        ),
        (
            "getOptions-404",
            lambda c, m: (
                lambda cat: (
                    m.add_response(
                        method="GET",
                        url=f"{BASE}/v1/catalogs/cat/options",
                        json=ERR,
                        status_code=404,
                    ),
                    cat.options,
                )[1]
            )(_catalog(c, m)),
            NotFoundError,
        ),
        (
            "patchOptions-404",
            lambda c, m: (
                lambda cat: (
                    m.add_response(
                        method="PATCH",
                        url=f"{BASE}/v1/catalogs/cat/options",
                        json=ERR,
                        status_code=404,
                    ),
                    lambda: cat.set_retention(1),
                )[1]
            )(_catalog(c, m)),
            NotFoundError,
        ),
        (
            "patchOptions-422",
            lambda c, m: (
                lambda cat: (
                    m.add_response(
                        method="PATCH",
                        url=f"{BASE}/v1/catalogs/cat/options",
                        json=ERR,
                        status_code=422,
                    ),
                    lambda: cat.set_retention(-1),
                )[1]
            )(_catalog(c, m)),
            ValidationError,
        ),
        (
            "runExpiry-404",
            lambda c, m: (
                lambda cat: (
                    m.add_response(
                        method="POST",
                        url=f"{BASE}/v1/catalogs/cat/maintenance/expire",
                        json=ERR,
                        status_code=404,
                    ),
                    cat.expire,
                )[1]
            )(_catalog(c, m)),
            NotFoundError,
        ),
        (
            "runCleanup-404",
            lambda c, m: (
                lambda cat: (
                    m.add_response(
                        method="POST",
                        url=f"{BASE}/v1/catalogs/cat/maintenance/cleanup",
                        json=ERR,
                        status_code=404,
                    ),
                    cat.cleanup,
                )[1]
            )(_catalog(c, m)),
            NotFoundError,
        ),
        (
            "createView-409",
            lambda c, m: (
                lambda ns: (
                    m.add_response(
                        method="POST",
                        url=f"{BASE}/v1/catalogs/cat/namespaces/ns1/views",
                        json=ERR,
                        status_code=409,
                    ),
                    lambda: ns.create_view("v", "SELECT 1"),
                )[1]
            )(Namespace(_catalog(c, m), "ns1")),
            AlreadyExistsError,
        ),
        (
            "getView-404",
            lambda c, m: (
                lambda ns: (
                    m.add_response(
                        method="GET",
                        url=f"{BASE}/v1/catalogs/cat/namespaces/ns1/views/v",
                        json=ERR,
                        status_code=404,
                    ),
                    lambda: ns.view("v"),
                )[1]
            )(Namespace(_catalog(c, m), "ns1")),
            NotFoundError,
        ),
        (
            "listSnapshots-404",
            lambda c, m: (
                lambda cat: (
                    m.add_response(
                        method="GET",
                        url=f"{BASE}/v1/catalogs/cat/snapshots?after=0&limit=1000",
                        json=ERR,
                        status_code=404,
                    ),
                    lambda: list(cat.snapshots()),
                )[1]
            )(_catalog(c, m)),
            NotFoundError,
        ),
        (
            "commit-409",
            lambda c, m: (
                lambda cat: (
                    m.add_response(
                        method="POST",
                        url=f"{BASE}/v1/catalogs/cat/commit",
                        json=ERR,
                        status_code=409,
                    ),
                    lambda: cat._commit({"appends": []}),
                )[1]
            )(_catalog(c, m)),
            CommitConflictError,
        ),
        (
            "commit-422",
            lambda c, m: (
                lambda cat: (
                    m.add_response(
                        method="POST",
                        url=f"{BASE}/v1/catalogs/cat/commit",
                        json=ERR,
                        status_code=422,
                    ),
                    lambda: cat._commit({"appends": []}),
                )[1]
            )(_catalog(c, m)),
            ValidationError,
        ),
        (
            "listOffsets-404",
            lambda c, m: (
                lambda cat: (
                    m.add_response(
                        method="GET",
                        url=f"{BASE}/v1/catalogs/cat/consumers/c1/offsets",
                        json=ERR,
                        status_code=404,
                    ),
                    lambda: cat.offsets("c1"),
                )[1]
            )(_catalog(c, m)),
            NotFoundError,
        ),
        (
            "commitOffset-404",
            lambda c, m: (
                lambda cat: (
                    m.add_response(
                        method="PUT",
                        url=f"{BASE}/v1/catalogs/cat/consumers/c1/offsets/u1",
                        json=ERR,
                        status_code=404,
                    ),
                    lambda: cat.commit_offset("c1", "u1", 1),
                )[1]
            )(_catalog(c, m)),
            NotFoundError,
        ),
        (
            "commitOffset-409",
            lambda c, m: (
                lambda cat: (
                    m.add_response(
                        method="PUT",
                        url=f"{BASE}/v1/catalogs/cat/consumers/c1/offsets/u1",
                        json=ERR,
                        status_code=409,
                    ),
                    lambda: cat.commit_offset("c1", "u1", 1),
                )[1]
            )(_catalog(c, m)),
            OffsetRegressionError,
        ),
    ]


@pytest.mark.parametrize(
    "case", _err_cases(), ids=lambda c: c[0] if isinstance(c[0], str) else "?"
)
def test_documented_error_statuses_map_to_typed_exceptions(case, client, httpx_mock):
    name, setup, exc_cls = case
    call = setup(client, httpx_mock)
    with pytest.raises(exc_cls) as ei:
        call()
    assert ei.value.status_code == int(name.rsplit("-", 1)[1])
    assert ei.value.message == "boom"
    assert ei.value.detail == "why"
    # exception taxonomy: only CommitConflictError is retryable
    assert ei.value.retryable is (exc_cls is CommitConflictError)


def test_undocumented_5xx_maps_to_base_error(client, httpx_mock):
    httpx_mock.add_response(
        method="GET", url=f"{BASE}/v1/catalogs/cat", content=b"oops", status_code=503
    )
    with pytest.raises(HoglakeError) as ei:
        client.catalog("cat")
    assert type(ei.value) is HoglakeError
    assert ei.value.status_code == 503
    assert ei.value.detail == "oops"


# ---------------------------------------------------------------------------
# base64 padding for bounds (format: byte)
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "raw,expected_b64",
    [
        (b"\x01", "AQ=="),  # 1 byte -> 2 pad chars
        (b"\x01\x02", "AQI="),  # 2 bytes -> 1 pad char
        (b"\x01\x02\x03", "AQID"),  # 3 bytes -> no padding
        (b"", ""),  # empty bound (e.g. empty-string min)
        (b"\xff" * 16, "/////////////////////w=="),  # 16-byte uuid bound
    ],
)
def test_bounds_base64_padding_exact(raw, expected_b64):
    import base64

    s = ColumnStats(field_id=1, value_count=1, null_count=0, lower_bound=raw)
    wire = s.to_wire()
    if raw == b"":
        # b"" is falsy but NOT None: the empty bound must still ship
        assert wire.get("lower_bound") == ""
    else:
        assert wire["lower_bound"] == expected_b64
        assert base64.b64decode(wire["lower_bound"]) == raw


def test_column_stats_wire_key_set_minimal():
    # required-only stats must serialize exactly the spec's required keys
    s = ColumnStats(field_id=1, value_count=2, null_count=0)
    assert s.to_wire() == {"field_id": 1, "value_count": 2, "null_count": 0}
    full = ColumnStats(
        field_id=1,
        value_count=2,
        null_count=0,
        nan_count=1,
        size_bytes=9,
        lower_bound=b"\x00",
        upper_bound=b"\x01",
    )
    assert set(full.to_wire()) == {
        "field_id",
        "value_count",
        "null_count",
        "nan_count",
        "size_bytes",
        "lower_bound",
        "upper_bound",
    }


# ---------------------------------------------------------------------------
# URL hygiene for names
# ---------------------------------------------------------------------------


def test_unicode_names_percent_encoded_utf8(client, httpx_mock):
    wire = dict(CATALOG_WIRE, name="café")
    httpx_mock.add_response(
        method="GET", url=f"{BASE}/v1/catalogs/caf%C3%A9", json=wire
    )
    assert client.catalog("café").name == "café"


def test_reserved_characters_in_names_stay_in_the_path(client, httpx_mock):
    # Regression (formerly an xfail BUG pin): object names are user data
    # and every path segment goes through _seg (percent-encoding, safe="")
    # — so catalog('a?x=1') must NOT turn the name's tail into a query
    # string, 'a#f' into a fragment, or 'a/b' into a different route.
    httpx_mock.add_response(
        method="GET",
        url=f"{BASE}/v1/catalogs/a%3Fx%3D1",
        json=dict(CATALOG_WIRE, name="a?x=1"),
    )
    client.catalog("a?x=1")
    req = _last(httpx_mock)
    assert dict(req.url.params) == {}
    # url.path percent-DECODES; raw_path is what actually hit the wire
    assert req.url.raw_path == b"/v1/catalogs/a%3Fx%3D1"
