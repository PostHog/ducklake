"""Live adversarial integration tests (skip cleanly with no server).

Everything created here is qe-* prefixed and disposable: catalog
``qe-adv-<runid>`` with data under ``s3://qe-itest/<runid>/``.

Server behaviors OBSERVED on 2026-09-05 and pinned here:

* Catalog names enforce ``^[a-z][a-z0-9_-]{0,62}$`` (422 otherwise).
  Namespace/table/view/column names enforce
  ``^[A-Za-z_][A-Za-z0-9_-]{0,127}$`` at every DDL surface (422
  otherwise; policy change 2026-09-06 — verbatim round-trip was the
  old pinned contract). author, message, and consumer ids stay
  free-text and round-trip verbatim.
* record_count=0 files register with a zero-width row-id range; the
  next file starts at the same row_id_start (adjacent zero-width is
  legal). file_size_bytes=0 is accepted; negatives are 422.
* Duplicate paths are accepted both within one commit and across
  commits (no path-uniqueness constraint; each registration is a new
  data_file_id with its own row-id range).
* Deletes: delete_count > record_count -> 422; shrinking a DV -> 422;
  missing read_snapshot with deletes -> 422 (all as documented).
* Time-travel: out-of-range snapshot -> 422 (SPEC GAP: getTable
  documents only 200/404); name resolution is snapshot-scoped (the
  historical name works at its own snapshot; the current name 404s
  there); at_timestamp before earliest -> 410; after head -> head.
* read_snapshot=0 against a table with post-creation DDL -> 409
  commit_conflict (retryable).
"""

import threading
import time as _time
import uuid

import pyarrow as pa
import pytest
from conftest import S3_ACCESS_KEY, S3_ENDPOINT, S3_SECRET_KEY

from pyhoglake import (
    CommitConflictError,
    ExpiredError,
    HoglakeClient,
    HoglakeError,
    NotFoundError,
    OffsetRegressionError,
    S3Config,
    ValidationError,
    ops,
)

pytestmark = pytest.mark.integration

RUN_ID = uuid.uuid4().hex[:8]
BUCKET = "qe-itest"

INJECTION = "x'; DROP TABLE hog_catalog;--"


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
        s3config.filesystem().create_dir(BUCKET)
        yield c


@pytest.fixture(scope="module")
def catalog(client):
    return client.create_catalog(f"qe-adv-{RUN_ID}", f"s3://{BUCKET}/{RUN_ID}/")


@pytest.fixture(scope="module")
def ns(catalog):
    return catalog.create_namespace("ns1")


def _schema():
    return pa.schema(
        [pa.field("id", pa.int64(), nullable=False), pa.field("s", pa.string())]
    )


def _rows(*ids):
    return pa.table({"id": list(ids), "s": [f"s{i}" for i in ids]}, schema=_schema())


def _fab(path, record_count=1, file_size_bytes=1):
    return {
        "path": path,
        "record_count": record_count,
        "file_size_bytes": file_size_bytes,
    }


def _sanity(client, catalog):
    """The catalog must still function after every adversarial poke."""
    assert catalog.name in [c.name for c in client.list_catalogs()]
    assert catalog.refresh().head_snapshot_id >= 0


# ---------------------------------------------------------------------------
# names
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "bad",
    [
        "qe-café",  # unicode
        "qe-日本語",  # more unicode
        "qe-\U0001f994",  # astral-plane emoji
        "9qe-leading-digit",
        "QE-UPPER",
        INJECTION,
        "qe-" + "y" * 61,  # 64 chars: one past the cap
        "qe-" + "z" * 197,  # 200 chars
    ],
)
def test_catalog_name_charset_rejected_and_catalog_survives(client, catalog, bad):
    # observed CHECK: ^[a-z][a-z0-9_-]{0,62}$ -> 422, never a 5xx/corruption
    with pytest.raises(ValidationError) as ei:
        client.create_catalog(bad, f"s3://{BUCKET}/{RUN_ID}-never/")
    assert ei.value.status_code == 422
    _sanity(client, catalog)


def test_catalog_name_63_chars_is_accepted(client):
    name = f"qe-{RUN_ID}-" + "x" * (63 - len(f"qe-{RUN_ID}-"))
    assert len(name) == 63
    cat = client.create_catalog(name, f"s3://{BUCKET}/{RUN_ID}-63/")
    assert client.catalog(name).name == name
    assert cat.refresh().head_snapshot_id >= 0


@pytest.mark.parametrize(
    "bad",
    [
        INJECTION,
        "café",  # unicode
        "日本語テーブル",  # more unicode
        "\U0001f994-hog",  # astral-plane emoji
        "9leading-digit",
        "a/b",  # path separator
        "x" * 129,  # one past the 128 cap
        "x" * 10_000,  # 10KB name
    ],
)
def test_ns_table_view_hostile_names_rejected(client, catalog, ns, bad):
    # policy change 2026-09-06: identifier pattern enforced at every
    # DDL surface -> 422, never a 5xx, never stored (was: verbatim
    # round-trip). The catalog must keep working after each poke.
    with pytest.raises(ValidationError) as ei:
        catalog.create_namespace(bad)
    assert ei.value.status_code == 422
    assert bad not in catalog.list_namespaces()

    with pytest.raises(ValidationError) as ei:
        ns.create_table(bad, _schema())
    assert ei.value.status_code == 422
    assert bad not in [x.name for x in ns.list_tables()]

    with pytest.raises(ValidationError) as ei:
        ns.create_view(bad, "SELECT 1")
    assert ei.value.status_code == 422

    _sanity(client, catalog)


def test_identifier_boundary_names_accepted(client, catalog, ns):
    # the pattern's accept edge: leading underscore, uppercase, digits,
    # hyphens, exactly 128 chars.
    for name in ("_leading", "UPPER-Case_9", "x" * 128):
        t = ns.create_table(name, _schema())
        assert ns.table(name).table_uuid == t.table_uuid
        t.append(_rows(1))
        assert ns.table(name).info().record_count == 1
        t.drop()
    _sanity(client, catalog)


def test_author_message_consumer_id_injection_roundtrip(catalog, ns):
    t = ns.create_table("inj_meta", _schema())
    res = t.append(_rows(1), author=INJECTION, message=INJECTION)
    snap = next(s for s in catalog.snapshots() if s.snapshot_id == res.snapshot_id)
    assert snap.author == INJECTION
    assert snap.message == INJECTION

    off = catalog.commit_offset(INJECTION, t.table_uuid, res.snapshot_id)
    assert off.consumer_id == INJECTION
    assert [o.committed_snapshot for o in catalog.offsets(INJECTION)] == [
        res.snapshot_id
    ]
    t.drop()


def test_slash_in_table_name_rejected_and_lookup_misses(client, catalog, ns):
    # 'a/b' is 422 at create (identifier pattern). Reads don't validate:
    # a hostile lookup simply 404s (the client still percent-encodes
    # path segments, so the request is well-formed either way).
    with pytest.raises(ValidationError):
        ns.create_table(f"a/b-{RUN_ID}", _schema())
    with pytest.raises(NotFoundError):
        ns.table(f"a/b-{RUN_ID}")
    _sanity(client, catalog)


# ---------------------------------------------------------------------------
# boundary numerics
# ---------------------------------------------------------------------------


def test_zero_row_append_zero_width_rowid_range(catalog, ns):
    t = ns.create_table("zero_rows", _schema())
    empty = pa.table(
        {"id": pa.array([], pa.int64()), "s": pa.array([], pa.string())},
        schema=_schema(),
    )
    r0 = t.append(empty)
    assert r0.snapshot_id > 0
    (f0,) = t.files()
    assert f0.record_count == 0
    assert f0.row_id_start == 0  # zero-width range at 0

    t.append(_rows(1, 2, 3, 4, 5))
    files = sorted(t.files(), key=lambda f: f.data_file_id)
    assert [f.record_count for f in files] == [0, 5]
    # adjacent zero-width: the 5-row file starts where the empty one did
    assert files[1].row_id_start == files[0].row_id_start == 0
    assert t.info().record_count == 5


def test_zero_size_fabricated_file_accepted_negatives_rejected(catalog, ns):
    ns.create_table("sizes", _schema())
    res = catalog._commit(
        {
            "appends": [
                {
                    "namespace": "ns1",
                    "table": "sizes",
                    "files": [_fab(f"s3://{BUCKET}/{RUN_ID}/fab/zero.parquet", 0, 0)],
                }
            ]
        }
    )
    assert res.snapshot_id > 0
    for bad in (_fab("s3://x/n1.parquet", -5, 1), _fab("s3://x/n2.parquet", 1, -1)):
        with pytest.raises(ValidationError):
            catalog._commit(
                {"appends": [{"namespace": "ns1", "table": "sizes", "files": [bad]}]}
            )


def test_duplicate_paths_accepted_within_and_across_commits(catalog, ns):
    # OBSERVED (documented, arguably a server wart): no path-uniqueness
    # constraint. The same URI registered twice in one commit and again
    # in a later commit yields three distinct data_file_ids with
    # correctly stacked row-id ranges.
    ns.create_table("dups", _schema())
    dup = f"s3://{BUCKET}/{RUN_ID}/fab/dup.parquet"
    catalog._commit(
        {
            "appends": [
                {
                    "namespace": "ns1",
                    "table": "dups",
                    "files": [_fab(dup, 1, 10), _fab(dup, 2, 20)],
                }
            ]
        }
    )
    catalog._commit(
        {
            "appends": [
                {"namespace": "ns1", "table": "dups", "files": [_fab(dup, 3, 30)]}
            ]
        }
    )
    files = sorted(ns.table("dups").files(), key=lambda f: f.data_file_id)
    assert [f.path for f in files] == [dup, dup, dup]
    assert len({f.data_file_id for f in files}) == 3
    assert [(f.row_id_start, f.record_count) for f in files] == [
        (0, 1),
        (1, 2),
        (3, 3),
    ]


def test_delete_count_bounds_and_read_snapshot_requirement(catalog, ns):
    t = ns.create_table("dv", _schema())
    t.append(_rows(1, 2, 3, 4, 5))
    (df,) = t.files()
    base = f"s3://{BUCKET}/{RUN_ID}/fab/dv"

    def deletes(count, path, read_snapshot=...):
        payload = {
            "deletes": [
                {
                    "namespace": "ns1",
                    "table": "dv",
                    "files": [
                        {
                            "data_file_id": df.data_file_id,
                            "path": path,
                            "delete_count": count,
                            "file_size_bytes": 10,
                        }
                    ],
                }
            ]
        }
        if read_snapshot is ...:
            payload["read_snapshot"] = catalog.refresh().head_snapshot_id
        return catalog._commit(payload)

    # spec: read_snapshot REQUIRED when deletes are present
    with pytest.raises(ValidationError):
        deletes(1, base + "-nors.puffin", read_snapshot=None)

    # delete_count == record_count is the legal maximum
    res = deletes(5, base + "-eq.puffin")
    assert res.snapshot_id > 0
    (sf,) = t.scan_plan()
    assert sf.delete_file is not None
    assert sf.delete_file.delete_count == 5

    # > record_count -> 422
    with pytest.raises(ValidationError):
        deletes(6, base + "-over.puffin")
    # vectors only grow: shrinking -> 422
    with pytest.raises(ValidationError):
        deletes(2, base + "-shrink.puffin")


def test_empty_commit_variants_rejected(catalog):
    for payload in (
        {},
        {"appends": []},
        {"appends": [{"namespace": "ns1", "table": "zero_rows", "files": []}]},
    ):
        with pytest.raises(ValidationError):
            catalog._commit(payload)


def test_commit_validation_unknown_table_and_field_id(catalog, ns):
    with pytest.raises(ValidationError):
        catalog._commit(
            {
                "appends": [
                    {
                        "namespace": "ns1",
                        "table": f"ghost-{RUN_ID}",
                        "files": [_fab("s3://x/g.parquet")],
                    }
                ]
            }
        )
    ns.create_table("statcheck", _schema())
    with pytest.raises(ValidationError):
        catalog._commit(
            {
                "appends": [
                    {
                        "namespace": "ns1",
                        "table": "statcheck",
                        "files": [
                            {
                                **_fab("s3://x/s.parquet"),
                                "column_stats": [
                                    {
                                        "field_id": 99999,
                                        "value_count": 1,
                                        "null_count": 0,
                                    }
                                ],
                            }
                        ],
                    }
                ]
            }
        )


def test_snapshot_2_pow_62_and_time_travel_edges(catalog, ns):
    t = ns.create_table("travel_edge", _schema())
    t.append(_rows(1))

    # out-of-range snapshot: 422 (SPEC GAP: getTable documents only
    # 200/404 — the 422 is undocumented but is the sane behavior)
    with pytest.raises(ValidationError):
        t.info(snapshot=2**62)
    with pytest.raises(ValidationError):
        t.files(snapshot=2**62)
    with pytest.raises(ValidationError):
        t.changes(0, 2**62)

    # snapshot 0 exists (genesis) but predates the table -> 404
    with pytest.raises(NotFoundError):
        t.info(snapshot=0)

    # at_timestamp: before earliest -> 410; far future -> head
    with pytest.raises(ExpiredError):
        t.info(at_timestamp="1970-01-01T00:00:00Z")
    assert t.info(at_timestamp="2100-01-01T00:00:00Z").file_count == 1


def test_offset_snapshot_2_pow_62_rejected_or_stored_sanely(catalog, ns):
    t = ns.create_table("bigoff", _schema())
    r = t.append(_rows(1))
    consumer = f"qe-bigoff-{RUN_ID}"
    try:
        off = catalog.commit_offset(consumer, t.table_uuid, 2**62)
    except (ValidationError, HoglakeError) as e:
        assert e.status_code in (404, 409, 422)  # rejected: fine
        catalog.commit_offset(consumer, t.table_uuid, r.snapshot_id)
        return
    # stored: it must round-trip and then enforce monotonicity from there
    assert off.committed_snapshot == 2**62
    with pytest.raises(OffsetRegressionError):
        catalog.commit_offset(consumer, t.table_uuid, r.snapshot_id)


# ---------------------------------------------------------------------------
# pagination edges
# ---------------------------------------------------------------------------


def test_snapshot_pagination_edges(catalog):
    head = catalog.refresh().head_snapshot_id
    raw = catalog._client._request

    page = raw(
        "GET",
        f"/catalogs/{catalog.name}/snapshots",
        params={"after": head, "limit": 10},
    )
    assert page == {"snapshots": [], "has_more": False}  # after=head

    page = raw(
        "GET",
        f"/catalogs/{catalog.name}/snapshots",
        params={"after": head + 100, "limit": 10},
    )
    assert page == {"snapshots": [], "has_more": False}  # after>head

    with pytest.raises(ValidationError):
        raw(
            "GET",
            f"/catalogs/{catalog.name}/snapshots",
            params={"after": 0, "limit": 0},
        )

    # limit=1 pagination sees the same ids as one big page
    one_page = [s.snapshot_id for s in catalog.snapshots(limit=1000)]
    paged = [s.snapshot_id for s in catalog.snapshots(limit=1)]
    assert paged == one_page
    assert paged == sorted(set(paged))

    # OBSERVED: the genesis snapshot 0 is only reachable with after < 0
    # (the default after=0 filter is exclusive) — a documented quirk of
    # the "id > after" contract.
    with_genesis = [s.snapshot_id for s in catalog.snapshots(after=-1, limit=1000)]
    assert with_genesis[0] == 0
    assert with_genesis[1:] == one_page


def test_fresh_namespace_lists_empty(catalog):
    empty_ns = catalog.create_namespace(f"empty-{RUN_ID}")
    assert empty_ns.list_tables() == []
    assert empty_ns.list_views() == []


# ---------------------------------------------------------------------------
# scale
# ---------------------------------------------------------------------------


def test_1000_file_commit(catalog, ns):
    ns.create_table("many_files", _schema())
    files = [
        _fab(f"s3://{BUCKET}/{RUN_ID}/fab/many/{i:04d}.parquet", 1, 100)
        for i in range(1000)
    ]
    start = _time.monotonic()
    res = catalog._commit(
        {"appends": [{"namespace": "ns1", "table": "many_files", "files": files}]}
    )
    commit_s = _time.monotonic() - start
    assert res.snapshot_id > 0
    # timing note: observed ~1s locally; 60s is the "something is
    # pathologically wrong" line, not a performance target
    assert commit_s < 60, f"1000-file commit took {commit_s:.1f}s"

    listed = ns.table("many_files").files()
    assert len(listed) == 1000
    # row-id lineage: 1000 contiguous width-1 ranges starting at 0
    starts = sorted(f.row_id_start for f in listed)
    assert starts == list(range(1000))
    assert ns.table("many_files").info().record_count == 1000


def test_500_column_table(catalog, ns):
    schema = pa.schema(
        [pa.field("id", pa.int64(), nullable=False)]
        + [pa.field(f"c{i:03d}", pa.float64()) for i in range(499)]
    )
    t = ns.create_table("wide", schema)
    assert len(t.columns) == 500
    assert len({c.field_id for c in t.columns}) == 500  # unique field ids
    assert [c.ordinal for c in sorted(t.columns, key=lambda c: c.ordinal)] == list(
        range(500)
    )

    data = pa.table(
        {"id": pa.array([1, 2], pa.int64())}
        | {f"c{i:03d}": pa.array([float(i), None]) for i in range(499)},
        schema=schema,
    )
    t.append(data)
    (f,) = t.files()
    assert f.record_count == 2
    assert f.stats_state == "provided"
    assert ns.table("wide").info().record_count == 2


def test_view_with_100kb_sql(catalog, ns):
    big_sql = (
        "SELECT id, s FROM ns1.zero_rows WHERE s IN ("
        + ", ".join(f"'pad-{i:06d}'" for i in range(9000))
        + ")"
    )
    assert len(big_sql) > 100_000
    ns.create_view(f"big_view_{RUN_ID}", big_sql)
    got = ns.view(f"big_view_{RUN_ID}")
    assert got.sql == big_sql  # byte-for-byte round-trip
    got.drop()


# ---------------------------------------------------------------------------
# concurrency / storms / churn
# ---------------------------------------------------------------------------


def test_concurrent_same_consumer_offset_commits(catalog, ns):
    t = ns.create_table("racing", _schema())
    snaps = [t.append(_rows(i)).snapshot_id for i in range(6)]
    consumer = f"qe-race-{RUN_ID}"

    errors = []
    outcomes = []

    def worker(snapshot_id):
        try:
            off = catalog.commit_offset(consumer, t.table_uuid, snapshot_id)
            outcomes.append(off.committed_snapshot)
        except OffsetRegressionError:
            outcomes.append(None)  # legitimate loser of the race
        except Exception as e:  # anything else is a real failure
            errors.append(e)

    threads = [
        threading.Thread(target=worker, args=(s,)) for s in snaps for _ in (0, 1)
    ]
    for th in threads:
        th.start()
    for th in threads:
        th.join()

    assert errors == []
    # monotonicity invariant: the stored offset is the max anyone
    # successfully committed — never a regression, never garbage
    (final,) = [o.committed_snapshot for o in catalog.offsets(consumer)]
    committed = [o for o in outcomes if o is not None]
    assert committed  # at least one writer won
    assert final == max(committed) == max(snaps)


def test_alter_storm_20_renames_with_time_travel(catalog, ns):
    t = ns.create_table(f"storm0-{RUN_ID}", _schema())
    t.append(_rows(1))
    names_at = {}
    for i in range(1, 21):
        name = f"storm{i}-{RUN_ID}"
        t.alter([ops.rename_table(name)])
        names_at[catalog.refresh().head_snapshot_id] = name
    assert t.info().name == f"storm20-{RUN_ID}"

    raw = catalog._client._request
    for snap, name in names_at.items():
        # name resolution is snapshot-scoped: the historical name works
        # AT its snapshot...
        body = raw(
            "GET",
            f"/catalogs/{catalog.name}/namespaces/ns1/tables/{name}",
            params={"snapshot": snap},
        )
        assert body["name"] == name
        assert body["table_uuid"] == t.table_uuid
        assert body["record_count"] == 1

    # ...and the current name does NOT exist at the earliest storm snapshot
    first_snap = min(names_at)
    with pytest.raises(NotFoundError):
        raw(
            "GET",
            f"/catalogs/{catalog.name}/namespaces/ns1/tables/storm20-{RUN_ID}",
            params={"snapshot": first_snap},
        )


def test_create_drop_recreate_uuid_uniqueness(catalog, ns):
    uuids = []
    for i in range(10):
        t = ns.create_table("phoenix", _schema())
        uuids.append(t.table_uuid)
        t.append(_rows(i))
        res = t.drop()
        assert res.snapshot_id > 0
    assert len(set(uuids)) == 10  # every incarnation is a new identity
    with pytest.raises(NotFoundError):
        ns.table("phoenix")


def test_read_snapshot_zero_after_heavy_ddl_conflicts(catalog, ns):
    t = ns.create_table("ddl_heavy", _schema())
    t.append(_rows(1))
    t.alter([ops.add_column("extra", pa.float64())])
    t.alter([ops.rename_column("extra", "extra2")])
    t.alter([ops.drop_column("extra2")])

    with pytest.raises(CommitConflictError) as ei:
        t.append(_rows(2), read_snapshot=0)
    assert ei.value.retryable is True
    # the retry contract works: refresh and go again
    res = t.append(_rows(2), read_snapshot=catalog.refresh().head_snapshot_id)
    assert res.snapshot_id > 0
    assert t.info().record_count == 2
