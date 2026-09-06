"""Hypothesis mutation-fuzzing of wire-model parsing and error mapping.

Claims under test:

* Unknown extra fields at any level are ignored (forward compat).
* Missing optionals parse to their defaults.
* Error responses (any status 400-599, any body JSON shape or raw
  bytes) always map to the right HoglakeError subclass, never crash the
  mapper, and always stringify.
* Missing REQUIRED fields / wrong-typed nested containers raise ONE
  typed client-side error — MalformedResponseError — naming the model
  and the offending field (bugs.md #24; formerly leaked KeyError,
  AttributeError, or TypeError depending on the model's parse style).
"""

import httpx
import pytest
from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st

from pyhoglake import (
    AlreadyExistsError,
    CommitConflictError,
    ExpiredError,
    HoglakeError,
    MalformedResponseError,
    NotFoundError,
    ValidationError,
)
from pyhoglake.client import HoglakeClient
from pyhoglake.models import (
    CatalogInfo,
    CatalogOptions,
    ChangesPlan,
    CleanupResult,
    CommitResult,
    ConsumerOffset,
    DataFile,
    DeleteFile,
    ExpiryResult,
    ScanFile,
    Snapshot,
    TableInfo,
    ViewInfo,
)

# The "qe" hypothesis profile (deadline=None) is loaded in conftest.py.
# The composite model strategies are generation-heavy; cap explicitly.
MODEL_SETTINGS = settings(max_examples=50, suppress_health_check=[HealthCheck.too_slow])

# JSON-shaped junk for unknown extra fields
json_scalars = st.one_of(
    st.none(),
    st.booleans(),
    st.integers(),
    # bodies must be JSON-encodable: stdlib json rejects inf/-inf/nan
    st.floats(allow_nan=False, allow_infinity=False),
    st.text(),
)
json_values = st.recursive(
    json_scalars,
    lambda inner: st.one_of(
        st.lists(inner, max_size=3),
        st.dictionaries(st.text(max_size=8), inner, max_size=3),
    ),
    max_leaves=6,
)
extra_fields = st.dictionaries(
    st.text(min_size=1, max_size=16).filter(lambda k: not k.startswith("_")),
    json_values,
    max_size=4,
)

I64 = st.integers(-(2**63), 2**63 - 1)
ISO = st.sampled_from(
    [
        "2026-09-04T12:00:00Z",
        "2026-09-04T12:00:00+00:00",
        "2026-09-04T12:00:00.123456-05:00",
        "2026-01-01T00:00:00.000001+14:00",
    ]
)

DATA_FILE = st.fixed_dictionaries(
    {
        "data_file_id": I64,
        "path": st.text(max_size=50),
        "file_format": st.just("parquet"),
        "record_count": st.integers(0, 2**62),
        "file_size_bytes": st.integers(0, 2**62),
        "row_id_start": st.integers(0, 2**62),
        "stats_state": st.sampled_from(["provided", "pending", "failed"]),
        "begin_snapshot": st.integers(0, 2**62),
    },
    optional={
        "footer_size": I64,
        "spec_id": I64,
        "partition_values": st.lists(
            st.one_of(st.none(), st.text(max_size=8)), max_size=3
        ),
    },
)

DELETE_FILE = st.fixed_dictionaries(
    {
        "delete_file_id": I64,
        "data_file_id": I64,
        "path": st.text(max_size=50),
        "file_format": st.just("puffin-dv"),
        "delete_count": st.integers(0, 2**62),
        "file_size_bytes": st.integers(0, 2**62),
        "begin_snapshot": st.integers(0, 2**62),
    }
)

SNAPSHOT = st.fixed_dictionaries(
    {"snapshot_id": I64, "snapshot_time": ISO, "schema_version": I64},
    optional={
        "author": st.text(max_size=20),
        "message": st.text(max_size=40),
        "changes": st.lists(
            st.fixed_dictionaries(
                {"kind": st.text(max_size=12)}, optional={"object_id": I64}
            ),
            max_size=3,
        ),
    },
)

MODEL_CASES = [
    (
        CatalogInfo,
        st.fixed_dictionaries(
            {
                "name": st.text(max_size=20),
                "data_path": st.text(max_size=40),
                "head_snapshot_id": I64,
                "schema_version": I64,
            }
        ),
    ),
    (
        CatalogOptions,
        st.fixed_dictionaries(
            {"consumer_floor": st.booleans(), "earliest_snapshot_id": I64},
            optional={"snapshot_retention_seconds": st.one_of(st.none(), I64)},
        ),
    ),
    (
        ExpiryResult,
        st.fixed_dictionaries(
            {
                "snapshots_expired": I64,
                "data_files_queued": I64,
                "delete_files_queued": I64,
                "new_earliest_snapshot_id": I64,
            },
            optional={"floored_by_consumer": st.text(max_size=16)},
        ),
    ),
    (
        CleanupResult,
        st.fixed_dictionaries(
            {"removed": I64, "missing": I64, "still_referenced": I64}
        ),
    ),
    (Snapshot, SNAPSHOT),
    (
        ConsumerOffset,
        st.fixed_dictionaries(
            {
                "consumer_id": st.text(max_size=16),
                "table_uuid": st.uuids().map(str),
                "committed_snapshot": I64,
                "updated_at": ISO,
            }
        ),
    ),
    (DataFile, DATA_FILE),
    (DeleteFile, DELETE_FILE),
    (
        ScanFile,
        st.fixed_dictionaries(
            {"data_file": DATA_FILE},
            optional={"delete_file": st.one_of(st.none(), DELETE_FILE)},
        ),
    ),
    (
        ChangesPlan,
        st.fixed_dictionaries(
            {
                "table_uuid": st.uuids().map(str),
                "from_snapshot": I64,
                "to_snapshot": I64,
            },
            optional={
                "files": st.lists(DATA_FILE, max_size=2),
                "delete_files": st.lists(DELETE_FILE, max_size=2),
            },
        ),
    ),
    (
        ViewInfo,
        st.fixed_dictionaries(
            {
                "name": st.text(max_size=16),
                "namespace": st.text(max_size=16),
                "view_uuid": st.uuids().map(str),
                "dialect": st.text(max_size=8),
                "sql": st.text(max_size=200),
            }
        ),
    ),
    (
        CommitResult,
        st.fixed_dictionaries({"snapshot_id": I64}, optional={"schema_version": I64}),
    ),
    (
        TableInfo,
        st.fixed_dictionaries(
            {
                "name": st.text(max_size=16),
                "namespace": st.text(max_size=16),
                "table_uuid": st.uuids().map(str),
                "record_count": I64,
                "file_count": I64,
                "file_size_bytes": I64,
            },
            optional={
                "columns": st.lists(
                    st.fixed_dictionaries(
                        {
                            "name": st.text(max_size=12),
                            "type": st.sampled_from(["long", "string", "double"]),
                            "field_id": I64,
                            "ordinal": st.integers(0, 100),
                        },
                        optional={
                            "nullable": st.booleans(),
                            "type_params": st.dictionaries(
                                st.text(max_size=6), json_scalars, max_size=2
                            ),
                        },
                    ),
                    max_size=3,
                ),
                "partition_spec": st.one_of(
                    st.none(),
                    st.fixed_dictionaries(
                        {
                            "spec_id": I64,
                            "fields": st.lists(
                                st.fixed_dictionaries(
                                    {
                                        "source_field_id": I64,
                                        "transform": st.sampled_from(
                                            ["identity", "bucket", "year"]
                                        ),
                                    },
                                    optional={"transform_param": I64},
                                ),
                                max_size=2,
                            ),
                        }
                    ),
                ),
            },
        ),
    ),
]

model_case = st.sampled_from(range(len(MODEL_CASES))).flatmap(
    lambda i: st.tuples(st.just(MODEL_CASES[i][0]), MODEL_CASES[i][1], extra_fields)
)


@MODEL_SETTINGS
@given(model_case)
def test_models_parse_with_extra_unknown_fields(case):
    cls, wire, extras = case
    # extras must not shadow real fields
    extras = {k: v for k, v in extras.items() if k not in wire}
    polluted = {**wire, **extras}
    obj = cls.from_wire(polluted)
    clean = cls.from_wire(dict(wire))
    assert obj == clean  # unknown fields ignored, known fields identical


@MODEL_SETTINGS
@given(model_case)
def test_models_required_values_survive_verbatim(case):
    cls, wire, _ = case
    obj = cls.from_wire(dict(wire))
    # every scalar top-level field that is not container/datetime-parsed
    # must be stored verbatim
    parsed_specially = {
        "snapshot_time",
        "updated_at",
        "changes",
        "columns",
        "files",
        "delete_files",
        "partition_spec",
        "data_file",
        "delete_file",
        "partition_values",
    }
    for k, v in wire.items():
        if k in parsed_specially:
            continue
        assert getattr(obj, k) == v


# Regression (bugs.md #24, formerly an xfail BUG pin): a response
# missing a required field must raise MalformedResponseError — never a
# leaked KeyError (direct-index parsers) or TypeError (_pick parsers).
# Removing an OPTIONAL field must still parse cleanly.
@given(model_case, st.data())
def test_missing_required_field_raises_malformed_response(case, data):
    cls, wire, _ = case
    victim = data.draw(st.sampled_from(sorted(wire)))
    broken = {k: v for k, v in wire.items() if k != victim}
    try:
        cls.from_wire(broken)  # a removed OPTIONAL field parses fine
    except MalformedResponseError as e:
        # the one sanctioned parse error, naming the model
        assert cls.__name__ in str(e)
    # anything else (KeyError, TypeError, AttributeError...) propagates
    # out of the test and fails it


# Regression (bugs.md #24, formerly an xfail BUG pin): non-dict entries
# inside nested arrays (Snapshot.changes, TableInfo.columns,
# ChangesPlan.files) must raise MalformedResponseError naming the nested
# model — never AttributeError("'str' object has no attribute 'items'")
# out of _pick.
@given(
    st.one_of(st.text(max_size=4), st.integers(), st.lists(st.integers(), max_size=2))
)
def test_wrong_typed_nested_entries_raise_malformed_response(junk):
    wire = {
        "snapshot_id": 1,
        "snapshot_time": "2026-09-04T12:00:00Z",
        "schema_version": 1,
        "changes": [junk],
    }
    with pytest.raises(MalformedResponseError) as ei:
        Snapshot.from_wire(wire)
    assert "SnapshotChange" in str(ei.value)


def test_missing_required_field_message_names_model_and_field():
    with pytest.raises(MalformedResponseError, match="Snapshot.*snapshot_id"):
        Snapshot.from_wire({})
    with pytest.raises(MalformedResponseError, match="CatalogInfo.*name"):
        CatalogInfo.from_wire({"data_path": "s3://x/"})
    with pytest.raises(MalformedResponseError, match="ScanFile.*data_file"):
        ScanFile.from_wire({})


def test_non_object_body_raises_malformed_response():
    for junk in ("nope", 7, [1, 2], None):
        with pytest.raises(MalformedResponseError, match="expected a JSON object"):
            CommitResult.from_wire(junk)


def test_malformed_response_is_a_hoglake_error():
    # callers catching the taxonomy root must see parse failures too
    assert issubclass(MalformedResponseError, HoglakeError)


# -- datetime parsing pedantry ---------------------------------------------


@given(ISO)
def test_snapshot_time_parses_every_documented_iso_form(iso):
    s = Snapshot.from_wire(
        {"snapshot_id": 1, "snapshot_time": iso, "schema_version": 1}
    )
    assert s.snapshot_time.tzinfo is not None  # offset forms keep tz
    assert s.snapshot_time.year == 2026


def test_changes_and_files_default_to_empty_tuples():
    s = Snapshot.from_wire(
        {"snapshot_id": 1, "snapshot_time": "2026-09-04T12:00:00Z", "schema_version": 1}
    )
    assert s.changes == ()
    p = ChangesPlan.from_wire({"table_uuid": "u", "from_snapshot": 1, "to_snapshot": 2})
    assert p.files == () and p.delete_files == ()
    # explicit null arrays behave like absent ones
    p2 = ChangesPlan.from_wire(
        {
            "table_uuid": "u",
            "from_snapshot": 1,
            "to_snapshot": 2,
            "files": None,
            "delete_files": None,
        }
    )
    assert p2 == p


# -- ApiError mapping under fire -------------------------------------------

error_bodies = st.one_of(
    st.fixed_dictionaries({}, optional={"error": json_scalars, "detail": json_values}),
    json_values,  # non-dict JSON bodies
)


@MODEL_SETTINGS
@given(
    st.integers(400, 599),
    st.one_of(
        error_bodies.map(lambda b: ("json", b)),
        st.binary(max_size=64).map(lambda b: ("raw", b)),
        st.just(("raw", b"")),
    ),
    st.sampled_from([AlreadyExistsError, CommitConflictError]),
)
def test_error_mapping_total_and_correct(status, body, conflict_cls):
    kind, payload = body
    if kind == "json":
        resp = httpx.Response(status, json=payload)
    else:
        resp = httpx.Response(status, content=payload)
    with pytest.raises(HoglakeError) as ei:
        HoglakeClient._raise(resp, conflict_cls)
    exc = ei.value
    expected = {
        404: NotFoundError,
        409: conflict_cls,
        410: ExpiredError,
        422: ValidationError,
    }.get(status, HoglakeError)
    assert type(exc) is expected
    assert exc.status_code == status
    assert isinstance(str(exc), str)  # stringification never explodes
    # retryability contract: only commit conflicts retry
    assert exc.retryable is (status == 409 and conflict_cls is CommitConflictError)
