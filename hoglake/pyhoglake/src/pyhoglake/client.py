"""The hoglake client: a thin wrapper over the control-plane REST API.

Data never flows through the server: ``Table.append`` writes parquet to
object storage itself (pyarrow S3FileSystem) and registers the file with
footer-derived stats via the commit endpoint (footer-shipping commits).
"""

from __future__ import annotations

import io
import struct
import uuid as _uuid
from collections.abc import Iterator
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, Self
from urllib.parse import quote

import httpx
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

from .errors import (
    AlreadyExistsError,
    CommitConflictError,
    ExpiredError,
    HoglakeError,
    IncarnationChangedError,
    NotFoundError,
    OffsetRegressionError,
    ValidationError,
)
from .models import (
    AppendedFile,
    AppendResult,
    CatalogInfo,
    CatalogOptions,
    ChangesPlan,
    CleanupResult,
    CommitResult,
    ConsumerOffset,
    DataFile,
    ExpiryResult,
    ScanFile,
    Snapshot,
    TableInfo,
    TableSummary,
    ViewInfo,
)
from .ops import AlterOp
from .stats import extract_column_stats
from .transforms import transform_strings
from .types import columns_to_arrow_schema, schema_to_column_defs

DEFAULT_TIMEOUT = 30.0

# Sentinel for Table.append(expected_table_uuid=...): opt out of the
# incarnation guard entirely — no pre-flight uuid check, and the commit
# carries no expected_table_uuid field (name-only resolution).
UNGUARDED = object()

# The server's commit 409 for a mismatched expected_table_uuid carries
# this phrase in its ApiError message/detail; it is how the client tells
# a recreation refusal (IncarnationChangedError, never retryable) from an
# ordinary commit conflict (CommitConflictError, retryable).
_RECREATED_MARKER = "the table was recreated"

# COLUMN names starting with this prefix are reserved for hoglake internals
# (``_hog_row_id`` is compaction's row-id carrier); the server 422s them at
# create/add/rename. Namespace/table/view names are NOT affected.
_RESERVED_COLUMN_PREFIX = "_hog"


def _check_reserved_columns(schema: pa.Schema) -> None:
    """Fast-fail schema field names using the reserved ``_hog`` column
    prefix BEFORE any request or parquet upload (the server would 422 the
    create, and an append would waste the S3 write)."""
    reserved = [n for n in schema.names if n.startswith(_RESERVED_COLUMN_PREFIX)]
    if reserved:
        raise ValidationError(
            f"column names {reserved} use the reserved "
            f"'{_RESERVED_COLUMN_PREFIX}' prefix (hoglake internal columns, "
            "e.g. _hog_row_id); the server refuses these with 422",
            status_code=None,
        )


def _seg(name: object) -> str:
    """Percent-encode one URL path segment. Identifiers are user data:
    a table named "a/b" or "a?x" must stay inside its segment, not
    rewrite the route (QE find, 2026-09-05)."""
    return quote(str(name), safe="")


@dataclass
class S3Config:
    """Object-store connection settings for the parquet write path.

    ``endpoint_override`` may carry the scheme (``http://localhost:19000``);
    path-style addressing is used when an endpoint override is set.
    """

    access_key: str | None = None
    secret_key: str | None = None
    endpoint_override: str | None = None
    region: str | None = None
    allow_bucket_creation: bool = False

    def filesystem(self):
        from pyarrow import fs

        kwargs: dict[str, Any] = {}
        if self.access_key is not None:
            kwargs["access_key"] = self.access_key
        if self.secret_key is not None:
            kwargs["secret_key"] = self.secret_key
        if self.endpoint_override is not None:
            kwargs["endpoint_override"] = self.endpoint_override
        if self.region is not None:
            kwargs["region"] = self.region
        if self.allow_bucket_creation:
            kwargs["allow_bucket_creation"] = True
        return fs.S3FileSystem(**kwargs)


def _ts_param(value: datetime | str | None) -> str | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        if value.tzinfo is None:
            # the server requires an ISO-8601 instant (with offset);
            # naive datetimes are taken as UTC

            value = value.replace(tzinfo=UTC)
        return value.isoformat()
    return value


def _travel_params(
    snapshot: int | None, at_timestamp: datetime | str | None
) -> dict[str, Any]:
    if snapshot is not None and at_timestamp is not None:
        raise ValueError("snapshot and at_timestamp are mutually exclusive")
    params: dict[str, Any] = {}
    if snapshot is not None:
        params["snapshot"] = snapshot
    if at_timestamp is not None:
        params["at_timestamp"] = _ts_param(at_timestamp)
    return params


class HoglakeClient:
    """Entry point. ``base_url`` is the server root (``/v1`` is appended)."""

    def __init__(
        self,
        base_url: str,
        *,
        s3: S3Config | None = None,
        timeout: float = DEFAULT_TIMEOUT,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.s3 = s3
        self._http = httpx.Client(base_url=self.base_url + "/v1", timeout=timeout)
        self._fs = None

    # -- lifecycle ---------------------------------------------------------

    def close(self) -> None:
        self._http.close()

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    # -- transport ---------------------------------------------------------

    def _request(
        self,
        method: str,
        path: str,
        *,
        json: Any = None,
        params: dict[str, Any] | None = None,
        conflict: type[HoglakeError] = AlreadyExistsError,
    ) -> Any:
        if params:
            params = {k: v for k, v in params.items() if v is not None}
        resp = self._http.request(method, path, json=json, params=params or None)
        if resp.status_code < 300:
            if not resp.content:
                return None
            return resp.json()
        if resp.status_code < 400:
            # Redirects are not followed (httpx default) and the hoglake
            # API never issues them: treating a 3xx as success would feed
            # an empty/HTML body to resp.json() and leak a raw
            # JSONDecodeError outside the error taxonomy (bugs.md #19).
            location = resp.headers.get("location")
            raise HoglakeError(
                f"unexpected redirect HTTP {resp.status_code} from the "
                "hoglake API (redirects are not followed; check base_url)",
                status_code=resp.status_code,
                detail=f"Location: {location}" if location else None,
            )
        self._raise(resp, conflict)

    @staticmethod
    def _raise(resp: httpx.Response, conflict: type[HoglakeError]) -> None:
        message = f"HTTP {resp.status_code}"
        detail = None
        try:
            body = resp.json()
            if isinstance(body, dict):
                message = body.get("error", message)
                detail = body.get("detail")
        except Exception:  # noqa: BLE001  # any body-parse failure falls back to raw text
            detail = resp.text[:500] or None
        cls: type[HoglakeError]
        if resp.status_code == 404:
            cls = NotFoundError
        elif resp.status_code == 409:
            cls = conflict
        elif resp.status_code == 410:
            cls = ExpiredError
        elif resp.status_code == 422:
            cls = ValidationError
        else:
            cls = HoglakeError
        raise cls(message, status_code=resp.status_code, detail=detail)

    def _filesystem(self):
        if self._fs is None:
            if self.s3 is None:
                raise HoglakeError(
                    "no S3 configuration: pass s3=S3Config(...) to "
                    "HoglakeClient to enable the parquet write path"
                )
            self._fs = self.s3.filesystem()
        return self._fs

    # -- catalogs ----------------------------------------------------------

    def create_catalog(self, name: str, data_path: str) -> Catalog:
        body = self._request(
            "POST", "/catalogs", json={"name": name, "data_path": data_path}
        )
        return Catalog(self, CatalogInfo.from_wire(body))

    def catalog(self, name: str) -> Catalog:
        body = self._request("GET", f"/catalogs/{_seg(name)}")
        return Catalog(self, CatalogInfo.from_wire(body))

    def list_catalogs(self) -> list[CatalogInfo]:
        body = self._request("GET", "/catalogs")
        return [CatalogInfo.from_wire(c) for c in body]


class Catalog:
    def __init__(self, client: HoglakeClient, info: CatalogInfo) -> None:
        self._client = client
        self._info = info

    # -- identity ----------------------------------------------------------

    @property
    def name(self) -> str:
        return self._info.name

    @property
    def data_path(self) -> str:
        return self._info.data_path

    @property
    def info(self) -> CatalogInfo:
        return self._info

    def refresh(self) -> CatalogInfo:
        body = self._client._request("GET", f"/catalogs/{_seg(self.name)}")
        self._info = CatalogInfo.from_wire(body)
        return self._info

    def _path(self, suffix: str = "") -> str:
        return f"/catalogs/{_seg(self.name)}{suffix}"

    def __repr__(self) -> str:  # pragma: no cover
        return f"<Catalog {self.name!r} data_path={self.data_path!r}>"

    # -- namespaces --------------------------------------------------------

    def create_namespace(self, name: str) -> Namespace:
        self._client._request("POST", self._path("/namespaces"), json={"name": name})
        return Namespace(self, name)

    def namespace(self, name: str) -> Namespace:
        # No per-namespace GET in the API; existence-check via the listing.
        if name not in self.list_namespaces():
            raise NotFoundError(
                f"namespace {name!r} not found in catalog {self.name!r}",
                status_code=404,
            )
        return Namespace(self, name)

    def list_namespaces(self) -> list[str]:
        body = self._client._request("GET", self._path("/namespaces"))
        return [ns["name"] for ns in body]

    # -- snapshots ---------------------------------------------------------

    def snapshots(
        self, after: int = 0, before: int | None = None, limit: int = 1000
    ) -> Iterator[Snapshot]:
        """Iterate snapshots; auto-paginates on has_more.

        Ascending (default): id > ``after``, oldest first. Descending:
        pass ``before`` to walk id < ``before``, newest first (a UI pages
        down from head + 1 with the last id of each page). ``before`` is
        mutually exclusive with a non-zero ``after`` — enforced here with
        a ValueError before any request (the server 422s the same
        combination).
        """
        if before is not None and after != 0:
            # eager (not deferred to first iteration): the bad call fails
            # where it is written, before any request
            raise ValueError(
                "snapshots(): 'before' and a non-zero 'after' are mutually "
                "exclusive cursors — pass exactly one (ascending walks use "
                "'after', descending walks use 'before')"
            )
        return self._snapshot_pages(
            "after" if before is None else "before",
            after if before is None else before,
            limit,
        )

    def _snapshot_pages(self, key: str, cursor: int, limit: int) -> Iterator[Snapshot]:
        while True:
            body = self._client._request(
                "GET",
                self._path("/snapshots"),
                params={key: cursor, "limit": limit},
            )
            snaps = body.get("snapshots") or []
            for s in snaps:
                yield Snapshot.from_wire(s)
            if not body.get("has_more") or not snaps:
                return
            cursor = snaps[-1]["snapshot_id"]

    # -- options / retention ----------------------------------------------

    def options(self) -> CatalogOptions:
        body = self._client._request("GET", self._path("/options"))
        return CatalogOptions.from_wire(body)

    def set_retention(
        self, seconds: int | None, consumer_floor: bool | None = None
    ) -> CatalogOptions:
        payload: dict[str, Any] = {"snapshot_retention_seconds": seconds}
        if consumer_floor is not None:
            payload["consumer_floor"] = consumer_floor
        body = self._client._request("PATCH", self._path("/options"), json=payload)
        return CatalogOptions.from_wire(body)

    # -- maintenance -------------------------------------------------------

    def expire(self, batch: int | None = None) -> ExpiryResult:
        body = self._client._request(
            "POST", self._path("/maintenance/expire"), params={"batch": batch}
        )
        return ExpiryResult.from_wire(body)

    def cleanup(self, batch: int | None = None) -> CleanupResult:
        body = self._client._request(
            "POST", self._path("/maintenance/cleanup"), params={"batch": batch}
        )
        return CleanupResult.from_wire(body)

    # -- consumer offsets --------------------------------------------------

    def commit_offset(
        self, consumer_id: str, table_uuid: str, snapshot_id: int
    ) -> ConsumerOffset:
        body = self._client._request(
            "PUT",
            self._path(f"/consumers/{_seg(consumer_id)}/offsets/{_seg(table_uuid)}"),
            json={"snapshot_id": snapshot_id},
            conflict=OffsetRegressionError,
        )
        return ConsumerOffset.from_wire(body)

    def offsets(self, consumer_id: str) -> list[ConsumerOffset]:
        body = self._client._request(
            "GET", self._path(f"/consumers/{_seg(consumer_id)}/offsets")
        )
        return [ConsumerOffset.from_wire(o) for o in body]

    def offset(self, consumer_id: str, table_uuid: str) -> ConsumerOffset | None:
        """One (consumer, table_uuid) offset, or None when the server has
        none stored (404).

        Deliberate divergence from the client's raise-on-404 idiom: an
        absent offset is the routine state of every consumer that has not
        committed yet, not an error condition.
        """
        try:
            body = self._client._request(
                "GET",
                self._path(
                    f"/consumers/{_seg(consumer_id)}/offsets/{_seg(table_uuid)}"
                ),
            )
        except NotFoundError:
            return None
        return ConsumerOffset.from_wire(body)

    # -- commit (internal; Table.append is the public writer path) ---------

    def _commit(self, payload: dict[str, Any]) -> CommitResult:
        try:
            body = self._client._request(
                "POST",
                self._path("/commit"),
                json=payload,
                conflict=CommitConflictError,
            )
        except CommitConflictError as e:
            # Discriminate the expected_table_uuid guard's 409 from an
            # ordinary (retryable) commit conflict: the server's
            # recreation refusal says "the table was recreated" in its
            # message/detail. That refusal is atomic (zero writes) and
            # never retryable — surface it as IncarnationChangedError.
            text = f"{e.message} {e.detail or ''}".lower()
            if _RECREATED_MARKER in text:
                raise IncarnationChangedError(
                    e.message, status_code=e.status_code, detail=e.detail
                ) from e
            raise
        return CommitResult.from_wire(body)


class Namespace:
    def __init__(self, catalog: Catalog, name: str) -> None:
        self._catalog = catalog
        self.name = name

    def _path(self, suffix: str = "") -> str:
        return self._catalog._path(f"/namespaces/{_seg(self.name)}{suffix}")

    def __repr__(self) -> str:  # pragma: no cover
        return f"<Namespace {self._catalog.name}.{self.name}>"

    # -- tables ------------------------------------------------------------

    def create_table(self, name: str, schema: pa.Schema) -> Table:
        _check_reserved_columns(schema)
        body = self._catalog._client._request(
            "POST",
            self._path("/tables"),
            json={"name": name, "columns": schema_to_column_defs(schema)},
        )
        return Table(self, TableInfo.from_wire(body))

    def table(self, name: str) -> Table:
        body = self._catalog._client._request(
            "GET", self._path(f"/tables/{_seg(name)}")
        )
        return Table(self, TableInfo.from_wire(body))

    def list_tables(self) -> list[TableSummary]:
        body = self._catalog._client._request("GET", self._path("/tables"))
        return [TableSummary.from_wire(t) for t in body]

    # -- views -------------------------------------------------------------

    def create_view(self, name: str, sql: str, dialect: str = "trino") -> View:
        body = self._catalog._client._request(
            "POST",
            self._path("/views"),
            json={"name": name, "sql": sql, "dialect": dialect},
        )
        return View(self, ViewInfo.from_wire(body))

    def view(self, name: str) -> View:
        body = self._catalog._client._request("GET", self._path(f"/views/{_seg(name)}"))
        return View(self, ViewInfo.from_wire(body))

    def list_views(self) -> list[ViewInfo]:
        body = self._catalog._client._request("GET", self._path("/views"))
        return [ViewInfo.from_wire(v) for v in body]


class View:
    def __init__(self, namespace: Namespace, info: ViewInfo) -> None:
        self._namespace = namespace
        self._info = info

    @property
    def name(self) -> str:
        return self._info.name

    @property
    def sql(self) -> str:
        return self._info.sql

    @property
    def dialect(self) -> str:
        return self._info.dialect

    @property
    def view_uuid(self) -> str:
        return self._info.view_uuid

    @property
    def info(self) -> ViewInfo:
        return self._info

    def drop(self) -> CommitResult:
        body = self._namespace._catalog._client._request(
            "DELETE", self._namespace._path(f"/views/{_seg(self.name)}")
        )
        return CommitResult.from_wire(body)


class Table:
    def __init__(self, namespace: Namespace, info: TableInfo) -> None:
        self._namespace = namespace
        self._info = info

    # -- identity ----------------------------------------------------------

    @property
    def name(self) -> str:
        return self._info.name

    @property
    def namespace(self) -> str:
        return self._namespace.name

    @property
    def table_uuid(self) -> str:
        return self._info.table_uuid

    @property
    def columns(self):
        return self._info.columns

    def _path(self, suffix: str = "") -> str:
        return self._namespace._path(f"/tables/{_seg(self._info.name)}{suffix}")

    def __repr__(self) -> str:  # pragma: no cover
        return (
            f"<Table {self._namespace._catalog.name}.{self.namespace}."
            f"{self.name} uuid={self.table_uuid}>"
        )

    # -- reads -------------------------------------------------------------

    def info(
        self,
        snapshot: int | None = None,
        at_timestamp: datetime | str | None = None,
    ) -> TableInfo:
        body = self._namespace._catalog._client._request(
            "GET", self._path(), params=_travel_params(snapshot, at_timestamp)
        )
        info = TableInfo.from_wire(body)
        if snapshot is None and at_timestamp is None:
            self._info = info
        return info

    def files(
        self,
        snapshot: int | None = None,
        at_timestamp: datetime | str | None = None,
    ) -> list[DataFile]:
        body = self._namespace._catalog._client._request(
            "GET",
            self._path("/files"),
            params=_travel_params(snapshot, at_timestamp),
        )
        return [DataFile.from_wire(f) for f in body]

    def scan_plan(
        self,
        snapshot: int | None = None,
        at_timestamp: datetime | str | None = None,
    ) -> list[ScanFile]:
        body = self._namespace._catalog._client._request(
            "GET",
            self._path("/scan"),
            params=_travel_params(snapshot, at_timestamp),
        )
        return [ScanFile.from_wire(f) for f in body]

    def changes(
        self, from_snapshot: int, to_snapshot: int | None = None
    ) -> ChangesPlan:
        """Changefeed plan for rows appended in (from_snapshot, to_snapshot].

        Raises :class:`ExpiredError` (410) when part of the range has been
        expired — reconcile from a full scan.
        """
        body = self._namespace._catalog._client._request(
            "GET",
            self._path("/changes"),
            params={"from_snapshot": from_snapshot, "to_snapshot": to_snapshot},
        )
        return ChangesPlan.from_wire(body)

    # -- DDL ---------------------------------------------------------------

    def alter(self, ops: list[AlterOp]) -> TableInfo:
        body = self._namespace._catalog._client._request(
            "POST",
            self._path("/alter"),
            json={"ops": [op.to_wire() for op in ops]},
            conflict=CommitConflictError,
        )
        self._info = TableInfo.from_wire(body)
        return self._info

    def drop(self) -> CommitResult:
        body = self._namespace._catalog._client._request("DELETE", self._path())
        return CommitResult.from_wire(body)

    # -- THE writer path ---------------------------------------------------

    def append(
        self,
        data: pa.Table,
        *,
        expected_table_uuid: _uuid.UUID | str | object | None = None,
        deferred_stats: bool = False,
        read_snapshot: int | None = None,
        author: str | None = None,
        message: str | None = None,
        row_group_size: int | None = None,
    ) -> AppendResult:
        """Write ``data`` to the catalog's data path and register it via a
        footer-shipping commit: one parquet file for an unpartitioned
        table, or — when the table has a live partition spec — one file
        per distinct partition tuple (fanout), all registered in ONE
        atomic commit.

        The parquet schema carries the catalog's field ids
        (``PARQUET:field_id``). Unless ``deferred_stats``, per-column stats
        are extracted from the writer's own footer metadata (never re-read
        from object storage) and shipped with the commit.

        **Partitioned tables.** Each row's partition tuple is computed
        client-side under the table's CURRENT spec (Iceberg-semantics
        transforms — see :mod:`pyhoglake.transforms`); rows with a null
        source value land in a null partition group, per Iceberg. Every
        registered file carries its ``partition_values`` (transformed
        values as wire strings, by key_index), which the returned
        :class:`AppendResult` exposes per file. The spec used is the one
        the pre-flight resolve returned; if the spec changes between that
        resolve and the commit, the server refuses the commit itself
        (409 -> :class:`CommitConflictError` for concurrent DDL, 422 ->
        :class:`ValidationError` for an arity mismatch) — the client
        never silently re-specs. A refused commit orphans the uploaded
        parquet files (cleanup's problem, never the catalog's).

        **Incarnation guard — atomic at commit.** The commit payload is
        addressed by (namespace, table) NAME, so it lands on whatever
        table currently holds that name. ``expected_table_uuid``
        (default: the ``table_uuid`` this ``Table`` object was resolved
        as; pass one explicitly to pin a specific incarnation) is shipped
        ON the commit body, and the server rejects the whole commit with
        409 — atomically, zero writes — when the live table's uuid
        differs (drop + recreate under the same name). The client maps
        that refusal to :class:`IncarnationChangedError`; ordinary commit
        conflicts stay :class:`CommitConflictError` (retryable).

        A single cheap pre-flight re-resolve runs before the parquet
        upload as an optimization only (fast-fail on an already-dead
        incarnation saves the S3 write); the server-side guard is the
        safety mechanism. A commit-time refusal orphans the uploaded
        parquet (cleanup's problem, never the catalog's).

        Pass ``expected_table_uuid=pyhoglake.UNGUARDED`` to opt out: no
        pre-flight uuid check, and the commit carries no
        ``expected_table_uuid`` field (name-only resolution).
        """
        catalog = self._namespace._catalog
        client = catalog._client

        # Reserved-prefix fast-fail before ANY request or upload: a user
        # `_hog*` field could otherwise reach parquet on a pre-reservation
        # table and waste the S3 write.
        _check_reserved_columns(data.schema)

        if expected_table_uuid is UNGUARDED:
            expected = None
        elif expected_table_uuid is None:
            expected = self._info.table_uuid
        else:
            expected = str(expected_table_uuid)

        if expected is not None:
            # Pre-flight fast-fail (optimization, not the guarantee):
            # re-resolve by name before paying for the parquet upload.
            info = self._check_incarnation(expected)  # current columns + spec
        else:
            info = self.info()  # UNGUARDED: name-only resolution

        target_schema = columns_to_arrow_schema(info.columns)
        data = _align_table(data, target_schema)

        partitioned = info.partition_spec is not None and info.partition_spec.fields
        if partitioned:
            if data.num_rows == 0:
                raise ValidationError(
                    f"cannot append 0 rows to partitioned table "
                    f"{self.namespace}.{self.name}: no partition tuple is "
                    "derivable and a commit registers at least one file",
                    status_code=None,
                )
            groups = _partition_groups(data, info)
        else:
            groups = [(None, data)]

        file_regs: list[dict[str, Any]] = []
        appended: list[AppendedFile] = []
        for partition_values, part in groups:
            reg = _write_one_file(
                part,
                info,
                catalog,
                client,
                deferred_stats=deferred_stats,
                row_group_size=row_group_size,
                partition_values=partition_values,
            )
            file_regs.append(reg)
            appended.append(
                AppendedFile(
                    path=reg["path"],
                    record_count=reg["record_count"],
                    partition_values=partition_values,
                )
            )

        append_entry: dict[str, Any] = {
            "namespace": self.namespace,
            "table": self.name,
            "files": file_regs,
        }
        if expected is not None:
            # The atomic guard: the server 409s the whole commit (zero
            # writes) when the live table's uuid differs.
            append_entry["expected_table_uuid"] = expected

        payload: dict[str, Any] = {"appends": [append_entry]}
        if read_snapshot is not None:
            payload["read_snapshot"] = read_snapshot
        if author is not None:
            payload["author"] = author
        if message is not None:
            payload["message"] = message

        # No second re-resolve: the server enforces expected_table_uuid
        # atomically at commit time (409, zero writes), superseding the
        # old post-upload check. A refusal orphans the uploaded parquet
        # (cleanup's problem, never the catalog's).
        result = catalog._commit(payload)
        return AppendResult(
            snapshot_id=result.snapshot_id,
            schema_version=result.schema_version,
            files=tuple(appended),
        )

    def _check_incarnation(self, expected_uuid: str) -> TableInfo:
        """Pre-flight fast-fail: re-resolve this table by name and raise
        IncarnationChangedError if the name now binds to a different
        table_uuid. Purely an optimization — it saves the parquet upload
        when the incarnation is already dead; the server-side
        ``expected_table_uuid`` commit guard is the atomic safety
        mechanism. ``self._info`` is only adopted when the incarnation
        matches, so the pinned identity (and the default
        ``expected_table_uuid`` of later appends) is never silently
        rebased onto a recreated table."""
        body = self._namespace._catalog._client._request("GET", self._path())
        info = TableInfo.from_wire(body)
        if info.table_uuid != expected_uuid:
            raise IncarnationChangedError(
                f"table {self._namespace._catalog.name}/{self.namespace}."
                f"{self.name} was recreated: expected table_uuid "
                f"{expected_uuid}, name now resolves to {info.table_uuid}. "
                "Refusing to append across incarnations."
            )
        self._info = info
        return info


def _align_table(data: pa.Table, target: pa.Schema) -> pa.Table:
    """Reorder ``data`` to the catalog column order and cast to the target
    schema (which carries the field-id metadata)."""
    have = set(data.schema.names)
    want = list(target.names)
    missing = [n for n in want if n not in have]
    if missing:
        raise ValidationError(
            f"data is missing table columns: {missing}", status_code=None
        )
    extra = [n for n in data.schema.names if n not in set(want)]
    if extra:
        raise ValidationError(
            f"data has columns not in the table schema: {extra}",
            status_code=None,
        )
    data = data.select(want)
    return data.cast(target)


def _write_one_file(
    part: pa.Table,
    info: TableInfo,
    catalog: Catalog,
    client: HoglakeClient,
    *,
    deferred_stats: bool,
    row_group_size: int | None,
    partition_values: tuple[str | None, ...] | None,
) -> dict[str, Any]:
    """THE single-file writer path: serialize ``part`` to parquet (field
    ids already on the schema), upload it under the catalog's data path,
    and build its FileRegistration wire dict — unchanged conventions
    (footer stats from the writer's own metadata, exact ``footer_size``),
    plus ``partition_values`` when the table is partitioned."""
    sink = io.BytesIO()
    if row_group_size is not None:
        pq.write_table(part, sink, row_group_size=row_group_size)
    else:
        pq.write_table(part, sink)
    raw = sink.getvalue()
    metadata = pq.read_metadata(io.BytesIO(raw))

    column_stats = None
    if not deferred_stats:
        column_stats = extract_column_stats(metadata, info.columns)

    data_path = catalog.data_path
    if not data_path.endswith("/"):
        data_path += "/"
    file_uri = f"{data_path}data/{info.namespace}/{info.name}/{_uuid.uuid4()}.parquet"
    _upload(client._filesystem(), file_uri, raw)

    file_reg: dict[str, Any] = {
        "path": file_uri,
        "record_count": metadata.num_rows,
        "file_size_bytes": len(raw),
        "footer_size": _footer_size(raw),
    }
    if column_stats is not None:
        file_reg["column_stats"] = [s.to_wire() for s in column_stats]
    if partition_values is not None:
        file_reg["partition_values"] = list(partition_values)
    return file_reg


def _partition_groups(
    data: pa.Table, info: TableInfo
) -> list[tuple[tuple[str | None, ...], pa.Table]]:
    """Split an aligned batch by partition tuple under the table's live
    spec: one (wire-string tuple, sub-table) per distinct tuple, ordered
    by first occurrence in the batch (so file registration — and the
    server's rows-then-offset row-id assignment — follows input order).

    Transforms run arrow-native where possible and per UNIQUE value in
    Python otherwise (see :func:`pyhoglake.transforms.transform_strings`);
    a null source value yields a null partition value forming its own
    group, per Iceberg.
    """
    spec = info.partition_spec
    by_field_id = {c.field_id: c for c in info.columns}
    key_names = [f"__hog_pk_{i}" for i in range(len(spec.fields))]
    key_arrays: list[pa.Array] = []
    for pf in spec.fields:
        col = by_field_id.get(pf.source_field_id)
        if col is None:
            raise ValidationError(
                f"partition spec (spec_id={spec.spec_id}) references "
                f"field_id {pf.source_field_id}, which is not a live column "
                f"of {info.namespace}.{info.name}",
                status_code=None,
            )
        key_arrays.append(
            transform_strings(
                pf.transform,
                pf.transform_param,
                data.column(col.name),
                col.type,
                col.type_params,
            )
        )
    keyed = pa.table(
        {
            **dict(zip(key_names, key_arrays, strict=True)),
            "__hog_row": pa.array(range(data.num_rows), pa.int64()),
        }
    )
    combos = (
        keyed.group_by(key_names)
        .aggregate([("__hog_row", "min")])
        .sort_by("__hog_row_min")
    )
    out: list[tuple[tuple[str | None, ...], pa.Table]] = []
    for i in range(combos.num_rows):
        values = tuple(combos.column(k)[i].as_py() for k in key_names)
        mask = None
        for name, value in zip(key_names, values, strict=True):
            key_col = keyed.column(name)
            if value is None:
                field_mask = pc.is_null(key_col)
            else:
                field_mask = pc.fill_null(pc.equal(key_col, value), False)
            mask = field_mask if mask is None else pc.and_(mask, field_mask)
        out.append((values, data.filter(mask)))
    return out


def _footer_size(raw: bytes) -> int:
    """Thrift footer-metadata length for the commit's ``footer_size``.

    Wire convention (bugs.md #7): ``footer_size`` is EXACTLY the 4-byte
    LE length stored in the parquet trailer — the serialized thrift
    FileMetaData size, EXCLUDING the trailing 8-byte suffix (4-byte
    length + ``PAR1`` magic). The server's hydrator tail-reads
    ``[file_size - footer_size - 8, file_size)`` and compaction stores
    the same value for its own outputs; shipping ``meta_len + 8`` here
    (the old behavior) made every client-written file 8 bytes off.
    """
    (meta_len,) = struct.unpack("<I", raw[-8:-4])
    return meta_len


def _upload(fs, uri: str, raw: bytes) -> None:
    if not uri.startswith("s3://"):
        raise HoglakeError(
            f"unsupported data_path scheme for the write path: {uri!r} "
            "(only s3:// is supported)"
        )
    key = uri[len("s3://") :]
    with fs.open_output_stream(key) as out:
        out.write(raw)
