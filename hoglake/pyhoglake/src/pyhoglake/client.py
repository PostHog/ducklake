"""The hoglake client: a thin wrapper over the control-plane REST API.

Data never flows through the server: ``Table.append`` writes parquet to
object storage itself (pyarrow S3FileSystem) and registers the file with
footer-derived stats via the commit endpoint (footer-shipping commits).
"""

from __future__ import annotations

import io
import struct
import uuid as _uuid
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Iterator

import httpx
from urllib.parse import quote
import pyarrow as pa
import pyarrow.parquet as pq

from .errors import (
    AlreadyExistsError,
    CommitConflictError,
    ExpiredError,
    HoglakeError,
    NotFoundError,
    OffsetRegressionError,
    ValidationError,
)
from .models import (
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
from .types import columns_to_arrow_schema, schema_to_column_defs

DEFAULT_TIMEOUT = 30.0


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


def _ts_param(value: "datetime | str | None") -> str | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        if value.tzinfo is None:
            # the server requires an ISO-8601 instant (with offset);
            # naive datetimes are taken as UTC
            from datetime import timezone

            value = value.replace(tzinfo=timezone.utc)
        return value.isoformat()
    return value


def _travel_params(
    snapshot: int | None, at_timestamp: "datetime | str | None"
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

    def __enter__(self) -> "HoglakeClient":
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
        if resp.status_code < 400:
            if not resp.content:
                return None
            return resp.json()
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
        except Exception:
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

    def create_catalog(self, name: str, data_path: str) -> "Catalog":
        body = self._request(
            "POST", "/catalogs", json={"name": name, "data_path": data_path}
        )
        return Catalog(self, CatalogInfo.from_wire(body))

    def catalog(self, name: str) -> "Catalog":
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

    def create_namespace(self, name: str) -> "Namespace":
        self._client._request(
            "POST", self._path("/namespaces"), json={"name": name}
        )
        return Namespace(self, name)

    def namespace(self, name: str) -> "Namespace":
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

    def snapshots(self, after: int = 0, limit: int = 1000) -> Iterator[Snapshot]:
        """Iterate snapshots with id > ``after``; auto-paginates on has_more."""
        cursor = after
        while True:
            body = self._client._request(
                "GET",
                self._path("/snapshots"),
                params={"after": cursor, "limit": limit},
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
            self._path(f"/consumers/{_seg(consumer_id)}/offsets/{table_uuid}"),
            json={"snapshot_id": snapshot_id},
            conflict=OffsetRegressionError,
        )
        return ConsumerOffset.from_wire(body)

    def offsets(self, consumer_id: str) -> list[ConsumerOffset]:
        body = self._client._request(
            "GET", self._path(f"/consumers/{_seg(consumer_id)}/offsets")
        )
        return [ConsumerOffset.from_wire(o) for o in body]

    # -- commit (internal; Table.append is the public writer path) ---------

    def _commit(self, payload: dict[str, Any]) -> CommitResult:
        body = self._client._request(
            "POST",
            self._path("/commit"),
            json=payload,
            conflict=CommitConflictError,
        )
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

    def create_table(self, name: str, schema: pa.Schema) -> "Table":
        body = self._catalog._client._request(
            "POST",
            self._path("/tables"),
            json={"name": name, "columns": schema_to_column_defs(schema)},
        )
        return Table(self, TableInfo.from_wire(body))

    def table(self, name: str) -> "Table":
        body = self._catalog._client._request(
            "GET", self._path(f"/tables/{_seg(name)}")
        )
        return Table(self, TableInfo.from_wire(body))

    def list_tables(self) -> list[TableSummary]:
        body = self._catalog._client._request("GET", self._path("/tables"))
        return [TableSummary.from_wire(t) for t in body]

    # -- views -------------------------------------------------------------

    def create_view(self, name: str, sql: str, dialect: str = "trino") -> "View":
        body = self._catalog._client._request(
            "POST",
            self._path("/views"),
            json={"name": name, "sql": sql, "dialect": dialect},
        )
        return View(self, ViewInfo.from_wire(body))

    def view(self, name: str) -> "View":
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
        at_timestamp: "datetime | str | None" = None,
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
        at_timestamp: "datetime | str | None" = None,
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
        at_timestamp: "datetime | str | None" = None,
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

    def alter(self, ops: "list[AlterOp]") -> TableInfo:
        body = self._namespace._catalog._client._request(
            "POST",
            self._path("/alter"),
            json={"ops": [op.to_wire() for op in ops]},
            conflict=CommitConflictError,
        )
        self._info = TableInfo.from_wire(body)
        return self._info

    def drop(self) -> CommitResult:
        body = self._namespace._catalog._client._request(
            "DELETE", self._path()
        )
        return CommitResult.from_wire(body)

    # -- THE writer path ---------------------------------------------------

    def append(
        self,
        data: pa.Table,
        *,
        deferred_stats: bool = False,
        read_snapshot: int | None = None,
        author: str | None = None,
        message: str | None = None,
        row_group_size: int | None = None,
    ) -> CommitResult:
        """Write ``data`` as one parquet file to the catalog's data path and
        register it via a footer-shipping commit.

        The parquet schema carries the catalog's field ids
        (``PARQUET:field_id``). Unless ``deferred_stats``, per-column stats
        are extracted from the writer's own footer metadata (never re-read
        from object storage) and shipped with the commit.
        """
        catalog = self._namespace._catalog
        client = catalog._client

        info = self.info()  # refresh: current columns + partition spec
        if info.partition_spec is not None and info.partition_spec.fields:
            raise HoglakeError(
                "pyhoglake 0.1 cannot append to partitioned tables "
                "(client-side partition-value transformation not implemented)"
            )

        target_schema = columns_to_arrow_schema(info.columns)
        data = _align_table(data, target_schema)

        sink = io.BytesIO()
        if row_group_size is not None:
            pq.write_table(data, sink, row_group_size=row_group_size)
        else:
            pq.write_table(data, sink)
        raw = sink.getvalue()
        metadata = pq.read_metadata(io.BytesIO(raw))

        column_stats = None
        if not deferred_stats:
            column_stats = extract_column_stats(metadata, info.columns)

        data_path = catalog.data_path
        if not data_path.endswith("/"):
            data_path += "/"
        file_uri = (
            f"{data_path}data/{self.namespace}/{self.name}/"
            f"{_uuid.uuid4()}.parquet"
        )
        _upload(client._filesystem(), file_uri, raw)

        file_reg: dict[str, Any] = {
            "path": file_uri,
            "record_count": metadata.num_rows,
            "file_size_bytes": len(raw),
            "footer_size": _footer_size(raw),
        }
        if column_stats is not None:
            file_reg["column_stats"] = [s.to_wire() for s in column_stats]

        payload: dict[str, Any] = {
            "appends": [
                {
                    "namespace": self.namespace,
                    "table": self.name,
                    "files": [file_reg],
                }
            ]
        }
        if read_snapshot is not None:
            payload["read_snapshot"] = read_snapshot
        if author is not None:
            payload["author"] = author
        if message is not None:
            payload["message"] = message
        return catalog._commit(payload)


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


def _footer_size(raw: bytes) -> int:
    # trailing 8 bytes: 4-byte LE footer length + b"PAR1"
    (meta_len,) = struct.unpack("<I", raw[-8:-4])
    return meta_len + 8


def _upload(fs, uri: str, raw: bytes) -> None:
    if not uri.startswith("s3://"):
        raise HoglakeError(
            f"unsupported data_path scheme for the write path: {uri!r} "
            "(only s3:// is supported)"
        )
    key = uri[len("s3://") :]
    with fs.open_output_stream(key) as out:
        out.write(raw)
