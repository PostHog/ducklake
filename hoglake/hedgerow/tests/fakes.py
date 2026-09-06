"""Scripted fake pyhoglake objects for driving Hedgerow cycles in-process.

All fakes share a ``calls`` journal (list of tuples) so tests can assert
call ORDER — most importantly that every destination append precedes the
source offset commit (viaduck lesson #2).

The fakes speak real pyhoglake model types (Column, DataFile,
ChangesPlan, ConsumerOffset) so the daemon exercises the same shapes it
sees in production.
"""

from __future__ import annotations

from collections.abc import Iterator, Sequence
from dataclasses import dataclass
from dataclasses import field as dc_field
from datetime import UTC, datetime
from typing import Any

import pyarrow as pa
from pyhoglake import (
    ChangesPlan,
    Column,
    CommitConflictError,
    CommitResult,
    ConsumerOffset,
    DataFile,
    DeleteFile,
    ExpiredError,
    IncarnationChangedError,
    NotFoundError,
)


def col(
    name: str,
    type_: str,
    field_id: int,
    ordinal: int,
    nullable: bool = True,
    type_params: dict | None = None,
) -> Column:
    return Column(
        name=name,
        type=type_,
        field_id=field_id,
        ordinal=ordinal,
        nullable=nullable,
        type_params=type_params,
    )


def data_file(
    path: str, record_count: int, begin_snapshot: int, file_id: int = 0
) -> DataFile:
    return DataFile(
        data_file_id=file_id,
        path=path,
        file_format="parquet",
        record_count=record_count,
        file_size_bytes=record_count * 10,
        row_id_start=0,
        stats_state="provided",
        begin_snapshot=begin_snapshot,
    )


def delete_file(path: str, data_file_id: int, begin_snapshot: int) -> DeleteFile:
    return DeleteFile(
        delete_file_id=1,
        data_file_id=data_file_id,
        path=path,
        file_format="puffin-dv",
        delete_count=1,
        file_size_bytes=64,
        begin_snapshot=begin_snapshot,
    )


class CrashRequested(Exception):
    """Injected failure standing in for a process crash."""


@dataclass
class FakeSourceTable:
    table_uuid: str
    columns: tuple[Column, ...]
    # (begin_snapshot -> list[DataFile]) appended in that snapshot
    files_by_snapshot: dict[int, list[DataFile]] = dc_field(default_factory=dict)
    delete_files_by_snapshot: dict[int, list[DeleteFile]] = dc_field(
        default_factory=dict
    )
    calls: list = dc_field(default_factory=list)
    expired_below: int = 0  # changes(from < expired_below) -> 410
    # if set, changes() reports this uuid in the plan (recreate-mid-flight)
    plan_uuid_override: str | None = None

    def changes(
        self, from_snapshot: int, to_snapshot: int | None = None
    ) -> ChangesPlan:
        self.calls.append(("changes", from_snapshot, to_snapshot))
        if from_snapshot < self.expired_below:
            raise ExpiredError(
                "snapshot range expired",
                status_code=410,
                detail=(
                    f"from_snapshot {from_snapshot} is below the expiry floor "
                    f"{self.expired_below}; reconcile from a full scan"
                ),
            )
        hi = (
            to_snapshot
            if to_snapshot is not None
            else max([0, *self.files_by_snapshot, *self.delete_files_by_snapshot])
        )
        files = [
            f
            for s in sorted(self.files_by_snapshot)
            if from_snapshot < s <= hi
            for f in self.files_by_snapshot[s]
        ]
        dels = [
            f
            for s in sorted(self.delete_files_by_snapshot)
            if from_snapshot < s <= hi
            for f in self.delete_files_by_snapshot[s]
        ]
        return ChangesPlan(
            table_uuid=self.plan_uuid_override or self.table_uuid,
            from_snapshot=from_snapshot,
            to_snapshot=hi,
            files=tuple(files),
            delete_files=tuple(dels),
        )


@dataclass
class FakeDestTable:
    table_uuid: str
    columns: tuple[Column, ...]
    calls: list = dc_field(default_factory=list)
    appended: list[pa.Table] = dc_field(default_factory=list)
    fail_on_append_call: int | None = None  # 1-based index of the call to fail
    # first N append calls 409 with a RETRYABLE commit conflict
    conflict_first_n_appends: int = 0
    _append_calls: int = 0
    _next_snapshot: int = 100

    def append(self, data: pa.Table, **kwargs: Any) -> CommitResult:
        self._append_calls += 1
        if self.fail_on_append_call == self._append_calls:
            raise CrashRequested("crash injected during append")
        if self._append_calls <= self.conflict_first_n_appends:
            raise CommitConflictError(
                "commit_conflict: concurrent DDL touched the table",
                status_code=409,
            )
        # Mimic the SERVER-side atomic commit guard: expected_table_uuid
        # rides the commit body, and the server 409s the whole commit —
        # zero writes — when the live table's uuid differs. pyhoglake
        # maps that 409 ("the table was recreated") to
        # IncarnationChangedError, which is what the daemon sees.
        expected = kwargs.get("expected_table_uuid")
        if expected is not None and str(expected) != self.table_uuid:
            raise IncarnationChangedError(
                f"commit_conflict: the table was recreated — expected "
                f"table_uuid {expected}, live table is {self.table_uuid}",
                status_code=409,
            )
        self.calls.append(("append", data.num_rows))
        self.appended.append(data)
        self._next_snapshot += 1
        return CommitResult(snapshot_id=self._next_snapshot)

    @property
    def total_rows(self) -> int:
        return sum(t.num_rows for t in self.appended)


@dataclass
class FakeNamespace:
    name: str
    tables: dict[str, Any] = dc_field(default_factory=dict)

    def table(self, name: str):
        if name not in self.tables:
            raise NotFoundError(f"table {name!r} not found", status_code=404)
        return self.tables[name]


@dataclass
class _Head:
    head_snapshot_id: int


@dataclass
class FakeCatalog:
    name: str
    namespaces: dict[str, FakeNamespace] = dc_field(default_factory=dict)
    head_snapshot_id: int = 0
    offsets_store: dict[tuple[str, str], int] = dc_field(default_factory=dict)
    calls: list = dc_field(default_factory=list)
    fail_on_offset_commit: int | None = None  # 1-based index of the call to fail
    _offset_calls: int = 0

    def namespace(self, name: str) -> FakeNamespace:
        if name not in self.namespaces:
            raise NotFoundError(f"namespace {name!r} not found", status_code=404)
        return self.namespaces[name]

    def refresh(self) -> _Head:
        return _Head(head_snapshot_id=self.head_snapshot_id)

    def offset(self, consumer_id: str, table_uuid: str) -> ConsumerOffset | None:
        """Single-offset GET (pyhoglake Catalog.offset): the stored
        offset for exactly (consumer, table_uuid), None when absent."""
        snap = self.offsets_store.get((consumer_id, table_uuid))
        if snap is None:
            return None
        return ConsumerOffset(
            consumer_id=consumer_id,
            table_uuid=table_uuid,
            committed_snapshot=snap,
            updated_at=datetime.now(UTC),
        )

    def offsets(self, consumer_id: str) -> list[ConsumerOffset]:
        now = datetime.now(UTC)
        return [
            ConsumerOffset(
                consumer_id=c, table_uuid=u, committed_snapshot=s, updated_at=now
            )
            for (c, u), s in self.offsets_store.items()
            if c == consumer_id
        ]

    def commit_offset(
        self, consumer_id: str, table_uuid: str, snapshot_id: int
    ) -> ConsumerOffset:
        self._offset_calls += 1
        if self.fail_on_offset_commit == self._offset_calls:
            raise CrashRequested("crash injected before offset commit")
        self.calls.append(("commit_offset", consumer_id, table_uuid, snapshot_id))
        self.offsets_store[(consumer_id, table_uuid)] = snapshot_id
        return ConsumerOffset(
            consumer_id=consumer_id,
            table_uuid=table_uuid,
            committed_snapshot=snapshot_id,
            updated_at=datetime.now(UTC),
        )


@dataclass
class FakeClient:
    catalogs: dict[str, FakeCatalog] = dc_field(default_factory=dict)

    def catalog(self, name: str) -> FakeCatalog:
        if name not in self.catalogs:
            raise NotFoundError(f"catalog {name!r} not found", status_code=404)
        return self.catalogs[name]

    def close(self) -> None:
        pass


def table_batch_reader(tables_by_path: dict[str, pa.Table]):
    """A BatchReader over in-memory tables keyed by 'path'."""

    def read(
        path: str, columns: Sequence[str], batch_size: int
    ) -> Iterator[pa.RecordBatch]:
        table = tables_by_path[path].select(list(columns))
        yield from table.to_batches(max_chunksize=batch_size)

    return read
