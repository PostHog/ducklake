"""Typed exceptions mapped from hoglake ApiError responses."""

from __future__ import annotations


class HoglakeError(Exception):
    """Base error for pyhoglake. Carries the server's ApiError fields."""

    retryable: bool = False

    def __init__(
        self,
        message: str,
        *,
        status_code: int | None = None,
        detail: str | None = None,
    ) -> None:
        super().__init__(message)
        self.message = message
        self.status_code = status_code
        self.detail = detail

    def __str__(self) -> str:  # pragma: no cover - repr sugar
        base = self.message
        if self.detail:
            base = f"{base}: {self.detail}"
        if self.status_code is not None:
            base = f"{base} (HTTP {self.status_code})"
        return base


class NotFoundError(HoglakeError):
    """404 — the named catalog/namespace/table/view/consumer does not exist."""


class AlreadyExistsError(HoglakeError):
    """409 on a create — an object with that name already exists."""


class CommitConflictError(HoglakeError):
    """409 on commit/alter — concurrent DDL on a touched table.

    Retryable: refresh your read snapshot and retry.
    """

    retryable = True


class ValidationError(HoglakeError):
    """422 — invalid request (bad op, bad stats shape, unknown field ids...)."""


class OffsetRegressionError(HoglakeError):
    """409 on consumer-offset commit — offsets are monotonic; the snapshot
    is below the stored offset."""


class ExpiredError(HoglakeError):
    """410 — the requested snapshot range falls below the catalog's expiry
    floor. Reconcile from a full scan rather than silently skipping."""


class IncarnationChangedError(HoglakeError):
    """The table resolved by name no longer carries the ``table_uuid``
    the caller expected (drop + recreate under the same name).

    Raised on the append path in two places: by the server — the commit
    ships ``expected_table_uuid`` and a mismatch 409s the whole commit
    atomically with zero writes ("the table was recreated") — and by the
    client's cheap pre-flight re-resolve, which fast-fails an
    already-dead incarnation before the parquet upload. Never retry
    blindly: the expected incarnation is gone and its history does not
    carry over."""


class MalformedResponseError(HoglakeError):
    """A response body did not match the wire contract: a required field
    was missing, or a field/nested object had the wrong shape.

    Raised client-side by every model's ``from_wire``; the message names
    the model and the offending field. Replaces the leaked
    ``KeyError``/``AttributeError``/``TypeError`` trio the hand-rolled
    parsers used to disagree on (bugs.md #24)."""


class UnsupportedTypeError(HoglakeError, TypeError):
    """An Arrow type with no hoglake column-type mapping."""
