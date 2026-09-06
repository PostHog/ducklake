"""The hedgerow daemon: one loop replicating ONE source table to ONE
destination table, append-only, at-least-once.

The viaduck lessons this encodes (each is a requirement, not a style):

1. NO central scheduler, no flush-cadence coupling. One writer loop with
   its own clock; the write IS the pacing: poll -> write -> commit
   offset -> repeat. Nothing external tells this loop when to flush.
2. Offset commits strictly AFTER destination durability (rows first,
   then offset), and only ever to a fully applied window's
   ``plan.to_snapshot`` (commit-through-complete-windows). A crash
   between append and offset-commit replays the whole window:
   AT-LEAST-ONCE, duplicates possible, documented loudly.
3. Incarnation guard: source and destination table_uuids are pinned at
   startup; any cycle that resolves a different uuid HALTS loudly.
   Never silently continue against a recreated table.
4. ExpiredError (410) from changes() HALTS loudly with the server's
   reconcile instructions. Never skip a gap silently (the
   retention-clamp lesson).
5. Bounded memory: never more than ``max_rows_per_append`` rows
   materialized; files are streamed batch-by-batch, sequentially. No
   queues, no buffers beyond the current batch.
6. Fail-fast config validation at startup: missing tables and schema
   mismatches refuse to start with a precise diff.
7. Observability: one structured log line per cycle + optional
   prometheus metrics — passive reporting only, never control flow.
8. Deletes in the plan HALT loudly: append-only mode cannot represent
   them (v1 contract).

The 2026-09-05 adversarial review added four requirements on top:

9.  The incarnation guard extends to the WRITE path: every append ships
    ``expected_table_uuid`` on the commit body, and the SERVER enforces
    it atomically at commit time (409, zero writes, on mismatch) — so a
    destination recreate mid-window halts instead of splitting appends
    across incarnations (BUG-1). The old resolve->POST residual race is
    closed; pyhoglake's pre-flight re-resolve remains only as an
    upload-saving fast-fail.
10. Rows read per file are reconciled against the plan's record_count;
    a short read HALTS (DataIntegrityError) before any offset commit
    (BUG-2).
11. Transient retries are bounded: at most max_window_replays window
    replays (each loudly logged — duplicates possible), then a
    PersistentFailureError HALT; permanent 4xx client errors halt
    immediately (BUG-3).
12. An offset regression on commit is split-brain evidence (foreign
    writer on our consumer_id) and HALTS as SplitBrainError; the
    foreign offset is never adopted (BUG-4).
"""

from __future__ import annotations

import logging
import time
from collections.abc import Callable, Iterator, Sequence
from dataclasses import dataclass

import pyarrow as pa
import pyarrow.parquet as pq
from pyhoglake import (
    AlreadyExistsError,
    CommitConflictError,
    ExpiredError,
    HoglakeClient,
    NotFoundError,
    OffsetRegressionError,
    ValidationError,
)
from pyhoglake import IncarnationChangedError as ClientIncarnationChangedError
from pyhoglake.types import columns_to_arrow_schema

from .config import HedgerowConfig
from .filtering import RowFilter, build_filter
from .halts import (
    DataIntegrityError,
    DeletesPresentError,
    FeedExpiredError,
    HaltError,
    IncarnationChangedError,
    PersistentFailureError,
    SplitBrainError,
)
from .metrics import NullMetrics, build_metrics
from .projection import ProjectionPlan, validate_projection
from .window import Window, plan_window

log = logging.getLogger("hedgerow")

# pyhoglake 4xx classes that a retry can never fix: replaying the window
# hits the identical refusal, so run_forever halts immediately instead of
# burning the replay budget (and duplicating rows) on them.
PERMANENT_CLIENT_ERRORS = (ValidationError, NotFoundError, AlreadyExistsError)

# reader(path, columns, batch_size) -> iterator of record batches
BatchReader = Callable[[str, Sequence[str], int], Iterator[pa.RecordBatch]]

_READ_BATCH_CAP = 65_536


@dataclass(frozen=True)
class CycleResult:
    """What one ``run_once`` did. ``idle`` means the consumer was already
    at head and nothing was read or committed."""

    idle: bool
    from_snapshot: int
    to_snapshot: int
    head_snapshot: int
    files: int
    rows_read: int
    rows_appended: int
    appends: int
    committed_offset: int
    lag_snapshots: int
    duration_s: float

    @property
    def backlog_remains(self) -> bool:
        return self.lag_snapshots > 0


def make_s3_batch_reader(fs) -> BatchReader:
    """Default reader: stream a parquet file from object storage
    batch-by-batch (bounded memory, lesson #5 — never the whole file)."""

    def read(
        path: str, columns: Sequence[str], batch_size: int
    ) -> Iterator[pa.RecordBatch]:
        if not path.startswith("s3://"):
            raise ValueError(f"unsupported data file scheme: {path!r}")
        key = path[len("s3://") :]
        with fs.open_input_file(key) as fh:
            pf = pq.ParquetFile(fh)
            yield from pf.iter_batches(batch_size=batch_size, columns=list(columns))

    return read


class Hedgerow:
    """``Hedgerow(config).run_once()`` drives exactly one cycle (tests
    call it synchronously); ``run_forever()`` is the daemon loop.

    ``source_client`` / ``dest_client`` / ``batch_reader`` are seams for
    tests; production leaves them None and they are built from config.
    """

    def __init__(
        self,
        config: HedgerowConfig,
        *,
        source_client=None,
        dest_client=None,
        batch_reader: BatchReader | None = None,
        metrics=None,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        self.config = config
        self._source_client = source_client
        self._dest_client = dest_client
        self._batch_reader = batch_reader
        self._metrics = metrics
        self._sleep = sleep
        self._started = False

        self._source_catalog = None
        self._source_ns = None
        self._dest_ns = None
        self.source_uuid: str | None = None  # pinned at startup (lesson #3)
        self.dest_uuid: str | None = None
        self._projection: ProjectionPlan | None = None
        self._append_schema: pa.Schema | None = None
        self._filter: RowFilter | None = None

    # -- startup (fail-fast, lesson #6) ------------------------------------

    def start(self) -> None:
        """Resolve both tables, pin incarnations, validate the projection.
        Idempotent; raises (and refuses to run) on any config problem."""
        if self._started:
            return
        cfg = self.config

        if self._source_client is None:
            self._source_client = HoglakeClient(
                cfg.source.url, s3=cfg.source.s3.to_pyhoglake()
            )
        if self._dest_client is None:
            self._dest_client = HoglakeClient(
                cfg.destination.url, s3=cfg.destination.s3.to_pyhoglake()
            )

        self._source_catalog = self._source_client.catalog(cfg.source.catalog)
        self._source_ns = self._source_catalog.namespace(cfg.source.namespace)
        source_table = self._source_ns.table(cfg.source.table)
        self.source_uuid = source_table.table_uuid

        dest_catalog = self._dest_client.catalog(cfg.destination.catalog)
        self._dest_ns = dest_catalog.namespace(cfg.destination.namespace)
        dest_table = self._dest_ns.table(cfg.destination.table)
        self.dest_uuid = dest_table.table_uuid

        # Destination columns define the projected set; source must cover
        # them by name AND type or we refuse to start with a precise diff.
        filter_col = cfg.filter.column if cfg.filter else None
        self._projection = validate_projection(
            source_table.columns, dest_table.columns, filter_col
        )
        # The append schema is derived from the DESTINATION's catalog
        # columns (the projection truth), never from whichever file's
        # batch happened to come first — files in one window can disagree
        # on arrow-level nullability (BUG-6).
        dest_by_name = {c.name: c for c in dest_table.columns}
        self._append_schema = columns_to_arrow_schema(
            [dest_by_name[n] for n in self._projection.dest_columns]
        )
        self._filter = build_filter(cfg.filter, source_table.columns)

        if self._batch_reader is None:
            fs = cfg.source.s3.to_pyhoglake().filesystem()
            self._batch_reader = make_s3_batch_reader(fs)
        if self._metrics is None:
            self._metrics = build_metrics(cfg.metrics)

        self._started = True
        log.info(
            "started source=%s/%s.%s uuid=%s dest=%s/%s.%s uuid=%s "
            "consumer_id=%s columns=%s filter=%s",
            cfg.source.catalog,
            cfg.source.namespace,
            cfg.source.table,
            self.source_uuid,
            cfg.destination.catalog,
            cfg.destination.namespace,
            cfg.destination.table,
            self.dest_uuid,
            cfg.source.consumer_id,
            ",".join(self._projection.dest_columns),
            f"{cfg.filter.column}=={cfg.filter.equals!r}" if cfg.filter else "none",
        )

    # -- incarnation guard (lesson #3) -------------------------------------

    def _resolve_source_table(self):
        cfg = self.config.source
        try:
            table = self._source_ns.table(cfg.table)
        except NotFoundError:
            raise IncarnationChangedError(
                f"source table {cfg.catalog}/{cfg.namespace}.{cfg.table} "
                f"(pinned uuid {self.source_uuid}) no longer exists. "
                "HALT: refusing to continue against a dropped table."
            ) from None
        if table.table_uuid != self.source_uuid:
            raise IncarnationChangedError(
                f"source table {cfg.catalog}/{cfg.namespace}.{cfg.table} was "
                f"recreated: pinned uuid {self.source_uuid}, resolved uuid "
                f"{table.table_uuid}. HALT: the committed consumer offset "
                "belongs to the old incarnation; operator must re-point "
                "hedgerow (new consumer_id or reset offset) deliberately."
            )
        return table

    def _resolve_dest_table(self):
        cfg = self.config.destination
        try:
            table = self._dest_ns.table(cfg.table)
        except NotFoundError:
            raise IncarnationChangedError(
                f"destination table {cfg.catalog}/{cfg.namespace}.{cfg.table} "
                f"(pinned uuid {self.dest_uuid}) no longer exists. HALT."
            ) from None
        if table.table_uuid != self.dest_uuid:
            raise IncarnationChangedError(
                f"destination table {cfg.catalog}/{cfg.namespace}.{cfg.table} "
                f"was recreated: pinned uuid {self.dest_uuid}, resolved uuid "
                f"{table.table_uuid}. HALT: appended history would be split "
                "across incarnations."
            )
        return table

    # -- offsets -----------------------------------------------------------

    def _read_offset(self) -> int:
        # single-offset GET: the server answers for exactly
        # (consumer_id, table_uuid); None means no offset stored yet
        cfg = self.config.source
        off = self._source_catalog.offset(cfg.consumer_id, self.source_uuid)
        if off is None:
            return cfg.start_snapshot
        return off.committed_snapshot

    # -- one cycle ---------------------------------------------------------

    def run_once(self) -> CycleResult:
        t0 = time.monotonic()
        self.start()
        cfg = self.config

        source_table = self._resolve_source_table()
        dest_table = self._resolve_dest_table()

        committed = self._read_offset()
        head = self._source_catalog.refresh().head_snapshot_id
        win: Window | None = plan_window(
            committed, head, cfg.replication.max_snapshot_window
        )
        if win is None:
            result = CycleResult(
                idle=True,
                from_snapshot=committed,
                to_snapshot=committed,
                head_snapshot=head,
                files=0,
                rows_read=0,
                rows_appended=0,
                appends=0,
                committed_offset=committed,
                lag_snapshots=0,
                duration_s=time.monotonic() - t0,
            )
            self._finish_cycle(result)
            return result

        try:
            plan = source_table.changes(win.from_snapshot, win.to_snapshot)
        except ExpiredError as e:
            # Lesson #4: never skip a gap silently.
            raise FeedExpiredError(
                f"changefeed window ({win.from_snapshot}, {win.to_snapshot}] "
                f"is partially expired (HTTP 410). HALT: continuing would "
                f"silently skip data. Server reconcile instructions: "
                f"{e.detail or e.message}"
            ) from e

        if plan.table_uuid != self.source_uuid:
            raise IncarnationChangedError(
                f"changes() returned table_uuid {plan.table_uuid} but "
                f"hedgerow pinned {self.source_uuid}. HALT: source table "
                "was recreated mid-flight."
            )

        if plan.delete_files:
            # Lesson #8: v1 contract — append-only cannot represent deletes.
            paths = ", ".join(f.path for f in plan.delete_files[:5])
            raise DeletesPresentError(
                f"change plan ({plan.from_snapshot}, {plan.to_snapshot}] "
                f"contains {len(plan.delete_files)} delete file(s) "
                f"(e.g. {paths}). HALT: hedgerow v1 is append-only and "
                "cannot represent deletions in the destination. Operator "
                "must reconcile the destination manually (or stop deleting "
                "from the source)."
            )

        rows_read = rows_appended = appends = 0
        max_rows = cfg.replication.max_rows_per_append
        max_append_retries = cfg.replication.max_append_retries
        batch_size = min(max_rows, _READ_BATCH_CAP)
        buffer: list[pa.RecordBatch] = []
        buffered = 0

        append_schema = self._append_schema

        def flush() -> None:
            nonlocal buffer, buffered, rows_appended, appends
            if buffered == 0:
                return
            # Normalize every batch to the destination projection schema
            # (BUG-6): files in one window can carry differing arrow-level
            # nullability (or field metadata) for the same column, and
            # from_batches with an explicit schema raises on ANY mismatch.
            # safe=True: only metadata/nullability-level diffs cast
            # silently; a lossy value cast still fails loudly.
            table = pa.Table.from_batches(
                [b.cast(append_schema, safe=True) for b in buffer],
                schema=append_schema,
            )
            # Retryable commit conflicts (409, concurrent DDL touched the
            # destination) get bounded SINGLE-APPEND retries here, before
            # anything escalates to the window-replay path: a re-append of
            # this one buffer is duplicate-free, while a window replay
            # duplicates every row appended before the failure (bugs.md
            # #13). Incarnation/not-found behavior is unchanged — those
            # halt, never retry.
            attempts = 0
            while True:
                try:
                    dest_table.append(
                        table,
                        expected_table_uuid=self.dest_uuid,
                        author=f"hedgerow/{cfg.source.consumer_id}",
                        message=(
                            f"replicated from {cfg.source.catalog}/"
                            f"{cfg.source.namespace}.{cfg.source.table} "
                            f"window=({plan.from_snapshot},{plan.to_snapshot}]"
                        ),
                    )
                    break
                except ClientIncarnationChangedError as e:
                    # BUG-1: the destination was dropped/recreated
                    # mid-window. The guard is atomic at commit time — the
                    # server 409s a mismatched expected_table_uuid with
                    # ZERO writes (and pyhoglake's pre-flight may
                    # fast-fail even earlier). HALT — never split appends
                    # across incarnations, never commit the offset over
                    # them.
                    raise IncarnationChangedError(
                        f"destination table {cfg.destination.catalog}/"
                        f"{cfg.destination.namespace}.{cfg.destination.table} "
                        f"changed incarnation mid-window: {e}. HALT: the "
                        "offset was not committed; rows appended to the "
                        "dropped incarnation are gone with it and will be "
                        "replayed."
                    ) from e
                except NotFoundError as e:
                    raise IncarnationChangedError(
                        f"destination table {cfg.destination.catalog}/"
                        f"{cfg.destination.namespace}.{cfg.destination.table} "
                        f"disappeared mid-window (pinned uuid "
                        f"{self.dest_uuid}): {e}. HALT."
                    ) from e
                except CommitConflictError as e:
                    if not e.retryable or attempts >= max_append_retries:
                        # Budget exhausted (or a non-retryable conflict):
                        # let the existing window-replay machinery in
                        # run_forever take over.
                        raise
                    attempts += 1
                    backoff = min(0.1 * 2 ** (attempts - 1), 1.0)
                    log.warning(
                        "destination commit conflict (retryable); "
                        "re-appending the same buffer (duplicate-free), "
                        "attempt %d/%d after %.1fs: %s",
                        attempts,
                        max_append_retries,
                        backoff,
                        e,
                    )
                    self._sleep(backoff)
            rows_appended += buffered
            appends += 1
            buffer = []
            buffered = 0

        # Files sequentially, batches streamed: at most max_rows_per_append
        # rows are ever materialized (lesson #5).
        for f in plan.files:
            file_rows = 0
            for batch in self._batch_reader(
                f.path, self._projection.read_columns, batch_size
            ):
                file_rows += batch.num_rows
                rows_read += batch.num_rows
                if self._filter is not None:
                    batch = self._filter.apply(batch)
                batch = batch.select(list(self._projection.dest_columns))
                if batch.num_rows == 0:
                    continue
                if buffered + batch.num_rows > max_rows:
                    flush()
                buffer.append(batch)
                buffered += batch.num_rows
            if file_rows != f.record_count:
                # BUG-2: reconcile rows actually read against the change
                # plan's server-side record_count. A short (or long) read
                # means the offset would cover rows never appended. HALT
                # before any offset movement.
                raise DataIntegrityError(
                    f"data file {f.path} delivered {file_rows} row(s) but "
                    f"the change plan records record_count="
                    f"{f.record_count}. HALT: committing the offset would "
                    "cover rows that were never read/appended (short read "
                    "or object-store misbehavior)."
                )
        flush()

        # Lesson #2: the offset moves ONLY after every row of the window
        # is durably appended, and only to the fully applied
        # plan.to_snapshot. Rows-then-offset; crash in between -> the
        # window replays -> duplicates possible (at-least-once).
        try:
            self._source_catalog.commit_offset(
                cfg.source.consumer_id, self.source_uuid, plan.to_snapshot
            )
        except OffsetRegressionError as e:
            # BUG-4: a 409 here means a FOREIGN writer sharing our
            # consumer_id advanced the offset past this window —
            # split-brain evidence, never a transient. Adopting the
            # foreign offset would silently skip rows this destination
            # never received.
            raise SplitBrainError(
                f"offset commit for consumer {cfg.source.consumer_id!r} "
                f"(table {self.source_uuid}) to snapshot {plan.to_snapshot} "
                f"was rejected as a regression: {e}. Another writer shares "
                "this consumer_id. HALT: never adopt a foreign offset — "
                "fix the consumer_id collision, then decide the correct "
                "offset deliberately."
            ) from e

        # Pacing must not trust the head sampled BEFORE the cycle: a long
        # cycle (big window, slow object store) leaves that observation
        # stale, and a "caught up" verdict against it makes run_forever
        # sleep a full poll interval on top of fresh backlog (bugs.md
        # #22). Re-observe the head after the cycle completes — but only
        # when the stale head claims we are caught up; a clamped window
        # already knows backlog remains.
        head_after = win.head_snapshot
        if head_after <= plan.to_snapshot:
            head_after = self._source_catalog.refresh().head_snapshot_id

        result = CycleResult(
            idle=False,
            from_snapshot=plan.from_snapshot,
            to_snapshot=plan.to_snapshot,
            head_snapshot=head_after,
            files=len(plan.files),
            rows_read=rows_read,
            rows_appended=rows_appended,
            appends=appends,
            committed_offset=plan.to_snapshot,
            lag_snapshots=max(0, head_after - plan.to_snapshot),
            duration_s=time.monotonic() - t0,
        )
        self._finish_cycle(result)
        return result

    def _finish_cycle(self, result: CycleResult) -> None:
        # Lesson #7: one structured line per cycle; metrics passive.
        log.info(
            "cycle idle=%s window=(%d,%d] head=%d files=%d rows_read=%d "
            "rows_appended=%d appends=%d offset=%d lag=%d duration_ms=%.1f",
            result.idle,
            result.from_snapshot,
            result.to_snapshot,
            result.head_snapshot,
            result.files,
            result.rows_read,
            result.rows_appended,
            result.appends,
            result.committed_offset,
            result.lag_snapshots,
            result.duration_s * 1000.0,
        )
        try:
            (self._metrics or NullMetrics()).observe_cycle(result)
        except Exception:  # passive reporting (lesson #7): never control flow
            log.exception("metrics observation failed (ignored)")

    # -- the loop (lesson #1: its own clock, write IS the pacing) ----------

    def run_forever(self) -> None:
        """Poll -> write -> commit offset -> repeat. Backlog is drained
        immediately; only a caught-up (or failed) cycle sleeps. Halt
        conditions propagate — the process must die loudly.

        Failure taxonomy (BUG-3 + the permanent/transient split):

        - ``HaltError`` propagates immediately.
        - Permanent client errors (:data:`PERMANENT_CLIENT_ERRORS`) halt
          immediately as :class:`PersistentFailureError` — a retry
          replays the window and fails identically, so retrying only
          manufactures duplicates. They are counted separately from the
          replay budget and the halt reason names the error.
        - Anything else is transient: the next cycle re-reads the
          committed consumer offset from the source catalog (the
          authoritative restart point — ``run_once`` always starts
          there) and replays the window. Each replay can duplicate every
          row appended before the failure, so replays are capped at
          ``replication.max_window_replays`` consecutive failures; the
          cap exhausting halts as :class:`PersistentFailureError`. The
          counter resets on any successful cycle.
        """
        self.start()
        poll = self.config.replication.poll_interval_s
        max_replays = self.config.replication.max_window_replays
        replays = 0  # consecutive failed cycles = window replays burned
        while True:
            try:
                result = self.run_once()
            except HaltError:
                raise
            except PERMANENT_CLIENT_ERRORS as e:
                self._observe_error()
                raise PersistentFailureError(
                    f"permanent client error {type(e).__name__}: {e}. "
                    "HALT: a retry replays the window and fails "
                    "identically — retrying cannot succeed and would only "
                    "duplicate already-appended rows."
                ) from e
            except Exception as e:  # transient: bounded replay, then halt
                self._observe_error()
                replays += 1
                if replays > max_replays:
                    raise PersistentFailureError(
                        f"window replay budget exhausted: the initial "
                        f"attempt and {max_replays} replay(s) "
                        f"(replication.max_window_replays={max_replays}) "
                        f"all failed; last error: {type(e).__name__}: {e}. "
                        "HALT: treating as persistent — every further "
                        "retry replays the whole window and can duplicate "
                        "all of its rows."
                    ) from e
                log.warning(
                    "window replay %d/%d: duplicates possible — rows "
                    "appended before the failure will be appended again "
                    "(at-least-once); the committed offset is re-read "
                    "from the source catalog before the retry. error: %s",
                    replays,
                    max_replays,
                    e,
                )
                self._sleep(poll)
                continue
            replays = 0
            if not result.backlog_remains:
                self._sleep(poll)

    def _observe_error(self) -> None:
        try:
            (self._metrics or NullMetrics()).observe_error()
        except Exception:  # passive reporting (lesson #7)
            log.exception("metrics observation failed (ignored)")

    def close(self) -> None:
        for client in (self._source_client, self._dest_client):
            try:
                if client is not None and hasattr(client, "close"):
                    client.close()
            except Exception:  # noqa: BLE001, S110  # best-effort close during shutdown
                pass
