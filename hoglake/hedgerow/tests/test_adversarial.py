"""Adversarial QE pins (2026-09-05 review) — now FIXED and pinned green.

Each BUG below was found by the adversarial review, pinned as xfail, then
fixed; the tests remain (numbered for greppability) as regression pins
asserting the DESIRED behavior that now holds. Claims that survived
attack stay pinned green at the bottom.

Findings index (severity order) and their fixes:

- BUG-1  destination name-rebind race: recreate mid-window used to split
         appends across incarnations and still commit the offset. FIXED
         — and now ATOMIC: every append ships expected_table_uuid on the
         commit body and the SERVER 409s a mismatch with zero writes
         ("the table was recreated"); pyhoglake maps that to
         IncarnationChangedError and keeps one pre-flight re-resolve as
         an upload-saving fast-fail. hedgerow passes its pinned dest
         uuid and converts the client error into an
         IncarnationChangedError HALT. The old resolve->POST residual
         race is closed.
- BUG-2  plan record_count is now reconciled per file against rows
         actually read; a short read HALTS (DataIntegrityError) before
         any offset movement.
- BUG-3  duplicate amplification across transient retries is now bounded:
         at most max_window_replays (default 3) consecutive window
         replays, each loudly logged ("window replay N: duplicates
         possible"), then a PersistentFailureError HALT.
- BUG-4  OffsetRegressionError on commit_offset (foreign writer on the
         same consumer_id) is now a SplitBrainError HALT; the foreign
         offset is never adopted.
- BUG-5  ``filter.equals: null`` is refused at config validation (both
         YAML parse and direct construction): a null filter matches
         nothing.
- BUG-6  flush() casts every batch to the destination projection schema,
         so files with differing arrow-level nullability in one window
         replicate instead of looping forever.
"""

import io
import logging

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from fakes import FakeDestTable, col, data_file, delete_file
from pyhoglake import IncarnationChangedError as ClientIncarnationChangedError
from pyhoglake import OffsetRegressionError, ValidationError
from test_daemon_unit import SRC_COLS, build_env, make_config, src_data

from hedgerow import (
    DataIntegrityError,
    FilterConfig,
    IncarnationChangedError,
    PersistentFailureError,
    SplitBrainError,
)
from hedgerow.config import ConfigError
from hedgerow.daemon import make_s3_batch_reader


class StopLoop(Exception):
    pass


# ---------------------------------------------------------------------------
# BUG-1 regression: destination recreate mid-window is detected on the
# WRITE path, now ATOMICALLY at commit time. pyhoglake's Table.append is
# name-addressed; every commit ships expected_table_uuid and the server
# 409s a mismatch with zero writes ("the table was recreated"). The
# client keeps one cheap pre-flight re-resolve purely to save the parquet
# upload. hedgerow passes its pinned dest uuid on every append and
# converts the client error into an IncarnationChangedError halt — the
# offset never moves over a split window, and (unlike the old
# double-resolve narrowing) no recreate window exists in which the new
# incarnation can accept rows.
# ---------------------------------------------------------------------------


class NameAddressedDestTable(FakeDestTable):
    """FakeDestTable that mimics pyhoglake Table.append's real semantics:
    the commit is addressed by NAME, so it lands on (and the SERVER-side
    atomic guard is evaluated against) whatever table object currently
    holds the name — not the incarnation this object was resolved as.
    ``pre_commit_hook`` (if set) runs between the client's pre-flight
    resolve and the commit reaching the server, to inject a recreation
    into exactly that gap."""

    pre_commit_hook = None

    def bind(self, ns, name):
        self._ns = ns
        self._table_name = name
        return self

    def append(self, data: pa.Table, **kwargs):
        # client pre-flight (optimization): fast-fail when the name
        # already resolves to a different incarnation
        expected = kwargs.get("expected_table_uuid")
        current = self._ns.tables.get(self._table_name)
        if (
            expected is not None
            and current is not None
            and str(expected) != current.table_uuid
        ):
            raise ClientIncarnationChangedError(
                f"table was recreated: expected table_uuid {expected}, "
                f"name now resolves to {current.table_uuid} (pre-flight)"
            )
        if self.pre_commit_hook is not None:
            self.pre_commit_hook()
        # the commit POST: the server resolves the NAME and enforces
        # expected_table_uuid atomically (FakeDestTable's 409 mimicry)
        current = self._ns.tables.get(self._table_name)
        if current is None or current is self:
            return super().append(data, **kwargs)
        return current.append(data, **kwargs)  # the NEW incarnation decides


def test_dest_recreate_mid_window_halts_and_never_commits_offset():
    env = build_env(
        files={1: src_data([1, 2, 3, 4, 5, 6])},
        config=make_config(max_rows=2),
    )
    old_dest = NameAddressedDestTable("dst-uuid-1", SRC_COLS, calls=env.journal).bind(
        env.dest_ns, "devents"
    )
    env.dest_ns.tables["devents"] = old_dest
    env.daemon.start()

    new_dest = FakeDestTable("dst-uuid-NEW", SRC_COLS)
    real_reader = env.daemon._batch_reader
    state = {"yields": 0}

    def hostile_reader(path, columns, batch_size):
        for b in real_reader(path, columns, batch_size):
            state["yields"] += 1
            if state["yields"] == 3:
                # drop + recreate between append #1 and append #2
                env.dest_ns.tables["devents"] = new_dest
            yield b

    env.daemon._batch_reader = hostile_reader

    # The recreate is detected on the write path (expected_table_uuid
    # guard); the cycle halts and the offset does not move.
    with pytest.raises(IncarnationChangedError) as ei:
        env.daemon.run_once()
    assert ei.value.exit_code != 0  # a real halt, not a soft failure
    assert env.offset() is None, (
        f"offset advanced past a split window: old incarnation has "
        f"{old_dest.total_rows} rows, new has {new_dest.total_rows}"
    )
    # nothing ever landed in the new incarnation; the guard refused first
    assert new_dest.total_rows == 0


def test_recreate_after_preflight_before_commit_halts_with_zero_rows():
    """The EXACT race the old client-side double-resolve only narrowed:
    the drop+recreate lands AFTER the client's pre-flight re-resolve and
    BEFORE the commit reaches the server. The server-side atomic
    expected_table_uuid guard 409s the commit with zero writes — the new
    incarnation never accepts a single row, hedgerow halts (exit 3), and
    the offset is never committed."""
    env = build_env(files={1: src_data([1, 2, 3])})
    old_dest = NameAddressedDestTable("dst-uuid-1", SRC_COLS, calls=env.journal).bind(
        env.dest_ns, "devents"
    )
    env.dest_ns.tables["devents"] = old_dest
    env.daemon.start()

    new_dest = FakeDestTable("dst-uuid-NEW", SRC_COLS)

    def recreate_in_the_gap():
        # pre-flight already passed against the OLD incarnation; the
        # commit will hit the NEW one
        env.dest_ns.tables["devents"] = new_dest

    old_dest.pre_commit_hook = recreate_in_the_gap

    with pytest.raises(IncarnationChangedError) as ei:
        env.daemon.run_once()
    assert ei.value.exit_code == 3  # hedgerow's incarnation halt
    assert env.offset() is None  # offset never committed
    assert new_dest.total_rows == 0  # commit-time 409: ZERO writes
    assert old_dest.total_rows == 0  # nothing split across incarnations


# ---------------------------------------------------------------------------
# BUG-2 regression: the change plan carries record_count per file
# (server-side truth) and the cycle reconciles it with rows actually read.
# A short read (stale/truncated object-store response, reader bug, wrong
# file content) HALTS with DataIntegrityError before any offset movement.
# ---------------------------------------------------------------------------


def test_short_read_must_not_advance_offset():
    env = build_env(files={1: src_data([1, 2, 3])})
    # The catalog metadata claims 5 rows in this file; the reader (object
    # store) only delivers 3.
    path = next(iter(env.tables_by_path))
    env.source_table.files_by_snapshot[1] = [data_file(path, 5, 1, 1)]

    with pytest.raises(DataIntegrityError, match="record_count"):
        env.daemon.run_once()
    assert env.offset() is None, (
        "offset advanced although the plan promised 5 rows and only 3 "
        "were read and appended"
    )
    assert env.dest_table.total_rows == 0  # halted before the final flush


# ---------------------------------------------------------------------------
# BUG-3 regression: duplicate amplification across transient retries is
# BOUNDED. When an append lands server-side but the response is lost, each
# retry replays the window (at-least-once); the replay budget is
# max_window_replays (default 3) consecutive failures, each loudly logged,
# after which the daemon halts as persistent instead of duplicating
# forever.
# ---------------------------------------------------------------------------


class AppliedButClientErroredDest(FakeDestTable):
    """Append commits server-side, but the first N responses are lost."""

    lost_responses = 3

    def append(self, data: pa.Table, **kwargs):
        result = super().append(data, **kwargs)  # the commit LANDED
        if self._append_calls <= self.lost_responses:
            raise OSError("response lost after the server applied the commit")
        return result


def test_duplicate_amplification_bounded_to_the_replay_cap(caplog):
    env = build_env(files={1: src_data([1, 2, 3])}, config=make_config(poll=0.0))
    max_replays = env.daemon.config.replication.max_window_replays
    dest = AppliedButClientErroredDest("dst-uuid-1", SRC_COLS, calls=env.journal)
    dest.lost_responses = max_replays  # last allowed replay succeeds
    env.dest_ns.tables["devents"] = dest
    env.daemon.start()

    def stop_after_commit(_s: float) -> None:
        if env.offset() is not None:
            raise StopLoop()

    env.daemon._sleep = stop_after_commit
    with (
        caplog.at_level(logging.WARNING, logger="hedgerow"),
        pytest.raises(StopLoop),
    ):
        env.daemon.run_forever()

    assert env.offset() == 1
    # worst case is one initial attempt + max_window_replays replays
    assert dest.total_rows <= 3 * (1 + max_replays), (
        f"amplification past the cap: {dest.total_rows} rows appended for "
        f"a 3-row window with max_window_replays={max_replays}"
    )
    # every replay announced itself loudly
    replay_lines = [
        r for r in caplog.records if "duplicates possible" in r.getMessage()
    ]
    assert len(replay_lines) == max_replays


def test_replay_budget_exhaustion_halts_as_persistent():
    env = build_env(files={1: src_data([1, 2, 3])}, config=make_config(poll=0.0))
    max_replays = env.daemon.config.replication.max_window_replays
    dest = AppliedButClientErroredDest("dst-uuid-1", SRC_COLS, calls=env.journal)
    dest.lost_responses = max_replays + 5  # never recovers in budget
    env.dest_ns.tables["devents"] = dest
    env.daemon.start()
    env.daemon._sleep = lambda s: None

    with pytest.raises(PersistentFailureError, match="max_window_replays"):
        env.daemon.run_forever()
    assert env.offset() is None  # halted without ever covering the window
    # amplification stayed within initial attempt + the replay budget
    assert dest.total_rows <= 3 * (1 + max_replays)


def test_permanent_client_error_halts_immediately_without_replays():
    """Permanent 4xx client errors (ValidationError & co) never enter the
    replay loop: a retry fails identically, so retrying only manufactures
    duplicates. The halt reason names the error."""

    class RejectingDest(FakeDestTable):
        def append(self, data: pa.Table, **kwargs):
            raise ValidationError("bad stats shape", status_code=422)

    env = build_env(files={1: src_data([1, 2, 3])}, config=make_config(poll=0.0))
    env.dest_ns.tables["devents"] = RejectingDest(
        "dst-uuid-1", SRC_COLS, calls=env.journal
    )
    env.daemon.start()
    sleeps = {"n": 0}

    def counting_sleep(_s: float) -> None:
        sleeps["n"] += 1

    env.daemon._sleep = counting_sleep
    with pytest.raises(PersistentFailureError, match="ValidationError"):
        env.daemon.run_forever()
    assert sleeps["n"] == 0  # no retry, no replay
    assert env.offset() is None


# ---------------------------------------------------------------------------
# BUG-4 regression: OffsetRegressionError on commit_offset is split-brain
# evidence (a foreign writer sharing our consumer_id) and HALTS as
# SplitBrainError. The foreign offset is never adopted.
# ---------------------------------------------------------------------------


def test_offset_regression_halts_instead_of_adopting_foreign_offset():
    env = build_env(
        files={1: src_data([1, 2]), 2: src_data([3, 4])},
        config=make_config(max_window=1, poll=0.0),
    )
    env.daemon.start()

    def racing_commit(consumer_id, table_uuid, snapshot_id):
        # a foreign consumer with the same consumer_id already moved to 2
        env.source_catalog.offsets_store[(consumer_id, table_uuid)] = 2
        raise OffsetRegressionError(
            f"snapshot {snapshot_id} is below the stored offset 2",
            status_code=409,
        )

    env.source_catalog.commit_offset = racing_commit
    env.daemon._sleep = lambda s: None

    with pytest.raises(SplitBrainError, match="consumer_id"):
        env.daemon.run_forever()
    # the halt fired on the FIRST cycle: the foreign offset was never
    # read back and replicated from (no second changes() call)
    changes_calls = [c for c in env.source_table.calls if c[0] == "changes"]
    assert changes_calls == [("changes", 0, 1)]


# ---------------------------------------------------------------------------
# BUG-5 regression: filter.equals=null is refused at startup (and at YAML
# parse — see test_config.py): a null filter matches nothing, so it would
# silently drop 100% of rows while the offset advances.
# ---------------------------------------------------------------------------


def test_null_filter_value_refused_at_startup():
    env = build_env(
        files={1: src_data([1, 2], teams=[None, 5])},
        config=make_config(filter_cfg=FilterConfig(column="team_id", equals=None)),
    )
    with pytest.raises(ConfigError, match="non-null"):
        env.daemon.start()


# ---------------------------------------------------------------------------
# BUG-6 regression: flush() normalizes every batch to the destination
# projection schema, so files with differing arrow-level nullability for
# the same column in one window replicate instead of making
# pa.Table.from_batches raise forever.
# ---------------------------------------------------------------------------


def test_mixed_file_nullability_in_one_window_still_replicates():
    all_nullable = pa.schema(
        [
            pa.field("id", pa.int64()),  # nullable variant of the same column
            pa.field("team_id", pa.int64()),
            pa.field("name", pa.string()),
        ]
    )
    env = build_env(
        files={
            1: src_data([1, 2]),  # id non-nullable (SRC_SCHEMA)
            2: src_data([3, 4]).cast(all_nullable),
        }
    )
    result = env.daemon.run_once()
    assert result.rows_appended == 4
    assert env.offset() == 2


# ===========================================================================
# Claims that SURVIVED attack — pinned green so they stay that way.
# ===========================================================================


def test_pre_evolution_file_fails_loudly_never_silently_skips():
    """README known-limitation claim, attacked: pyarrow's iter_batches does
    NOT fail on a requested column missing from the file — it silently
    omits it (verified pyarrow 25). The daemon still fails loudly because
    batch.select()/filter.apply() raise KeyError downstream, so the offset
    never advances past unread data. (Since the BUG-3 fix the retried
    KeyError is bounded: max_window_replays, then a persistent halt.)"""
    evolved_src = SRC_COLS + (col("extra", "long", 4, 3),)
    dest_cols = evolved_src

    env = build_env(
        files={1: src_data([1, 2])},  # pre-evolution file: no "extra"
        dest_cols=dest_cols,
    )
    env.source_table.columns = evolved_src

    def lenient_reader(path, columns, batch_size):
        # mimic real pyarrow semantics: silently omit missing columns
        t = env.tables_by_path[path]
        present = [c for c in columns if c in t.column_names]
        yield from t.select(present).to_batches(max_chunksize=batch_size)

    env.daemon._batch_reader = lenient_reader

    with pytest.raises(KeyError):
        env.daemon.run_once()
    assert env.offset() is None
    assert env.dest_table.total_rows == 0


def test_dv_registered_after_plan_halts_next_cycle_not_never():
    """Hunt: can a DV registered mid-cycle (after the plan was fetched)
    slip through? No: committed snapshots are immutable, so the DV lands
    in a later snapshot and the NEXT window halts before consuming it."""
    env = build_env(files={1: src_data([1, 2])})
    assert env.daemon.run_once().committed_offset == 1

    # DV registered while (0,1] was being replicated -> snapshot 2
    env.source_table.delete_files_by_snapshot = {
        2: [delete_file("s3://fake/dv.puffin", 1, 2)]
    }
    env.source_catalog.head_snapshot_id = 2

    from hedgerow import DeletesPresentError

    with pytest.raises(DeletesPresentError):
        env.daemon.run_once()
    assert env.offset() == 1  # never advanced past the delete


def test_memory_bound_holds_for_single_huge_row_group():
    """README lesson #5, attacked via iter_batches semantics: one parquet
    row group far larger than max_rows_per_append. Verified: pyarrow 25
    streams within a row group — batches never exceed batch_size and the
    arrow allocation stays near the batch size, not the row-group size
    (measured ~2MB peak for a 16MB single-row-group file)."""
    n = 1_000_000
    table = pa.table({"id": pa.array(range(n), pa.int64()), "v": pa.array([1.0] * n)})
    sink = io.BytesIO()
    pq.write_table(table, sink, row_group_size=n)  # ONE row group, ~16MB
    raw = sink.getvalue()
    del table

    class _LocalFS:
        def open_input_file(self, key):
            return pa.BufferReader(raw)

    reader = make_s3_batch_reader(_LocalFS())
    base = pa.total_allocated_bytes()
    peak = 0
    max_batch = 0
    rows = 0
    for batch in reader("s3://fake/huge.parquet", ["id", "v"], 4096):
        max_batch = max(max_batch, batch.num_rows)
        rows += batch.num_rows
        peak = max(peak, pa.total_allocated_bytes() - base)

    assert rows == n
    assert max_batch <= 4096  # iter_batches batch_size IS a hard cap
    # generous bound: far below the 16MB row group
    assert peak < 8 * 1024 * 1024, f"peak arrow allocation {peak} bytes"


def test_source_recreate_between_resolve_and_changes_is_caught():
    """Hunt: source recreated between _resolve_source_table and changes().
    Survives: the plan's table_uuid is checked against the pinned uuid, so
    the recreate is caught before any read or offset movement (matching
    the destination-side write-path guard from the BUG-1 fix)."""
    env = build_env(files={1: src_data([1])})
    env.daemon.start()
    env.source_table.plan_uuid_override = "recreated-mid-flight"
    with pytest.raises(IncarnationChangedError):
        env.daemon.run_once()
    assert env.offset() is None
    assert env.dest_table.total_rows == 0
