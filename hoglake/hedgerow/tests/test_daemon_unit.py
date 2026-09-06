"""Daemon cycle logic against scripted fakes: offset-commit ordering,
halt paths, at-least-once replay, batching/bounded memory, pacing."""

from dataclasses import dataclass
from dataclasses import field as dc_field

import pyarrow as pa
import pytest
from fakes import (
    CrashRequested,
    FakeCatalog,
    FakeClient,
    FakeDestTable,
    FakeNamespace,
    FakeSourceTable,
    col,
    data_file,
    delete_file,
    table_batch_reader,
)
from pyhoglake import NotFoundError

from hedgerow import (
    DeletesPresentError,
    DestinationConfig,
    FeedExpiredError,
    FilterConfig,
    Hedgerow,
    HedgerowConfig,
    IncarnationChangedError,
    MetricsConfig,
    ReplicationConfig,
    SchemaMismatchError,
    SourceConfig,
)

SRC_COLS = (
    col("id", "long", 1, 0, nullable=False),
    col("team_id", "long", 2, 1),
    col("name", "string", 3, 2),
)

SRC_SCHEMA = pa.schema(
    [
        pa.field("id", pa.int64(), nullable=False),
        pa.field("team_id", pa.int64()),
        pa.field("name", pa.string()),
    ]
)


def src_data(ids, teams=None, names=None) -> pa.Table:
    n = len(ids)
    return pa.table(
        {
            "id": pa.array(ids, pa.int64()),
            "team_id": pa.array(teams if teams is not None else [1] * n, pa.int64()),
            "name": pa.array(names if names is not None else [f"n{i}" for i in ids]),
        },
        schema=SRC_SCHEMA,
    )


def make_config(
    *,
    start_snapshot: int = 0,
    max_window: int = 1000,
    max_rows: int = 100,
    filter_cfg: FilterConfig | None = None,
    poll: float = 0.0,
    append_retries: int = 3,
) -> HedgerowConfig:
    return HedgerowConfig(
        source=SourceConfig(
            url="http://src",
            catalog="cat",
            namespace="ns",
            table="events",
            consumer_id="c1",
            start_snapshot=start_snapshot,
        ),
        destination=DestinationConfig(
            url="http://dst", catalog="dcat", namespace="dns", table="devents"
        ),
        replication=ReplicationConfig(
            poll_interval_s=poll,
            max_snapshot_window=max_window,
            max_rows_per_append=max_rows,
            max_append_retries=append_retries,
        ),
        metrics=MetricsConfig(port=0),
        filter=filter_cfg,
    )


@dataclass
class Env:
    daemon: Hedgerow
    source_catalog: FakeCatalog
    source_ns: FakeNamespace
    source_table: FakeSourceTable
    dest_catalog: FakeCatalog
    dest_ns: FakeNamespace
    dest_table: FakeDestTable
    journal: list
    tables_by_path: dict = dc_field(default_factory=dict)

    def offset(self) -> int | None:
        return self.source_catalog.offsets_store.get(("c1", "src-uuid-1"))

    def rebuild_daemon(self, config: HedgerowConfig) -> Hedgerow:
        """A 'process restart': fresh Hedgerow over the same fakes."""
        self.daemon = Hedgerow(
            config,
            source_client=FakeClient({"cat": self.source_catalog}),
            dest_client=FakeClient({"dcat": self.dest_catalog}),
            batch_reader=table_batch_reader(self.tables_by_path),
        )
        return self.daemon


def build_env(
    *,
    files: dict[int, pa.Table] | None = None,
    head: int | None = None,
    dest_cols=None,
    config: HedgerowConfig | None = None,
    offsets: dict | None = None,
) -> Env:
    journal: list = []
    tables_by_path: dict[str, pa.Table] = {}
    files_by_snapshot: dict[int, list] = {}
    for fid, (snap, tbl) in enumerate((files or {}).items(), start=1):
        path = f"s3://fake/{snap}.parquet"
        tables_by_path[path] = tbl
        files_by_snapshot[snap] = [data_file(path, tbl.num_rows, snap, fid)]

    source_table = FakeSourceTable("src-uuid-1", SRC_COLS, files_by_snapshot)
    source_ns = FakeNamespace("ns", {"events": source_table})
    source_catalog = FakeCatalog(
        "cat",
        {"ns": source_ns},
        head_snapshot_id=head
        if head is not None
        else max(files_by_snapshot, default=0),
        calls=journal,
    )
    if offsets:
        source_catalog.offsets_store.update(offsets)

    dest_table = FakeDestTable("dst-uuid-1", dest_cols or SRC_COLS, calls=journal)
    dest_ns = FakeNamespace("dns", {"devents": dest_table})
    dest_catalog = FakeCatalog("dcat", {"dns": dest_ns})

    config = config or make_config()
    daemon = Hedgerow(
        config,
        source_client=FakeClient({"cat": source_catalog}),
        dest_client=FakeClient({"dcat": dest_catalog}),
        batch_reader=table_batch_reader(tables_by_path),
    )
    return Env(
        daemon=daemon,
        source_catalog=source_catalog,
        source_ns=source_ns,
        source_table=source_table,
        dest_catalog=dest_catalog,
        dest_ns=dest_ns,
        dest_table=dest_table,
        journal=journal,
        tables_by_path=tables_by_path,
    )


# -- the happy path ---------------------------------------------------------


def test_replicates_one_window():
    env = build_env(files={1: src_data([1, 2, 3]), 2: src_data([4, 5])})
    result = env.daemon.run_once()
    assert not result.idle
    assert (result.from_snapshot, result.to_snapshot) == (0, 2)
    assert result.files == 2
    assert result.rows_read == 5
    assert result.rows_appended == 5
    assert result.appends == 1
    assert result.committed_offset == 2
    assert result.lag_snapshots == 0
    assert not result.backlog_remains
    assert env.dest_table.total_rows == 5
    assert env.offset() == 2


def test_append_strictly_before_offset_commit():
    """Lesson #2: rows-then-offset, asserted on the shared call journal."""
    env = build_env(
        files={1: src_data(list(range(10))), 2: src_data(list(range(10, 20)))},
        config=make_config(max_rows=4),  # forces several appends
    )
    env.daemon.run_once()
    kinds = [c[0] for c in env.journal]
    assert "append" in kinds and "commit_offset" in kinds
    last_append = max(i for i, k in enumerate(kinds) if k == "append")
    first_offset = min(i for i, k in enumerate(kinds) if k == "commit_offset")
    assert last_append < first_offset, f"offset committed before appends: {kinds}"
    # and the offset covers exactly the fully-applied window
    assert env.journal[first_offset] == ("commit_offset", "c1", "src-uuid-1", 2)


def test_bounded_appends_never_exceed_max_rows():
    """Lesson #5: at most max_rows_per_append rows per materialized batch."""
    env = build_env(
        files={1: src_data(list(range(12)))},
        config=make_config(max_rows=5),
    )
    result = env.daemon.run_once()
    assert result.appends == 3
    sizes = [t.num_rows for t in env.dest_table.appended]
    assert sizes == [5, 5, 2]
    assert all(s <= 5 for s in sizes)


def test_window_spans_multiple_appends_offset_only_at_end():
    env = build_env(
        files={1: src_data(list(range(6)))},
        config=make_config(max_rows=2),
    )
    env.daemon.run_once()
    appends = [c for c in env.journal if c[0] == "append"]
    offsets = [c for c in env.journal if c[0] == "commit_offset"]
    assert len(appends) == 3 and len(offsets) == 1
    assert env.journal.index(offsets[0]) > env.journal.index(appends[-1])


def test_idle_cycle_when_caught_up():
    env = build_env(
        files={1: src_data([1])},
        offsets={("c1", "src-uuid-1"): 1},
    )
    result = env.daemon.run_once()
    assert result.idle
    assert result.rows_appended == 0
    assert env.dest_table.total_rows == 0
    assert env.offset() == 1  # untouched
    assert not any(c[0] == "commit_offset" for c in env.journal)


def test_empty_window_still_advances_offset():
    # head moved (other tables' commits) but no files for OUR table
    env = build_env(files={}, head=7)
    result = env.daemon.run_once()
    assert not result.idle
    assert result.files == 0 and result.rows_appended == 0 and result.appends == 0
    assert result.committed_offset == 7
    assert env.offset() == 7


def test_window_clamped_and_backlog_drained_across_cycles():
    env = build_env(
        files={1: src_data([1]), 2: src_data([2]), 3: src_data([3])},
        config=make_config(max_window=1),
    )
    r1 = env.daemon.run_once()
    assert (r1.from_snapshot, r1.to_snapshot) == (0, 1)
    assert r1.backlog_remains and r1.lag_snapshots == 2
    r2 = env.daemon.run_once()
    assert (r2.from_snapshot, r2.to_snapshot) == (1, 2)
    r3 = env.daemon.run_once()
    assert (r3.from_snapshot, r3.to_snapshot) == (2, 3)
    assert not r3.backlog_remains
    assert env.dest_table.total_rows == 3
    assert env.offset() == 3


def test_resume_from_committed_offset():
    env = build_env(
        files={1: src_data([1, 2]), 2: src_data([3, 4])},
        offsets={("c1", "src-uuid-1"): 1},
    )
    result = env.daemon.run_once()
    assert (result.from_snapshot, result.to_snapshot) == (1, 2)
    assert env.dest_table.total_rows == 2  # only snapshot 2's file
    assert ("changes", 1, 2) in env.source_table.calls


def test_start_snapshot_used_when_no_offset():
    env = build_env(
        files={3: src_data([1]), 4: src_data([2])},
        head=4,
        config=make_config(start_snapshot=3),
    )
    result = env.daemon.run_once()
    assert (result.from_snapshot, result.to_snapshot) == (3, 4)
    assert env.dest_table.total_rows == 1


# -- projection + filter ----------------------------------------------------


def test_projection_drops_extra_source_columns():
    dest_cols = (col("id", "long", 1, 0, nullable=False), col("name", "string", 2, 1))
    env = build_env(files={1: src_data([1, 2])}, dest_cols=dest_cols)
    env.daemon.run_once()
    (appended,) = env.dest_table.appended
    assert appended.column_names == ["id", "name"]


def test_filter_applied_and_filter_column_dropped():
    dest_cols = (col("id", "long", 1, 0, nullable=False),)
    env = build_env(
        files={1: src_data([1, 2, 3, 4], teams=[7, 8, 7, None])},
        dest_cols=dest_cols,
        config=make_config(filter_cfg=FilterConfig(column="team_id", equals=7)),
    )
    result = env.daemon.run_once()
    assert result.rows_read == 4
    assert result.rows_appended == 2
    (appended,) = env.dest_table.appended
    assert appended.column_names == ["id"]
    assert appended.column("id").to_pylist() == [1, 3]


def test_all_rows_filtered_out_still_commits_offset():
    env = build_env(
        files={1: src_data([1, 2], teams=[5, 5])},
        config=make_config(filter_cfg=FilterConfig(column="team_id", equals=42)),
    )
    result = env.daemon.run_once()
    assert result.rows_appended == 0 and result.appends == 0
    assert env.offset() == 1
    assert not any(c[0] == "append" for c in env.journal)


# -- at-least-once replay (lesson #2's failure mode, on purpose) ------------


def test_crash_between_append_and_offset_replays_with_duplicates():
    config = make_config()
    env = build_env(files={1: src_data([1, 2, 3])}, config=config)
    env.source_catalog.fail_on_offset_commit = 1  # crash AFTER append, BEFORE offset

    with pytest.raises(CrashRequested):
        env.daemon.run_once()

    assert env.dest_table.total_rows == 3  # rows landed
    assert env.offset() is None  # offset did NOT move

    # "restart the process": fresh daemon over the same catalog state
    daemon2 = env.rebuild_daemon(config)
    result = daemon2.run_once()

    # the window replayed: duplicates land (AT-LEAST-ONCE), offset correct
    assert env.dest_table.total_rows == 6
    assert result.committed_offset == 1
    assert env.offset() == 1

    # ... and a further cycle is idle: no re-replay once committed
    assert daemon2.run_once().idle


def test_crash_mid_window_never_moves_offset():
    env = build_env(
        files={1: src_data([1, 2]), 2: src_data([3, 4])},
        config=make_config(max_rows=2),
    )
    env.dest_table.fail_on_append_call = 2  # first append lands, second dies
    with pytest.raises(CrashRequested):
        env.daemon.run_once()
    assert env.dest_table.total_rows == 2  # partial window in dest
    assert env.offset() is None  # offset NEVER covers unappended rows
    # replay after restart appends the whole window again
    env.rebuild_daemon(make_config(max_rows=2)).run_once()
    assert env.dest_table.total_rows == 6  # 2 (partial) + 4 (full replay)
    assert env.offset() == 2


# -- retryable commit conflicts (bugs.md #13) --------------------------------


def test_retryable_conflict_retries_single_append_no_window_replay():
    """A transient destination commit conflict is retried as a
    duplicate-free single append; the window-replay path (which
    duplicates prior appends) is never engaged."""
    env = build_env(files={1: src_data([1, 2, 3])})
    env.dest_table.conflict_first_n_appends = 2  # two conflicts, then success
    sleeps: list[float] = []
    env.daemon._sleep = sleeps.append

    result = env.daemon.run_once()

    assert result.rows_appended == 3 and result.appends == 1
    assert env.dest_table.total_rows == 3  # appended exactly once — no dupes
    assert env.offset() == 1  # offset correct after in-place retries
    # the window was planned exactly once: no replay
    assert [c for c in env.source_table.calls if c[0] == "changes"] == [
        ("changes", 0, 1)
    ]
    assert sleeps == [0.1, 0.2]  # one short backoff per retry


def test_conflict_retries_exhausted_escalate_to_window_replay():
    """When the per-append retry budget runs out, the conflict propagates
    and the EXISTING window-replay machinery takes over (unchanged)."""
    env = build_env(
        files={1: src_data([1, 2])},
        config=make_config(poll=1.0, append_retries=1),
    )
    # cycle 1 burns conflicts 1+2 (initial + 1 retry) and escalates;
    # the replayed cycle 2 hits conflict 3, retries, and succeeds.
    env.dest_table.conflict_first_n_appends = 3

    sleeps: list[float] = []

    def fake_sleep(s: float) -> None:
        sleeps.append(s)
        if env.offset() == 1 and s == 1.0:  # caught-up sleep after success
            raise StopLoop()

    env.daemon._sleep = fake_sleep
    with pytest.raises(StopLoop):
        env.daemon.run_forever()

    assert env.dest_table.total_rows == 2  # nothing landed twice
    assert env.offset() == 1
    # the window WAS replayed once (changes planned twice)
    assert len([c for c in env.source_table.calls if c[0] == "changes"]) == 2
    # backoff, replay sleep, backoff, caught-up sleep
    assert sleeps == [0.1, 1.0, 0.1, 1.0]


def test_conflict_retries_disabled_escalates_immediately():
    from pyhoglake import CommitConflictError

    env = build_env(files={1: src_data([1])}, config=make_config(append_retries=0))
    env.dest_table.conflict_first_n_appends = 1
    with pytest.raises(CommitConflictError):
        env.daemon.run_once()
    assert env.offset() is None  # offset never moves over a failed append


# -- halt paths -------------------------------------------------------------


def test_halt_on_source_recreate():
    env = build_env(files={1: src_data([1])})
    env.daemon.run_once()
    # drop + recreate: same name, new uuid
    env.source_ns.tables["events"] = FakeSourceTable("src-uuid-NEW", SRC_COLS, {})
    with pytest.raises(
        IncarnationChangedError, match="recreated.*src-uuid-1.*src-uuid-NEW"
    ):
        env.daemon.run_once()


def test_halt_on_source_dropped():
    env = build_env(files={1: src_data([1])})
    env.daemon.run_once()
    del env.source_ns.tables["events"]
    with pytest.raises(IncarnationChangedError, match="no longer exists"):
        env.daemon.run_once()


def test_halt_on_dest_recreate():
    env = build_env(files={1: src_data([1]), 2: src_data([2])})
    env.daemon.start()
    env.dest_ns.tables["devents"] = FakeDestTable("dst-uuid-NEW", SRC_COLS)
    with pytest.raises(IncarnationChangedError, match="destination.*recreated"):
        env.daemon.run_once()
    assert env.offset() is None  # nothing was consumed


def test_halt_on_plan_uuid_mismatch():
    env = build_env(files={1: src_data([1])})
    env.daemon.start()
    env.source_table.plan_uuid_override = "some-other-uuid"
    with pytest.raises(IncarnationChangedError, match="changes\\(\\) returned"):
        env.daemon.run_once()
    assert env.offset() is None


def test_halt_on_expired_feed_carries_reconcile_detail():
    env = build_env(files={5: src_data([1])}, head=5)
    env.source_table.expired_below = 3  # offset 0 is below the floor
    with pytest.raises(FeedExpiredError) as ei:
        env.daemon.run_once()
    msg = str(ei.value)
    assert "410" in msg
    assert "reconcile from a full scan" in msg  # server detail passed through
    assert env.offset() is None
    assert env.dest_table.total_rows == 0


def test_halt_on_deletes_present():
    env = build_env(files={1: src_data([1, 2])})
    env.source_table.delete_files_by_snapshot = {
        1: [delete_file("s3://fake/dv1.puffin", 1, 1)]
    }
    with pytest.raises(DeletesPresentError, match="append-only"):
        env.daemon.run_once()
    # deletes halt BEFORE any append or offset movement
    assert env.dest_table.total_rows == 0
    assert env.offset() is None


# -- fail-fast startup (lesson #6) ------------------------------------------


def test_start_refuses_on_schema_mismatch():
    dest_cols = (col("id", "string", 1, 0),)  # long vs string
    env = build_env(files={}, dest_cols=dest_cols)
    with pytest.raises(SchemaMismatchError, match="id: type mismatch"):
        env.daemon.start()


def test_start_refuses_on_missing_dest_table():
    env = build_env(files={})
    del env.dest_ns.tables["devents"]
    with pytest.raises(NotFoundError):
        env.daemon.start()


def test_start_refuses_on_missing_source_catalog():
    env = build_env(files={})
    daemon = Hedgerow(
        make_config(),
        source_client=FakeClient({}),  # no catalogs at all
        dest_client=FakeClient({"dcat": env.dest_catalog}),
        batch_reader=table_batch_reader({}),
    )
    with pytest.raises(NotFoundError):
        daemon.start()


def test_start_refuses_on_bad_filter_value():
    from hedgerow.config import ConfigError

    env = build_env(
        files={},
        config=make_config(filter_cfg=FilterConfig(column="team_id", equals="zap")),
    )
    with pytest.raises(ConfigError, match="not valid for column"):
        env.daemon.start()


def test_start_is_idempotent():
    env = build_env(files={1: src_data([1])})
    env.daemon.start()
    env.daemon.start()
    assert env.daemon.source_uuid == "src-uuid-1"
    assert env.daemon.dest_uuid == "dst-uuid-1"


# -- run_forever pacing (lesson #1) -----------------------------------------


class StopLoop(Exception):
    pass


def test_run_forever_drains_backlog_without_sleeping():
    sleeps: list[float] = []

    def fake_sleep(s: float) -> None:
        sleeps.append(s)
        raise StopLoop()  # first sleep ends the test

    env = build_env(
        files={1: src_data([1]), 2: src_data([2]), 3: src_data([3])},
        config=make_config(max_window=1, poll=9.0),
    )
    env.daemon._sleep = fake_sleep
    with pytest.raises(StopLoop):
        env.daemon.run_forever()
    # all three windows drained back-to-back; the ONLY sleep is after catch-up
    assert env.offset() == 3
    assert env.dest_table.total_rows == 3
    assert sleeps == [9.0]


def test_run_forever_halt_propagates():
    env = build_env(files={1: src_data([1])})
    env.source_table.delete_files_by_snapshot = {
        1: [delete_file("s3://fake/dv.puffin", 1, 1)]
    }
    env.daemon._sleep = lambda s: None
    with pytest.raises(DeletesPresentError):
        env.daemon.run_forever()


def test_backlog_pacing_reobserves_head_after_cycle():
    """bugs.md #22: the sleep decision must not trust the head sampled
    BEFORE a (possibly long) cycle — head advancing mid-cycle is fresh
    backlog, not caught-up."""
    env = build_env(files={1: src_data([1])})  # head = 1 at cycle start
    orig_append = env.dest_table.append

    def append_then_advance(data, **kw):
        r = orig_append(data, **kw)
        env.source_catalog.head_snapshot_id = 2  # head moved during the cycle
        return r

    env.dest_table.append = append_then_advance
    result = env.daemon.run_once()
    assert result.committed_offset == 1
    assert result.head_snapshot == 2  # the post-cycle observation
    assert result.lag_snapshots == 1
    assert result.backlog_remains  # run_forever drains instead of sleeping


def test_run_forever_no_spurious_sleep_when_head_advances_mid_cycle():
    env = build_env(files={1: src_data([1])}, config=make_config(poll=9.0))
    orig_append = env.dest_table.append
    advanced = {"done": False}

    def append_then_advance(data, **kw):
        r = orig_append(data, **kw)
        if not advanced["done"]:
            advanced["done"] = True
            tbl = src_data([2])
            env.tables_by_path["s3://fake/2.parquet"] = tbl
            env.source_table.files_by_snapshot[2] = [
                data_file("s3://fake/2.parquet", tbl.num_rows, 2, 2)
            ]
            env.source_catalog.head_snapshot_id = 2
        return r

    env.dest_table.append = append_then_advance

    sleeps: list[float] = []

    def fake_sleep(s: float) -> None:
        sleeps.append(s)
        raise StopLoop()  # first sleep ends the test

    env.daemon._sleep = fake_sleep
    with pytest.raises(StopLoop):
        env.daemon.run_forever()
    # both windows drained back-to-back; the only sleep is after real
    # catch-up — never a nap on top of the mid-cycle backlog
    assert env.offset() == 2
    assert env.dest_table.total_rows == 2
    assert sleeps == [9.0]


def test_run_forever_retries_transient_errors():
    env = build_env(files={1: src_data([1, 2])}, config=make_config(poll=1.0))
    real_reader = env.daemon._batch_reader
    calls = {"n": 0}

    def flaky_reader(path, columns, batch_size):
        calls["n"] += 1
        if calls["n"] == 1:
            raise OSError("transient S3 blip")
        return real_reader(path, columns, batch_size)

    env.daemon.start()
    env.daemon._batch_reader = flaky_reader

    sleeps: list[float] = []

    def fake_sleep(s: float) -> None:
        sleeps.append(s)
        if len(sleeps) >= 2:  # error sleep + caught-up sleep
            raise StopLoop()

    env.daemon._sleep = fake_sleep
    with pytest.raises(StopLoop):
        env.daemon.run_forever()
    assert env.dest_table.total_rows == 2  # recovered and replicated
    assert env.offset() == 1


# -- metrics (lesson #7: passive) -------------------------------------------


def test_metrics_observed_per_cycle():
    prom = pytest.importorskip("prometheus_client")
    from hedgerow.metrics import PrometheusMetrics

    registry = prom.CollectorRegistry()
    metrics = PrometheusMetrics(0, registry=registry, start_server=False)
    env = build_env(
        files={1: src_data([1, 2]), 2: src_data([3])},
        config=make_config(max_window=1),
    )
    env.daemon._metrics = metrics
    env.daemon.run_once()

    assert registry.get_sample_value("hedgerow_cycles_total") == 1
    assert registry.get_sample_value("hedgerow_rows_replicated_total") == 2
    assert registry.get_sample_value("hedgerow_last_committed_snapshot") == 1
    assert (
        registry.get_sample_value("hedgerow_lag_snapshots") == 1
    )  # head 2, committed 1

    env.daemon.run_once()
    assert registry.get_sample_value("hedgerow_cycles_total") == 2
    assert registry.get_sample_value("hedgerow_rows_replicated_total") == 3
    assert registry.get_sample_value("hedgerow_lag_snapshots") == 0


def test_metrics_failure_never_stops_replication():
    class ExplodingMetrics:
        def observe_cycle(self, result):
            raise RuntimeError("metrics backend down")

        def observe_error(self):
            raise RuntimeError("metrics backend down")

    env = build_env(files={1: src_data([1])})
    env.daemon._metrics = ExplodingMetrics()
    # passive reporting must never control flow: replication proceeds
    result = env.daemon.run_once()
    assert result.committed_offset == 1
    assert env.offset() == 1
    assert env.dest_table.total_rows == 1


def test_metrics_port_zero_is_null_metrics():
    from hedgerow.metrics import NullMetrics, build_metrics

    assert isinstance(build_metrics(MetricsConfig(port=0)), NullMetrics)
