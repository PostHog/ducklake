# hedgerow

The hoglake-native replication daemon — successor to viaduck, encoding
its hard-won lessons. One process replicates ONE source table to ONE
destination table, append-only, at-least-once. Python, built on
[pyhoglake](../pyhoglake/README.md); the changefeed and consumer offsets
are hoglake catalog primitives, so hedgerow carries no cursor tables, no
scheduler, and no embedded query engine.

```
source catalog ──changes()──▶ hedgerow ──append()──▶ destination catalog
       ▲                          │
       └────── commit_offset ─────┘   (offset lives in the SOURCE catalog)
```

The loop: read the committed consumer offset for
`(consumer_id, source table_uuid)` → plan
`changes(from=offset, to=min(head, offset+max_snapshot_window))` → stream
each data file's parquet from object storage (project to the
destination's columns, optionally filter) → append to the destination in
batches of at most `max_rows_per_append` rows → after ALL of the
window's rows are durably appended, commit the offset to
`plan.to_snapshot` → repeat immediately if backlog remains, else sleep
`poll_interval_s`.

## The viaduck lessons → behaviors

These are requirements, not suggestions. Each is named in code comments
where it is enforced.

| # | Lesson | hedgerow behavior |
|---|---|---|
| 1 | No central scheduler / no flush-cadence coupling | One writer loop, its own clock; the write IS the pacing (`Hedgerow.run_forever`: poll → write → commit-offset → repeat). Nothing external tells the loop when to flush. |
| 2 | Offsets strictly after durability | Rows-then-offset, always. The offset only ever moves to a fully applied `plan.to_snapshot` (commit-through-complete-windows). Crash between append and offset-commit → the window replays → **duplicates possible: AT-LEAST-ONCE** (see below). |
| 3 | Incarnation guard | Source and destination `table_uuid`s are pinned at startup; every cycle re-resolves by name and HALTS loudly (nonzero exit, clear message) on any mismatch or disappearance. The guard extends to the **write path**: every append ships the pinned uuid as pyhoglake's `expected_table_uuid` on the commit body, and the destination server enforces it **atomically at commit time** (409, zero writes, on mismatch — see pyhoglake's `append` docs), so a destination drop+recreate mid-window halts the cycle instead of splitting appends across incarnations. Never silently continue against a recreated table. |
| 4 | 410 is a stop sign | `ExpiredError` from `changes()` → HALT loudly, carrying the server's reconcile instructions. Never skip a gap silently (the retention-clamp lesson). |
| 5 | Bounded memory | Never more than `max_rows_per_append` rows materialized. Files are processed sequentially; each parquet is streamed batch-by-batch. No queues, no buffers beyond the current batch. |
| 6 | Fail-fast config validation | Startup resolves both tables and validates the projection (destination columns must exist in the source by NAME and TYPE); refusal messages carry a precise per-column diff. Bad filter values refuse to start too. |
| 7 | Observability is passive | One structured log line per cycle (window, files, rows, appends, offset, lag, duration) + optional `prometheus_client` metrics when `metrics.port > 0`. Metrics never feed control flow, and a metrics failure never stops replication. |
| 8 | Deletes cannot be papered over | `delete_files` in the change plan → HALT loudly: append-only mode cannot represent them (v1 contract; the message says exactly that). |

## At-least-once semantics — read this

hedgerow is **at-least-once, never exactly-once**. The offset commit
happens strictly after every row of the window is durably appended to
the destination. If the process dies between the last append and the
offset commit, the next run replays the entire window and the
destination receives **duplicate rows**. This is deliberate: the
alternative failure mode (offset ahead of data) silently loses rows.
Downstream consumers that need exactly-once must deduplicate (e.g. on
the source's row identity or an event key). The offset never covers rows
that have not been appended.

Duplicate amplification is **bounded**: a transient failure mid-window
makes the next cycle re-read the committed offset from the source
catalog (the authoritative restart point) and replay the window — and
each replay can duplicate every row appended before the failure, so
replays are capped at `replication.max_window_replays` (default 3)
consecutive failures. Every replay logs a loud
`window replay N: duplicates possible` line; exhausting the budget HALTS
as a persistent failure (exit 9) instead of duplicating forever.
Permanent client errors (validation, not-found, already-exists) skip the
replay budget entirely and halt immediately — a retry would fail
identically while still re-appending rows.

Retryable **commit conflicts** on a destination append (409, concurrent
DDL) never reach the replay path directly: the single append is retried
in place, duplicate-free, up to `replication.max_append_retries` times
with a short backoff. Only an exhausted (or non-retryable) conflict
escalates to the window replay above.

## Config reference

```yaml
source:
  url: http://localhost:8080        # hoglake control plane
  catalog: my-catalog
  namespace: analytics
  table: events
  consumer_id: hedgerow-events-1    # offset identity in the SOURCE catalog
  start_snapshot: 0                 # used only when no offset exists yet
  s3:                               # object store holding the source parquet
    endpoint: http://localhost:19000
    access_key: hoglake
    secret_key: hoglake123
    path_style: true

destination:                        # may be the same server
  url: http://localhost:8080
  catalog: my-catalog-replica
  namespace: analytics
  table: events
  s3: { endpoint: ..., access_key: ..., secret_key: ..., path_style: true }

filter:                             # optional client-side row filter
  column: team_id                   # may be a column the destination drops
  equals: 42                        # NULLs never match; null is REFUSED
                                    # (a null filter matches nothing);
                                    # NaN/Inf are REFUSED too (IEEE
                                    # NaN != NaN would silently drop
                                    # every row while the offset advances)

replication:
  poll_interval_s: 5                # sleep when caught up (or after an error);
                                    # minimum 0.1 (0 would busy-spin the
                                    # loop); NaN/Inf refused at startup
  max_snapshot_window: 1000         # snapshots per cycle, max
  max_rows_per_append: 100000       # rows per destination append, max
  max_window_replays: 3             # transient-failure retries per window;
                                    # each replay may duplicate the window's
                                    # rows; exhausting it HALTS (exit 9)
  max_append_retries: 3             # retryable commit conflicts per append:
                                    # duplicate-free single-append retries
                                    # (short backoff) BEFORE escalating to
                                    # the window-replay path above

metrics:
  port: 0                           # 0 = disabled; >0 serves /metrics
```

Run it:

```sh
hedgerow --config config.yaml          # daemon
hedgerow --config config.yaml --once   # exactly one cycle (ops/debug)
```

Env-var overrides: `HEDGEROW__<PATH>__<TO>__<KEY>=value` (double
underscore separators, YAML-parsed values), e.g.
`HEDGEROW__SOURCE__S3__SECRET_KEY=...`,
`HEDGEROW__REPLICATION__POLL_INTERVAL_S=1`.

Column projection: the destination table's columns define the projected
set. The source must have all of them (same name, same type); extra
source columns are dropped; the filter column may be a dropped column.
Known limitation: files written before a source `add_column` do not
contain that column. The parquet read itself does NOT fail — pyarrow
silently omits a requested column that is absent from the file — but the
cycle still fails loudly downstream (the projection/filter step raises
on the missing column), so the offset never advances past unread data;
after `max_window_replays` retries it halts as persistent. Evolve the
destination first, or start the destination at a post-alter offset.

Metrics (when enabled): `hedgerow_rows_replicated_total`,
`hedgerow_cycles_total`, `hedgerow_errors_total`,
`hedgerow_last_committed_snapshot`, `hedgerow_lag_snapshots`
(source head − committed offset). Caveat: `hedgerow_lag_snapshots` is
**catalog-scoped** — snapshot ids are dense per *catalog*, and the head
advances on every commit to ANY table in the source catalog, so on a
busy shared catalog the lag counts catalog snapshots, not pending data
for this table. A nonzero lag can be entirely other tables' commits
(the next cycle drains it as an empty window); use it as a
staleness/liveness signal, not a volume estimate.

## Runbook: the seven halt conditions

hedgerow HALTS (exits nonzero, no retry) when continuing would be wrong.
A supervisor must NOT blindly restart these — the same condition will
halt again (or worse, resume into a wrong state). Exit codes distinguish
them: 3 incarnation changed, 4 changefeed expired, 5 deletes present,
6 schema mismatch (startup refusal), 7 split-brain offset, 8 data
integrity, 9 persistent failure.

### 1. Incarnation changed (exit 3)

*Message:* `source/destination table ... was recreated: pinned uuid X,
resolved uuid Y` (or `... no longer exists`).

The table hedgerow was replicating was dropped (and possibly recreated
under the same name). The committed consumer offset belongs to the OLD
incarnation; snapshot ranges and row ids do not carry over.

The semantics of this exit tightened with the server-side commit guard:
a destination recreation is now caught **atomically at commit time**
(the server 409s any append carrying a stale `expected_table_uuid`,
with zero writes), so exit 3 guarantees the NEW incarnation accepted
none of this window's rows — the previous resolve-then-commit race is
closed. The remaining exposure is only rows appended to an old
incarnation before it was dropped: they are gone with it, and the
offset never covers them (it only moves after a fully applied window),
so the window replays once hedgerow is re-pointed. Recovery:
decide deliberately what the new table means. If the recreate was
intentional and you want replication of the new incarnation from
scratch, point hedgerow at it with a NEW `consumer_id` (or after
resetting the destination) and restart. If the destination was
recreated, verify what data it lost before resuming.

### 2. Changefeed expired — 410 (exit 4)

*Message:* `changefeed window (a, b] is partially expired (HTTP 410) ...
Server reconcile instructions: ...`

Snapshot expiry on the source catalog overtook this consumer's offset:
part of the un-replicated range is gone. The destination is now missing
data that can only be recovered by reconciliation, not by reading the
feed. Recovery: re-derive the destination from a full scan of the
source table (backfill), then commit the consumer offset at the
snapshot the scan was taken at, and restart. Prevent recurrence:
enable `consumer_floor` retention on the source catalog and/or alert on
`hedgerow_lag_snapshots`.

### 3. Deletes present (exit 5)

*Message:* `change plan (a, b] contains N delete file(s) ... hedgerow v1
is append-only and cannot represent deletions in the destination.`

Someone registered deletion vectors on the source table. Append-only
replication cannot express them; continuing would replicate inserts
while silently ignoring deletes. Recovery: either stop deleting from
the source, or rebuild the destination from a full scan at a snapshot
past the deletes and manually commit the offset there. (Delete-aware
replication is explicitly out of v1's contract.)

### 4. Schema mismatch (exit 6)

A refusal to start, not a runtime halt: the destination's columns do not
project from the source. Fix the destination schema or the source, per
the precise per-column diff in the message.

### 5. Split-brain offset (exit 7)

*Message:* `offset commit for consumer '...' was rejected as a
regression ... Another writer shares this consumer_id.`

The source catalog refused our offset commit (409) because the stored
offset is already PAST our window: some other writer — almost always a
second hedgerow with a copy-pasted `consumer_id` — advanced it. If that
twin writes to a different destination, the rows between our window and
the foreign offset were never replicated to OUR destination; adopting
the foreign offset would make that loss permanent and silent, so
hedgerow never does. Recovery: find and stop (or rename) the colliding
consumer, audit which destination actually received which snapshots,
then set the offset deliberately (or backfill the gap from a scan) and
restart.

### 6. Data integrity (exit 8)

*Message:* `data file ... delivered N row(s) but the change plan records
record_count=M.`

A data file yielded a different row count than the server-side metadata
promised — a truncated/stale object-store read, a reader bug, or file
content that does not match the catalog. The offset was NOT committed.
Recovery: verify the file in object storage against the catalog entry
(`record_count`, `file_size_bytes`); a transient object-store issue
clears on restart (the window replays), a corrupt or replaced file is a
source-catalog incident to resolve before resuming.

### 7. Persistent failure (exit 9)

*Message:* `permanent client error ...` or `window replay budget
exhausted: ... (replication.max_window_replays=N) all failed`.

Either a permanent client error (validation/not-found/already-exists —
retrying fails identically), or `max_window_replays` consecutive
transient failures. Every replay attempt may have appended duplicate
rows (each was logged `window replay N: duplicates possible`); the
offset never moved. Recovery: fix the underlying cause (the halt names
the last error), expect up to `1 + max_window_replays` copies of the
window's rows in the destination worst-case, and restart — the window
replays once more from the committed offset.

## Development

```sh
flox activate -- uv sync
flox activate -- uv run pytest                      # unit + integration
flox activate -- uv run pytest -m "not integration" # unit only
```

Integration tests need a live hoglake server (`HOGLAKE_URL`, default
`http://localhost:8080`) and MinIO (`HOGLAKE_S3_ENDPOINT`, default
`http://localhost:19000`, key `hoglake`/`hoglake123`); they create
`hedgerow-*`-prefixed catalogs and buckets and skip cleanly when the
server is unreachable.
