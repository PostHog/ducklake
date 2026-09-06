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
| 3 | Incarnation guard | Source and destination `table_uuid`s are pinned at startup; every cycle re-resolves by name and HALTS loudly (nonzero exit, clear message) on any mismatch or disappearance. Never silently continue against a recreated table. |
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
  equals: 42                        # NULLs never match

replication:
  poll_interval_s: 5                # sleep when caught up (or after an error)
  max_snapshot_window: 1000         # snapshots per cycle, max
  max_rows_per_append: 100000       # rows per destination append, max

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
contain that column and will fail the read — evolve the destination
first, or start the destination at a post-alter offset.

Metrics (when enabled): `hedgerow_rows_replicated_total`,
`hedgerow_cycles_total`, `hedgerow_errors_total`,
`hedgerow_last_committed_snapshot`, `hedgerow_lag_snapshots`
(source head − committed offset).

## Runbook: the three halt conditions

hedgerow HALTS (exits nonzero, no retry) when continuing would be wrong.
A supervisor must NOT blindly restart these — the same condition will
halt again. Exit codes distinguish them.

### 1. Incarnation changed (exit 3)

*Message:* `source/destination table ... was recreated: pinned uuid X,
resolved uuid Y` (or `... no longer exists`).

The table hedgerow was replicating was dropped (and possibly recreated
under the same name). The committed consumer offset belongs to the OLD
incarnation; snapshot ranges and row ids do not carry over. Recovery:
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

(Schema mismatch — exit 6 — is a refusal to start, not a runtime halt:
fix the destination schema or the source, per the diff in the message.)

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
