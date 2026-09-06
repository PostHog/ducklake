# Split-Validation Commit (2026-09-05)

Refinement to the commit protocol (README.md commit-protocol /
commit-serialization sections; implementation in
`server/.../commit/CommitService.kt`): validation happens in front of
the serialization point, not inside it. The question this answers: *if
validation can fail after snapshots are minted, don't you have to
unwind subsequent snapshots?* No — unwinding is only necessary in
designs that publish first and validate after (event-sourced logs with
post-hoc reconciliation). OCC is the opposite order: validate against a
pinned base, then mint. A failed validation means no snapshot was ever
allocated; PG transaction atomicity makes the abort a rollback, not an
unwind.

## Two validation classes

| Class | Examples | Depends on catalog state? | Where it runs |
|---|---|---|---|
| Structural | blank paths, negative sizes, `record_count` cap (2^48), stats field-ids exist and are unique, partition arity vs live spec, DV `delete_count > 0`, no same-commit delete targets | No (or only on immutable/resolved state) | Outside the tail — pure function of the request |
| Base-dependent | tables still live, spec still current, no `table_dropped`/`table_altered` since `readSnapshot`, DV not superseded past `readSnapshot` | Yes — against a pinned base snapshot S | Pre-checked at S outside; re-verified inside the tail |

Today `doCommit` takes the per-catalog advisory lock first and runs
both classes inside it — simple and correct, but the tail's length
equals validation + allocation + insertion, so a 1000-file commit or an
alter storm serializes everything behind it.

## The refined flow

```
1. Read head S                        (no lock)
2. Resolve tables at S                (no lock — a hint)
3. Structural validation              (no lock — pure)
      fail → 422, nothing written, nothing to unwind
4. TAIL:
   a. acquire per-catalog advisory lock      (serialization point starts)
   b. re-read head; conflict check:
      'table_dropped'/'table_altered' on touched tables
      with snapshot_id > S
      → conflict? 409, txn rolls back, client retries from 1
   c. allocate snapshot id, file ids, row-id ranges
   d. insert rows
   e. commit                            (lock releases)
```

Every failure exit is at or before the gate (4b); success is the only
path past 4c. Ids are allocated only after winning the gate, and the
lock excludes concurrent allocators, so ids stay dense — there is no
hole at snapshot 43 while 44 committed, because 43 was never handed
out. Density and id-order = commit-order are preserved by construction.

## Why no unwind machinery is needed

Unwinding is required only if all three hold:

1. Snapshots are minted before their contents are validated, and
2. Later snapshots may build on an unvalidated predecessor, and
3. Validation can fail after the fact.

Hoglake denies (1): `readSnapshot` pins a *validated* base, structural
checks are synchronous, and base-dependent checks are re-verified
atomically with the write. The near-miss is the post-commit actors —
hydrator, expiry, GC — but they never invalidate a snapshot: the
hydrator flips `stats_state` to `failed` (degrades pruning quality, not
correctness), expiry moves the floor forward, GC only removes
unreferenced files. "Snapshot N retroactively didn't happen" is
unrepresentable — the same class of property as the FK strategy:
invalid states are excluded structurally, not cleaned up.

## Consistency rule for any non-locking tail

If a future implementation moves the conflict check off the advisory
lock (optimistic, version-checked read of head), the discipline is:
outside-tail reads are *hints for efficiency only*; the tail re-reads
the minimal authority set — the head key/row and, per touched table,
the live version row — as cheap point reads in the same transaction as
the write. The lock is what currently makes "nothing relevant changed
since S" true by construction; any replacement must restore that
guarantee explicitly, or the conflict check silently validates against
a stale base.
