# Schema Assessment & Suggestions (2026-09-05)

Review of `server/schema.sql` (≡ `db/migration/V1__init.sql`; the
equivalence gate keeps them identical). Verdict up front: the schema is
the strongest part of the system — it's where the defect-ledger
lessons are *encoded*, not just described. Every versioned table
carries the same `begin/end_snapshot` shape with the same CHECK, every
FK is CASCADE, vocabularies are CHECK-closed, hot predicates get
partial indexes. That's a style guide that was actually followed.

## What's genuinely good

| Decision | Why it's right |
|---|---|
| Identity/version split (`hog_table` vs `hog_table_version`) | The `table_uuid`-as-incarnation contract *requires* identity to be immutable; splitting it out makes "rename doesn't touch identity" structural rather than conventional |
| `hog_snapshot_change` typed rows + `(catalog, object, kind, snapshot)` index | Conflict check is one index-only anti-join, O(changes since read_snapshot) not O(catalog); DuckLake's comma-string parse is dead |
| `hog_table_stats` = allocator anchor only, never read-path aggregates | Two-layer defense for the lineage guarantee: app checks the sums, `CHECK (next_row_id >= 0)` backstops overflow |
| `hog_delete_file_one_live_per_data_file` partial unique index | The entire "one live DV per file" invariant is one index; concurrent supersession races become a 23505 mapped to retry, not a lost update |
| Partial unique live-name indexes (`hog_table_version_live_name`, etc.) | Rename-collision correctness without a lock — a race converted into a constraint violation |
| `explicit_row_ids` + reserved parquet field id 2147483646 | Compaction-proofing baked in before compaction exists; the sorted-compaction rowid-remap defect class becomes unrepresentable |
| `hog_file_removal` reason CHECK + drain-time liveness | The phantom-queue incident encoded as "a queue entry is a suggestion, never an authorization" |

## Where to push

### 1. The `hog_catalog` allocator row will become the hotspot

Nine counters on one row, `UPDATE ... RETURNING` on every commit and
every DDL; the row lock is held from allocation to txn commit. Under
the advisory-lock tail this is serialized anyway, so contention is
bounded — fine. Two notes:

- The row is updated constantly (HOT-update/vacuum pressure). Keep
  allocator columns unindexed (they are) so updates stay HOT on one
  page; a schema-lint rule or comment should guard this.
- `schema_version` lives on `hog_catalog` while
  `hog_snapshot.schema_version` denormalizes it per snapshot. The
  denormalization is correct for time travel; no constraint ties them
  (a CHECK can't). Document that `schema_version` bumps are DDL-only.

### 2. `hog_snapshot` is append-only at scale with no partitioning story

At the documented 15M-snapshot precedent and ~138K snapshots/day on a
busy writer, a hot catalog accrues ~50M snapshot rows/year, each
cascading `hog_snapshot_change` children. Expiry range-deletes by
`(catalog_id, snapshot_id)` (indexed, batched — fine), but both tables
carry bloat between vacuums. Declarative range partitioning on
`snapshot_id` (~1M per partition) would turn expiry into
`DROP PARTITION` — but it interacts awkwardly with the CASCADE-FK
design (partition key must be in the PK; FKs referencing partitioned
tables have edge semantics) and complicates the schema-equivalence
gate. Defer; it's the known pressure point with a named escape hatch.

### 3. Namespace drop: `hog_table_version` FK→`hog_namespace` CASCADE vs unversioned namespaces

`namespace_dropped` / `hog_namespace.dropped` exist in the vocabulary
but nothing writes them — half-built. If a namespace is ever dropped,
CASCADE deletes `hog_table_version` rows while `hog_table` survives →
versioned history with holes. The schema prevents this only via the
service's emptiness rule, not structurally. Before the drop endpoint
exists: either decide the namespace versioning story, or change the FK
to RESTRICT so the trap is loud instead of latent.

### 4. `hog_column.ordinal` collision hole

`ordinal` sits on the versioned row (good — reorders are
snapshot-scoped), but the PK is
`(catalog, table, field_id, begin_snapshot)`: nothing prevents two live
columns sharing an ordinal. The writer path stamps parquet field order
from ordinals, so a dup is a silent writer-corruption vector. Fix
structurally, same class as everything else in this schema:

```sql
CREATE UNIQUE INDEX hog_column_live_ordinal
    ON hog_column (catalog_id, table_id, ordinal)
    WHERE end_snapshot IS NULL;
```

### 5. `hog_file_partition_value.value text` — writer-trusted correctness

Transformed partition values are string-encoded and opaque to the
server — consistent with footer-shipping (the server never computes
transforms) and Iceberg-mappable. But no CHECK binds value presence to
transform nullability, and string encoding means the server can't
validate e.g. a bucket value is in `[0, n)`. This is the one place the
"server validates structure" promise is waived; it should say so in
the docs — partition-value *correctness* is writer-trusted, unlike
everything else.

### 6. `hog_file_removal` inherits the arbitrary-path problem

`data_path` is stored but never consulted; commit paths are arbitrary
writer-supplied URIs; the cleanup queue liveness-checks paths but only
verifies they're not referenced — not that they were ever ours. A
hostile/buggy writer can queue `s3://anything` and the drain will
HEAD+DELETE it (outside our references). When credential vending
lands, add path-prefix validation at *enqueue* time; before that, a
cheap mitigation is rejecting registration paths outside
`hog_catalog.data_path` at commit.

### 7. `hog_view` breaks the identity/version pattern

Deliberate ("views carry no lineage"), and the comment says so — but
`view_uuid` lives on the unversioned identity row, so drop+recreate
changes the uuid with no history to observe it. Fine for v1 (no view
consumers); the asymmetry with `table_uuid`'s carefully built contract
will confuse the next reader. One comment line suffices.

### 8. `hog_snapshot.schema_version` status

Time-travel reads resolve schemas via versioned `hog_column` rows, so
`snapshot.schema_version` is informational. Confirm nothing on the
read path joins through it (aggregates come from files-at-snapshot —
correct); if purely diagnostic, label it as such.

## Summary

| # | Item | Class | Action |
|---|---|---|---|
| 1 | `hog_catalog` hot allocator row | Known, bounded | Keep allocators unindexed; document |
| 2 | `hog_snapshot` bloat at 50M+/yr | Known deferral | Revisit range partitioning when expiry lags |
| 3 | Namespace-drop CASCADE holes | Latent trap | Decide versioning story or FK→RESTRICT before the endpoint |
| 4 | `hog_column.ordinal` dup | Cheap structural fix | Add partial unique index (V1 still squashable) |
| 5 | Partition values writer-trusted | Doc gap | State the trust boundary |
| 6 | Removal queue path trust | Security-shaped | Prefix validation at enqueue; commit-time `data_path` check |
| 7 | `hog_view` uuid asymmetry | Cosmetic | Comment |
| 8 | `snapshot.schema_version` | Cosmetic | Comment |

~90% of the defect ledger is encoded structurally. The remaining gaps
are #3 and #4 (cheap now, expensive after the V1 chain freezes) and #2
(known, deferred, with an escape hatch).
