-- hoglake catalog schema v1.
--
-- Pre-release: migrations are squashed into this single file (decision
-- 2026-09-05); the migration chain becomes append-only at the first
-- real release. schema.sql is the canonical copy; the equivalence test
-- asserts the two stay identical.

--
-- Design positions this encodes (see hoglake/README.md):
--  * Real integrity: PKs, FKs with ON DELETE CASCADE, NOT NULL where the
--    code assumes it, partial indexes for the hot predicates.
--  * Versioned-row pattern where time travel needs it: a row is visible
--    at snapshot S iff begin_snapshot <= S AND (end_snapshot IS NULL OR
--    S < end_snapshot).
--  * Identity vs. version split: hog_table is the immutable identity
--    (table_id, table_uuid); hog_table_version carries the mutable,
--    versioned bits (name, namespace).
--  * Id allocation: per-catalog counters on hog_catalog / hog_table,
--    advanced inside the serialized commit tail (advisory xact lock), so
--    ids are dense and ordered with commit order.
--  * Typed conflict vocabulary: hog_snapshot_change replaces DuckLake's
--    comma-encoded changes_made string.
--  * Row-level deletes are deletion vectors (one live DV per data file);
--    no inlined data, macros, or tags — by design, not omission.

CREATE TABLE hog_catalog (
    catalog_id       bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    name             text NOT NULL UNIQUE CHECK (name ~ '^[a-z][a-z0-9_-]{0,62}$'),
    data_path        text NOT NULL,
    -- Allocators, advanced only inside the commit tail / DDL txn.
    last_snapshot_id bigint NOT NULL DEFAULT 0,
    schema_version   bigint NOT NULL DEFAULT 0,
    next_table_id    bigint NOT NULL DEFAULT 1,
    next_file_id     bigint NOT NULL DEFAULT 1,
    next_namespace_id bigint NOT NULL DEFAULT 1,
    snapshot_retention_seconds bigint
        CHECK (snapshot_retention_seconds IS NULL OR snapshot_retention_seconds > 0),
    consumer_floor   boolean NOT NULL DEFAULT true,
    earliest_snapshot_id bigint NOT NULL DEFAULT 0,
    -- snapshot_time of the snapshot earliest_snapshot_id points at,
    -- captured by the expiry sweep in the same UPDATE that advances the
    -- floor. Survives the snapshot rows below the floor (their times die
    -- with them), so a 410 can say WHEN the floor was reached. NULL =
    -- expiry has never advanced the floor.
    earliest_snapshot_time timestamptz,
    next_view_id     bigint NOT NULL DEFAULT 1,
    created_at       timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE hog_snapshot (
    catalog_id     bigint NOT NULL REFERENCES hog_catalog ON DELETE CASCADE,
    snapshot_id    bigint NOT NULL,
    snapshot_time  timestamptz NOT NULL DEFAULT now(),
    schema_version bigint NOT NULL,
    author         text,
    commit_message text,
    PRIMARY KEY (catalog_id, snapshot_id)
);

-- Typed change vocabulary. kind is closed: conflict detection joins on it.
CREATE TABLE hog_snapshot_change (
    catalog_id  bigint NOT NULL,
    snapshot_id bigint NOT NULL,
    kind        text   NOT NULL CHECK (kind IN (
                    'namespace_created', 'namespace_dropped',
                    'table_created', 'table_dropped', 'table_altered',
                    'table_inserted_into', 'table_deleted_from',
                    'table_compacted',
                    'view_created', 'view_dropped')),
    -- table_id, namespace_id, or view_id. Every kind in the vocabulary
    -- names an object; a NULL here would be a conflict row the conflict
    -- index silently never matches (NULL never equals) — an invisible
    -- OCC bypass — so the DB refuses it outright.
    object_id   bigint NOT NULL,
    FOREIGN KEY (catalog_id, snapshot_id)
        REFERENCES hog_snapshot ON DELETE CASCADE
);
-- The conflict check: changes of these kinds against this object since my
-- read snapshot. One indexed anti-join, never a string parse.
CREATE INDEX hog_snapshot_change_conflict
    ON hog_snapshot_change (catalog_id, object_id, kind, snapshot_id);
CREATE INDEX hog_snapshot_change_by_snapshot
    ON hog_snapshot_change (catalog_id, snapshot_id);

-- Namespaces are not snapshot-versioned in v1: rename is disallowed,
-- drop requires emptiness (live-table check in the service).
CREATE TABLE hog_namespace (
    catalog_id   bigint NOT NULL REFERENCES hog_catalog ON DELETE CASCADE,
    namespace_id bigint NOT NULL,
    -- Identifier policy (shared with table/view/column names; enforced
    -- service-side too): letter/underscore start, then [A-Za-z0-9_-],
    -- max 128 chars.
    name         text NOT NULL CHECK (name ~ '^[A-Za-z_][A-Za-z0-9_-]{0,127}$'),
    dropped      boolean NOT NULL DEFAULT false,
    created_at   timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (catalog_id, namespace_id)
);
CREATE UNIQUE INDEX hog_namespace_live_name
    ON hog_namespace (catalog_id, name) WHERE NOT dropped;

-- Immutable table identity. table_uuid is the contract consumers key
-- cursors to: it survives rename, changes on drop+recreate.
CREATE TABLE hog_table (
    catalog_id    bigint NOT NULL REFERENCES hog_catalog ON DELETE CASCADE,
    table_id      bigint NOT NULL,
    table_uuid    uuid   NOT NULL DEFAULT gen_random_uuid(),
    created_snapshot bigint NOT NULL,
    dropped_snapshot bigint,
    next_field_id bigint NOT NULL DEFAULT 1,
    PRIMARY KEY (catalog_id, table_id),
    UNIQUE (catalog_id, table_uuid)
);

-- Versioned mutable bits of a table (name, namespace).
CREATE TABLE hog_table_version (
    catalog_id     bigint NOT NULL,
    table_id       bigint NOT NULL,
    begin_snapshot bigint NOT NULL,
    end_snapshot   bigint,
    namespace_id   bigint NOT NULL,
    name           text   NOT NULL CHECK (name ~ '^[A-Za-z_][A-Za-z0-9_-]{0,127}$'),
    PRIMARY KEY (catalog_id, table_id, begin_snapshot),
    FOREIGN KEY (catalog_id, table_id) REFERENCES hog_table ON DELETE CASCADE,
    FOREIGN KEY (catalog_id, namespace_id) REFERENCES hog_namespace ON DELETE CASCADE,
    CHECK (end_snapshot IS NULL OR end_snapshot > begin_snapshot)
);
CREATE UNIQUE INDEX hog_table_version_live_name
    ON hog_table_version (catalog_id, namespace_id, name)
    WHERE end_snapshot IS NULL;
CREATE INDEX hog_table_version_live
    ON hog_table_version (catalog_id, table_id) WHERE end_snapshot IS NULL;

-- Versioned column definitions. field_id is the stable identity embedded
-- in parquet files (PARQUET:field_id) — see iceberg-federation.md.
CREATE TABLE hog_column (
    catalog_id     bigint NOT NULL,
    table_id       bigint NOT NULL,
    field_id       bigint NOT NULL,
    begin_snapshot bigint NOT NULL,
    end_snapshot   bigint,
    name           text   NOT NULL CHECK (name ~ '^[A-Za-z_][A-Za-z0-9_-]{0,127}$'),
    -- Closed type set: every member has a defined Iceberg mapping
    -- (iceberg-federation.md §2). Extend by migration, never ad hoc.
    col_type       text   NOT NULL CHECK (col_type IN (
                       'boolean', 'int', 'long', 'float', 'double',
                       'decimal', 'date', 'time', 'timestamp',
                       'timestamptz', 'string', 'uuid', 'binary')),
    type_params    jsonb,          -- e.g. {"precision":38,"scale":9} for decimal
    nullable       boolean NOT NULL DEFAULT true,
    ordinal        int    NOT NULL CHECK (ordinal >= 0),
    PRIMARY KEY (catalog_id, table_id, field_id, begin_snapshot),
    FOREIGN KEY (catalog_id, table_id) REFERENCES hog_table ON DELETE CASCADE,
    CHECK (end_snapshot IS NULL OR end_snapshot > begin_snapshot)
);
CREATE INDEX hog_column_live
    ON hog_column (catalog_id, table_id) WHERE end_snapshot IS NULL;
-- Writers stamp parquet field order from ordinals: a duplicate live
-- ordinal is a silent corruption vector, so the DB refuses it.
CREATE UNIQUE INDEX hog_column_live_ordinal
    ON hog_column (catalog_id, table_id, ordinal)
    WHERE end_snapshot IS NULL;

-- Table-level rollup + the row-id allocator. One row per table, created
-- with the table.
CREATE TABLE hog_table_stats (
    catalog_id      bigint NOT NULL,
    table_id        bigint NOT NULL,
    record_count    bigint NOT NULL DEFAULT 0,
    -- CHECK backstops the commit tail's overflow-checked byte rollup: a
    -- wrapped (negative) sum must fail loudly, never land as drift.
    file_size_bytes bigint NOT NULL DEFAULT 0 CHECK (file_size_bytes >= 0),
    -- CHECK is the DB backstop against row-id allocator overflow: a
    -- wrapped (negative) allocator would silently break the lineage
    -- guarantee. CommitService rejects overflowing sums before this.
    next_row_id     bigint NOT NULL DEFAULT 0 CHECK (next_row_id >= 0),
    PRIMARY KEY (catalog_id, table_id),
    FOREIGN KEY (catalog_id, table_id) REFERENCES hog_table ON DELETE CASCADE
);

-- The file manifest. Deletes never end-snapshot a data file (DVs mask
-- rows); end_snapshot comes from drop, compaction, or expiry.
CREATE TABLE hog_data_file (
    catalog_id      bigint NOT NULL,
    data_file_id    bigint NOT NULL,
    table_id        bigint NOT NULL,
    begin_snapshot  bigint NOT NULL,
    end_snapshot    bigint,
    path            text   NOT NULL,   -- absolute object-store URI; no relative chains
    file_format     text   NOT NULL DEFAULT 'parquet'
                    CHECK (file_format IN ('parquet')),
    record_count    bigint NOT NULL CHECK (record_count >= 0),
    file_size_bytes bigint NOT NULL CHECK (file_size_bytes >= 0),
    footer_size     bigint,
    row_id_start    bigint NOT NULL,   -- lineage guarantee: always assigned at commit
    -- Deferred-stats mode: 'pending' files are never pruned; the hydrator
    -- flips them to 'provided' or 'failed'.
    stats_state     text   NOT NULL DEFAULT 'provided'
                    CHECK (stats_state IN ('provided', 'pending', 'failed')),
    spec_id         bigint,
    -- Compaction outputs merge inputs whose row-id ranges need not be
    -- contiguous, so their row ids cannot be positional: the file carries
    -- an explicit physical int64 column `_hog_row_id` (reserved parquet
    -- field id 2147483646) holding each row's id, and this flag tells
    -- readers so. When true, row_id_start is only min(input row ids) —
    -- its positional meaning (row_id_start + offset) is void.
    explicit_row_ids boolean NOT NULL DEFAULT false,
    -- Field-id contract flag, set by the hydrator's footer read: true
    -- when any leaf of the file's parquet schema lacks a PARQUET:field_id
    -- (such files bind columns by NAME; renaming a column would silently
    -- NULL their history in readers, so AlterService refuses renames
    -- while a live flagged file exists). The reserved `_hog_row_id` id
    -- 2147483646 counts as an id like any other.
    missing_field_ids boolean NOT NULL DEFAULT false,
    PRIMARY KEY (catalog_id, data_file_id),
    FOREIGN KEY (catalog_id, table_id) REFERENCES hog_table ON DELETE CASCADE,
    CHECK (end_snapshot IS NULL OR end_snapshot > begin_snapshot)
);
CREATE INDEX hog_data_file_live
    ON hog_data_file (catalog_id, table_id, begin_snapshot)
    WHERE end_snapshot IS NULL;
CREATE INDEX hog_data_file_pending
    ON hog_data_file (catalog_id, data_file_id)
    WHERE stats_state = 'pending';

-- Per-file, per-column zone maps. Bounds are stored in Iceberg
-- single-value binary serialization (opaque to Postgres) so manifest
-- generation for the facade is a mechanical re-encode.
CREATE TABLE hog_file_column_stats (
    catalog_id   bigint NOT NULL,
    data_file_id bigint NOT NULL,
    field_id     bigint NOT NULL,
    value_count  bigint NOT NULL CHECK (value_count >= 0),
    null_count   bigint NOT NULL CHECK (null_count >= 0),
    nan_count    bigint,
    size_bytes   bigint,
    lower_bound  bytea,
    upper_bound  bytea,
    PRIMARY KEY (catalog_id, data_file_id, field_id),
    FOREIGN KEY (catalog_id, data_file_id)
        REFERENCES hog_data_file ON DELETE CASCADE
);

-- Log primitives: per-consumer committed offsets, keyed by table_uuid so
-- an incarnation change is visible to the consumer, not deduced.
CREATE TABLE hog_consumer_offset (
    catalog_id         bigint NOT NULL REFERENCES hog_catalog ON DELETE CASCADE,
    consumer_id        text   NOT NULL CHECK (length(consumer_id) BETWEEN 1 AND 128),
    table_uuid         uuid   NOT NULL,
    committed_snapshot bigint NOT NULL,
    updated_at         timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (catalog_id, consumer_id, table_uuid)
);
CREATE INDEX hog_consumer_offset_retention_floor
    ON hog_consumer_offset (catalog_id, committed_snapshot);

-- ---- Partitioning ----
-- Versioned spec header + ordered fields. Transforms are the
-- Iceberg-semantics set only (iceberg-federation.md §3).
CREATE TABLE hog_partition_spec (
    catalog_id     bigint NOT NULL,
    table_id       bigint NOT NULL,
    spec_id        bigint NOT NULL,
    begin_snapshot bigint NOT NULL,
    end_snapshot   bigint,
    PRIMARY KEY (catalog_id, table_id, spec_id),
    FOREIGN KEY (catalog_id, table_id) REFERENCES hog_table ON DELETE CASCADE,
    CHECK (end_snapshot IS NULL OR end_snapshot > begin_snapshot)
);
CREATE INDEX hog_partition_spec_live
    ON hog_partition_spec (catalog_id, table_id) WHERE end_snapshot IS NULL;

CREATE TABLE hog_partition_field (
    catalog_id      bigint NOT NULL,
    table_id        bigint NOT NULL,
    spec_id         bigint NOT NULL,
    key_index       int    NOT NULL CHECK (key_index >= 0),
    source_field_id bigint NOT NULL,
    transform       text   NOT NULL CHECK (transform IN
                        ('identity', 'bucket', 'year', 'month', 'day', 'hour')),
    transform_param int    CHECK ((transform = 'bucket') = (transform_param IS NOT NULL)),
    PRIMARY KEY (catalog_id, table_id, spec_id, key_index),
    FOREIGN KEY (catalog_id, table_id, spec_id)
        REFERENCES hog_partition_spec ON DELETE CASCADE
);

-- Files bind to the spec they were written under; values are the
-- TRANSFORMED partition values, string-encoded, one per key_index.

CREATE TABLE hog_file_partition_value (
    catalog_id   bigint NOT NULL,
    data_file_id bigint NOT NULL,
    key_index    int    NOT NULL,
    value        text,   -- NULL = null partition value
    PRIMARY KEY (catalog_id, data_file_id, key_index),
    FOREIGN KEY (catalog_id, data_file_id)
        REFERENCES hog_data_file ON DELETE CASCADE
);

-- ---- Sort orders ----
-- Versioned sort spec, mirroring the partition-spec pair: header +
-- ordered fields. ADVISORY for writers (the server never verifies file
-- sortedness) and BINDING for compaction rewrites, which sort merged
-- rows by the live spec — safe only because compaction outputs carry
-- explicit row ids (hog_data_file.explicit_row_ids above).
CREATE TABLE hog_sort_spec (
    catalog_id     bigint NOT NULL,
    table_id       bigint NOT NULL,
    sort_id        bigint NOT NULL,
    begin_snapshot bigint NOT NULL,
    end_snapshot   bigint,
    PRIMARY KEY (catalog_id, table_id, sort_id),
    FOREIGN KEY (catalog_id, table_id) REFERENCES hog_table ON DELETE CASCADE,
    CHECK (end_snapshot IS NULL OR end_snapshot > begin_snapshot)
);
CREATE INDEX hog_sort_spec_live
    ON hog_sort_spec (catalog_id, table_id) WHERE end_snapshot IS NULL;

CREATE TABLE hog_sort_field (
    catalog_id      bigint NOT NULL,
    table_id        bigint NOT NULL,
    sort_id         bigint NOT NULL,
    key_index       int    NOT NULL CHECK (key_index >= 0),
    source_field_id bigint NOT NULL,
    direction       text   NOT NULL CHECK (direction IN ('asc', 'desc')),
    null_order      text   NOT NULL CHECK (null_order IN ('nulls_first', 'nulls_last')),
    PRIMARY KEY (catalog_id, table_id, sort_id, key_index),
    FOREIGN KEY (catalog_id, table_id, sort_id)
        REFERENCES hog_sort_spec ON DELETE CASCADE
);

-- ---- Row-level deletes: deletion vectors ----
-- One DV file per data file per range; a new DV supersedes the old
-- (end_snapshot) and must cover it (delete_count monotonic — DVs only
-- grow). Registration is metadata-only (footer-shipping philosophy);
-- the ONE server-side reader of DV content is the compaction rewrite,
-- which applies a group's live DVs (iceberg puffin `deletion-vector-v1`
-- blobs) and end-snapshots them with their data files.
CREATE TABLE hog_delete_file (
    catalog_id      bigint NOT NULL,
    delete_file_id  bigint NOT NULL,
    table_id        bigint NOT NULL,
    data_file_id    bigint NOT NULL,
    begin_snapshot  bigint NOT NULL,
    end_snapshot    bigint,
    path            text   NOT NULL,
    file_format     text   NOT NULL DEFAULT 'puffin-dv'
                    CHECK (file_format IN ('puffin-dv')),
    delete_count    bigint NOT NULL CHECK (delete_count > 0),
    file_size_bytes bigint NOT NULL CHECK (file_size_bytes >= 0),
    PRIMARY KEY (catalog_id, delete_file_id),
    FOREIGN KEY (catalog_id, table_id) REFERENCES hog_table ON DELETE CASCADE,
    FOREIGN KEY (catalog_id, data_file_id)
        REFERENCES hog_data_file ON DELETE CASCADE,
    CHECK (end_snapshot IS NULL OR end_snapshot > begin_snapshot)
);
CREATE UNIQUE INDEX hog_delete_file_one_live_per_data_file
    ON hog_delete_file (catalog_id, data_file_id) WHERE end_snapshot IS NULL;
CREATE INDEX hog_delete_file_live
    ON hog_delete_file (catalog_id, table_id) WHERE end_snapshot IS NULL;

-- Versioned views: SQL text stored with its dialect. Single versioned
-- table (no identity split: views carry no lineage/state that must
-- survive re-creation).
CREATE TABLE hog_view (
    catalog_id     bigint NOT NULL REFERENCES hog_catalog ON DELETE CASCADE,
    view_id        bigint NOT NULL,
    view_uuid      uuid   NOT NULL DEFAULT gen_random_uuid(),
    namespace_id   bigint NOT NULL,
    name           text   NOT NULL CHECK (name ~ '^[A-Za-z_][A-Za-z0-9_-]{0,127}$'),
    dialect        text   NOT NULL DEFAULT 'trino',
    sql            text   NOT NULL,
    begin_snapshot bigint NOT NULL,
    end_snapshot   bigint,
    PRIMARY KEY (catalog_id, view_id),
    FOREIGN KEY (catalog_id, namespace_id)
        REFERENCES hog_namespace ON DELETE CASCADE,
    CHECK (end_snapshot IS NULL OR end_snapshot > begin_snapshot)
);
CREATE UNIQUE INDEX hog_view_live_name
    ON hog_view (catalog_id, namespace_id, name) WHERE end_snapshot IS NULL;

-- The file-removal queue AND ledger. Physical deletion is decoupled
-- from metadata deletion, batched, and ALWAYS liveness-checked at drain
-- time — a queue entry is a suggestion, never an authorization
-- (README.md §8). Draining SOFT-deletes: a completed entry keeps its
-- row (drained_at + drained_outcome) so "what did cleanup touch, when,
-- after how many attempts" stays queryable; the cleanup sweep purges
-- drained rows past a retention window so the ledger itself never
-- accumulates without bound.
CREATE TABLE hog_file_removal (
    removal_id   bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    catalog_id   bigint NOT NULL REFERENCES hog_catalog ON DELETE CASCADE,
    path         text   NOT NULL,
    file_kind    text   NOT NULL CHECK (file_kind IN ('data', 'delete')),
    -- 'compaction_staging': a claim ticket the compactor inserts for its
    -- OUTPUT path before uploading — if the group's commit never lands
    -- (crash, plan-to-commit race), the normal cleanup drain reclaims the
    -- orphaned object; a successful group commit settles the row as
    -- 'registered' in the same transaction that makes the path live.
    reason       text   NOT NULL CHECK (reason IN ('snapshot_expiry', 'table_drop_gc',
                                                   'compaction_staging')),
    scheduled_at timestamptz NOT NULL DEFAULT now(),
    -- Drain bookkeeping: attempts counts every touch that did NOT drain
    -- the row (still-referenced skips, transient S3 failures).
    attempts        int NOT NULL DEFAULT 0,
    last_attempt_at timestamptz,
    -- Soft-delete: non-null once the entry is settled. 'deleted' = the
    -- object was physically removed; 'absent' = it was verified already
    -- gone; 'registered' = a compaction_staging claim whose group commit
    -- landed (the path became a live catalog file instead of garbage).
    -- Outcome and timestamp travel together.
    drained_at      timestamptz,
    drained_outcome text CHECK (drained_outcome IN ('deleted', 'absent', 'registered')),
    CHECK ((drained_at IS NULL) = (drained_outcome IS NULL))
);
CREATE INDEX hog_file_removal_drain
    ON hog_file_removal (catalog_id, removal_id)
    WHERE drained_at IS NULL;
