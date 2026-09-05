-- Milestone 2: DuckLake parity — row-level deletes (deletion vectors),
-- partitioning, and the change-kind vocabulary for both. Schema
-- evolution needs no new tables (hog_column/hog_table_version are
-- already snapshot-versioned).

-- New change kind for delete commits.
ALTER TABLE hog_snapshot_change DROP CONSTRAINT hog_snapshot_change_kind_check;
ALTER TABLE hog_snapshot_change ADD CONSTRAINT hog_snapshot_change_kind_check
    CHECK (kind IN (
        'namespace_created', 'namespace_dropped',
        'table_created', 'table_dropped', 'table_altered',
        'table_inserted_into', 'table_deleted_from'));

-- Wave-1 review item: catalog-level cascade must reach table versions.
ALTER TABLE hog_table_version
    DROP CONSTRAINT hog_table_version_catalog_id_namespace_id_fkey;
ALTER TABLE hog_table_version
    ADD CONSTRAINT hog_table_version_catalog_id_namespace_id_fkey
    FOREIGN KEY (catalog_id, namespace_id)
    REFERENCES hog_namespace ON DELETE CASCADE;

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
ALTER TABLE hog_data_file ADD COLUMN spec_id bigint;

CREATE TABLE hog_file_partition_value (
    catalog_id   bigint NOT NULL,
    data_file_id bigint NOT NULL,
    key_index    int    NOT NULL,
    value        text,   -- NULL = null partition value
    PRIMARY KEY (catalog_id, data_file_id, key_index),
    FOREIGN KEY (catalog_id, data_file_id)
        REFERENCES hog_data_file ON DELETE CASCADE
);

-- ---- Row-level deletes: deletion vectors ----
-- One DV file per data file per range; a new DV supersedes the old
-- (end_snapshot) and must cover it (delete_count monotonic — DVs only
-- grow). The server never opens DV files: registration is metadata-only
-- (footer-shipping philosophy), verification is a background job later.
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
