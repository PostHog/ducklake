// Hand-written fixtures shaped exactly like the OpenAPI schemas in
// server/src/main/resources/openapi/hoglake.yaml (snake_case wire format).
// int64-typed fields are decimal strings on the client side (see
// src/api/int64.ts); the parser accepts them as strings from JSON too, so
// serializing these fixtures with JSON.stringify round-trips exactly.

import type {
  ApiErrorBody,
  Catalog,
  ConsumerOffset,
  DataFile,
  Namespace,
  PartitionStatsResponse,
  ScanFile,
  SnapshotPage,
  Table,
  TableSummary,
} from "../src/api/types";

export const catalogsFixture: Catalog[] = [
  {
    name: "analytics",
    data_path: "s3://hog-lake/analytics",
    head_snapshot_id: "4211",
    schema_version: "7",
  },
  {
    name: "scratch",
    data_path: "s3://hog-lake/scratch",
    head_snapshot_id: "12",
    schema_version: "1",
  },
];

export const namespacesFixture: Namespace[] = [
  { name: "events" },
  { name: "sessions" },
];

export const tablesFixture: TableSummary[] = [
  {
    name: "pageviews",
    table_uuid: "3f2c9c04-8a1b-4c7e-9f10-6d2a5b3e8c71",
  },
  {
    name: "clicks",
    table_uuid: "b7e6d9a2-15f3-4b08-a4c9-0e8f7d6c5b4a",
  },
];

export const tableFixture: Table = {
  name: "pageviews",
  namespace: "events",
  table_uuid: "3f2c9c04-8a1b-4c7e-9f10-6d2a5b3e8c71",
  columns: [
    { field_id: "1", ordinal: 0, name: "ts", type: "timestamptz", nullable: false },
    { field_id: "2", ordinal: 1, name: "user_id", type: "long", nullable: false },
    { field_id: "3", ordinal: 2, name: "url", type: "string", nullable: true },
  ],
  record_count: "1234567",
  file_count: "42",
  file_size_bytes: "5368709120",
  partition_spec: {
    spec_id: "1",
    fields: [
      { source_field_id: "1", transform: "day" },
      { source_field_id: "3", transform: "bucket", transform_param: 16 },
    ],
  },
};

/** One file per stats_state variant: provided / pending / failed. */
export const filesFixture: DataFile[] = [
  {
    data_file_id: "101",
    path: "s3://hog-lake/analytics/events/pageviews/data-00101.parquet",
    file_format: "parquet",
    record_count: "500000",
    file_size_bytes: "268435456",
    footer_size: "4096",
    row_id_start: "0",
    stats_state: "provided",
    begin_snapshot: "4001",
    spec_id: "1",
    partition_values: ["2026-09-01", "7"],
  },
  {
    data_file_id: "102",
    path: "s3://hog-lake/analytics/events/pageviews/data-00102.parquet",
    file_format: "parquet",
    record_count: "480000",
    file_size_bytes: "251658240",
    row_id_start: "500000",
    stats_state: "pending",
    begin_snapshot: "4100",
    spec_id: "1",
    partition_values: ["2026-09-02", null],
  },
  {
    data_file_id: "103",
    path: "s3://hog-lake/analytics/events/pageviews/data-00103.parquet",
    file_format: "parquet",
    record_count: "254567",
    file_size_bytes: "134217728",
    row_id_start: "980000",
    stats_state: "failed",
    begin_snapshot: "4200",
  },
];

/** Scan plan pairing one data file with its live deletion vector. */
export const scanFixture: ScanFile[] = [
  {
    data_file: filesFixture[0],
    delete_file: {
      delete_file_id: "900",
      data_file_id: "101",
      path: "s3://hog-lake/analytics/events/pageviews/dv-00900.puffin",
      file_format: "puffin-dv",
      delete_count: "1250",
      file_size_bytes: "8192",
      begin_snapshot: "4150",
    },
  },
  {
    data_file: filesFixture[1],
  },
];

// The timeline pages DESCENDING with the `before` cursor: page 1 covers
// before=head+1 (4212), page 2 continues below the last id of page 1.
export const snapshotsPage1: SnapshotPage = {
  snapshots: [
    {
      snapshot_id: "4211",
      snapshot_time: "2026-09-04T10:17:00Z",
      schema_version: "7",
      author: "ops",
      message: "create table events.clicks",
      changes: [{ kind: "table_created", object_id: "56" }],
    },
    {
      snapshot_id: "4210",
      snapshot_time: "2026-09-04T10:16:00Z",
      schema_version: "7",
      author: "millpond",
      message: "append 1 file",
      changes: [{ kind: "files_added", object_id: "55" }],
    },
  ],
  has_more: true,
};

export const snapshotsPage2: SnapshotPage = {
  snapshots: [
    {
      snapshot_id: "4209",
      snapshot_time: "2026-09-04T10:15:00Z",
      schema_version: "7",
      author: "viaduck",
      message: "append 3 files",
      changes: [
        { kind: "files_added", object_id: "55" },
        { kind: "rows_appended", object_id: "55" },
      ],
    },
  ],
  has_more: false,
};

export const consumerOffsetsFixture: ConsumerOffset[] = [
  {
    consumer_id: "viaduck-sink-7",
    table_uuid: "3f2c9c04-8a1b-4c7e-9f10-6d2a5b3e8c71",
    committed_snapshot: "4205",
    updated_at: "2026-09-04T10:14:30Z",
  },
  {
    consumer_id: "viaduck-sink-7",
    table_uuid: "b7e6d9a2-15f3-4b08-a4c9-0e8f7d6c5b4a",
    committed_snapshot: "4198",
    updated_at: "2026-09-04T10:12:11Z",
  },
];

// GET /v1/catalogs/{c}/stats/partitions — server-ordered by debt_score desc.
export const partitionStatsFixture: PartitionStatsResponse = {
  partitions: [
    {
      namespace: "events",
      table: "pageviews",
      table_uuid: "3f2c9c04-8a1b-4c7e-9f10-6d2a5b3e8c71",
      partition_values: [
        { field: "team_id", value: "42" },
        { field: "month", value: "2026-09" },
      ],
      spec_id: "3",
      file_count: "120",
      small_file_count: "118",
      total_bytes: "5368709120",
      small_file_bytes: "943718400",
      avg_file_bytes: "44739242",
      dv_count: "2",
      debt_score: "118",
    },
    {
      namespace: "events",
      table: "clicks",
      table_uuid: "b7e6d9a2-15f3-4b08-a4c9-0e8f7d6c5b4a",
      partition_values: [],
      spec_id: "0",
      file_count: "40",
      small_file_count: "9",
      total_bytes: "268435456",
      small_file_bytes: "9437184",
      avg_file_bytes: "6710886",
      dv_count: "0",
      debt_score: "9",
    },
  ],
  truncated: false,
};

export const notFoundError: ApiErrorBody = {
  error: "not_found",
  detail: "catalog 'nope' does not exist",
};

export const conflictError: ApiErrorBody = {
  error: "conflict",
  detail: "catalog 'analytics' already exists",
};

export const unprocessableError: ApiErrorBody = {
  error: "validation_failed",
  detail: "unknown field ids in column_stats: [99]",
};
