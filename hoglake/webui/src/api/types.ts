// Hand-written TypeScript mirrors of the OpenAPI schemas in
// server/src/main/resources/openapi/hoglake.yaml. Wire format is snake_case;
// these types pass it through untouched.
//
// Every field the spec types as `integer, format: int64` is carried as an
// exact decimal string (`Int64`), never a JS number: snapshot ids, row
// counts, file sizes and row-id starts can exceed 2^53 and would be silently
// rounded by JSON.parse. See src/api/int64.ts.

/** An int64 wire value, held as its exact decimal string representation. */
export type Int64 = string;

export interface ApiErrorBody {
  error: string;
  detail?: string;
}

export interface Catalog {
  name: string;
  data_path: string;
  head_snapshot_id: Int64;
  schema_version: Int64;
}

export interface CreateCatalogRequest {
  name: string;
  data_path: string;
}

export interface Namespace {
  name: string;
}

export const COLUMN_TYPES = [
  "boolean",
  "int",
  "long",
  "float",
  "double",
  "decimal",
  "date",
  "time",
  "timestamp",
  "timestamptz",
  "string",
  "uuid",
  "binary",
] as const;

export type ColumnType = (typeof COLUMN_TYPES)[number];

export interface ColumnDef {
  name: string;
  type: ColumnType;
  type_params?: Record<string, unknown>;
  nullable?: boolean;
}

export interface Column extends ColumnDef {
  field_id: Int64;
  ordinal: number;
}

export interface CreateTableRequest {
  name: string;
  columns: ColumnDef[];
}

export interface TableSummary {
  name: string;
  table_uuid: string;
}

export type PartitionTransform =
  | "identity"
  | "bucket"
  | "year"
  | "month"
  | "day"
  | "hour";

export interface PartitionField {
  source_field_id: Int64;
  transform: PartitionTransform;
  transform_param?: number;
}

export interface PartitionSpec {
  spec_id: Int64;
  fields: PartitionField[];
}

export interface Table {
  name: string;
  namespace: string;
  table_uuid: string;
  columns: Column[];
  record_count: Int64;
  file_count: Int64;
  file_size_bytes: Int64;
  partition_spec?: PartitionSpec;
}

export type StatsState = "provided" | "pending" | "failed";

export interface DataFile {
  data_file_id: Int64;
  path: string;
  file_format: string;
  record_count: Int64;
  file_size_bytes: Int64;
  footer_size?: Int64;
  row_id_start: Int64;
  stats_state: StatsState;
  begin_snapshot: Int64;
  spec_id?: Int64;
  partition_values?: (string | null)[];
}

export interface DeleteFile {
  delete_file_id: Int64;
  data_file_id: Int64;
  path: string;
  file_format: "puffin-dv";
  delete_count: Int64;
  file_size_bytes: Int64;
  begin_snapshot: Int64;
}

export interface ScanFile {
  data_file: DataFile;
  delete_file?: DeleteFile;
}

export interface SnapshotChange {
  kind: string;
  object_id?: Int64;
}

export interface Snapshot {
  snapshot_id: Int64;
  snapshot_time: string;
  schema_version: Int64;
  author?: string;
  message?: string;
  changes?: SnapshotChange[];
}

export interface SnapshotPage {
  snapshots: Snapshot[];
  has_more: boolean;
}

export interface ConsumerOffset {
  consumer_id: string;
  table_uuid: string;
  committed_snapshot: Int64;
  updated_at: string;
}

export interface ConsumerTableOffset {
  table_uuid: string;
  committed_snapshot: Int64;
  updated_at: string;
  namespace?: string;
  table_name?: string;
  table_dropped: boolean;
}

export interface ConsumerSummary {
  consumer_id: string;
  offsets: ConsumerTableOffset[];
}

export interface ConsumerList {
  consumers: ConsumerSummary[];
}

export interface PartitionValueEntry {
  field: string;
  value: string;
}

/** One leaf partition's compaction-debt stats, ranked by debt_score. */
export interface PartitionStats {
  namespace: string;
  table: string;
  table_uuid: string;
  partition_values: PartitionValueEntry[];
  spec_id: Int64;
  file_count: Int64;
  small_file_count: Int64;
  total_bytes: Int64;
  small_file_bytes: Int64;
  avg_file_bytes: Int64;
  dv_count: Int64;
  debt_score: Int64;
}

export interface PartitionStatsResponse {
  partitions: PartitionStats[];
  truncated: boolean;
}

export interface CommitResult {
  snapshot_id: Int64;
  schema_version?: Int64;
}
