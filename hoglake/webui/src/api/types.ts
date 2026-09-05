// Hand-written TypeScript mirrors of the OpenAPI schemas in
// server/src/main/resources/openapi/hoglake.yaml. Wire format is snake_case;
// these types pass it through untouched.

export interface ApiErrorBody {
  error: string;
  detail?: string;
}

export interface Catalog {
  name: string;
  data_path: string;
  head_snapshot_id: number;
  schema_version: number;
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
  field_id: number;
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
  source_field_id: number;
  transform: PartitionTransform;
  transform_param?: number;
}

export interface PartitionSpec {
  spec_id: number;
  fields: PartitionField[];
}

export interface Table {
  name: string;
  namespace: string;
  table_uuid: string;
  columns: Column[];
  record_count: number;
  file_count: number;
  file_size_bytes: number;
  partition_spec?: PartitionSpec;
}

export type StatsState = "provided" | "pending" | "failed";

export interface DataFile {
  data_file_id: number;
  path: string;
  file_format: string;
  record_count: number;
  file_size_bytes: number;
  footer_size?: number;
  row_id_start: number;
  stats_state: StatsState;
  begin_snapshot: number;
  spec_id?: number;
  partition_values?: (string | null)[];
}

export interface DeleteFile {
  delete_file_id: number;
  data_file_id: number;
  path: string;
  file_format: "puffin-dv";
  delete_count: number;
  file_size_bytes: number;
  begin_snapshot: number;
}

export interface ScanFile {
  data_file: DataFile;
  delete_file?: DeleteFile;
}

export interface SnapshotChange {
  kind: string;
  object_id?: number;
}

export interface Snapshot {
  snapshot_id: number;
  snapshot_time: string;
  schema_version: number;
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
  committed_snapshot: number;
  updated_at: string;
}

export interface CommitResult {
  snapshot_id: number;
  schema_version?: number;
}
