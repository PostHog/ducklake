package com.posthog.hoglake.trino.rest;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;
import java.util.Map;

/**
 * Wire DTOs for the hoglake REST API (openapi/hoglake.yaml, snake_case
 * wire). Only the read-planning subset the connector needs.
 */
public final class HoglakeDtos
{
    private HoglakeDtos() {}

    public record Catalog(
            @JsonProperty("name") String name,
            @JsonProperty("data_path") String dataPath,
            @JsonProperty("head_snapshot_id") long headSnapshotId,
            @JsonProperty("schema_version") long schemaVersion) {}

    public record Namespace(
            @JsonProperty("name") String name) {}

    public record TableSummary(
            @JsonProperty("name") String name,
            @JsonProperty("table_uuid") String tableUuid) {}

    public record Column(
            @JsonProperty("field_id") long fieldId,
            @JsonProperty("ordinal") int ordinal,
            @JsonProperty("name") String name,
            @JsonProperty("type") String type,
            @JsonProperty("type_params") Map<String, Object> typeParams,
            @JsonProperty("nullable") Boolean nullable)
    {
        public boolean isNullable()
        {
            return nullable == null || nullable;
        }
    }

    public record Table(
            @JsonProperty("name") String name,
            @JsonProperty("namespace") String namespace,
            @JsonProperty("table_uuid") String tableUuid,
            @JsonProperty("columns") List<Column> columns,
            @JsonProperty("record_count") long recordCount,
            @JsonProperty("file_count") long fileCount,
            @JsonProperty("file_size_bytes") long fileSizeBytes) {}

    public record DataFile(
            @JsonProperty("data_file_id") long dataFileId,
            @JsonProperty("path") String path,
            @JsonProperty("file_format") String fileFormat,
            @JsonProperty("record_count") long recordCount,
            @JsonProperty("file_size_bytes") long fileSizeBytes,
            @JsonProperty("footer_size") Long footerSize,
            @JsonProperty("row_id_start") long rowIdStart,
            @JsonProperty("stats_state") String statsState,
            @JsonProperty("begin_snapshot") long beginSnapshot) {}

    public record DeleteFile(
            @JsonProperty("delete_file_id") long deleteFileId,
            @JsonProperty("data_file_id") long dataFileId,
            @JsonProperty("path") String path,
            @JsonProperty("file_format") String fileFormat,
            @JsonProperty("delete_count") long deleteCount,
            @JsonProperty("file_size_bytes") long fileSizeBytes,
            @JsonProperty("begin_snapshot") long beginSnapshot) {}

    /** One /scan entry: a data file paired with its live deletion vector, if any. */
    public record ScanFile(
            @JsonProperty("data_file") DataFile dataFile,
            @JsonProperty("delete_file") DeleteFile deleteFile) {}
}
