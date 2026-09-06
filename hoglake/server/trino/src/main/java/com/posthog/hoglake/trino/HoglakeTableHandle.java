package com.posthog.hoglake.trino;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.SchemaTableName;

import java.util.List;

import static java.util.Objects.requireNonNull;

/**
 * A hoglake table, resolved once at analysis time and pinned: schema =
 * hoglake namespace, table = table name, plus the snapshot the query
 * runs at, the incarnation it bound to, and the resolved columns.
 *
 * <p>The pin is what closes the head race: getColumnHandles and
 * getTableMetadata serve from this handle without re-fetching head, and
 * getSplits scans at {@code snapshotId} — so a concurrent commit (or
 * DROP + CREATE, which changes {@code tableUuid}) cannot rebind the
 * query to a different table state between analysis and execution. One
 * query, one consistent snapshot.
 */
public record HoglakeTableHandle(
        @JsonProperty("schemaName") String schemaName,
        @JsonProperty("tableName") String tableName,
        @JsonProperty("snapshotId") long snapshotId,
        @JsonProperty("tableUuid") String tableUuid,
        @JsonProperty("columns") List<HoglakeColumnHandle> columns)
        implements ConnectorTableHandle
{
    @JsonCreator
    public HoglakeTableHandle
    {
        requireNonNull(schemaName, "schemaName is null");
        requireNonNull(tableName, "tableName is null");
        requireNonNull(tableUuid, "tableUuid is null");
        columns = List.copyOf(requireNonNull(columns, "columns is null"));
    }

    public SchemaTableName schemaTableName()
    {
        return new SchemaTableName(schemaName, tableName);
    }

    @Override
    public String toString()
    {
        return schemaName + "." + tableName + "@" + snapshotId;
    }
}
