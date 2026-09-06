package com.posthog.hoglake.trino;

import com.posthog.hoglake.trino.rest.HoglakeClient;
import com.posthog.hoglake.trino.rest.HoglakeDtos;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.ConnectorTableVersion;
import io.trino.spi.connector.SchemaNotFoundException;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.SchemaTablePrefix;
import io.trino.spi.connector.TableColumnsMetadata;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static java.util.Objects.requireNonNull;

/**
 * Read-only metadata: hoglake namespaces are Trino schemas, hoglake
 * tables are Trino tables, columns map through {@link HoglakeTypes}.
 *
 * <p>Query consistency: {@link #getTableHandle} resolves the catalog's
 * head snapshot once and pins it — snapshot id, table_uuid, and the
 * column list all ride the handle, and every later call in the query
 * (getColumnHandles, getTableMetadata, getSplits) serves from or scans
 * at that pin. Listing surfaces (SHOW SCHEMAS/TABLES, DESCRIBE via
 * streamTableColumns) read head; they are not part of any query's
 * consistency contract.
 */
public class HoglakeMetadata
        implements ConnectorMetadata
{
    private final HoglakeClient client;

    public HoglakeMetadata(HoglakeClient client)
    {
        this.client = requireNonNull(client, "client is null");
    }

    @Override
    public List<String> listSchemaNames(ConnectorSession session)
    {
        return client.listNamespaces().stream()
                .map(HoglakeDtos.Namespace::name)
                .toList();
    }

    @Override
    public ConnectorTableHandle getTableHandle(
            ConnectorSession session,
            SchemaTableName tableName,
            Optional<ConnectorTableVersion> startVersion,
            Optional<ConnectorTableVersion> endVersion)
    {
        if (startVersion.isPresent() || endVersion.isPresent()) {
            throw new TrinoException(NOT_SUPPORTED,
                    "Table versioning (time travel) not yet supported by the hoglake connector");
        }
        // Pin the query's snapshot: resolve head once, then fetch the
        // table AT that snapshot (not at whatever head is by the time the
        // second request lands) so the handle is consistent-at-N.
        long snapshot = client.getCatalog().headSnapshotId();
        return client.getTable(tableName.getSchemaName(), tableName.getTableName(), snapshot)
                .map(table -> (ConnectorTableHandle) new HoglakeTableHandle(
                        tableName.getSchemaName(),
                        tableName.getTableName(),
                        snapshot,
                        table.tableUuid(),
                        table.columns().stream().map(HoglakeMetadata::toColumnHandle).toList()))
                .orElse(null);
    }

    @Override
    public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle table)
    {
        HoglakeTableHandle handle = (HoglakeTableHandle) table;
        return new ConnectorTableMetadata(
                handle.schemaTableName(),
                handle.columns().stream().map(HoglakeColumnHandle::columnMetadata).toList());
    }

    @Override
    public List<SchemaTableName> listTables(ConnectorSession session, Optional<String> schemaName)
    {
        List<SchemaTableName> tables = new ArrayList<>();
        if (schemaName.isPresent()) {
            // Nonexistent schema is the user's error, surfaced typed.
            for (HoglakeDtos.TableSummary summary : client.listTables(schemaName.get())) {
                tables.add(new SchemaTableName(schemaName.get(), summary.name()));
            }
            return tables;
        }
        for (String schema : listSchemaNames(session)) {
            try {
                for (HoglakeDtos.TableSummary summary : client.listTables(schema)) {
                    tables.add(new SchemaTableName(schema, summary.name()));
                }
            }
            catch (SchemaNotFoundException e) {
                // Namespace dropped between the listing and this fetch; skip.
            }
        }
        return tables;
    }

    @Override
    public Map<String, ColumnHandle> getColumnHandles(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        HoglakeTableHandle handle = (HoglakeTableHandle) tableHandle;
        Map<String, ColumnHandle> handles = new LinkedHashMap<>();
        for (HoglakeColumnHandle column : handle.columns()) {
            handles.put(column.name(), column);
        }
        return handles;
    }

    @Override
    public ColumnMetadata getColumnMetadata(
            ConnectorSession session,
            ConnectorTableHandle tableHandle,
            ColumnHandle columnHandle)
    {
        return ((HoglakeColumnHandle) columnHandle).columnMetadata();
    }

    @Override
    public java.util.Iterator<TableColumnsMetadata> streamTableColumns(
            ConnectorSession session,
            SchemaTablePrefix prefix)
    {
        List<SchemaTableName> names = prefix.getTable()
                .map(table -> List.of(new SchemaTableName(prefix.getSchema().orElseThrow(), table)))
                .orElseGet(() -> listTables(session, prefix.getSchema()));
        List<TableColumnsMetadata> result = new ArrayList<>();
        for (SchemaTableName name : names) {
            client.getTable(name.getSchemaName(), name.getTableName())
                    .ifPresent(table -> result.add(
                            TableColumnsMetadata.forTable(name, columnMetadata(table))));
        }
        return result.iterator();
    }

    // ---- helpers -----------------------------------------------------------

    static HoglakeColumnHandle toColumnHandle(HoglakeDtos.Column column)
    {
        return new HoglakeColumnHandle(
                column.name(),
                column.fieldId(),
                HoglakeTypes.toTrinoType(column.type(), column.typeParams()),
                column.isNullable());
    }

    private static List<ColumnMetadata> columnMetadata(HoglakeDtos.Table table)
    {
        return table.columns().stream()
                .map(column -> toColumnHandle(column).columnMetadata())
                .toList();
    }
}
