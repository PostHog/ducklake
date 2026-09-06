package com.posthog.hoglake.trino;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.type.Type;

import static java.util.Objects.requireNonNull;

/**
 * A hoglake column. {@code fieldId} is the catalog's stable field id —
 * the same id writers embed into parquet as PARQUET:field_id — and is
 * the primary binding between catalog columns and file columns; name
 * binding is the fallback for files written without ids.
 */
public record HoglakeColumnHandle(
        @JsonProperty("name") String name,
        @JsonProperty("fieldId") long fieldId,
        @JsonProperty("type") Type type,
        @JsonProperty("nullable") boolean nullable)
        implements ColumnHandle
{
    @JsonCreator
    public HoglakeColumnHandle
    {
        requireNonNull(name, "name is null");
        requireNonNull(type, "type is null");
    }

    public ColumnMetadata columnMetadata()
    {
        return ColumnMetadata.builder()
                .setName(name)
                .setType(type)
                .setNullable(nullable)
                .build();
    }

    @Override
    public String toString()
    {
        return name + ":" + type.getDisplayName();
    }
}
