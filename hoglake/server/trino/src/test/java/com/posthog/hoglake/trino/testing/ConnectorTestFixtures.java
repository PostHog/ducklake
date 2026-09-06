package com.posthog.hoglake.trino.testing;

import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.filesystem.memory.MemoryFileSystemFactory;
import io.trino.parquet.writer.ParquetWriter;
import io.trino.parquet.writer.ParquetWriterOptions;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.security.ConnectorIdentity;
import io.trino.spi.type.TimeZoneKey;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;
import org.apache.parquet.format.CompressionCodec;
import org.apache.parquet.schema.MessageType;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;

/**
 * Unit-level fixtures for exercising the connector's parquet read path
 * without Docker: real parquet bytes written with trino-parquet's own
 * writer (which, unlike Hardwood, round-trips PARQUET:field_id from
 * MessageType ids — verified against MessageTypeConverter bytecode), an
 * in-memory Trino filesystem, and a minimal ConnectorSession.
 */
public final class ConnectorTestFixtures
{
    private ConnectorTestFixtures() {}

    /** A file column: parquet schema field + the Trino type the writer encodes it as + values (null = SQL null). */
    public record FileColumn(org.apache.parquet.schema.Type field, Type writeType, List<Object> values) {}

    public static ConnectorSession session()
    {
        return new ConnectorSession()
        {
            @Override
            public String getQueryId()
            {
                return "hoglake_test_query";
            }

            @Override
            public Optional<String> getSource()
            {
                return Optional.empty();
            }

            @Override
            public ConnectorIdentity getIdentity()
            {
                return ConnectorIdentity.ofUser("test");
            }

            @Override
            public TimeZoneKey getTimeZoneKey()
            {
                return TimeZoneKey.UTC_KEY;
            }

            @Override
            public Locale getLocale()
            {
                return Locale.ENGLISH;
            }

            @Override
            public Optional<String> getTraceToken()
            {
                return Optional.empty();
            }

            @Override
            public Instant getStart()
            {
                return Instant.EPOCH;
            }

            @Override
            public <T> T getProperty(String name, Class<T> type)
            {
                throw new UnsupportedOperationException("no session properties in tests");
            }
        };
    }

    /** Write a single-page parquet file with trino-parquet's writer. Field ids on the MessageType land in the footer. */
    public static byte[] writeParquet(List<FileColumn> columns)
    {
        MessageType schema = new MessageType("hoglake_test",
                columns.stream().map(FileColumn::field).toList());
        Map<List<String>, Type> primitiveTypes = new HashMap<>();
        for (FileColumn column : columns) {
            primitiveTypes.put(List.of(column.field().getName()), column.writeType());
        }
        int positions = columns.get(0).values().size();
        Block[] blocks = new Block[columns.size()];
        for (int i = 0; i < columns.size(); i++) {
            blocks[i] = buildBlock(columns.get(i).writeType(), columns.get(i).values());
        }
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        try (ParquetWriter writer = new ParquetWriter(
                out,
                schema,
                primitiveTypes,
                ParquetWriterOptions.builder().build(),
                CompressionCodec.UNCOMPRESSED,
                "hoglake-test",
                Optional.empty(),
                Optional.empty())) {
            writer.write(new Page(positions, blocks));
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return out.toByteArray();
    }

    private static Block buildBlock(Type type, List<Object> values)
    {
        BlockBuilder builder = type.createBlockBuilder(null, values.size());
        for (Object value : values) {
            if (value == null) {
                builder.appendNull();
            }
            else if (type.getJavaType() == long.class) {
                type.writeLong(builder, ((Number) value).longValue());
            }
            else if (type.getJavaType() == int.class) {
                type.writeLong(builder, ((Number) value).longValue());
            }
            else if (type.getJavaType() == double.class) {
                type.writeDouble(builder, ((Number) value).doubleValue());
            }
            else if (type.getJavaType() == boolean.class) {
                type.writeBoolean(builder, (Boolean) value);
            }
            else if (type.getJavaType() == Slice.class) {
                type.writeSlice(builder, Slices.utf8Slice((String) value));
            }
            else {
                throw new IllegalArgumentException("Unsupported test value type for " + type);
            }
        }
        return builder.build();
    }

    /** An in-memory filesystem factory preloaded with the given files (location -> bytes). */
    public static TrinoFileSystemFactory memoryFileSystem(Map<String, byte[]> files)
    {
        MemoryFileSystemFactory factory = new MemoryFileSystemFactory();
        try {
            for (Map.Entry<String, byte[]> file : files.entrySet()) {
                factory.create(ConnectorIdentity.ofUser("test"))
                        .newOutputFile(Location.of(file.getKey()))
                        .createOrOverwrite(file.getValue());
            }
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return factory;
    }

    /**
     * Drain a page source into row-major values (VARCHAR as String,
     * integral types as Long, null as null).
     */
    public static List<List<Object>> readAll(ConnectorPageSource pageSource, List<Type> types)
    {
        List<List<Object>> rows = new ArrayList<>();
        while (!pageSource.isFinished()) {
            Page page = pageSource.getNextPage();
            if (page == null) {
                break;
            }
            for (int position = 0; position < page.getPositionCount(); position++) {
                List<Object> row = new ArrayList<>();
                for (int channel = 0; channel < types.size(); channel++) {
                    row.add(cell(types.get(channel), page.getBlock(channel), position));
                }
                rows.add(row);
            }
        }
        return rows;
    }

    private static Object cell(Type type, Block block, int position)
    {
        if (block.isNull(position)) {
            return null;
        }
        if (type instanceof VarcharType varchar) {
            return varchar.getSlice(block, position).toStringUtf8();
        }
        if (type.getJavaType() == long.class) {
            return type.getLong(block, position);
        }
        if (type.getJavaType() == double.class) {
            return type.getDouble(block, position);
        }
        if (type.getJavaType() == boolean.class) {
            return type.getBoolean(block, position);
        }
        throw new IllegalArgumentException("Unsupported test read type: " + type);
    }
}
