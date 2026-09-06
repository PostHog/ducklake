package com.posthog.hoglake.trino;

import io.trino.parquet.ParquetCorruptionException;
import io.trino.parquet.ParquetDataSourceId;
import io.trino.parquet.reader.ParquetReader;
import io.trino.spi.Page;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.block.RunLengthEncodedBlock;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.metrics.Metrics;
import io.trino.spi.type.Type;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.List;
import java.util.OptionalLong;

import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static java.util.Objects.requireNonNull;

/**
 * Adapts ParquetReader pages to the requested column order, filling
 * catalog columns absent from the file (schema evolution: column added
 * after the file was written) with null blocks. The pattern is
 * trino-hive's ParquetPageSource, trimmed to the two adaptations this
 * connector needs.
 */
public class HoglakePageSource
        implements ConnectorPageSource
{
    /** Either "channel i of the reader page" or "all nulls of this type". */
    sealed interface ColumnAdaptation
            permits SourceColumn, NullColumn
    {
    }

    record SourceColumn(int sourceChannel)
            implements ColumnAdaptation {}

    record NullColumn(Type type)
            implements ColumnAdaptation {}

    private final ParquetReader parquetReader;
    private final List<ColumnAdaptation> columns;
    private final List<Block> nullBlocks;

    private boolean closed;
    private long completedPositions;

    public HoglakePageSource(ParquetReader parquetReader, List<ColumnAdaptation> columns)
    {
        this.parquetReader = requireNonNull(parquetReader, "parquetReader is null");
        this.columns = List.copyOf(columns);
        this.nullBlocks = new ArrayList<>(columns.size());
        for (ColumnAdaptation column : columns) {
            nullBlocks.add(column instanceof NullColumn(Type type)
                    ? type.createBlockBuilder(null, 1, 0).appendNull().build()
                    : null);
        }
    }

    @Override
    public Page getNextPage()
    {
        Page page;
        try {
            page = parquetReader.nextPage();
        }
        catch (IOException | RuntimeException e) {
            close();
            throw handleException(parquetReader.getDataSource().getId(), e);
        }
        if (page == null) {
            close();
            return null;
        }
        completedPositions += page.getPositionCount();
        return adapt(page);
    }

    private Page adapt(Page page)
    {
        Block[] blocks = new Block[columns.size()];
        for (int channel = 0; channel < columns.size(); channel++) {
            blocks[channel] = switch (columns.get(channel)) {
                case SourceColumn(int sourceChannel) -> page.getBlock(sourceChannel);
                case NullColumn ignored ->
                        RunLengthEncodedBlock.create(nullBlocks.get(channel), page.getPositionCount());
            };
        }
        return new Page(page.getPositionCount(), blocks);
    }

    @Override
    public long getCompletedBytes()
    {
        return parquetReader.getDataSource().getReadBytes();
    }

    @Override
    public OptionalLong getCompletedPositions()
    {
        return OptionalLong.of(completedPositions);
    }

    @Override
    public long getReadTimeNanos()
    {
        return parquetReader.getDataSource().getReadTimeNanos();
    }

    @Override
    public boolean isFinished()
    {
        return closed;
    }

    @Override
    public long getMemoryUsage()
    {
        return parquetReader.getMemoryContext().getBytes();
    }

    @Override
    public Metrics getMetrics()
    {
        return parquetReader.getMetrics();
    }

    @Override
    public void close()
    {
        if (closed) {
            return;
        }
        closed = true;
        try {
            parquetReader.close();
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    static TrinoException handleException(ParquetDataSourceId dataSourceId, Exception exception)
    {
        if (exception instanceof TrinoException trinoException) {
            return trinoException;
        }
        if (exception instanceof ParquetCorruptionException) {
            return new TrinoException(GENERIC_INTERNAL_ERROR, "Corrupt parquet data: " + dataSourceId, exception);
        }
        return new TrinoException(GENERIC_INTERNAL_ERROR, "Failed to read parquet file: " + dataSourceId, exception);
    }
}
