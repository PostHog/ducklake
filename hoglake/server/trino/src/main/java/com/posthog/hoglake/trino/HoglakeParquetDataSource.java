package com.posthog.hoglake.trino;

import io.airlift.slice.Slice;
import io.trino.filesystem.TrinoInput;
import io.trino.filesystem.TrinoInputFile;
import io.trino.parquet.AbstractParquetDataSource;
import io.trino.parquet.ParquetDataSourceId;
import io.trino.parquet.ParquetReaderOptions;

import java.io.IOException;

/**
 * ParquetDataSource over a TrinoInputFile — the same shim every bundled
 * connector carries (trino-hive's TrinoParquetDataSource, minus its
 * stats plumbing). The known file length from the catalog rides in via
 * the input file, so no HEAD request is needed.
 */
public class HoglakeParquetDataSource
        extends AbstractParquetDataSource
{
    private final TrinoInput input;

    public HoglakeParquetDataSource(TrinoInputFile file, long fileSize, ParquetReaderOptions options)
            throws IOException
    {
        super(new ParquetDataSourceId(file.location().toString()), fileSize, options);
        this.input = file.newInput();
    }

    @Override
    public void close()
            throws IOException
    {
        input.close();
    }

    @Override
    protected Slice readTailInternal(int length)
            throws IOException
    {
        return input.readTail(length);
    }

    @Override
    protected void readInternal(long position, byte[] buffer, int bufferOffset, int bufferLength)
            throws IOException
    {
        input.readFully(position, buffer, bufferOffset, bufferLength);
    }
}
