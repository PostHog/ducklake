package com.posthog.hoglake.trino;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.trino.spi.HostAddress;
import io.trino.spi.connector.ConnectorSplit;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

/**
 * One split per data file from GET /scan. When the scan paired the file
 * with a live deletion vector, the split carries it — and the page
 * source refuses the query (DV application is not implemented in v1;
 * silently returning deleted rows is not an option).
 */
public record HoglakeSplit(
        @JsonProperty("path") String path,
        @JsonProperty("fileSizeBytes") long fileSizeBytes,
        @JsonProperty("recordCount") long recordCount,
        @JsonProperty("deleteFilePath") Optional<String> deleteFilePath,
        @JsonProperty("deleteCount") long deleteCount)
        implements ConnectorSplit
{
    @JsonCreator
    public HoglakeSplit
    {
        requireNonNull(path, "path is null");
        requireNonNull(deleteFilePath, "deleteFilePath is null");
    }

    /** Object-store data: any worker can read any split. */
    @Override
    public boolean isRemotelyAccessible()
    {
        return true;
    }

    @Override
    public List<HostAddress> getAddresses()
    {
        return List.of();
    }

    @Override
    public Map<String, String> getSplitInfo()
    {
        return Map.of(
                "path", path,
                "fileSizeBytes", String.valueOf(fileSizeBytes),
                "recordCount", String.valueOf(recordCount),
                "deleteFilePath", deleteFilePath.orElse(""));
    }
}
