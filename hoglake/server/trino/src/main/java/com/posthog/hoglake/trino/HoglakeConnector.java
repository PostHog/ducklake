package com.posthog.hoglake.trino;

import com.posthog.hoglake.trino.rest.HoglakeClient;
import io.trino.filesystem.s3.S3FileSystemFactory;
import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorPageSourceProvider;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplitManager;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.transaction.IsolationLevel;

import static java.util.Objects.requireNonNull;

/** Read-only hoglake connector: metadata + splits from the REST control plane, parquet from S3. */
public class HoglakeConnector
        implements Connector
{
    private final HoglakeMetadata metadata;
    private final HoglakeSplitManager splitManager;
    private final HoglakePageSourceProvider pageSourceProvider;
    private final S3FileSystemFactory fileSystemFactory;
    private final HoglakeClient client;

    public HoglakeConnector(
            HoglakeMetadata metadata,
            HoglakeSplitManager splitManager,
            HoglakePageSourceProvider pageSourceProvider,
            S3FileSystemFactory fileSystemFactory,
            HoglakeClient client)
    {
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.splitManager = requireNonNull(splitManager, "splitManager is null");
        this.pageSourceProvider = requireNonNull(pageSourceProvider, "pageSourceProvider is null");
        this.fileSystemFactory = requireNonNull(fileSystemFactory, "fileSystemFactory is null");
        this.client = requireNonNull(client, "client is null");
    }

    @Override
    public ConnectorTransactionHandle beginTransaction(
            IsolationLevel isolationLevel,
            boolean readOnly,
            boolean autoCommit)
    {
        return HoglakeTransactionHandle.INSTANCE;
    }

    @Override
    public ConnectorMetadata getMetadata(ConnectorSession session, ConnectorTransactionHandle transactionHandle)
    {
        return metadata;
    }

    @Override
    public ConnectorSplitManager getSplitManager()
    {
        return splitManager;
    }

    @Override
    public ConnectorPageSourceProvider getPageSourceProvider()
    {
        return pageSourceProvider;
    }

    @Override
    public void shutdown()
    {
        // Both hold threads: the S3 factory its transfer machinery, the
        // REST client the JDK HttpClient's selector thread + executor.
        client.close();
        fileSystemFactory.destroy();
    }
}
