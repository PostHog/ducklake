package com.posthog.hoglake.trino;

import com.posthog.hoglake.trino.rest.HoglakeClient;
import io.opentelemetry.api.OpenTelemetry;
import io.trino.filesystem.s3.S3FileSystemConfig;
import io.trino.filesystem.s3.S3FileSystemFactory;
import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorContext;
import io.trino.spi.connector.ConnectorFactory;

import java.util.Map;

public class HoglakeConnectorFactory
        implements ConnectorFactory
{
    @Override
    public String getName()
    {
        return "hoglake";
    }

    @Override
    public Connector create(String catalogName, Map<String, String> config, ConnectorContext context)
    {
        HoglakeConfig hoglakeConfig = HoglakeConfig.fromMap(config);

        HoglakeClient client = new HoglakeClient(
                hoglakeConfig.uri(), hoglakeConfig.catalog(), hoglakeConfig.requestTimeout());

        S3FileSystemConfig s3Config = new S3FileSystemConfig()
                .setRegion(hoglakeConfig.s3Region())
                .setEndpoint(hoglakeConfig.s3Endpoint())
                .setAwsAccessKey(hoglakeConfig.s3AccessKey())
                .setAwsSecretKey(hoglakeConfig.s3SecretKey())
                .setPathStyleAccess(hoglakeConfig.s3PathStyle());
        S3FileSystemFactory fileSystemFactory =
                new S3FileSystemFactory(OpenTelemetry.noop(), s3Config);

        return new HoglakeConnector(
                new HoglakeMetadata(client),
                new HoglakeSplitManager(client),
                new HoglakePageSourceProvider(fileSystemFactory),
                fileSystemFactory,
                client);
    }
}
