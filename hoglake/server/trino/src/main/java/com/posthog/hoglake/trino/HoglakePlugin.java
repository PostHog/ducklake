package com.posthog.hoglake.trino;

import io.trino.spi.Plugin;
import io.trino.spi.connector.ConnectorFactory;

import java.util.List;

/** The hoglake Trino plugin (read-only connector, v1). */
public class HoglakePlugin
        implements Plugin
{
    @Override
    public Iterable<ConnectorFactory> getConnectorFactories()
    {
        return List.of(new HoglakeConnectorFactory());
    }
}
