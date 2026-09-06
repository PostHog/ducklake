package com.posthog.hoglake.trino;

import io.trino.spi.connector.ConnectorTransactionHandle;

/** Read-only connector: transactions carry no state. */
public enum HoglakeTransactionHandle
        implements ConnectorTransactionHandle
{
    INSTANCE
}
