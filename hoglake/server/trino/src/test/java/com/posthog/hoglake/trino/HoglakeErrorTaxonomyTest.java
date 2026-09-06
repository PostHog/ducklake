package com.posthog.hoglake.trino;

import com.posthog.hoglake.trino.rest.HoglakeClient;
import com.sun.net.httpserver.HttpServer;
import io.trino.spi.ErrorType;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.SchemaNotFoundException;
import io.trino.spi.connector.TableNotFoundException;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static com.posthog.hoglake.trino.HoglakeErrorCode.HOGLAKE_CATALOG_NOT_FOUND;
import static com.posthog.hoglake.trino.HoglakeErrorCode.HOGLAKE_CATALOG_UNAVAILABLE;
import static com.posthog.hoglake.trino.HoglakeErrorCode.HOGLAKE_INVALID_RESPONSE;
import static com.posthog.hoglake.trino.HoglakeErrorCode.HOGLAKE_SNAPSHOT_EXPIRED;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The connector's REST error taxonomy (regressions for the S3 findings;
 * these used to all collapse into GENERIC_INTERNAL_ERROR or escape as
 * non-Trino exceptions):
 *
 * <ul>
 * <li>unreachable control plane / 5xx -> HOGLAKE_CATALOG_UNAVAILABLE (EXTERNAL)</li>
 * <li>410 Gone -> HOGLAKE_SNAPSHOT_EXPIRED (the typed expiry signal)</li>
 * <li>malformed 200 body -> HOGLAKE_INVALID_RESPONSE, never a bare
 *     UncheckedIOException</li>
 * <li>configured catalog missing -> HOGLAKE_CATALOG_NOT_FOUND (USER_ERROR)</li>
 * <li>table/schema 404 at execution -> the engine's typed not-found
 *     exceptions</li>
 * <li>malformed base URI -> rejected at client construction (catalog
 *     load), not per query</li>
 * </ul>
 */
class HoglakeErrorTaxonomyTest
{
    private record CannedResponse(int status, String body) {}

    private static final ConcurrentMap<String, CannedResponse> RESPONSES = new ConcurrentHashMap<>();
    private static HttpServer server;
    private static HoglakeClient client;

    private static final String SCAN_PATH = "/v1/catalogs/lake/namespaces/analytics/tables/events/scan";

    @BeforeAll
    static void startServer()
            throws IOException
    {
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/", exchange -> {
            CannedResponse response = RESPONSES.getOrDefault(
                    exchange.getRequestURI().getPath(),
                    new CannedResponse(404, "{\"error\":\"not_found\"}"));
            byte[] payload = response.body().getBytes(UTF_8);
            exchange.getResponseHeaders().set("Content-Type", "application/json");
            exchange.sendResponseHeaders(response.status(), payload.length);
            try (OutputStream out = exchange.getResponseBody()) {
                out.write(payload);
            }
        });
        server.start();
        client = new HoglakeClient("http://127.0.0.1:" + server.getAddress().getPort(), "lake");
    }

    @AfterAll
    static void stopServer()
    {
        server.stop(0);
    }

    @BeforeEach
    void reset()
    {
        RESPONSES.clear();
    }

    @Test
    void serverError500_surfacesAsExternalCatalogUnavailable()
    {
        RESPONSES.put(SCAN_PATH, new CannedResponse(500, "{\"error\":\"boom\"}"));

        // A control-plane 5xx is the catalog service failing, not the
        // engine: EXTERNAL, so operators and ErrorType-keyed retry policies
        // classify it correctly.
        assertThatThrownBy(() -> client.scan("analytics", "events", 3))
                .isInstanceOfSatisfying(TrinoException.class, e -> {
                    assertThat(e.getErrorCode()).isEqualTo(HOGLAKE_CATALOG_UNAVAILABLE.toErrorCode());
                    assertThat(e.getErrorCode().getType()).isEqualTo(ErrorType.EXTERNAL);
                })
                .hasMessageContaining("HTTP 500");
    }

    @Test
    void gone410_surfacesAsTypedSnapshotExpired()
    {
        // 410 is the catalog's typed "below the expiry floor" signal (the
        // AGENT.md invariant: consumers reconcile, never silently skip).
        // With pinned-snapshot planning it means "your read snapshot
        // expired mid-query" — its own user-visible error.
        RESPONSES.put(SCAN_PATH, new CannedResponse(410, "{\"error\":\"gone\"}"));

        assertThatThrownBy(() -> client.scan("analytics", "events", 3))
                .isInstanceOfSatisfying(TrinoException.class, e ->
                        assertThat(e.getErrorCode()).isEqualTo(HOGLAKE_SNAPSHOT_EXPIRED.toErrorCode()))
                .hasMessageContaining("snapshot expired")
                .hasMessageContaining("HTTP 410");
    }

    @Test
    void malformedResponseBody_surfacesAsCodedTrinoException()
    {
        RESPONSES.put(SCAN_PATH, new CannedResponse(200, "this is not json"));

        // Used to escape as a bare UncheckedIOException with no error code.
        assertThatThrownBy(() -> client.scan("analytics", "events", 3))
                .isInstanceOfSatisfying(TrinoException.class, e ->
                        assertThat(e.getErrorCode()).isEqualTo(HOGLAKE_INVALID_RESPONSE.toErrorCode()))
                .hasMessageContaining("Malformed hoglake response");
    }

    @Test
    void missingCatalog404_surfacesAsUserFacingCatalogNotFound()
    {
        // No paths registered: /namespaces 404s, i.e. the configured
        // hoglake catalog does not exist — a configuration/user error.
        assertThatThrownBy(() -> client.listNamespaces())
                .isInstanceOfSatisfying(TrinoException.class, e -> {
                    assertThat(e.getErrorCode()).isEqualTo(HOGLAKE_CATALOG_NOT_FOUND.toErrorCode());
                    assertThat(e.getErrorCode().getType()).isEqualTo(ErrorType.USER_ERROR);
                })
                .hasMessageContaining("lake");
    }

    @Test
    void missingTable404OnScan_surfacesAsTableNotFound()
    {
        // Table vanishing at execution is not an engine bug: typed
        // TABLE_NOT_FOUND, message naming the table.
        assertThatThrownBy(() -> client.scan("analytics", "events", 3))
                .isInstanceOf(TableNotFoundException.class)
                .hasMessageContaining("analytics.events");
    }

    @Test
    void missingNamespaceOnListTables_surfacesAsSchemaNotFound()
    {
        // SHOW TABLES on a nonexistent schema: typed SCHEMA_NOT_FOUND.
        assertThatThrownBy(() -> client.listTables("nope"))
                .isInstanceOf(SchemaNotFoundException.class)
                .hasMessageContaining("nope");
    }

    @Test
    void unreachableControlPlane_surfacesAsExternalCatalogUnavailable()
            throws IOException
    {
        // Bind-then-close to get a port that refuses connections.
        int port;
        try (java.net.ServerSocket socket = new java.net.ServerSocket(0, 1, java.net.InetAddress.getLoopbackAddress())) {
            port = socket.getLocalPort();
        }
        HoglakeClient unreachable = new HoglakeClient("http://127.0.0.1:" + port, "lake");

        assertThatThrownBy(unreachable::listNamespaces)
                .isInstanceOfSatisfying(TrinoException.class, e -> {
                    assertThat(e.getErrorCode()).isEqualTo(HOGLAKE_CATALOG_UNAVAILABLE.toErrorCode());
                    assertThat(e.getErrorCode().getType()).isEqualTo(ErrorType.EXTERNAL);
                })
                .hasMessageContaining("request failed");
    }

    @Test
    void malformedUri_isRejectedAtClientConstructionNotPerQuery()
    {
        // Regression (S7): a typo'd URI used to be accepted silently and
        // fail every query at request-build time with a non-Trino
        // exception; now catalog registration fails fast.
        assertThatThrownBy(() -> new HoglakeClient("http://127.0.0.1:notaport", "lake"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("hoglake.uri");
        assertThatThrownBy(() -> new HoglakeClient("not a uri at all", "lake"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("hoglake.uri");
    }
}
