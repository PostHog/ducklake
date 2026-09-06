package com.posthog.hoglake.trino.rest;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.SchemaNotFoundException;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.TableNotFoundException;

import java.io.Closeable;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.List;
import java.util.Optional;

import static com.posthog.hoglake.trino.HoglakeErrorCode.HOGLAKE_CATALOG_NOT_FOUND;
import static com.posthog.hoglake.trino.HoglakeErrorCode.HOGLAKE_CATALOG_UNAVAILABLE;
import static com.posthog.hoglake.trino.HoglakeErrorCode.HOGLAKE_INVALID_RESPONSE;
import static com.posthog.hoglake.trino.HoglakeErrorCode.HOGLAKE_SNAPSHOT_EXPIRED;
import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;

/**
 * Minimal REST client for the hoglake control plane (/v1). Metadata
 * only: the connector reads parquet itself; the catalog service never
 * serves data.
 *
 * <p>Error taxonomy (see {@link com.posthog.hoglake.trino.HoglakeErrorCode}):
 * network/5xx failures are EXTERNAL, 410 is the typed snapshot-expired
 * signal, 404s become the engine's typed not-found exceptions at the
 * call sites, and malformed bodies are coded — nothing escapes without
 * a Trino error code.
 */
public class HoglakeClient
        implements Closeable
{
    public static final Duration DEFAULT_REQUEST_TIMEOUT = Duration.ofMinutes(2);

    private final HttpClient httpClient;
    private final ObjectMapper mapper;
    private final String baseUri;
    private final String catalog;
    private final Duration requestTimeout;

    public HoglakeClient(String baseUri, String catalog)
    {
        this(baseUri, catalog, DEFAULT_REQUEST_TIMEOUT);
    }

    public HoglakeClient(String baseUri, String catalog, Duration requestTimeout)
    {
        // Fail at construction (catalog registration), not per query.
        this.baseUri = validateBaseUri(requireNonNull(baseUri, "baseUri is null")).toString();
        this.catalog = requireNonNull(catalog, "catalog is null");
        this.requestTimeout = requireNonNull(requestTimeout, "requestTimeout is null");
        this.httpClient = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(10))
                .build();
        this.mapper = new ObjectMapper()
                .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);
    }

    /**
     * Validates a control-plane base URI: parseable, http(s) scheme, a
     * real host (a malformed port like "http://h:notaport" parses to a
     * null host and is rejected here instead of exploding at
     * request-build time on the first query). Trailing slashes are
     * collapsed.
     */
    public static URI validateBaseUri(String uri)
    {
        String trimmed = uri.replaceAll("/+$", "");
        URI parsed;
        try {
            parsed = new URI(trimmed);
        }
        catch (URISyntaxException e) {
            throw new IllegalArgumentException(
                    "Invalid hoglake.uri '" + uri + "': " + e.getMessage(), e);
        }
        String scheme = parsed.getScheme();
        if (!"http".equals(scheme) && !"https".equals(scheme)) {
            throw new IllegalArgumentException(
                    "Invalid hoglake.uri '" + uri + "': scheme must be http or https");
        }
        if (parsed.getHost() == null) {
            throw new IllegalArgumentException(
                    "Invalid hoglake.uri '" + uri + "': missing or unparseable host/port");
        }
        return parsed;
    }

    /** Catalog identity + head snapshot: the query's snapshot pin source. */
    public HoglakeDtos.Catalog getCatalog()
    {
        return get(catalogPath(""), new TypeReference<HoglakeDtos.Catalog>() {})
                .orElseThrow(this::catalogNotFound);
    }

    public List<HoglakeDtos.Namespace> listNamespaces()
    {
        return get(catalogPath("/namespaces"), new TypeReference<List<HoglakeDtos.Namespace>>() {})
                .orElseThrow(this::catalogNotFound);
    }

    public List<HoglakeDtos.TableSummary> listTables(String namespace)
    {
        return get(namespacePath(namespace, "/tables"), new TypeReference<List<HoglakeDtos.TableSummary>>() {})
                .orElseThrow(() -> new SchemaNotFoundException(namespace));
    }

    /** The table at head (listing surfaces; queries pin via the snapshot overload). */
    public Optional<HoglakeDtos.Table> getTable(String namespace, String table)
    {
        return get(tablePath(namespace, table, ""), new TypeReference<HoglakeDtos.Table>() {});
    }

    /** The table at a pinned snapshot. */
    public Optional<HoglakeDtos.Table> getTable(String namespace, String table, long snapshot)
    {
        return get(tablePath(namespace, table, "?snapshot=" + snapshot),
                new TypeReference<HoglakeDtos.Table>() {});
    }

    /** The split source: data files paired with live DVs at the pinned snapshot. */
    public List<HoglakeDtos.ScanFile> scan(String namespace, String table, long snapshot)
    {
        return get(tablePath(namespace, table, "/scan?snapshot=" + snapshot),
                new TypeReference<List<HoglakeDtos.ScanFile>>() {})
                .orElseThrow(() -> new TableNotFoundException(new SchemaTableName(namespace, table)));
    }

    @Override
    public void close()
    {
        // Release the selector thread and executor without waiting on
        // in-flight requests (connector shutdown must not hang on a
        // stuck control-plane call).
        httpClient.shutdownNow();
    }

    // ---- transport ---------------------------------------------------------

    /** GET + parse; empty on 404, coded TrinoException on every other failure. */
    private <T> Optional<T> get(String path, TypeReference<T> type)
    {
        URI uri = URI.create(baseUri + path);
        HttpRequest request = HttpRequest.newBuilder(uri)
                .timeout(requestTimeout)
                .header("Accept", "application/json")
                .GET()
                .build();
        HttpResponse<String> response;
        try {
            response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
        }
        catch (IOException e) {
            // Connection refused, DNS, timeouts: the control plane (or the
            // network to it) failed — an EXTERNAL fault, not an engine bug.
            throw new TrinoException(HOGLAKE_CATALOG_UNAVAILABLE,
                    "hoglake request failed: GET " + uri, e);
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new TrinoException(GENERIC_INTERNAL_ERROR, "hoglake request interrupted: GET " + uri, e);
        }
        int status = response.statusCode();
        if (status == 404) {
            return Optional.empty();
        }
        if (status == 410) {
            // The catalog's typed expiry signal: the requested snapshot fell
            // below the expiry floor. With pinned-snapshot planning this
            // means "your read snapshot expired during the query".
            throw new TrinoException(HOGLAKE_SNAPSHOT_EXPIRED,
                    "hoglake snapshot expired during query (history below the expiry floor): GET "
                            + uri + " -> HTTP 410: " + response.body());
        }
        if (status >= 500) {
            throw new TrinoException(HOGLAKE_CATALOG_UNAVAILABLE,
                    "hoglake catalog service error: GET " + uri + " -> HTTP " + status + ": " + response.body());
        }
        if (status != 200) {
            throw new TrinoException(HOGLAKE_INVALID_RESPONSE,
                    "hoglake request failed: GET " + uri + " -> HTTP " + status + ": " + response.body());
        }
        try {
            return Optional.of(mapper.readValue(response.body(), type));
        }
        catch (IOException e) {
            throw new TrinoException(HOGLAKE_INVALID_RESPONSE,
                    "Malformed hoglake response from GET " + uri, e);
        }
    }

    private TrinoException catalogNotFound()
    {
        return new TrinoException(HOGLAKE_CATALOG_NOT_FOUND,
                "hoglake catalog '" + catalog + "' not found (check the hoglake.catalog property)");
    }

    private String catalogPath(String suffix)
    {
        return "/v1/catalogs/" + encode(catalog) + suffix;
    }

    private String namespacePath(String namespace, String suffix)
    {
        return catalogPath("/namespaces/" + encode(namespace) + suffix);
    }

    private String tablePath(String namespace, String table, String suffix)
    {
        return namespacePath(namespace, "/tables/" + encode(table) + suffix);
    }

    private static String encode(String segment)
    {
        return URLEncoder.encode(segment, UTF_8).replace("+", "%20");
    }
}
