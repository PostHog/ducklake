package com.posthog.hoglake.trino;

import com.posthog.hoglake.trino.rest.HoglakeClient;

import java.time.Duration;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static java.util.Objects.requireNonNull;

/**
 * Connector configuration, parsed from the catalog properties file
 * (etc/catalog/hoglake.properties). Deliberately hand-rolled instead of
 * airlift bootstrap: the surface is eight keys. All validation happens
 * here, at catalog load — a misconfigured catalog fails server startup,
 * not every query.
 *
 * <pre>
 * connector.name=hoglake
 * hoglake.uri=http://hoglake:8080          # REST base (no /v1 suffix)
 * hoglake.catalog=lake                      # hoglake catalog to expose
 * hoglake.client.request-timeout=2m         # optional; airlift-style duration (500ms, 30s, 2m, 1h)
 * hoglake.s3.endpoint=http://minio:9000     # optional; empty = AWS default
 * hoglake.s3.region=us-east-1
 * hoglake.s3.access-key=...
 * hoglake.s3.secret-key=...
 * hoglake.s3.path-style=true
 * </pre>
 */
public record HoglakeConfig(
        String uri,
        String catalog,
        Duration requestTimeout,
        String s3Endpoint,
        String s3Region,
        String s3AccessKey,
        String s3SecretKey,
        boolean s3PathStyle)
{
    private static final Set<String> KNOWN_KEYS = Set.of(
            "hoglake.uri",
            "hoglake.catalog",
            "hoglake.client.request-timeout",
            "hoglake.s3.endpoint",
            "hoglake.s3.region",
            "hoglake.s3.access-key",
            "hoglake.s3.secret-key",
            "hoglake.s3.path-style");

    private static final Pattern DURATION = Pattern.compile("\\s*(\\d+(?:\\.\\d+)?)\\s*(ms|s|m|h|d)\\s*");

    public HoglakeConfig
    {
        requireNonNull(uri, "uri is null");
        requireNonNull(catalog, "catalog is null");
        requireNonNull(requestTimeout, "requestTimeout is null");
    }

    public static HoglakeConfig fromMap(Map<String, String> config)
    {
        for (String key : config.keySet()) {
            if (key.startsWith("hoglake.") && !KNOWN_KEYS.contains(key)) {
                throw new IllegalArgumentException("Unknown hoglake configuration property: " + key);
            }
        }
        // Validate + normalize at load: scheme/host checked, trailing
        // slashes collapsed (so "...//" cannot produce "...//v1" paths).
        String uri = HoglakeClient.validateBaseUri(required(config, "hoglake.uri")).toString();
        String catalog = config.get("hoglake.catalog");
        if (catalog == null) {
            catalog = "hoglake";
        }
        else if (catalog.isBlank()) {
            // Present-but-empty is a typo, not a request for the default:
            // it would silently target /v1/catalogs//... — reject it.
            throw new IllegalArgumentException("hoglake.catalog must not be empty");
        }
        return new HoglakeConfig(
                uri,
                catalog,
                parseDuration(config.getOrDefault("hoglake.client.request-timeout", "2m")),
                emptyToNull(config.get("hoglake.s3.endpoint")),
                config.getOrDefault("hoglake.s3.region", "us-east-1"),
                emptyToNull(config.get("hoglake.s3.access-key")),
                emptyToNull(config.get("hoglake.s3.secret-key")),
                Boolean.parseBoolean(config.getOrDefault("hoglake.s3.path-style", "false")));
    }

    /** Airlift-style duration: a positive decimal plus ms/s/m/h/d. */
    private static Duration parseDuration(String value)
    {
        Matcher matcher = DURATION.matcher(value);
        if (!matcher.matches()) {
            throw new IllegalArgumentException(
                    "Invalid hoglake.client.request-timeout '" + value + "' (expected e.g. 500ms, 30s, 2m, 1h)");
        }
        double amount = Double.parseDouble(matcher.group(1));
        double millisPerUnit = switch (matcher.group(2)) {
            case "ms" -> 1;
            case "s" -> 1_000;
            case "m" -> 60_000;
            case "h" -> 3_600_000;
            case "d" -> 86_400_000;
            default -> throw new IllegalStateException("unreachable");
        };
        long millis = Math.round(amount * millisPerUnit);
        if (millis <= 0) {
            throw new IllegalArgumentException(
                    "Invalid hoglake.client.request-timeout '" + value + "': must be positive");
        }
        return Duration.ofMillis(millis);
    }

    private static String required(Map<String, String> config, String key)
    {
        String value = config.get(key);
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException("Missing required configuration property: " + key);
        }
        return value;
    }

    private static String emptyToNull(String value)
    {
        return (value == null || value.isBlank()) ? null : value;
    }
}
