package com.posthog.hoglake.trino;

import com.posthog.hoglake.trino.rest.HoglakeClient;
import com.posthog.hoglake.trino.rest.HoglakeDtos;
import com.sun.net.httpserver.HttpServer;
import io.trino.spi.TrinoException;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * REST-client parsing against openapi-shaped fixtures (snake_case wire,
 * exactly the response shapes of openapi/hoglake.yaml), served from an
 * in-JVM HttpServer.
 */
class HoglakeClientTest
{
    private static final Map<String, String> RESPONSES = new HashMap<>();
    private static HttpServer server;
    private static HoglakeClient client;

    @BeforeAll
    static void startServer()
            throws IOException
    {
        RESPONSES.put("/v1/catalogs/lake",
                """
                {"name": "lake", "data_path": "s3://lake/", "head_snapshot_id": 7, "schema_version": 3}
                """);
        RESPONSES.put("/v1/catalogs/lake/namespaces",
                """
                [{"name": "analytics"}, {"name": "raw"}]
                """);
        RESPONSES.put("/v1/catalogs/lake/namespaces/analytics/tables",
                """
                [{"name": "events", "table_uuid": "0193c2b1-0000-7000-8000-000000000001"}]
                """);
        // A Table with every column shape: plain, decimal params, explicit
        // non-nullable, plus fields the client does not model (partition_spec).
        RESPONSES.put("/v1/catalogs/lake/namespaces/analytics/tables/events",
                """
                {
                  "name": "events",
                  "namespace": "analytics",
                  "table_uuid": "0193c2b1-0000-7000-8000-000000000001",
                  "columns": [
                    {"field_id": 1, "ordinal": 0, "name": "id", "type": "long", "nullable": false},
                    {"field_id": 2, "ordinal": 1, "name": "amount", "type": "decimal",
                     "type_params": {"precision": 10, "scale": 2}},
                    {"field_id": 3, "ordinal": 2, "name": "name", "type": "string"}
                  ],
                  "record_count": 42,
                  "file_count": 2,
                  "file_size_bytes": 4096,
                  "partition_spec": {"spec_id": 1, "fields": [{"source_field_id": 1, "transform": "identity"}]}
                }
                """);
        // /scan: one bare file, one file paired with a deletion vector.
        RESPONSES.put("/v1/catalogs/lake/namespaces/analytics/tables/events/scan",
                """
                [
                  {
                    "data_file": {
                      "data_file_id": 10, "path": "s3://lake/events/a.parquet",
                      "file_format": "parquet", "record_count": 25,
                      "file_size_bytes": 2048, "footer_size": 321,
                      "row_id_start": 0, "stats_state": "provided", "begin_snapshot": 3
                    }
                  },
                  {
                    "data_file": {
                      "data_file_id": 11, "path": "s3://lake/events/b.parquet",
                      "file_format": "parquet", "record_count": 17,
                      "file_size_bytes": 1024, "row_id_start": 25,
                      "stats_state": "pending", "begin_snapshot": 4
                    },
                    "delete_file": {
                      "delete_file_id": 5, "data_file_id": 11,
                      "path": "s3://lake/events/b.dv", "file_format": "puffin-dv",
                      "delete_count": 3, "file_size_bytes": 64, "begin_snapshot": 6
                    }
                  }
                ]
                """);

        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/", exchange -> {
            String body = RESPONSES.get(exchange.getRequestURI().getPath());
            byte[] payload = (body == null ? "{\"error\":\"not_found\"}" : body).getBytes(UTF_8);
            exchange.getResponseHeaders().set("Content-Type", "application/json");
            exchange.sendResponseHeaders(body == null ? 404 : 200, payload.length);
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

    @Test
    void parsesCatalogInfoWithHeadSnapshot()
    {
        HoglakeDtos.Catalog catalog = client.getCatalog();
        assertThat(catalog.name()).isEqualTo("lake");
        assertThat(catalog.headSnapshotId()).isEqualTo(7);
    }

    @Test
    void parsesNamespaces()
    {
        assertThat(client.listNamespaces())
                .extracting(HoglakeDtos.Namespace::name)
                .containsExactly("analytics", "raw");
    }

    @Test
    void parsesTableSummaries()
    {
        assertThat(client.listTables("analytics"))
                .extracting(HoglakeDtos.TableSummary::name)
                .containsExactly("events");
    }

    @Test
    void unknownNamespaceIsSchemaNotFound()
    {
        assertThatThrownBy(() -> client.listTables("nope"))
                .isInstanceOf(io.trino.spi.connector.SchemaNotFoundException.class)
                .hasMessageContaining("nope");
    }

    @Test
    void parsesTableWithColumnsAndDecimalParams()
    {
        HoglakeDtos.Table table = client.getTable("analytics", "events").orElseThrow();
        assertThat(table.namespace()).isEqualTo("analytics");
        assertThat(table.recordCount()).isEqualTo(42);
        assertThat(table.columns()).hasSize(3);

        HoglakeDtos.Column id = table.columns().get(0);
        assertThat(id.fieldId()).isEqualTo(1);
        assertThat(id.type()).isEqualTo("long");
        assertThat(id.isNullable()).isFalse();

        HoglakeDtos.Column amount = table.columns().get(1);
        assertThat(amount.type()).isEqualTo("decimal");
        assertThat(amount.typeParams()).containsEntry("precision", 10).containsEntry("scale", 2);
        assertThat(amount.isNullable()).isTrue();

        // Omitted nullable defaults to true (openapi: default true).
        assertThat(table.columns().get(2).isNullable()).isTrue();
    }

    @Test
    void missingTableIsEmpty()
    {
        assertThat(client.getTable("analytics", "nope")).isEmpty();
    }

    @Test
    void missingCatalogFailsLoudly()
    {
        HoglakeClient wrongCatalog =
                new HoglakeClient("http://127.0.0.1:" + server.getAddress().getPort(), "puddle");
        assertThatThrownBy(wrongCatalog::listNamespaces)
                .isInstanceOf(TrinoException.class)
                .hasMessageContaining("puddle");
    }

    @Test
    void parsesScanWithDataAndDeleteFilePairing()
    {
        List<HoglakeDtos.ScanFile> scan = client.scan("analytics", "events", 6);
        assertThat(scan).hasSize(2);

        HoglakeDtos.ScanFile bare = scan.get(0);
        assertThat(bare.dataFile().path()).isEqualTo("s3://lake/events/a.parquet");
        assertThat(bare.dataFile().recordCount()).isEqualTo(25);
        assertThat(bare.dataFile().fileSizeBytes()).isEqualTo(2048);
        assertThat(bare.deleteFile()).isNull();

        HoglakeDtos.ScanFile paired = scan.get(1);
        assertThat(paired.dataFile().statsState()).isEqualTo("pending");
        assertThat(paired.deleteFile()).isNotNull();
        assertThat(paired.deleteFile().path()).isEqualTo("s3://lake/events/b.dv");
        assertThat(paired.deleteFile().deleteCount()).isEqualTo(3);
    }

    @Test
    void scanOfMissingTableFailsLoudly()
    {
        assertThatThrownBy(() -> client.scan("analytics", "nope", 6))
                .isInstanceOf(io.trino.spi.connector.TableNotFoundException.class)
                .hasMessageContaining("analytics.nope");
    }
}
