package com.posthog.hoglake.trino;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.posthog.hoglake.trino.testing.TestHoglakeServer;
import com.posthog.hoglake.trino.testing.TestParquet;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.BindMode;
import org.testcontainers.containers.MinIOContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.containers.TrinoContainer;
import org.testcontainers.images.builder.Transferable;
import org.testcontainers.utility.DockerImageName;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The milestone gate: Trino reads a hoglake table through the native
 * connector. Full stack — Postgres + the hoglake server in-process,
 * MinIO holding real parquet (written with Hardwood, the same writer the
 * root hydrator tests use), and a trinodb/trino:446 container with the
 * assembled plugin directory mounted, its catalog pointed back at the
 * host via host.testcontainers.internal.
 */
@Tag("integration")
class HoglakeTrinoIntegrationTest
{
    private static final String BUCKET = "hoglake-trino";
    private static final int ROWS = TestParquet.ROWS;
    private static final long EPOCH0 = TestParquet.EPOCH0;

    private static final ObjectMapper JSON = new ObjectMapper();
    private static final HttpClient HTTP = HttpClient.newHttpClient();

    private static PostgreSQLContainer<?> postgres;
    private static MinIOContainer minio;
    private static TrinoContainer trino;
    private static TestHoglakeServer server;

    @BeforeAll
    static void setUpStack()
            throws Exception
    {
        postgres = new PostgreSQLContainer<>("postgres:16")
                .withUsername("hoglake")
                .withPassword("hoglake");
        minio = new MinIOContainer("minio/minio:RELEASE.2023-09-04T19-57-37Z");
        postgres.start();
        minio.start();

        server = TestHoglakeServer.start(
                postgres.getJdbcUrl(), postgres.getUsername(), postgres.getPassword());

        byte[] parquet = TestParquet.writeSampleParquet();
        uploadParquet(parquet);
        seedCatalog(parquet.length);

        startTrino();
    }

    @AfterAll
    static void tearDownStack()
    {
        if (trino != null) {
            trino.stop();
        }
        if (server != null) {
            server.close();
        }
        if (minio != null) {
            minio.stop();
        }
        if (postgres != null) {
            postgres.stop();
        }
    }

    private static void uploadParquet(byte[] parquet)
    {
        try (S3Client s3 = s3Client()) {
            s3.createBucket(b -> b.bucket(BUCKET));
            s3.putObject(b -> b.bucket(BUCKET).key("events/part-0.parquet"),
                    RequestBody.fromBytes(parquet));
            s3.putObject(b -> b.bucket(BUCKET).key("deleted_events/part-0.parquet"),
                    RequestBody.fromBytes(parquet));
        }
    }

    private static S3Client s3Client()
    {
        return S3Client.builder()
                .endpointOverride(URI.create(minio.getS3URL()))
                .region(Region.US_EAST_1)
                .credentialsProvider(StaticCredentialsProvider.create(
                        AwsBasicCredentials.create(minio.getUserName(), minio.getPassword())))
                .forcePathStyle(true)
                .build();
    }

    private static void uploadObject(String key, byte[] bytes)
    {
        try (S3Client s3 = s3Client()) {
            s3.putObject(b -> b.bucket(BUCKET).key(key), RequestBody.fromBytes(bytes));
        }
    }

    /** Catalog + namespace + two tables via REST; commits with footer stats. */
    private static void seedCatalog(long fileSize)
            throws Exception
    {
        post("/v1/catalogs",
                "{\"name\": \"lake\", \"data_path\": \"s3://" + BUCKET + "/\"}", 201);
        post("/v1/catalogs/lake/namespaces", "{\"name\": \"analytics\"}", 201);

        createTable("events");
        createTable("deleted_events");

        commitAppend("events", "s3://" + BUCKET + "/events/part-0.parquet", fileSize);
        long snapshot = commitAppend(
                "deleted_events", "s3://" + BUCKET + "/deleted_events/part-0.parquet", fileSize);

        // Register a deletion vector against deleted_events' single data
        // file. The server never opens DV files, so the path is nominal —
        // the point is that /scan now pairs the data file with a DV and
        // the connector must refuse the read.
        long dataFileId = JSON.readTree(
                get("/v1/catalogs/lake/namespaces/analytics/tables/deleted_events/files"))
                .get(0).get("data_file_id").asLong();
        post("/v1/catalogs/lake/commit",
                """
                {
                  "read_snapshot": %d,
                  "deletes": [{
                    "namespace": "analytics", "table": "deleted_events",
                    "files": [{
                      "data_file_id": %d,
                      "path": "s3://%s/deleted_events/part-0.dv",
                      "delete_count": 3,
                      "file_size_bytes": 64
                    }]
                  }]
                }
                """.formatted(snapshot, dataFileId, BUCKET),
                200);
    }

    private static void createTable(String name)
            throws Exception
    {
        post("/v1/catalogs/lake/namespaces/analytics/tables",
                """
                {
                  "name": "%s",
                  "columns": [
                    {"name": "id", "type": "long", "nullable": false},
                    {"name": "score", "type": "double"},
                    {"name": "name", "type": "string"},
                    {"name": "ts", "type": "timestamptz"}
                  ]
                }
                """.formatted(name),
                201);
    }

    private static long commitAppend(String table, String path, long fileSize)
            throws Exception
    {
        // Footer-derived stats shipped with the commit (value/null counts;
        // bounds are optional and omitted).
        String body = """
                {
                  "appends": [{
                    "namespace": "analytics", "table": "%s",
                    "files": [{
                      "path": "%s",
                      "record_count": %d,
                      "file_size_bytes": %d,
                      "column_stats": [
                        {"field_id": 1, "value_count": 25, "null_count": 0},
                        {"field_id": 2, "value_count": 25, "null_count": 5},
                        {"field_id": 3, "value_count": 25, "null_count": 1},
                        {"field_id": 4, "value_count": 25, "null_count": 0}
                      ]
                    }]
                  }],
                  "author": "trino-integration-test"
                }
                """.formatted(table, path, ROWS, fileSize);
        JsonNode result = JSON.readTree(post("/v1/catalogs/lake/commit", body, 200));
        return result.get("snapshot_id").asLong();
    }

    private static void startTrino()
    {
        String pluginDir = System.getProperty("hoglake.trino.plugin.dir");
        assertThat(pluginDir).as("hoglake.trino.plugin.dir system property").isNotNull();

        int minioPort = minio.getMappedPort(9000);
        org.testcontainers.Testcontainers.exposeHostPorts(server.getPort(), minioPort);

        String catalogProperties = """
                connector.name=hoglake
                hoglake.uri=http://host.testcontainers.internal:%d
                hoglake.catalog=lake
                hoglake.s3.endpoint=http://host.testcontainers.internal:%d
                hoglake.s3.region=us-east-1
                hoglake.s3.access-key=%s
                hoglake.s3.secret-key=%s
                hoglake.s3.path-style=true
                """.formatted(server.getPort(), minioPort, minio.getUserName(), minio.getPassword());

        trino = new TrinoContainer(DockerImageName.parse("trinodb/trino:446"))
                .withFileSystemBind(pluginDir, "/usr/lib/trino/plugin/hoglake", BindMode.READ_ONLY)
                .withCopyToContainer(
                        Transferable.of(catalogProperties.getBytes(UTF_8)),
                        "/etc/trino/catalog/hoglake.properties")
                .withStartupTimeout(Duration.ofMinutes(5));
        trino.start();
        awaitCatalogSchedulable();
    }

    /**
     * "SERVER STARTED" precedes the catalog-to-node announcement by a few
     * seconds; until it lands, any distributed query fails with "No nodes
     * available to run query". Wait for the catalog to become schedulable
     * — any other failure is a real bug and surfaces immediately.
     */
    private static void awaitCatalogSchedulable()
    {
        long deadline = System.nanoTime() + Duration.ofMinutes(2).toNanos();
        while (true) {
            try {
                query("SELECT count(*) FROM hoglake.analytics.events");
                return;
            }
            catch (SQLException e) {
                if (!String.valueOf(e.getMessage()).contains("No nodes available")) {
                    throw new IllegalStateException("hoglake catalog failed its first read", e);
                }
                if (System.nanoTime() > deadline) {
                    throw new IllegalStateException("hoglake catalog never became schedulable", e);
                }
                try {
                    Thread.sleep(500);
                }
                catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                    throw new IllegalStateException(interrupted);
                }
            }
        }
    }

    // ---- REST plumbing -----------------------------------------------------

    private static String post(String path, String body, int expectedStatus)
            throws Exception
    {
        HttpResponse<String> response = HTTP.send(
                HttpRequest.newBuilder(URI.create(server.getBaseUri() + path))
                        .header("Content-Type", "application/json")
                        .POST(HttpRequest.BodyPublishers.ofString(body))
                        .build(),
                HttpResponse.BodyHandlers.ofString());
        assertThat(response.statusCode())
                .as("POST %s -> %s", path, response.body())
                .isEqualTo(expectedStatus);
        return response.body();
    }

    private static String get(String path)
            throws Exception
    {
        HttpResponse<String> response = HTTP.send(
                HttpRequest.newBuilder(URI.create(server.getBaseUri() + path)).GET().build(),
                HttpResponse.BodyHandlers.ofString());
        assertThat(response.statusCode()).as("GET %s -> %s", path, response.body()).isEqualTo(200);
        return response.body();
    }

    private static Connection connect()
            throws SQLException
    {
        return DriverManager.getConnection(trino.getJdbcUrl(), "test", null);
    }

    // ---- the assertions ----------------------------------------------------

    @Test
    void showSchemasSeesHoglakeNamespaces()
            throws Exception
    {
        assertThat(query("SHOW SCHEMAS FROM hoglake"))
                .extracting(row -> row.get("Schema"))
                .contains("analytics");
    }

    @Test
    void showTablesSeesHoglakeTables()
            throws Exception
    {
        assertThat(query("SHOW TABLES FROM hoglake.analytics"))
                .extracting(row -> row.get("Table"))
                .containsExactlyInAnyOrder("events", "deleted_events");
    }

    @Test
    void describeMapsTypes()
            throws Exception
    {
        List<Map<String, Object>> columns = query("DESCRIBE hoglake.analytics.events");
        assertThat(columns).extracting(row -> row.get("Column"))
                .containsExactly("id", "score", "name", "ts");
        assertThat(columns).extracting(row -> row.get("Type"))
                .containsExactly("bigint", "double", "varchar", "timestamp(6) with time zone");
    }

    @Test
    void countStar()
            throws Exception
    {
        assertThat(query("SELECT count(*) AS n FROM hoglake.analytics.events"))
                .singleElement()
                .satisfies(row -> assertThat(row.get("n")).isEqualTo(25L));
    }

    @Test
    void projectionAndRowValues()
            throws Exception
    {
        List<Map<String, Object>> rows = query("""
                SELECT id, name, score, to_unixtime(ts) AS epoch
                FROM hoglake.analytics.events
                ORDER BY id
                """);
        assertThat(rows).hasSize(ROWS);
        for (int i = 0; i < ROWS; i++) {
            Map<String, Object> row = rows.get(i);
            assertThat(row.get("id")).isEqualTo((long) i);
            assertThat(row.get("name")).isEqualTo(i == 13 ? null : String.format("row-%02d", i));
            assertThat(row.get("score")).isEqualTo(i % 5 == 0 ? null : i * 1.5);
            assertThat(row.get("epoch")).isEqualTo((double) (EPOCH0 + i));
        }
    }

    @Test
    void filteredProjection()
            throws Exception
    {
        // No pushdown in v1: Trino filters above the connector's scan.
        assertThat(query("SELECT name, score FROM hoglake.analytics.events WHERE id = 7"))
                .singleElement()
                .satisfies(row -> {
                    assertThat(row.get("name")).isEqualTo("row-07");
                    assertThat(row.get("score")).isEqualTo(10.5);
                });
        assertThat(query("SELECT count(*) AS n FROM hoglake.analytics.events WHERE score IS NULL"))
                .singleElement()
                .satisfies(row -> assertThat(row.get("n")).isEqualTo(5L));
    }

    /**
     * Schema evolution against id-less files, end to end. Every file the
     * hydrator (Hardwood) writes carries no PARQUET:field_id, so the
     * connector's name-fallback binding is what production reads use —
     * and a RENAME COLUMN breaks it silently.
     */
    @Test
    void renameColumnOnIdlessFilesSilentlyReadsNulls()
            throws Exception
    {
        createTable("renamed_events");
        byte[] parquet = TestParquet.writeSampleParquet();
        uploadObject("renamed_events/part-0.parquet", parquet);
        commitAppend("renamed_events",
                "s3://" + BUCKET + "/renamed_events/part-0.parquet", parquet.length);
        post("/v1/catalogs/lake/namespaces/analytics/tables/renamed_events/alter",
                "{\"ops\": [{\"op\": \"rename_column\", \"from\": \"name\", \"to\": \"title\"}]}",
                200);

        List<Map<String, Object>> rows = query(
                "SELECT id, title FROM hoglake.analytics.renamed_events ORDER BY id");

        // S2 (agreed connector behavior; see HoglakeParquetBindingTest for
        // the unit-level matrix): the data exists in the file under the old
        // name "name", but the file has no field ids, the name fallback
        // misses, and every row of the renamed column reads NULL. The fix
        // is catalog-side — field ids become a registration contract and
        // the server will refuse renames while id-less files are live —
        // the connector's id-authoritative binding stays as-is. Once the
        // server-side refusal lands, this rename will 4xx and this test
        // changes to assert that refusal.
        assertThat(rows).hasSize(ROWS);
        assertThat(rows).allSatisfy(row -> assertThat(row.get("title")).isNull());
    }

    /**
     * Type promotion against id-less files, end to end: an int column's
     * int32 files must read as bigint after PROMOTE COLUMN. This is the
     * legal widening direction; trino-parquet coerces at decode time.
     */
    @Test
    void promoteIntToLongReadsWidenedValues()
            throws Exception
    {
        post("/v1/catalogs/lake/namespaces/analytics/tables",
                """
                {
                  "name": "int_events",
                  "columns": [
                    {"name": "id", "type": "long", "nullable": false},
                    {"name": "n", "type": "int"}
                  ]
                }
                """,
                201);
        byte[] parquet = TestParquet.writeIntColumnParquet();
        uploadObject("int_events/part-0.parquet", parquet);
        post("/v1/catalogs/lake/commit",
                """
                {
                  "appends": [{
                    "namespace": "analytics", "table": "int_events",
                    "files": [{
                      "path": "s3://%s/int_events/part-0.parquet",
                      "record_count": %d,
                      "file_size_bytes": %d,
                      "column_stats": [
                        {"field_id": 1, "value_count": %d, "null_count": 0},
                        {"field_id": 2, "value_count": %d, "null_count": 0}
                      ]
                    }]
                  }],
                  "author": "trino-integration-test"
                }
                """.formatted(BUCKET, TestParquet.INT_ROWS, parquet.length,
                        TestParquet.INT_ROWS, TestParquet.INT_ROWS),
                200);
        post("/v1/catalogs/lake/namespaces/analytics/tables/int_events/alter",
                "{\"ops\": [{\"op\": \"promote_column\", \"name\": \"n\", \"to\": \"long\"}]}",
                200);

        List<Map<String, Object>> columns = query("DESCRIBE hoglake.analytics.int_events");
        assertThat(columns).extracting(row -> row.get("Column")).containsExactly("id", "n");
        assertThat(columns).extracting(row -> row.get("Type")).containsExactly("bigint", "bigint");

        List<Map<String, Object>> rows = query(
                "SELECT id, n FROM hoglake.analytics.int_events ORDER BY id");
        assertThat(rows).hasSize(TestParquet.INT_ROWS);
        for (int i = 0; i < TestParquet.INT_ROWS; i++) {
            assertThat(rows.get(i).get("n")).isEqualTo((long) (i * 10));
        }
    }

    @Test
    void tableWithDeletionVectorRefusesTheQuery()
    {
        assertThatThrownBy(() -> query("SELECT count(*) AS n FROM hoglake.analytics.deleted_events"))
                .isInstanceOf(SQLException.class)
                .hasMessageContaining(
                        "table has row-level deletes; DV application not yet implemented in the hoglake connector");
    }

    private static List<Map<String, Object>> query(String sql)
            throws SQLException
    {
        try (Connection connection = connect();
                Statement statement = connection.createStatement();
                ResultSet resultSet = statement.executeQuery(sql)) {
            List<Map<String, Object>> rows = new ArrayList<>();
            int columnCount = resultSet.getMetaData().getColumnCount();
            while (resultSet.next()) {
                Map<String, Object> row = new LinkedHashMap<>();
                for (int i = 1; i <= columnCount; i++) {
                    row.put(resultSet.getMetaData().getColumnLabel(i), resultSet.getObject(i));
                }
                rows.add(row);
            }
            return rows;
        }
    }
}
