package com.posthog.hoglake.trino;

import com.posthog.hoglake.trino.rest.HoglakeClient;
import com.posthog.hoglake.trino.testing.ConnectorTestFixtures;
import com.posthog.hoglake.trino.testing.ConnectorTestFixtures.FileColumn;
import com.sun.net.httpserver.HttpServer;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.spi.ErrorType;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplitSource;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.TableNotFoundException;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;

import static com.posthog.hoglake.trino.HoglakeErrorCode.HOGLAKE_SNAPSHOT_EXPIRED;
import static io.trino.spi.StandardErrorCode.NOT_FOUND;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.type.BigintType.BIGINT;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT64;
import static org.apache.parquet.schema.Types.optional;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * THE HEAD RACE, closed (S1 regressions). getTableHandle resolves the
 * catalog head ONCE and pins it: snapshot id, table_uuid, and the
 * resolved columns ride the handle; getColumnHandles/getTableMetadata
 * serve from the handle without re-fetching; getSplits scans at the
 * pinned snapshot. One query, one consistent snapshot — a concurrent
 * commit or DROP + CREATE between analysis and execution can no longer
 * rebind the query to new-incarnation data.
 *
 * <p>This test replays the engine's exact call sequence against a
 * mutable in-JVM control plane (snapshot-aware: responses are keyed by
 * path + query, mimicking the real server's versioned-row visibility)
 * and real parquet in a memory filesystem, with a concurrent commit
 * injected between analysis and execution — fully deterministic, no
 * timing.
 */
class HoglakeHeadRaceTest
{
    private static final String CATALOG_PATH = "/v1/catalogs/lake";
    private static final String TABLE_PATH = CATALOG_PATH + "/namespaces/analytics/tables/metrics";
    private static final String SCAN_PATH = TABLE_PATH + "/scan";
    private static final String V1_FILE = "memory:///metrics-v1.parquet";
    private static final String V2_FILE = "memory:///metrics-v2.parquet";

    private record CannedResponse(int status, String body) {}

    private static final ConcurrentMap<String, CannedResponse> RESPONSES = new ConcurrentHashMap<>();
    private static HttpServer server;
    private static HoglakeClient client;
    private static byte[] v1Parquet;
    private static byte[] v2Parquet;
    private static TrinoFileSystemFactory fileSystem;

    private final ConnectorSession session = ConnectorTestFixtures.session();
    private final HoglakeMetadata metadata = new HoglakeMetadata(client);
    private final HoglakeSplitManager splitManager = new HoglakeSplitManager(client);
    private final HoglakePageSourceProvider pageSourceProvider = new HoglakePageSourceProvider(fileSystem);

    @BeforeAll
    static void startControlPlane()
            throws IOException
    {
        // Incarnation 1: metrics(total bigint) with one file [100, 200].
        v1Parquet = ConnectorTestFixtures.writeParquet(List.of(new FileColumn(
                optional(INT64).id(1).named("total"), BIGINT, Arrays.asList(100L, 200L))));
        // Incarnation 2 (after DROP + CREATE): metrics(user_id bigint),
        // one file [7, 8, 9]. Field ids restart at 1, as they do for a
        // recreated hoglake table.
        v2Parquet = ConnectorTestFixtures.writeParquet(List.of(new FileColumn(
                optional(INT64).id(1).named("user_id"), BIGINT, Arrays.asList(7L, 8L, 9L))));
        fileSystem = ConnectorTestFixtures.memoryFileSystem(Map.of(
                V1_FILE, v1Parquet,
                V2_FILE, v2Parquet));

        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/", exchange -> {
            // Snapshot-aware routing: the query string is part of the key,
            // exactly like the real server's ?snapshot= time travel.
            String key = exchange.getRequestURI().getPath()
                    + (exchange.getRequestURI().getQuery() == null
                            ? ""
                            : "?" + exchange.getRequestURI().getQuery());
            CannedResponse response = RESPONSES.getOrDefault(
                    key, new CannedResponse(404, "{\"error\":\"not_found\"}"));
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
    static void stopControlPlane()
    {
        server.stop(0);
    }

    @BeforeEach
    void resetToIncarnation1()
    {
        RESPONSES.clear();
        // Head at snapshot 4, incarnation 1 visible at it.
        RESPONSES.put(CATALOG_PATH, ok(catalogJson(4)));
        RESPONSES.put(TABLE_PATH + "?snapshot=4", ok(tableJson("uuid-incarnation-1", "total")));
        RESPONSES.put(SCAN_PATH + "?snapshot=4", ok(scanJson(V1_FILE, v1Parquet.length, 2)));
    }

    /**
     * DROP TABLE metrics; CREATE TABLE metrics(user_id bigint); INSERT —
     * head moves to 9 and the name resolves to the new incarnation at
     * head, but (versioned-row visibility) snapshot 4 still serves the
     * old incarnation, exactly like the real server.
     */
    private void switchToIncarnation2()
    {
        RESPONSES.put(CATALOG_PATH, ok(catalogJson(9)));
        RESPONSES.put(TABLE_PATH + "?snapshot=9", ok(tableJson("uuid-incarnation-2", "user_id")));
        RESPONSES.put(SCAN_PATH + "?snapshot=9", ok(scanJson(V2_FILE, v2Parquet.length, 3)));
    }

    // ---- the verdict -------------------------------------------------------

    @Test
    void dropRecreateBetweenAnalysisAndExecution_readsThePinnedIncarnation()
            throws Exception
    {
        // Analysis: engine resolves the table once; the handle pins
        // snapshot, incarnation, and columns.
        ConnectorTableHandle handle = metadata.getTableHandle(
                session, new SchemaTableName("analytics", "metrics"), Optional.empty(), Optional.empty());
        assertThat(handle).isNotNull();
        HoglakeTableHandle pinned = (HoglakeTableHandle) handle;
        assertThat(pinned.snapshotId()).isEqualTo(4);
        assertThat(pinned.tableUuid()).isEqualTo("uuid-incarnation-1");
        Map<String, ColumnHandle> columns = metadata.getColumnHandles(session, handle);
        assertThat(columns).containsOnlyKeys("total");
        ColumnHandle total = columns.get("total");

        // Concurrent client: DROP + CREATE + INSERT — a new incarnation
        // with a new table_uuid, new columns, new files, field ids
        // reassigned from 1.
        switchToIncarnation2();

        // Execution: getSplits scans at the PINNED snapshot, so it plans
        // the old incarnation's files, not the new one's.
        ConnectorSplitSource splitSource = splitManager.getSplits(
                HoglakeTransactionHandle.INSTANCE, session, handle, DynamicFilter.EMPTY, Constraint.alwaysTrue());
        List<HoglakeSplit> splits = getAllSplits(splitSource);
        assertThat(splits).hasSize(1);
        assertThat(splits.get(0).path()).isEqualTo(V1_FILE);

        // Regression (S1, the head race): SELECT total FROM metrics
        // returns the OLD incarnation's consistent rows — the exact data
        // that was analyzed — never the recreated table's user_id values
        // rebound through the stale handle (the pre-fix silently-wrong-rows
        // failure). Old handles never bind to new-incarnation data.
        ConnectorPageSource pageSource = pageSourceProvider.createPageSource(
                HoglakeTransactionHandle.INSTANCE, session, splits.get(0), handle,
                List.of(total), DynamicFilter.EMPTY);
        List<List<Object>> rows = ConnectorTestFixtures.readAll(pageSource, List.of(BIGINT));
        pageSource.close();

        assertThat(rows).containsExactly(List.of(100L), List.of(200L));
    }

    @Test
    void metadataCallsWithinOneQueryServeFromThePinnedHandle()
    {
        // Regression (S1): getColumnHandles/getTableMetadata serve from the
        // handle — a commit between them can no longer make the engine's
        // own view of the table self-inconsistent inside a single query.
        ConnectorTableHandle handle = metadata.getTableHandle(
                session, new SchemaTableName("analytics", "metrics"), Optional.empty(), Optional.empty());
        Map<String, ColumnHandle> analyzed = metadata.getColumnHandles(session, handle);
        assertThat(analyzed).containsOnlyKeys("total");

        switchToIncarnation2();

        assertThat(metadata.getColumnHandles(session, handle)).containsOnlyKeys("total");
        assertThat(metadata.getTableMetadata(session, handle).getColumns())
                .extracting(column -> column.getName())
                .containsExactly("total");
    }

    @Test
    void tableDroppedMidQuery_failsAsTableNotFound()
    {
        ConnectorTableHandle handle = metadata.getTableHandle(
                session, new SchemaTableName("analytics", "metrics"), Optional.empty(), Optional.empty());
        assertThat(handle).isNotNull();

        // DROP TABLE with no recreate, history hard-gone: /scan now 404s.
        RESPONSES.clear();

        // Regression (S3): a table vanishing under a query is not an engine
        // bug — the SPI's typed TableNotFoundException (NOT_FOUND,
        // USER_ERROR), not an internal error.
        assertThatThrownBy(() -> splitManager.getSplits(
                HoglakeTransactionHandle.INSTANCE, session, handle, DynamicFilter.EMPTY, Constraint.alwaysTrue()))
                .isInstanceOf(TableNotFoundException.class)
                .isInstanceOfSatisfying(TrinoException.class, e -> {
                    assertThat(e.getErrorCode()).isEqualTo(NOT_FOUND.toErrorCode());
                    assertThat(e.getErrorCode().getType()).isEqualTo(ErrorType.USER_ERROR);
                })
                .hasMessageContaining("analytics.metrics");
    }

    @Test
    void pinnedSnapshotExpiredMidQuery_failsWithTypedSnapshotExpired()
    {
        ConnectorTableHandle handle = metadata.getTableHandle(
                session, new SchemaTableName("analytics", "metrics"), Optional.empty(), Optional.empty());
        assertThat(((HoglakeTableHandle) handle).snapshotId()).isEqualTo(4);

        // Retention expires snapshot 4 while the query is between analysis
        // and execution: the pinned scan is now below the expiry floor.
        RESPONSES.put(SCAN_PATH + "?snapshot=4", new CannedResponse(410, "{\"error\":\"gone\"}"));

        // Regression (S1/S3): the typed "snapshot expired during query"
        // failure — never silently-wrong rows, never a generic internal
        // error.
        assertThatThrownBy(() -> splitManager.getSplits(
                HoglakeTransactionHandle.INSTANCE, session, handle, DynamicFilter.EMPTY, Constraint.alwaysTrue()))
                .isInstanceOfSatisfying(TrinoException.class, e ->
                        assertThat(e.getErrorCode()).isEqualTo(HOGLAKE_SNAPSHOT_EXPIRED.toErrorCode()))
                .hasMessageContaining("snapshot expired");
    }

    @Test
    void dvRefusalFiresAtSplitGeneration_beforeAnyPageExists()
    {
        // A scan whose second file carries a live DV. Regression (S4): the
        // refusal fires at split generation, where the DV pairing is first
        // visible — the query dies before ANY split is handed to the
        // engine, so no worker can stream pages from the clean split to a
        // client before the failure lands.
        RESPONSES.put(SCAN_PATH + "?snapshot=4", ok("""
                [
                  {"data_file": {"data_file_id": 1, "path": "%s", "file_format": "parquet",
                    "record_count": 2, "file_size_bytes": %d, "row_id_start": 0,
                    "stats_state": "provided", "begin_snapshot": 1}},
                  {"data_file": {"data_file_id": 2, "path": "%s", "file_format": "parquet",
                    "record_count": 3, "file_size_bytes": %d, "row_id_start": 2,
                    "stats_state": "provided", "begin_snapshot": 2},
                   "delete_file": {"delete_file_id": 9, "data_file_id": 2,
                    "path": "memory:///metrics.dv", "file_format": "puffin-dv",
                    "delete_count": 1, "file_size_bytes": 64, "begin_snapshot": 3}}
                ]
                """.formatted(V1_FILE, v1Parquet.length, V2_FILE, v2Parquet.length)));
        ConnectorTableHandle handle = metadata.getTableHandle(
                session, new SchemaTableName("analytics", "metrics"), Optional.empty(), Optional.empty());

        assertThatThrownBy(() -> splitManager.getSplits(
                HoglakeTransactionHandle.INSTANCE, session, handle, DynamicFilter.EMPTY, Constraint.alwaysTrue()))
                .isInstanceOfSatisfying(TrinoException.class, e ->
                        assertThat(e.getErrorCode()).isEqualTo(NOT_SUPPORTED.toErrorCode()))
                .hasMessageContaining("table has row-level deletes")
                .hasMessageContaining("memory:///metrics.dv");
    }

    @Test
    void appendBetweenAnalysisAndExecution_isInvisibleToThePinnedQuery()
            throws Exception
    {
        // Same schema, extra file committed between analysis and execution.
        // Pre-fix this was the "benign" flavor of the head race (per-call
        // read-committed); with pinning the query is snapshot-isolated: the
        // append is invisible to the in-flight query and visible to the
        // next one.
        ConnectorTableHandle handle = metadata.getTableHandle(
                session, new SchemaTableName("analytics", "metrics"), Optional.empty(), Optional.empty());

        // Commit: head moves to 5, scan at 5 has two files; snapshot 4
        // still serves the analyzed single file.
        RESPONSES.put(CATALOG_PATH, ok(catalogJson(5)));
        RESPONSES.put(TABLE_PATH + "?snapshot=5", ok(tableJson("uuid-incarnation-1", "total")));
        RESPONSES.put(SCAN_PATH + "?snapshot=5", ok("""
                [
                  {"data_file": {"data_file_id": 1, "path": "%s", "file_format": "parquet",
                    "record_count": 2, "file_size_bytes": %d, "row_id_start": 0,
                    "stats_state": "provided", "begin_snapshot": 1}},
                  {"data_file": {"data_file_id": 2, "path": "%s", "file_format": "parquet",
                    "record_count": 3, "file_size_bytes": %d, "row_id_start": 2,
                    "stats_state": "provided", "begin_snapshot": 5}}
                ]
                """.formatted(V1_FILE, v1Parquet.length, V2_FILE, v2Parquet.length)));

        // The in-flight query still reads exactly what it analyzed.
        ConnectorSplitSource pinnedSource = splitManager.getSplits(
                HoglakeTransactionHandle.INSTANCE, session, handle, DynamicFilter.EMPTY, Constraint.alwaysTrue());
        assertThat(getAllSplits(pinnedSource)).hasSize(1);

        // A NEW query pins the new head and sees the appended file.
        ConnectorTableHandle fresh = metadata.getTableHandle(
                session, new SchemaTableName("analytics", "metrics"), Optional.empty(), Optional.empty());
        assertThat(((HoglakeTableHandle) fresh).snapshotId()).isEqualTo(5);
        ConnectorSplitSource freshSource = splitManager.getSplits(
                HoglakeTransactionHandle.INSTANCE, session, fresh, DynamicFilter.EMPTY, Constraint.alwaysTrue());
        assertThat(getAllSplits(freshSource)).hasSize(2);
    }

    // ---- plumbing ----------------------------------------------------------

    private static CannedResponse ok(String body)
    {
        return new CannedResponse(200, body);
    }

    private static List<HoglakeSplit> getAllSplits(ConnectorSplitSource splitSource)
            throws InterruptedException, ExecutionException
    {
        return splitSource.getNextBatch(100).get().getSplits().stream()
                .map(HoglakeSplit.class::cast)
                .toList();
    }

    private static String catalogJson(long headSnapshotId)
    {
        return """
                {"name": "lake", "data_path": "s3://lake/", "head_snapshot_id": %d, "schema_version": 1}
                """.formatted(headSnapshotId);
    }

    private static String tableJson(String tableUuid, String columnName)
    {
        return """
                {
                  "name": "metrics", "namespace": "analytics",
                  "table_uuid": "%s",
                  "columns": [
                    {"field_id": 1, "ordinal": 0, "name": "%s", "type": "long", "nullable": true}
                  ],
                  "record_count": 0, "file_count": 1, "file_size_bytes": 0
                }
                """.formatted(tableUuid, columnName);
    }

    private static String scanJson(String path, int fileSize, int recordCount)
    {
        return """
                [
                  {"data_file": {"data_file_id": 1, "path": "%s", "file_format": "parquet",
                    "record_count": %d, "file_size_bytes": %d, "row_id_start": 0,
                    "stats_state": "provided", "begin_snapshot": 1}}
                ]
                """.formatted(path, recordCount, fileSize);
    }
}
