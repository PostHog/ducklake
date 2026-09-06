package com.posthog.hoglake.trino;

import com.posthog.hoglake.trino.testing.ConnectorTestFixtures;
import com.posthog.hoglake.trino.testing.ConnectorTestFixtures.FileColumn;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.DynamicFilter;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.apache.parquet.schema.Types;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The rename/evolution binding matrix, read through the REAL page-source
 * path (HoglakePageSourceProvider -> trino-parquet -> memory filesystem)
 * with real parquet bytes — no server, no Docker.
 *
 * Files are written with trino-parquet's writer, which round-trips
 * PARQUET:field_id (Hardwood does not, so hydrator-written files always
 * take the name-fallback path — exactly the fragile half of this matrix).
 */
class HoglakeParquetBindingTest
{
    private static final String PATH = "memory:///binding-test.parquet";

    // ---- rename matrix -----------------------------------------------------

    @Test
    void renamedColumn_fileWithFieldIds_bindsByIdCorrectly()
    {
        // File written before the rename: field 1 was called "user_id".
        byte[] file = ConnectorTestFixtures.writeParquet(List.of(new FileColumn(
                Types.optional(PrimitiveTypeName.INT64).id(1).named("user_id"),
                BIGINT, Arrays.asList(10L, 20L, 30L))));
        // Catalog after RENAME COLUMN user_id TO user_identifier: same field id.
        HoglakeColumnHandle renamed = new HoglakeColumnHandle("user_identifier", 1, BIGINT, true);

        List<List<Object>> rows = read(file, List.of(renamed), 3);

        assertThat(rows).containsExactly(List.of(10L), List.of(20L), List.of(30L));
    }

    @Test
    void renamedColumn_fileWithoutFieldIds_silentlyReadsAllNulls()
    {
        // Hardwood-style file (no field ids) written before the rename.
        byte[] file = ConnectorTestFixtures.writeParquet(List.of(new FileColumn(
                Types.optional(PrimitiveTypeName.INT64).named("user_id"),
                BIGINT, Arrays.asList(10L, 20L, 30L))));
        // Catalog after RENAME COLUMN user_id TO user_identifier. The name
        // fallback looks for "user_identifier", finds nothing, and the
        // column falls into the added-after-write null path.
        HoglakeColumnHandle renamed = new HoglakeColumnHandle("user_identifier", 1, BIGINT, true);

        List<List<Object>> rows = read(file, List.of(renamed), 3);

        // S2 (agreed connector behavior; catalog-side contract pending):
        // the data exists in the file (as "user_id") but a rename makes it
        // unreachable for every id-less file — silent NULLs. The connector's
        // id-authoritative binding is correct and stays (no name-fallback
        // heuristics); the fix is catalog-side: field ids are becoming a
        // registration contract, and the server will refuse renames while
        // id-less files are live, making this state unreachable. Until that
        // lands, this pins what the connector does with such files.
        assertThat(rows).containsExactly(
                Arrays.asList((Object) null),
                Arrays.asList((Object) null),
                Arrays.asList((Object) null));
    }

    @Test
    void renameThenReaddOldName_fileWithoutFieldIds_bindsOldDataToWrongColumn()
    {
        // File (no ids) written when the table had column "a" = 100, 200.
        byte[] file = ConnectorTestFixtures.writeParquet(List.of(new FileColumn(
                Types.optional(PrimitiveTypeName.INT64).named("a"),
                BIGINT, Arrays.asList(100L, 200L))));
        // Catalog since evolved: RENAME COLUMN a TO c, then ADD COLUMN a
        // (a brand-new column, field id 3, no data in old files).
        HoglakeColumnHandle c = new HoglakeColumnHandle("c", 1, BIGINT, true);
        HoglakeColumnHandle newA = new HoglakeColumnHandle("a", 3, BIGINT, true);

        List<List<Object>> rows = read(file, List.of(c, newA), 2);

        // S2 (agreed connector behavior; catalog-side contract pending):
        // exactly inverted from the truth — "c" (which IS the old "a",
        // renamed) reads null; the NEW column "a" reads the old column's
        // values via the name fallback. The server-side refusal of renames
        // while id-less files are live (field ids as a registration
        // contract) is what closes this; the connector binding stays
        // id-authoritative and unchanged.
        assertThat(rows).containsExactly(
                Arrays.asList(null, 100L),
                Arrays.asList(null, 200L));
    }

    // ---- type evolution ----------------------------------------------------

    @Test
    void typeWidening_int32FilePromotedToLong_coercesCorrectly()
    {
        // File written while the column was `int`; catalog since promoted it
        // to `long`. trino-parquet's INT32->BIGINT decoder handles this.
        byte[] file = ConnectorTestFixtures.writeParquet(List.of(new FileColumn(
                Types.optional(PrimitiveTypeName.INT32).id(1).named("n"),
                INTEGER, Arrays.asList(1, 2, 3))));
        HoglakeColumnHandle promoted = new HoglakeColumnHandle("n", 1, BIGINT, true);

        List<List<Object>> rows = read(file, List.of(promoted), 3);

        assertThat(rows).containsExactly(List.of(1L), List.of(2L), List.of(3L));
    }

    @Test
    void typeMismatch_int64FileReadAsInteger_succeedsInRangeThrowsOnOverflow()
    {
        // The inverse (illegal) direction: file has int64, catalog says int.
        // With the head race closed (pinned handles) this state is only
        // reachable through catalog corruption. trino-parquet narrows via
        // Math.toIntExact: in-range values read SILENTLY (no schema-level
        // rejection of the mismatch), out-of-range values throw at decode
        // time.
        byte[] inRange = ConnectorTestFixtures.writeParquet(List.of(new FileColumn(
                Types.optional(PrimitiveTypeName.INT64).id(1).named("n"),
                BIGINT, Arrays.asList(1L, 2L, 3L))));
        HoglakeColumnHandle narrowed = new HoglakeColumnHandle("n", 1, INTEGER, true);

        // Documented gap: an int64 file column read through an INTEGER
        // handle is a schema violation that goes undetected while the data
        // happens to fit; only an overflowing value surfaces it, mid-read.
        // Connector-side physical-type verification would close it, but the
        // state requires corrupted catalog metadata to reach at all.
        assertThat(read(inRange, List.of(narrowed), 3))
                .containsExactly(List.of(1L), List.of(2L), List.of(3L));

        byte[] overflowing = ConnectorTestFixtures.writeParquet(List.of(new FileColumn(
                Types.optional(PrimitiveTypeName.INT64).id(1).named("n"),
                BIGINT, Arrays.asList(1L, 5_000_000_000L))));
        assertThatThrownBy(() -> read(overflowing, List.of(narrowed), 2))
                .isInstanceOf(Exception.class);
    }

    @Test
    void typeMismatch_stringFileReadAsBigint_failsInsteadOfCorrupting()
    {
        // The old drop+recreate payload (now unreachable through the head
        // race thanks to pinned handles; still reachable via catalog
        // corruption): field id 1 in a foreign incarnation's file is a
        // string, but the column handle says BIGINT.
        byte[] file = ConnectorTestFixtures.writeParquet(List.of(new FileColumn(
                Types.optional(PrimitiveTypeName.BINARY)
                        .as(LogicalTypeAnnotation.stringType()).id(1).named("name"),
                VARCHAR, Arrays.asList("x", "y"))));
        HoglakeColumnHandle stale = new HoglakeColumnHandle("total", 1, BIGINT, true);

        assertThatThrownBy(() -> read(file, List.of(stale), 2))
                .isInstanceOf(Exception.class);
    }

    // ---- presence/absence --------------------------------------------------

    @Test
    void extraFileColumns_droppedFromCatalog_areIgnored()
    {
        // File carries a column that was dropped from the catalog after the
        // write; only requested columns are read.
        byte[] file = ConnectorTestFixtures.writeParquet(List.of(
                new FileColumn(Types.optional(PrimitiveTypeName.INT64).id(1).named("keep"),
                        BIGINT, Arrays.asList(1L, 2L)),
                new FileColumn(Types.optional(PrimitiveTypeName.INT64).id(9).named("dropped"),
                        BIGINT, Arrays.asList(99L, 98L))));
        HoglakeColumnHandle keep = new HoglakeColumnHandle("keep", 1, BIGINT, true);

        assertThat(read(file, List.of(keep), 2))
                .containsExactly(List.of(1L), List.of(2L));
    }

    @Test
    void catalogColumnAddedAfterWrite_readsAsNulls()
    {
        byte[] file = ConnectorTestFixtures.writeParquet(List.of(new FileColumn(
                Types.optional(PrimitiveTypeName.INT64).id(1).named("a"),
                BIGINT, Arrays.asList(1L, 2L))));
        HoglakeColumnHandle a = new HoglakeColumnHandle("a", 1, BIGINT, true);
        HoglakeColumnHandle added = new HoglakeColumnHandle("added", 7, VARCHAR, true);

        assertThat(read(file, List.of(a, added), 2))
                .containsExactly(Arrays.asList(1L, null), Arrays.asList(2L, null));
    }

    @Test
    void fileWithForeignFieldIds_nameMatchIsNotUsed()
    {
        // Id-authoritative binding: when the file DOES carry ids but none
        // matches, the name fallback is deliberately skipped (an id-bearing
        // file with a same-named field of a different id is a different
        // column lineage). The column reads as nulls.
        byte[] file = ConnectorTestFixtures.writeParquet(List.of(new FileColumn(
                Types.optional(PrimitiveTypeName.INT64).id(5).named("a"),
                BIGINT, Arrays.asList(1L, 2L))));
        HoglakeColumnHandle a = new HoglakeColumnHandle("a", 1, BIGINT, true);

        assertThat(read(file, List.of(a), 2))
                .containsExactly(
                        Arrays.asList((Object) null),
                        Arrays.asList((Object) null));
    }

    // ---- resource behavior through the same path ---------------------------

    @Test
    void pageSourceCloseIsIdempotentAndFinishes()
    {
        byte[] file = ConnectorTestFixtures.writeParquet(List.of(new FileColumn(
                Types.optional(PrimitiveTypeName.INT64).id(1).named("a"),
                BIGINT, Arrays.asList(1L, 2L))));
        ConnectorPageSource pageSource = open(file,
                List.of(new HoglakeColumnHandle("a", 1, BIGINT, true)), 2);

        // Close before reading anything (query cancellation path), twice.
        close(pageSource);
        close(pageSource);
        assertThat(pageSource.isFinished()).isTrue();
        assertThat(pageSource.getMemoryUsage()).isZero();
    }

    // ---- plumbing ----------------------------------------------------------

    private static List<List<Object>> read(byte[] file, List<HoglakeColumnHandle> columns, long recordCount)
    {
        ConnectorPageSource pageSource = open(file, columns, recordCount);
        try {
            return ConnectorTestFixtures.readAll(pageSource,
                    columns.stream().map(HoglakeColumnHandle::type).toList());
        }
        finally {
            close(pageSource);
        }
    }

    private static void close(ConnectorPageSource pageSource)
    {
        try {
            pageSource.close();
        }
        catch (java.io.IOException e) {
            throw new java.io.UncheckedIOException(e);
        }
    }

    private static ConnectorPageSource open(byte[] file, List<HoglakeColumnHandle> columns, long recordCount)
    {
        TrinoFileSystemFactory fileSystem = ConnectorTestFixtures.memoryFileSystem(Map.of(PATH, file));
        HoglakePageSourceProvider provider = new HoglakePageSourceProvider(fileSystem);
        HoglakeSplit split = new HoglakeSplit(PATH, file.length, recordCount, Optional.empty(), 0);
        return provider.createPageSource(
                HoglakeTransactionHandle.INSTANCE,
                ConnectorTestFixtures.session(),
                split,
                new HoglakeTableHandle("analytics", "binding_test", 1, "uuid-binding-test", List.of()),
                columns.stream().map(ColumnHandle.class::cast).toList(),
                DynamicFilter.EMPTY);
    }
}
