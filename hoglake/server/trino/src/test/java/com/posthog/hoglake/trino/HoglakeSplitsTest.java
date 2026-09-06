package com.posthog.hoglake.trino;

import com.posthog.hoglake.trino.rest.HoglakeDtos;
import io.trino.spi.TrinoException;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Types;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;

import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Split generation from /scan responses, including the DV refusal. */
class HoglakeSplitsTest
{
    private static final HoglakeDtos.DataFile DATA_FILE = new HoglakeDtos.DataFile(
            10, "s3://lake/t/a.parquet", "parquet", 25, 2048, 321L, 0, "provided", 3);
    private static final HoglakeDtos.DataFile DELETED_FROM = new HoglakeDtos.DataFile(
            11, "s3://lake/t/b.parquet", "parquet", 17, 1024, null, 25, "provided", 4);
    private static final HoglakeDtos.DeleteFile DELETE_FILE = new HoglakeDtos.DeleteFile(
            5, 11, "s3://lake/t/b.dv", "puffin-dv", 3, 64, 6);

    @Test
    void oneSplitPerDataFileCarryingScanMetadata()
    {
        List<HoglakeSplit> splits = HoglakeSplitManager.toSplits(List.of(
                new HoglakeDtos.ScanFile(DATA_FILE, null),
                new HoglakeDtos.ScanFile(DELETED_FROM, DELETE_FILE)));

        assertThat(splits).hasSize(2);

        HoglakeSplit bare = splits.get(0);
        assertThat(bare.path()).isEqualTo("s3://lake/t/a.parquet");
        assertThat(bare.fileSizeBytes()).isEqualTo(2048);
        assertThat(bare.recordCount()).isEqualTo(25);
        assertThat(bare.deleteFilePath()).isEmpty();

        HoglakeSplit paired = splits.get(1);
        assertThat(paired.path()).isEqualTo("s3://lake/t/b.parquet");
        assertThat(paired.deleteFilePath()).contains("s3://lake/t/b.dv");
        assertThat(paired.deleteCount()).isEqualTo(3);
    }

    @Test
    void scanWithoutDeleteFilesPassesThePlanningDvGuard()
    {
        assertThatCode(() -> HoglakeSplitManager.ensureNoRowLevelDeletes(
                List.of(new HoglakeDtos.ScanFile(DATA_FILE, null))))
                .doesNotThrowAnyException();
    }

    /**
     * Regression (S4): the DV refusal fires at split generation — where
     * the scan's DV pairing is first visible — so the query dies before
     * any split reaches the engine and no partial results can stream out
     * of clean splits first.
     */
    @Test
    void scanWithDeleteFileRefusesTheQueryAtPlanning()
    {
        assertThatThrownBy(() -> HoglakeSplitManager.ensureNoRowLevelDeletes(List.of(
                new HoglakeDtos.ScanFile(DATA_FILE, null),
                new HoglakeDtos.ScanFile(DELETED_FROM, DELETE_FILE))))
                .isInstanceOfSatisfying(TrinoException.class, e ->
                        assertThat(e.getErrorCode()).isEqualTo(NOT_SUPPORTED.toErrorCode()))
                .hasMessageContaining("table has row-level deletes; DV application not yet implemented in the hoglake connector")
                .hasMessageContaining("s3://lake/t/b.parquet")
                .hasMessageContaining("s3://lake/t/b.dv");
    }

    /**
     * Defense-in-depth: the worker-side page-source guard still refuses a
     * DV-carrying split should one ever arrive despite the planning
     * refusal.
     */
    @Test
    void splitWithDeleteFileRefusesAtThePageSourceToo()
    {
        HoglakeSplit split = HoglakeSplitManager
                .toSplits(List.of(new HoglakeDtos.ScanFile(DELETED_FROM, DELETE_FILE)))
                .get(0);
        assertThatThrownBy(() -> HoglakePageSourceProvider.ensureNoRowLevelDeletes(split))
                .isInstanceOfSatisfying(TrinoException.class, e ->
                        assertThat(e.getErrorCode()).isEqualTo(NOT_SUPPORTED.toErrorCode()))
                .hasMessageContaining("table has row-level deletes; DV application not yet implemented in the hoglake connector")
                .hasMessageContaining("s3://lake/t/b.parquet")
                .hasMessageContaining("s3://lake/t/b.dv");
    }

    /**
     * On a multi-node cluster every split crosses the wire as JSON
     * (coordinator -> worker task update). The engine's mapper carries
     * Jdk8Module, which the Optional field depends on; pin the round trip
     * for both DV shapes so a field change can't silently break workers.
     */
    @Test
    void splitSurvivesJsonRoundTripForWorkerDispatch()
            throws Exception
    {
        // Mirror airlift's ObjectMapperProvider: Jdk8Module registered,
        // auto-detection off (only @JsonProperty members ride the wire —
        // this is also what keeps the throwing getRetainedSizeInBytes()
        // default method out of serialization).
        com.fasterxml.jackson.databind.ObjectMapper mapper =
                new com.fasterxml.jackson.databind.ObjectMapper()
                        .registerModule(new com.fasterxml.jackson.datatype.jdk8.Jdk8Module())
                        .disable(
                                com.fasterxml.jackson.databind.MapperFeature.AUTO_DETECT_CREATORS,
                                com.fasterxml.jackson.databind.MapperFeature.AUTO_DETECT_FIELDS,
                                com.fasterxml.jackson.databind.MapperFeature.AUTO_DETECT_SETTERS,
                                com.fasterxml.jackson.databind.MapperFeature.AUTO_DETECT_GETTERS,
                                com.fasterxml.jackson.databind.MapperFeature.AUTO_DETECT_IS_GETTERS);

        for (HoglakeSplit split : HoglakeSplitManager.toSplits(List.of(
                new HoglakeDtos.ScanFile(DATA_FILE, null),
                new HoglakeDtos.ScanFile(DELETED_FROM, DELETE_FILE)))) {
            HoglakeSplit deserialized =
                    mapper.readValue(mapper.writeValueAsString(split), HoglakeSplit.class);
            assertThat(deserialized).isEqualTo(split);
        }
    }

    /** Object-store splits: any worker may run any split (3-node semantics). */
    @Test
    void splitsAreRemotelyAccessibleWithNoAddressAffinity()
    {
        HoglakeSplit split = HoglakeSplitManager
                .toSplits(List.of(new HoglakeDtos.ScanFile(DATA_FILE, null)))
                .get(0);
        assertThat(split.isRemotelyAccessible()).isTrue();
        assertThat(split.getAddresses()).isEmpty();
    }

    // ---- parquet column binding -------------------------------------------

    @Test
    void bindsByFieldIdWhenTheFileCarriesIds()
    {
        // File written after a rename: field id 1 is now called "renamed"
        // in the catalog but "id" in the file — the id wins.
        MessageType fileSchema = new MessageType("t",
                Types.required(PrimitiveType.PrimitiveTypeName.INT64).id(1).named("id"),
                Types.optional(PrimitiveType.PrimitiveTypeName.DOUBLE).id(2).named("renamed"));

        HoglakeColumnHandle catalogColumn = new HoglakeColumnHandle("renamed", 1, BIGINT, false);
        Optional<org.apache.parquet.schema.Type> bound =
                HoglakePageSourceProvider.bindColumn(fileSchema, catalogColumn);
        assertThat(bound).isPresent();
        assertThat(bound.orElseThrow().getName()).isEqualTo("id");
    }

    @Test
    void fallsBackToNameForFilesWithoutFieldIds()
    {
        MessageType fileSchema = new MessageType("t",
                Types.required(PrimitiveType.PrimitiveTypeName.INT64).named("id"),
                Types.optional(PrimitiveType.PrimitiveTypeName.DOUBLE).named("Score"));

        assertThat(HoglakePageSourceProvider.bindColumn(
                fileSchema, new HoglakeColumnHandle("id", 1, BIGINT, false)))
                .isPresent();
        // Case-insensitive fallback.
        assertThat(HoglakePageSourceProvider.bindColumn(
                fileSchema, new HoglakeColumnHandle("score", 2, DOUBLE, true))
                .orElseThrow().getName()).isEqualTo("Score");
    }

    @Test
    void columnAbsentFromTheFileIsUnbound()
    {
        MessageType fileSchema = new MessageType("t",
                Types.required(PrimitiveType.PrimitiveTypeName.INT64).id(1).named("id"));

        assertThat(HoglakePageSourceProvider.bindColumn(
                fileSchema, new HoglakeColumnHandle("added_later", 9, BIGINT, true)))
                .isEmpty();
    }
}
