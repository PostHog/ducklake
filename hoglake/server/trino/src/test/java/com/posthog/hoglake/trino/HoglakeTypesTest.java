package com.posthog.hoglake.trino;

import io.trino.spi.TrinoException;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.CharType;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.SmallintType;
import io.trino.spi.type.TimeType;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.TimeType.TIME_MICROS;
import static io.trino.spi.type.TimestampType.TIMESTAMP_MICROS;
import static io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_MICROS;
import static io.trino.spi.type.UuidType.UUID;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class HoglakeTypesTest
{
    // ---- hoglake -> Trino --------------------------------------------------

    @Test
    void mapsEveryHoglakeTypeToTrino()
    {
        assertThat(toTrino("boolean")).isEqualTo(BOOLEAN);
        assertThat(toTrino("int")).isEqualTo(INTEGER);
        assertThat(toTrino("long")).isEqualTo(BIGINT);
        assertThat(toTrino("float")).isEqualTo(REAL);
        assertThat(toTrino("double")).isEqualTo(DOUBLE);
        assertThat(toTrino("string")).isEqualTo(VARCHAR);
        assertThat(toTrino("binary")).isEqualTo(VARBINARY);
        assertThat(toTrino("date")).isEqualTo(DATE);
        assertThat(toTrino("time")).isEqualTo(TIME_MICROS);
        assertThat(toTrino("timestamp")).isEqualTo(TIMESTAMP_MICROS);
        assertThat(toTrino("timestamptz")).isEqualTo(TIMESTAMP_TZ_MICROS);
        assertThat(toTrino("uuid")).isEqualTo(UUID);
    }

    @Test
    void mapsDecimalWithParams()
    {
        assertThat(HoglakeTypes.toTrinoType("decimal", Map.of("precision", 10, "scale", 2)))
                .isEqualTo(DecimalType.createDecimalType(10, 2));
        assertThat(HoglakeTypes.toTrinoType("decimal", Map.of("precision", 38, "scale", 9)))
                .isEqualTo(DecimalType.createDecimalType(38, 9));
    }

    @Test
    void rejectsDecimalWithoutOrWithBadParams()
    {
        assertThatThrownBy(() -> HoglakeTypes.toTrinoType("decimal", null))
                .isInstanceOf(TrinoException.class)
                .hasMessageContaining("type_params.precision");
        assertThatThrownBy(() -> HoglakeTypes.toTrinoType("decimal", Map.of("precision", 10)))
                .isInstanceOf(TrinoException.class)
                .hasMessageContaining("type_params.scale");
        assertThatThrownBy(() -> HoglakeTypes.toTrinoType("decimal", Map.of("precision", "wide", "scale", 0)))
                .isInstanceOf(TrinoException.class)
                .hasMessageContaining("type_params.precision");
        // Out-of-range precision is rejected by Trino's own validation
        // (DecimalType.createDecimalType throws TrinoException directly).
        assertThatThrownBy(() -> HoglakeTypes.toTrinoType("decimal", Map.of("precision", 77, "scale", 0)))
                .isInstanceOf(TrinoException.class)
                .hasMessageContaining("DECIMAL precision must be in range");
    }

    @Test
    void rejectsUnknownHoglakeType()
    {
        assertThatThrownBy(() -> toTrino("variant"))
                .isInstanceOf(TrinoException.class)
                .hasMessageContaining("Unsupported hoglake type: variant");
    }

    // ---- Trino -> hoglake --------------------------------------------------

    @Test
    void mapsEveryTrinoTypeBackToHoglake()
    {
        assertThat(HoglakeTypes.toHoglakeType(BOOLEAN)).isEqualTo("boolean");
        assertThat(HoglakeTypes.toHoglakeType(INTEGER)).isEqualTo("int");
        assertThat(HoglakeTypes.toHoglakeType(BIGINT)).isEqualTo("long");
        assertThat(HoglakeTypes.toHoglakeType(REAL)).isEqualTo("float");
        assertThat(HoglakeTypes.toHoglakeType(DOUBLE)).isEqualTo("double");
        assertThat(HoglakeTypes.toHoglakeType(VARCHAR)).isEqualTo("string");
        assertThat(HoglakeTypes.toHoglakeType(VARBINARY)).isEqualTo("binary");
        assertThat(HoglakeTypes.toHoglakeType(DATE)).isEqualTo("date");
        assertThat(HoglakeTypes.toHoglakeType(TIME_MICROS)).isEqualTo("time");
        assertThat(HoglakeTypes.toHoglakeType(TIMESTAMP_MICROS)).isEqualTo("timestamp");
        assertThat(HoglakeTypes.toHoglakeType(TIMESTAMP_TZ_MICROS)).isEqualTo("timestamptz");
        assertThat(HoglakeTypes.toHoglakeType(UUID)).isEqualTo("uuid");
        assertThat(HoglakeTypes.toHoglakeType(DecimalType.createDecimalType(10, 2))).isEqualTo("decimal");
    }

    @Test
    void rejectsTrinoTypesOutsideTheHoglakeVocabulary()
    {
        for (Type type : new Type[] {
                SmallintType.SMALLINT,
                CharType.createCharType(10),
                VarcharType.createVarcharType(64),
                TimeType.TIME_MILLIS,
                TimestampType.TIMESTAMP_MILLIS,
                new ArrayType(BIGINT),
        }) {
            assertThatThrownBy(() -> HoglakeTypes.toHoglakeType(type))
                    .isInstanceOf(TrinoException.class)
                    .hasMessageContaining("no hoglake equivalent");
        }
    }

    @Test
    void roundTripsThroughBothDirections()
    {
        for (String wire : new String[] {
                "boolean", "int", "long", "float", "double", "string", "binary",
                "date", "time", "timestamp", "timestamptz", "uuid"}) {
            assertThat(HoglakeTypes.toHoglakeType(toTrino(wire))).isEqualTo(wire);
        }
    }

    private static Type toTrino(String hoglakeType)
    {
        return HoglakeTypes.toTrinoType(hoglakeType, null);
    }
}
