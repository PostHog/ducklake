package com.posthog.hoglake.trino;

import io.trino.spi.TrinoException;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.BooleanType;
import io.trino.spi.type.DateType;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.DoubleType;
import io.trino.spi.type.IntegerType;
import io.trino.spi.type.RealType;
import io.trino.spi.type.TimeType;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.TimestampWithTimeZoneType;
import io.trino.spi.type.Type;
import io.trino.spi.type.UuidType;
import io.trino.spi.type.VarbinaryType;
import io.trino.spi.type.VarcharType;

import java.util.Map;

import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;

/**
 * The hoglake &lt;-&gt; Trino type mapping. Hoglake's column vocabulary is the
 * closed set from openapi/hoglake.yaml (ColumnDef.type); time-bearing
 * types are fixed at microsecond precision by the catalog, hence the
 * (6)s on the Trino side.
 *
 * <pre>
 *   long        BIGINT                     int        INTEGER
 *   double      DOUBLE                     float      REAL
 *   boolean     BOOLEAN                    string     VARCHAR
 *   binary      VARBINARY                  date       DATE
 *   time        TIME(6)                    timestamp  TIMESTAMP(6)
 *   timestamptz TIMESTAMP(6) WITH TZ       uuid       UUID
 *   decimal(p,s) DECIMAL(p,s)  (type_params {"precision":p,"scale":s})
 * </pre>
 */
public final class HoglakeTypes
{
    private HoglakeTypes() {}

    /** Map a hoglake wire type (+ optional type_params) to the Trino type. */
    public static Type toTrinoType(String hoglakeType, Map<String, Object> typeParams)
    {
        return switch (hoglakeType) {
            case "boolean" -> BooleanType.BOOLEAN;
            case "int" -> IntegerType.INTEGER;
            case "long" -> BigintType.BIGINT;
            case "float" -> RealType.REAL;
            case "double" -> DoubleType.DOUBLE;
            case "string" -> VarcharType.VARCHAR;
            case "binary" -> VarbinaryType.VARBINARY;
            case "date" -> DateType.DATE;
            case "time" -> TimeType.TIME_MICROS;
            case "timestamp" -> TimestampType.TIMESTAMP_MICROS;
            case "timestamptz" -> TimestampWithTimeZoneType.TIMESTAMP_TZ_MICROS;
            case "uuid" -> UuidType.UUID;
            case "decimal" -> decimalType(typeParams);
            default -> throw new TrinoException(NOT_SUPPORTED,
                    "Unsupported hoglake type: " + hoglakeType);
        };
    }

    private static Type decimalType(Map<String, Object> typeParams)
    {
        int precision = intParam(typeParams, "precision");
        int scale = intParam(typeParams, "scale");
        try {
            return DecimalType.createDecimalType(precision, scale);
        }
        catch (IllegalArgumentException e) {
            throw new TrinoException(NOT_SUPPORTED,
                    "Invalid hoglake decimal parameters: precision=" + precision + ", scale=" + scale, e);
        }
    }

    private static int intParam(Map<String, Object> typeParams, String name)
    {
        Object value = typeParams == null ? null : typeParams.get(name);
        if (!(value instanceof Number number)) {
            throw new TrinoException(NOT_SUPPORTED,
                    "hoglake decimal type requires integer type_params." + name + ", got: " + value);
        }
        return number.intValue();
    }

    /**
     * Map a Trino type back to the hoglake wire type. Read-only v1 uses
     * this only for round-trip validation; it is the seed of the future
     * write-path (CREATE TABLE) mapping. Types outside hoglake's closed
     * vocabulary (SMALLINT, CHAR, non-micro precisions, ARRAY, ...) are
     * rejected.
     */
    public static String toHoglakeType(Type type)
    {
        if (type.equals(BooleanType.BOOLEAN)) {
            return "boolean";
        }
        if (type.equals(IntegerType.INTEGER)) {
            return "int";
        }
        if (type.equals(BigintType.BIGINT)) {
            return "long";
        }
        if (type.equals(RealType.REAL)) {
            return "float";
        }
        if (type.equals(DoubleType.DOUBLE)) {
            return "double";
        }
        if (type.equals(VarcharType.VARCHAR)) {
            return "string";
        }
        if (type.equals(VarbinaryType.VARBINARY)) {
            return "binary";
        }
        if (type.equals(DateType.DATE)) {
            return "date";
        }
        if (type.equals(TimeType.TIME_MICROS)) {
            return "time";
        }
        if (type.equals(TimestampType.TIMESTAMP_MICROS)) {
            return "timestamp";
        }
        if (type.equals(TimestampWithTimeZoneType.TIMESTAMP_TZ_MICROS)) {
            return "timestamptz";
        }
        if (type.equals(UuidType.UUID)) {
            return "uuid";
        }
        if (type instanceof DecimalType) {
            return "decimal";
        }
        throw new TrinoException(NOT_SUPPORTED, "Trino type has no hoglake equivalent: " + type);
    }
}
