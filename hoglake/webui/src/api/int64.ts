// Lossless int64 handling.
//
// The spec types snapshot ids, row counts, file sizes, row-id starts (and
// friends) as `integer, format: int64`, and the server emits them as bare
// JSON numbers — including values above Number.MAX_SAFE_INTEGER (2^53-1),
// which JSON.parse silently rounds to the nearest double. Every int64 wire
// field is therefore carried as a decimal STRING end-to-end: parsed straight
// from the raw response text (never through a double), displayed verbatim,
// and echoed verbatim into subsequent request URLs. Arithmetic and ordering
// go through BigInt.

/**
 * Wire field names declared `integer, format: int64` in
 * server/src/main/resources/openapi/hoglake.yaml. Keyed by property name:
 * the OpenAPI schemas never reuse one of these names with a non-int64 type.
 */
const INT64_FIELDS = new Set([
  "snapshot_id",
  "head_snapshot_id",
  "earliest_snapshot_id",
  "new_earliest_snapshot_id",
  "committed_snapshot",
  "begin_snapshot",
  "end_snapshot",
  "schema_version",
  "object_id",
  "record_count",
  "file_count",
  "file_size_bytes",
  "footer_size",
  "row_id_start",
  "delete_count",
  "data_file_id",
  "delete_file_id",
  "field_id",
  "source_field_id",
  "spec_id",
  "value_count",
  "null_count",
  "nan_count",
  "size_bytes",
  // partition compaction-debt stats (GET /v1/catalogs/{c}/stats/partitions)
  "small_file_count",
  "total_bytes",
  "small_file_bytes",
  "avg_file_bytes",
  "dv_count",
  "debt_score",
]);

const DECIMAL_INT_RE = /^-?\d+$/;

type ReviverContext = { source?: string };

/**
 * Whether JSON.parse exposes the raw source text to the reviver
 * (https://github.com/tc39/proposal-json-parse-with-source; Node >= 21 and
 * evergreen browsers). Without it we fall back to String(value), which is
 * exact up to 2^53 — i.e. no worse than plain JSON.parse ever was.
 */
const HAS_RAW_SOURCE: boolean = (() => {
  let source: unknown;
  JSON.parse("7", (_key: string, value: unknown, ctx?: ReviverContext) => {
    source = ctx?.source;
    return value;
  });
  return source === "7";
})();

/**
 * JSON.parse that returns every int64-typed field as its exact decimal
 * string. Non-int64 fields (int32 ordinals, booleans, strings, …) come back
 * unchanged; an int64 field already delivered as a string passes through.
 */
export function parseInt64Json(text: string): unknown {
  return JSON.parse(
    text,
    (key: string, value: unknown, ctx?: ReviverContext) => {
      if (typeof value === "number" && INT64_FIELDS.has(key)) {
        const source = ctx?.source;
        if (source !== undefined && DECIMAL_INT_RE.test(source)) return source;
        return String(value);
      }
      return value;
    },
  );
}

export { HAS_RAW_SOURCE };

/** True iff s is a plain non-negative decimal integer (a valid id/count). */
export function isInt64String(s: string): boolean {
  return /^\d+$/.test(s);
}

/** Numeric ordering for int64 decimal strings. */
export function compareInt64(a: string, b: string): number {
  const av = BigInt(a);
  const bv = BigInt(b);
  return av < bv ? -1 : av > bv ? 1 : 0;
}

/** Exact int64 addition on decimal strings. */
export function addInt64(a: string, n: number | bigint): string {
  return (BigInt(a) + BigInt(n)).toString();
}
