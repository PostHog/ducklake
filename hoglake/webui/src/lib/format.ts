import type { Column, PartitionField } from "../api/types";

const DECIMAL_INT_RE = /^-?\d+$/;

/**
 * Guard for the int64-string humanizers: missing values and anything that is
 * not an exact decimal integer render as an em-dash, never "NaN"/"undefined".
 */
function asDecimalInt(n: string | number | null | undefined): string | null {
  if (n === null || n === undefined) return null;
  const s = typeof n === "number" ? String(n) : n;
  return DECIMAL_INT_RE.test(s) ? s : null;
}

export function formatBytes(n: string | number | null | undefined): string {
  const s = asDecimalInt(n);
  if (s === null) return "—";
  // Number() here only picks the humanized magnitude; the exact value is the
  // decimal string itself (shown wherever exactness matters, e.g. `title`).
  const abs = Number(s);
  if (abs < 1024) return `${s} B`;
  const units = ["KiB", "MiB", "GiB", "TiB", "PiB"];
  let v = abs;
  let i = -1;
  while (v >= 1024 && i < units.length - 1) {
    v /= 1024;
    i++;
  }
  return `${v >= 100 ? v.toFixed(0) : v.toFixed(1)} ${units[i]}`;
}

export function formatCount(n: string | number | null | undefined): string {
  const s = asDecimalInt(n);
  if (s === null) return "—";
  // Group digits on the exact decimal string: values above 2^53 must not
  // round-trip through Number/toLocaleString.
  const neg = s.startsWith("-");
  const digits = neg ? s.slice(1) : s;
  const grouped = digits.replace(/\B(?=(\d{3})+$)/g, ",");
  return neg ? `-${grouped}` : grouped;
}

export function formatTime(iso: string): string {
  const d = new Date(iso);
  if (Number.isNaN(d.getTime())) return iso;
  return d.toISOString().replace("T", " ").replace(/\.\d+Z$/, "Z");
}

/** Render a partition field, e.g. "bucket(16, field 3)" or "identity(field 1)". */
export function formatPartitionField(
  f: PartitionField,
  columns?: Column[],
): string {
  const col = columns?.find((c) => c.field_id === f.source_field_id);
  const source = col ? col.name : `field ${f.source_field_id}`;
  if (f.transform === "bucket" && f.transform_param !== undefined) {
    return `bucket(${f.transform_param}, ${source})`;
  }
  return `${f.transform}(${source})`;
}
