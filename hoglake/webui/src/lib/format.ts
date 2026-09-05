import type { Column, PartitionField } from "../api/types";

export function formatBytes(n: number): string {
  if (n < 1024) return `${n} B`;
  const units = ["KiB", "MiB", "GiB", "TiB", "PiB"];
  let v = n;
  let i = -1;
  while (v >= 1024 && i < units.length - 1) {
    v /= 1024;
    i++;
  }
  return `${v >= 100 ? v.toFixed(0) : v.toFixed(1)} ${units[i]}`;
}

export function formatCount(n: number): string {
  return n.toLocaleString("en-US");
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
