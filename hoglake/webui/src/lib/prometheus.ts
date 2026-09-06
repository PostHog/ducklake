// Tolerant parser for the Prometheus text exposition format (what the
// server's Micrometer registry serves at /metrics). Deliberately small: it
// understands # HELP / # TYPE metadata, labeled samples with escaped label
// values, and groups histogram/summary component samples (_bucket/_count/
// _sum) under their declared family. Anything it cannot parse is skipped,
// never thrown — a half-written scrape should degrade, not blank the page.

export interface MetricSample {
  /** Full sample name as exposed, e.g. "hoglake_commit_lock_wait_seconds_bucket". */
  name: string;
  labels: Record<string, string>;
  /** Raw value token, e.g. "10.0", "6.43e-3", "NaN", "+Inf". */
  value: string;
}

export interface MetricFamily {
  name: string;
  help?: string;
  /** "counter" | "gauge" | "histogram" | "summary" | … ; undefined when never declared. */
  type?: string;
  samples: MetricSample[];
}

const NAME_RE = /^[a-zA-Z_:][a-zA-Z0-9_:]*/;

/** Unescape a HELP text: the format escapes backslash and newline. */
function unescapeHelp(s: string): string {
  return s.replace(/\\(.)/g, (_, c: string) => (c === "n" ? "\n" : c));
}

/** Parse one sample line; null for anything malformed. */
export function parseSampleLine(line: string): MetricSample | null {
  const nameMatch = NAME_RE.exec(line);
  if (!nameMatch) return null;
  const name = nameMatch[0];
  let i = name.length;
  const labels: Record<string, string> = {};
  if (line[i] === "{") {
    i++;
    for (;;) {
      while (line[i] === " " || line[i] === "\t") i++;
      if (line[i] === "}") {
        i++;
        break;
      }
      if (i >= line.length) return null; // unterminated label block
      const keyMatch = NAME_RE.exec(line.slice(i));
      if (!keyMatch) return null;
      const key = keyMatch[0];
      i += key.length;
      if (line[i] !== "=") return null;
      i++;
      if (line[i] !== '"') return null;
      i++;
      let value = "";
      while (i < line.length && line[i] !== '"') {
        if (line[i] === "\\") {
          const next = line[i + 1];
          value += next === "n" ? "\n" : (next ?? "");
          i += 2;
        } else {
          value += line[i];
          i++;
        }
      }
      if (line[i] !== '"') return null; // unterminated value
      i++;
      labels[key] = value;
      if (line[i] === ",") i++;
    }
  }
  const rest = line.slice(i).trim();
  if (!rest) return null;
  // Value, then an optional timestamp we ignore.
  const value = rest.split(/\s+/)[0];
  return { name, labels, value };
}

/** Suffixes that attach a component sample to its declared family. */
const COMPONENT_SUFFIXES = ["_bucket", "_count", "_sum", "_total", "_max"];

export function parsePrometheusText(text: string): MetricFamily[] {
  const families = new Map<string, MetricFamily>();

  const family = (name: string): MetricFamily => {
    let f = families.get(name);
    if (!f) {
      f = { name, samples: [] };
      families.set(name, f);
    }
    return f;
  };

  /** The family a sample belongs to: exact name, else a declared family the
   *  sample name extends by a well-known suffix, else a new HELP-less one. */
  const familyFor = (sampleName: string): MetricFamily => {
    const exact = families.get(sampleName);
    if (exact) return exact;
    for (const suffix of COMPONENT_SUFFIXES) {
      if (sampleName.endsWith(suffix)) {
        const base = families.get(sampleName.slice(0, -suffix.length));
        if (base) return base;
      }
    }
    return family(sampleName);
  };

  for (const rawLine of text.split("\n")) {
    const line = rawLine.endsWith("\r") ? rawLine.slice(0, -1) : rawLine;
    if (line.trim() === "") continue;
    if (line.startsWith("#")) {
      const meta = /^#\s+(HELP|TYPE)\s+([a-zA-Z_:][a-zA-Z0-9_:]*)(?:\s+(.*))?$/.exec(
        line,
      );
      if (!meta) continue; // ordinary comment
      const [, kind, name, restText] = meta;
      const rest = (restText ?? "").trim();
      if (kind === "HELP") {
        // Micrometer emits "# HELP name  " (empty help) for undescribed
        // meters; treat that as no help at all.
        if (rest !== "") family(name).help = unescapeHelp(rest);
        else family(name);
      } else {
        family(name).type = rest || undefined;
      }
      continue;
    }
    const sample = parseSampleLine(line);
    if (!sample) continue; // tolerate junk lines
    familyFor(sample.name).samples.push(sample);
  }

  return [...families.values()];
}

// -- value formatting ---------------------------------------------------------
//
// Exposition values are doubles (Micrometer), so Number() is faithful here —
// the int64-exact string discipline applies to the JSON API, not to /metrics.

function formatBytesNum(v: number): string {
  const neg = v < 0;
  let abs = Math.abs(v);
  if (abs < 1024) return `${neg ? "-" : ""}${abs % 1 === 0 ? abs : abs.toFixed(1)} B`;
  const units = ["KiB", "MiB", "GiB", "TiB", "PiB"];
  let i = -1;
  while (abs >= 1024 && i < units.length - 1) {
    abs /= 1024;
    i++;
  }
  const num = abs >= 100 ? abs.toFixed(0) : abs.toFixed(1);
  return `${neg ? "-" : ""}${num} ${units[i]}`;
}

function formatDuration(seconds: number): string {
  const neg = seconds < 0;
  const s = Math.abs(seconds);
  let out: string;
  if (s === 0) out = "0 s";
  else if (s >= 3600) out = `${(s / 3600).toFixed(1)} h`;
  else if (s >= 60) out = `${(s / 60).toFixed(1)} min`;
  else if (s >= 1) out = `${s >= 100 ? s.toFixed(0) : s.toFixed(2)} s`;
  else if (s >= 1e-3) out = `${(s * 1e3).toFixed(2)} ms`;
  else if (s >= 1e-6) out = `${(s * 1e6).toFixed(2)} µs`;
  else out = `${(s * 1e9).toFixed(0)} ns`;
  return neg ? `-${out}` : out;
}

function formatCountNum(v: number): string {
  if (Number.isInteger(v)) return v.toLocaleString("en-US");
  return v.toLocaleString("en-US", { maximumFractionDigits: 4 });
}

/**
 * Human formatting for a sample value, keyed off the sample NAME's unit
 * suffix: *_bytes* humanize as sizes, *_seconds* as durations, everything
 * else as grouped counts. Histogram/summary component samples whose values
 * are observation counts (_bucket, *_count) stay counts even under a
 * *_seconds* family name.
 */
export function formatMetricValue(sampleName: string, raw: string): string {
  if (raw === "NaN" || raw === "+Inf" || raw === "-Inf" || raw === "Inf") {
    return raw;
  }
  const v = Number(raw);
  if (!Number.isFinite(v)) return raw;
  const isCountComponent =
    sampleName.endsWith("_count") || sampleName.endsWith("_bucket");
  if (!isCountComponent) {
    if (/_bytes(_|$)/.test(sampleName)) return formatBytesNum(v);
    if (/_seconds(_|$)/.test(sampleName)) return formatDuration(v);
  }
  return formatCountNum(v);
}
