import { describe, expect, it } from "vitest";
import {
  formatMetricValue,
  parsePrometheusText,
  parseSampleLine,
} from "../src/lib/prometheus";

describe("parsePrometheusText", () => {
  it("parses a labeled counter family with HELP and TYPE", () => {
    const text = [
      "# HELP hoglake_commits_total Commits by result",
      "# TYPE hoglake_commits_total counter",
      'hoglake_commits_total{catalog="analytics",result="committed"} 34.0',
      'hoglake_commits_total{catalog="analytics",result="conflict"} 1.0',
      "",
    ].join("\n");
    const families = parsePrometheusText(text);
    expect(families).toHaveLength(1);
    const f = families[0];
    expect(f.name).toBe("hoglake_commits_total");
    expect(f.help).toBe("Commits by result");
    expect(f.type).toBe("counter");
    expect(f.samples).toHaveLength(2);
    expect(f.samples[0].labels).toEqual({
      catalog: "analytics",
      result: "committed",
    });
    expect(f.samples[1].value).toBe("1.0");
  });

  it("parses an unlabeled gauge", () => {
    const text = [
      "# HELP hoglake_table_count Live tables",
      "# TYPE hoglake_table_count gauge",
      "hoglake_table_count 42.0",
    ].join("\n");
    const [f] = parsePrometheusText(text);
    expect(f.type).toBe("gauge");
    expect(f.samples).toEqual([
      { name: "hoglake_table_count", labels: {}, value: "42.0" },
    ]);
  });

  it("groups histogram _bucket/_count/_sum samples under the declared family", () => {
    const text = [
      "# HELP hoglake_commit_lock_wait_seconds Lock wait",
      "# TYPE hoglake_commit_lock_wait_seconds histogram",
      'hoglake_commit_lock_wait_seconds_bucket{le="0.001"} 27538',
      'hoglake_commit_lock_wait_seconds_bucket{le="+Inf"} 27881',
      "hoglake_commit_lock_wait_seconds_count 27881",
      "hoglake_commit_lock_wait_seconds_sum 6.432959718",
    ].join("\n");
    const families = parsePrometheusText(text);
    expect(families).toHaveLength(1);
    const f = families[0];
    expect(f.type).toBe("histogram");
    expect(f.samples).toHaveLength(4);
    expect(f.samples.map((s) => s.name)).toContain(
      "hoglake_commit_lock_wait_seconds_count",
    );
    expect(f.samples[1].labels.le).toBe("+Inf");
  });

  it("attaches OpenMetrics-style _total samples to a family declared without the suffix", () => {
    const text = [
      "# TYPE http_requests counter",
      'http_requests_total{code="200"} 7',
    ].join("\n");
    const families = parsePrometheusText(text);
    expect(families).toHaveLength(1);
    expect(families[0].name).toBe("http_requests");
    expect(families[0].samples[0].name).toBe("http_requests_total");
  });

  it("unescapes backslash, quote and newline in label values", () => {
    const line =
      'weird_metric{path="C:\\\\temp\\\\x",msg="line\\nbreak",q="say \\"hi\\""} 1';
    const sample = parseSampleLine(line);
    expect(sample).not.toBeNull();
    expect(sample!.labels).toEqual({
      path: "C:\\temp\\x",
      msg: "line\nbreak",
      q: 'say "hi"',
    });
    expect(sample!.value).toBe("1");
  });

  it("skips blank lines, plain comments and malformed lines", () => {
    const text = [
      "",
      "# just a comment, not metadata",
      "   ",
      "!!!not a metric",
      'broken_labels{key="unterminated 3',
      "ok_metric 5",
      "",
    ].join("\n");
    const families = parsePrometheusText(text);
    expect(families).toHaveLength(1);
    expect(families[0].name).toBe("ok_metric");
    expect(families[0].samples[0].value).toBe("5");
  });

  it("creates a HELP-less, type-less family for a bare sample", () => {
    const [f] = parsePrometheusText('mystery_metric{a="b"} 9.5');
    expect(f.name).toBe("mystery_metric");
    expect(f.help).toBeUndefined();
    expect(f.type).toBeUndefined();
    expect(f.samples).toHaveLength(1);
  });

  it("treats Micrometer's empty HELP as no help and ignores timestamps", () => {
    const text = [
      "# HELP empty_help_total  ",
      "# TYPE empty_help_total counter",
      "empty_help_total 3 1726000000000",
    ].join("\n");
    const [f] = parsePrometheusText(text);
    expect(f.help).toBeUndefined();
    expect(f.samples[0].value).toBe("3");
  });
});

describe("formatMetricValue", () => {
  it("humanizes *_bytes samples as sizes", () => {
    expect(formatMetricValue("jvm_memory_used_bytes", "1073741824.0")).toBe(
      "1.0 GiB",
    );
    expect(formatMetricValue("jvm_memory_used_bytes", "512")).toBe("512 B");
  });

  it("humanizes *_seconds samples as durations", () => {
    expect(formatMetricValue("lock_wait_seconds_sum", "0.002684125")).toBe(
      "2.68 ms",
    );
    expect(formatMetricValue("process_uptime_seconds", "7200")).toBe("2.0 h");
  });

  it("keeps _count and _bucket samples as grouped counts even under a seconds family", () => {
    expect(formatMetricValue("lock_wait_seconds_count", "27881")).toBe(
      "27,881",
    );
    expect(formatMetricValue("lock_wait_seconds_bucket", "27538")).toBe(
      "27,538",
    );
  });

  it("groups plain counts and passes non-finite tokens through", () => {
    expect(formatMetricValue("hoglake_commits_total", "1234567.0")).toBe(
      "1,234,567",
    );
    expect(formatMetricValue("anything", "NaN")).toBe("NaN");
    expect(formatMetricValue("anything", "+Inf")).toBe("+Inf");
  });
});
