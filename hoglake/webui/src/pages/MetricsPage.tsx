import { useMemo, useState, type ReactNode } from "react";
import { useQuery } from "@tanstack/react-query";
import { fetchMetricsText } from "../api/client";
import { ErrorBox } from "../components/ErrorBox";
import { SkeletonBlock } from "../components/Skeleton";
import { formatTime } from "../lib/format";
import {
  formatMetricValue,
  parsePrometheusText,
  type MetricFamily,
  type MetricSample,
} from "../lib/prometheus";

// One snapshot, rendered visually: no timers, no history. The Refresh button
// refetches; families and series are compared WITHIN the snapshot only.

/** Stable key for a sample's label set, optionally ignoring some label names. */
function labelsKey(
  labels: Record<string, string>,
  exclude: readonly string[] = [],
): string {
  return Object.entries(labels)
    .filter(([k]) => !exclude.includes(k))
    .sort(([a], [b]) => (a < b ? -1 : a > b ? 1 : 0))
    .map(([k, v]) => `${k}=${v}`)
    .join(",");
}

function LabelChips({ text }: { text: string }) {
  if (text === "") return null;
  return (
    <span className="hbar-labels">
      {text.split(",").map((kv) => (
        <span key={kv} className="badge label-chip">
          {kv}
        </span>
      ))}
    </span>
  );
}

/** Horizontal bars: one per label set, scaled to the family max. */
function HorizontalBars({ samples }: { samples: MetricSample[] }) {
  const values = samples.map((s) => Number(s.value));
  const max = Math.max(0, ...values.filter((v) => Number.isFinite(v)));
  return (
    <div className="hbars">
      {samples.map((s, i) => {
        const v = values[i];
        const pct =
          max > 0 && Number.isFinite(v) ? Math.max(0, (v / max) * 100) : 0;
        return (
          <div className="hbar-row" key={i}>
            <LabelChips text={labelsKey(s.labels)} />
            <div className="hbar-track">
              <div className="hbar-fill" style={{ width: `${pct}%` }} />
            </div>
            <span className="mono hbar-value" title={s.value}>
              {formatMetricValue(s.name, s.value)}
            </span>
          </div>
        );
      })}
    </div>
  );
}

/** Scalar family: one unlabeled sample → a stat tile. */
function StatTile({ family, sample }: { family: MetricFamily; sample: MetricSample }) {
  return (
    <section className="metric-family stat-tile">
      <div className="stat-tile-value mono" title={sample.value}>
        {formatMetricValue(sample.name, sample.value)}
      </div>
      <div className="mono metric-family-name">
        {family.name} <span className="badge">{family.type ?? "untyped"}</span>
      </div>
      {family.help && <p className="metric-help">{family.help}</p>}
    </section>
  );
}

const numericLe = (le: string): number =>
  le === "+Inf" ? Number.POSITIVE_INFINITY : Number(le);

/** Cumulative `le` buckets → per-bucket deltas, rendered as a bar strip. */
function BucketStrip({ buckets }: { buckets: MetricSample[] }) {
  const sorted = [...buckets].sort(
    (a, b) => numericLe(a.labels.le ?? "") - numericLe(b.labels.le ?? ""),
  );
  let prev = 0;
  const deltas = sorted.map((b) => {
    const cum = Number(b.value);
    const d = Number.isFinite(cum) ? Math.max(0, cum - prev) : 0;
    if (Number.isFinite(cum)) prev = cum;
    return { le: b.labels.le ?? "?", count: d };
  });
  const max = Math.max(1, ...deltas.map((d) => d.count));
  return (
    <div className="bucket-strip" data-testid="bucket-strip">
      {deltas.map((d, i) => (
        <div
          key={i}
          className="bucket-bar"
          title={`≤ ${d.le}: ${d.count.toLocaleString("en-US")}`}
          style={{ height: `${d.count > 0 ? Math.max(4, (d.count / max) * 100) : 1}%` }}
        />
      ))}
    </div>
  );
}

/** One histogram series: bucket strip plus count and mean. */
function HistogramSeries({
  family,
  seriesLabels,
  buckets,
  count,
  sum,
}: {
  family: MetricFamily;
  seriesLabels: string;
  buckets: MetricSample[];
  count?: MetricSample;
  sum?: MetricSample;
}) {
  const n = count ? Number(count.value) : NaN;
  const total = sum ? Number(sum.value) : NaN;
  const mean =
    Number.isFinite(n) && n > 0 && Number.isFinite(total) ? total / n : null;
  return (
    <div className="histogram-series">
      <LabelChips text={seriesLabels} />
      <BucketStrip buckets={buckets} />
      <div className="histogram-stats">
        <span>
          count{" "}
          <span className="mono" title={count?.value}>
            {count ? formatMetricValue(`${family.name}_count`, count.value) : "—"}
          </span>
        </span>
        <span>
          mean{" "}
          <span className="mono">
            {mean === null
              ? "—"
              : formatMetricValue(family.name, String(mean))}
          </span>
        </span>
      </div>
    </div>
  );
}

/** Quantile-labeled summary series: quantiles as labeled bars. */
function QuantileSeries({
  familyName,
  seriesLabels,
  quantiles,
}: {
  familyName: string;
  seriesLabels: string;
  quantiles: MetricSample[];
}) {
  const bars = [...quantiles].sort(
    (a, b) => Number(a.labels.quantile) - Number(b.labels.quantile),
  );
  const max = Math.max(
    0,
    ...bars.map((q) => Number(q.value)).filter(Number.isFinite),
  );
  return (
    <div className="histogram-series">
      <LabelChips text={seriesLabels} />
      <div className="hbars">
        {bars.map((q, i) => {
          const v = Number(q.value);
          const pct =
            max > 0 && Number.isFinite(v) ? Math.max(0, (v / max) * 100) : 0;
          return (
            <div className="hbar-row" key={i}>
              <span className="badge label-chip">q={q.labels.quantile}</span>
              <div className="hbar-track">
                <div className="hbar-fill" style={{ width: `${pct}%` }} />
              </div>
              <span className="mono hbar-value" title={q.value}>
                {formatMetricValue(familyName, q.value)}
              </span>
            </div>
          );
        })}
      </div>
    </div>
  );
}

function FamilyCard({ family }: { family: MetricFamily }) {
  const buckets = family.samples.filter(
    (s) => s.name.endsWith("_bucket") && s.labels.le !== undefined,
  );
  const quantiles = family.samples.filter(
    (s) => s.labels.quantile !== undefined,
  );
  // _count/_sum are meter components only for histogram/summary families and
  // only when they EXTEND the family name — a gauge named *_count (e.g.
  // hoglake_table_count) is its own scalar sample, not a component.
  const isMeter = family.type === "histogram" || family.type === "summary";
  const counts = family.samples.filter(
    (s) => isMeter && s.name !== family.name && s.name.endsWith("_count"),
  );
  const sums = family.samples.filter(
    (s) => isMeter && s.name !== family.name && s.name.endsWith("_sum"),
  );
  const plain = family.samples.filter(
    (s) =>
      !buckets.includes(s) &&
      !quantiles.includes(s) &&
      !counts.includes(s) &&
      !sums.includes(s),
  );

  let body: ReactNode;
  if (buckets.length > 0) {
    // Histogram: one strip per label set (le excluded).
    const groups = new Map<string, MetricSample[]>();
    for (const b of buckets) {
      const key = labelsKey(b.labels, ["le"]);
      (groups.get(key) ?? groups.set(key, []).get(key)!).push(b);
    }
    body = [...groups.entries()].map(([key, group]) => (
      <HistogramSeries
        key={key}
        family={family}
        seriesLabels={key}
        buckets={group}
        count={counts.find((c) => labelsKey(c.labels) === key)}
        sum={sums.find((c) => labelsKey(c.labels) === key)}
      />
    ));
  } else if (quantiles.length > 0) {
    // Summary with quantile samples: labeled bars per series.
    const groups = new Map<string, MetricSample[]>();
    for (const q of quantiles) {
      const key = labelsKey(q.labels, ["quantile"]);
      (groups.get(key) ?? groups.set(key, []).get(key)!).push(q);
    }
    body = [...groups.entries()].map(([key, group]) => (
      <QuantileSeries
        key={key}
        familyName={family.name}
        seriesLabels={key}
        quantiles={group}
      />
    ));
  } else if (family.type === "summary" && counts.length > 0) {
    // Quantile-less summary (count/sum[/max]): count and mean per series.
    body = counts.map((c) => {
      const key = labelsKey(c.labels);
      const sum = sums.find((s) => labelsKey(s.labels) === key);
      const n = Number(c.value);
      const mean =
        sum && Number.isFinite(n) && n > 0 ? Number(sum.value) / n : null;
      return (
        <div className="histogram-stats" key={key}>
          <LabelChips text={key} />
          <span>
            count{" "}
            <span className="mono" title={c.value}>
              {formatMetricValue(c.name, c.value)}
            </span>
          </span>
          <span>
            mean{" "}
            <span className="mono">
              {mean === null ? "—" : formatMetricValue(family.name, String(mean))}
            </span>
          </span>
        </div>
      );
    });
  } else if (plain.length === 1 && Object.keys(plain[0].labels).length === 0) {
    // Scalar → stat tile (its own card layout).
    return <StatTile family={family} sample={plain[0]} />;
  } else if (plain.length > 0) {
    body = <HorizontalBars samples={plain} />;
  } else {
    body = <p className="empty">No samples.</p>;
  }

  return (
    <section className="metric-family">
      <header className="metric-family-head">
        <span className="mono metric-family-name">{family.name}</span>
        <span className="badge">{family.type ?? "untyped"}</span>
      </header>
      {family.help && <p className="metric-help">{family.help}</p>}
      {body}
    </section>
  );
}

export function MetricsPage() {
  const [filter, setFilter] = useState("");
  const query = useQuery({
    queryKey: ["metrics"],
    queryFn: fetchMetricsText,
    // One snapshot on load; refreshed only by the button below.
    refetchOnWindowFocus: false,
  });

  const families = useMemo(
    () => (query.data === undefined ? [] : parsePrometheusText(query.data)),
    [query.data],
  );

  const needle = filter.trim().toLowerCase();
  const matches = (f: MetricFamily) =>
    needle === "" ||
    f.name.toLowerCase().includes(needle) ||
    (f.help ?? "").toLowerCase().includes(needle);

  const hoglake = families.filter(
    (f) => f.name.startsWith("hoglake_") && matches(f),
  );
  const other = families.filter(
    (f) => !f.name.startsWith("hoglake_") && matches(f),
  );

  return (
    <section>
      <h2>
        Metrics <span className="subtle">/metrics snapshot</span>
      </h2>
      <div className="form-row metrics-toolbar">
        <label>
          filter
          <input
            value={filter}
            onChange={(e) => setFilter(e.target.value)}
            placeholder="hoglake_commits"
            aria-label="filter metrics"
          />
        </label>
        <button
          type="button"
          onClick={() => void query.refetch()}
          disabled={query.isFetching}
        >
          {query.isFetching ? "Refreshing…" : "Refresh"}
        </button>
        {query.dataUpdatedAt > 0 && (
          <span className="subtle metrics-fetched-at">
            fetched {formatTime(new Date(query.dataUpdatedAt).toISOString())}
          </span>
        )}
      </div>
      {query.isError && <ErrorBox error={query.error} />}
      {query.isPending && <SkeletonBlock />}
      {query.data !== undefined && (
        <>
          {hoglake.length === 0 && other.length === 0 && (
            <p className="empty">
              {needle === ""
                ? "The server exposed no metric families."
                : "No metric families match the filter."}
            </p>
          )}
          {hoglake.map((f) => (
            <FamilyCard key={f.name} family={f} />
          ))}
          {other.length > 0 && (
            <details className="metric-rest">
              <summary>
                everything else ({other.length}{" "}
                {other.length === 1 ? "family" : "families"})
              </summary>
              {other.map((f) => (
                <FamilyCard key={f.name} family={f} />
              ))}
            </details>
          )}
        </>
      )}
    </section>
  );
}
