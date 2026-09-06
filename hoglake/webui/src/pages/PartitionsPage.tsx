import { useState } from "react";
import { useParams, useSearchParams } from "react-router-dom";
import { useQuery } from "@tanstack/react-query";
import { listPartitionStats } from "../api/client";
import type { PartitionStats } from "../api/types";
import { ErrorBox } from "../components/ErrorBox";
import { SkeletonRows } from "../components/Skeleton";
import { formatBytes, formatCount } from "../lib/format";

const LIMIT = 50;

/** "team_id=42 / month=2026-09"; an empty spec is the unpartitioned leaf. */
export function formatPartition(p: PartitionStats): string {
  if (p.partition_values.length === 0) return "unpartitioned";
  return p.partition_values.map((v) => `${v.field}=${v.value}`).join(" / ");
}

/** small_file_count as a share of file_count. Counts are int64 strings but
 *  file counts fit a double comfortably; the ratio only drives pixel width. */
function DebtBar({ small, total }: { small: string; total: string }) {
  const t = Number(total);
  const s = Number(small);
  const pct = t > 0 ? Math.max(0, Math.min(100, (s / t) * 100)) : 0;
  return (
    <div
      className="debt-bar"
      role="meter"
      aria-valuemin={0}
      aria-valuemax={100}
      aria-valuenow={Math.round(pct)}
      title={`${small} of ${total} files under the small-file threshold`}
    >
      <div className="debt-bar-fill" style={{ width: `${pct}%` }} />
    </div>
  );
}

export function PartitionsPage() {
  const { catalog } = useParams();
  const [searchParams, setSearchParams] = useSearchParams();
  const namespace = searchParams.get("namespace") ?? "";
  const table = searchParams.get("table") ?? "";
  const [nsDraft, setNsDraft] = useState(namespace);
  const [tableDraft, setTableDraft] = useState(table);

  const query = useQuery({
    queryKey: ["partition-stats", catalog, namespace, table],
    queryFn: () =>
      listPartitionStats(catalog!, {
        namespace: namespace || undefined,
        table: table || undefined,
        limit: LIMIT,
      }),
    enabled: Boolean(catalog),
  });
  if (!catalog) return null;

  return (
    <section>
      <h2>
        Compaction debt <span className="subtle">{catalog}</span>
      </h2>
      <form
        className="inline-form"
        onSubmit={(e) => {
          e.preventDefault();
          const next: Record<string, string> = {};
          if (nsDraft.trim()) next.namespace = nsDraft.trim();
          if (tableDraft.trim()) next.table = tableDraft.trim();
          setSearchParams(next, { replace: true });
        }}
      >
        <div className="form-row">
          <label>
            namespace
            <input
              value={nsDraft}
              onChange={(e) => setNsDraft(e.target.value)}
              placeholder="events"
              aria-label="namespace filter"
            />
          </label>
          <label>
            table
            <input
              value={tableDraft}
              onChange={(e) => setTableDraft(e.target.value)}
              placeholder="pageviews"
              aria-label="table filter"
            />
          </label>
          <button type="submit">Apply</button>
        </div>
      </form>
      {query.isError && <ErrorBox error={query.error} />}
      {!query.isError && (
        <>
          {query.data?.truncated && (
            <p className="truncated-banner" role="status">
              Showing the top {LIMIT} partitions by debt — more exist. Narrow
              with the namespace/table filters.
            </p>
          )}
          <table className="data-table">
            <thead>
              <tr>
                <th>table</th>
                <th>partition</th>
                <th className="num">files</th>
                <th className="num">small files</th>
                <th>debt</th>
                <th className="num">total size</th>
                <th className="num">avg size</th>
                <th className="num">DVs</th>
                <th className="num">score</th>
              </tr>
            </thead>
            {query.isPending ? (
              <SkeletonRows rows={4} cols={9} />
            ) : (
              <tbody>
                {query.data.partitions.length === 0 && (
                  <tr>
                    <td colSpan={9} className="empty">
                      No partitions match — nothing owes compaction debt here.
                    </td>
                  </tr>
                )}
                {query.data.partitions.map((p, i) => (
                  <tr key={`${p.table_uuid}:${formatPartition(p)}:${i}`}>
                    <td className="mono">
                      {p.namespace}.{p.table}
                    </td>
                    <td className="mono">{formatPartition(p)}</td>
                    <td className="num mono" title={p.file_count}>
                      {formatCount(p.file_count)}
                    </td>
                    <td className="num mono" title={p.small_file_count}>
                      {formatCount(p.small_file_count)}
                    </td>
                    <td className="debt-cell">
                      <DebtBar small={p.small_file_count} total={p.file_count} />
                    </td>
                    <td className="num mono" title={p.total_bytes}>
                      {formatBytes(p.total_bytes)}
                    </td>
                    <td className="num mono" title={p.avg_file_bytes}>
                      {formatBytes(p.avg_file_bytes)}
                    </td>
                    <td className="num mono" title={p.dv_count}>
                      {formatCount(p.dv_count)}
                    </td>
                    <td className="num mono" title={p.debt_score}>
                      {formatCount(p.debt_score)}
                    </td>
                  </tr>
                ))}
              </tbody>
            )}
          </table>
        </>
      )}
    </section>
  );
}
