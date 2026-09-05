import { useState } from "react";
import { useParams, useSearchParams } from "react-router-dom";
import { useQuery } from "@tanstack/react-query";
import { getTable, listFiles, planScan } from "../api/client";
import type { Table } from "../api/types";
import { ErrorBox } from "../components/ErrorBox";
import { SkeletonBlock, SkeletonRows } from "../components/Skeleton";
import { StatsStateBadge } from "../components/badges";
import { CopyButton } from "../components/CopyButton";
import {
  formatBytes,
  formatCount,
  formatPartitionField,
} from "../lib/format";

const TABS = ["schema", "files", "scan"] as const;
type Tab = (typeof TABS)[number];

function StatsHeader({ table }: { table: Table }) {
  return (
    <dl className="stats-header">
      <div>
        <dt>record_count</dt>
        <dd className="mono">{formatCount(table.record_count)}</dd>
      </div>
      <div>
        <dt>file_count</dt>
        <dd className="mono">{formatCount(table.file_count)}</dd>
      </div>
      <div>
        <dt>file_size_bytes</dt>
        <dd className="mono" title={`${table.file_size_bytes}`}>
          {formatBytes(table.file_size_bytes)}
        </dd>
      </div>
      <div>
        <dt>table_uuid</dt>
        <dd className="mono">
          {table.table_uuid} <CopyButton text={table.table_uuid} label="table_uuid" />
        </dd>
      </div>
    </dl>
  );
}

function SnapshotSelector({
  snapshot,
  onChange,
}: {
  snapshot: number | undefined;
  onChange: (s: number | undefined) => void;
}) {
  const [draft, setDraft] = useState(snapshot?.toString() ?? "");
  return (
    <form
      className="snapshot-selector"
      onSubmit={(e) => {
        e.preventDefault();
        const trimmed = draft.trim();
        onChange(trimmed === "" ? undefined : Number(trimmed));
      }}
    >
      <label>
        snapshot
        <input
          type="number"
          min={0}
          value={draft}
          placeholder="head"
          onChange={(e) => setDraft(e.target.value)}
          aria-label="snapshot id"
        />
      </label>
      <button type="submit">Go</button>
      {snapshot !== undefined && (
        <button
          type="button"
          className="ghost"
          onClick={() => {
            setDraft("");
            onChange(undefined);
          }}
        >
          head
        </button>
      )}
    </form>
  );
}

function SchemaTab({ table }: { table: Table }) {
  const columns = [...table.columns].sort((a, b) => a.ordinal - b.ordinal);
  return (
    <div>
      <table className="data-table">
        <thead>
          <tr>
            <th className="num">field_id</th>
            <th>name</th>
            <th>type</th>
            <th>nullable</th>
            <th className="num">ordinal</th>
          </tr>
        </thead>
        <tbody>
          {columns.map((c) => (
            <tr key={c.field_id}>
              <td className="num mono">{c.field_id}</td>
              <td>{c.name}</td>
              <td className="mono">{c.type}</td>
              <td>{c.nullable === false ? "not null" : "nullable"}</td>
              <td className="num mono">{c.ordinal}</td>
            </tr>
          ))}
        </tbody>
      </table>
      {table.partition_spec && (
        <div className="partition-spec">
          <h3>
            Partition spec{" "}
            <span className="subtle">spec_id {table.partition_spec.spec_id}</span>
          </h3>
          {table.partition_spec.fields.length === 0 ? (
            <p className="empty">Unpartitioned.</p>
          ) : (
            <ul>
              {table.partition_spec.fields.map((f, i) => (
                <li key={i} className="mono">
                  {formatPartitionField(f, table.columns)}
                </li>
              ))}
            </ul>
          )}
        </div>
      )}
    </div>
  );
}

function FilesTab({
  catalog,
  namespace,
  table,
  snapshot,
}: {
  catalog: string;
  namespace: string;
  table: string;
  snapshot?: number;
}) {
  const { data, isPending, isError, error } = useQuery({
    queryKey: ["files", catalog, namespace, table, snapshot ?? "head"],
    queryFn: () => listFiles(catalog, namespace, table, snapshot),
  });
  if (isError) return <ErrorBox error={error} />;
  return (
    <table className="data-table">
      <thead>
        <tr>
          <th className="num">id</th>
          <th>path</th>
          <th className="num">record_count</th>
          <th className="num">size</th>
          <th className="num">row_id_start</th>
          <th>stats</th>
          <th className="num">begin_snapshot</th>
          <th>partition_values</th>
        </tr>
      </thead>
      {isPending ? (
        <SkeletonRows rows={5} cols={8} />
      ) : (
        <tbody>
          {data.length === 0 && (
            <tr>
              <td colSpan={8} className="empty">
                No data files at this snapshot.
              </td>
            </tr>
          )}
          {data.map((f) => (
            <tr key={f.data_file_id}>
              <td className="num mono">{f.data_file_id}</td>
              <td className="mono path-cell" title={f.path}>
                {f.path}
              </td>
              <td className="num mono">{formatCount(f.record_count)}</td>
              <td className="num mono" title={`${f.file_size_bytes}`}>
                {formatBytes(f.file_size_bytes)}
              </td>
              <td className="num mono">{f.row_id_start}</td>
              <td>
                <StatsStateBadge state={f.stats_state} />
              </td>
              <td className="num mono">{f.begin_snapshot}</td>
              <td className="mono">
                {f.partition_values
                  ? `[${f.partition_values.map((v) => v ?? "null").join(", ")}]`
                  : "—"}
              </td>
            </tr>
          ))}
        </tbody>
      )}
    </table>
  );
}

function ScanTab({
  catalog,
  namespace,
  table,
  snapshot,
}: {
  catalog: string;
  namespace: string;
  table: string;
  snapshot?: number;
}) {
  const { data, isPending, isError, error } = useQuery({
    queryKey: ["scan", catalog, namespace, table, snapshot ?? "head"],
    queryFn: () => planScan(catalog, namespace, table, snapshot),
  });
  if (isError) return <ErrorBox error={error} />;
  return (
    <table className="data-table">
      <thead>
        <tr>
          <th className="num">data_file</th>
          <th>path</th>
          <th className="num">record_count</th>
          <th>stats</th>
          <th>deletion vector</th>
          <th className="num">delete_count</th>
        </tr>
      </thead>
      {isPending ? (
        <SkeletonRows rows={5} cols={6} />
      ) : (
        <tbody>
          {data.length === 0 && (
            <tr>
              <td colSpan={6} className="empty">
                Empty scan plan at this snapshot.
              </td>
            </tr>
          )}
          {data.map((sf) => (
            <tr
              key={sf.data_file.data_file_id}
              className={sf.delete_file ? "has-deletes" : undefined}
            >
              <td className="num mono">{sf.data_file.data_file_id}</td>
              <td className="mono path-cell" title={sf.data_file.path}>
                {sf.data_file.path}
              </td>
              <td className="num mono">{formatCount(sf.data_file.record_count)}</td>
              <td>
                <StatsStateBadge state={sf.data_file.stats_state} />
              </td>
              <td className="mono path-cell">
                {sf.delete_file ? (
                  <span title={sf.delete_file.path}>{sf.delete_file.path}</span>
                ) : (
                  <span className="subtle">none</span>
                )}
              </td>
              <td className="num mono">
                {sf.delete_file ? formatCount(sf.delete_file.delete_count) : "—"}
              </td>
            </tr>
          ))}
        </tbody>
      )}
    </table>
  );
}

export function TablePage() {
  const { catalog, namespace, table } = useParams();
  const [searchParams, setSearchParams] = useSearchParams();

  const tabParam = searchParams.get("tab");
  const tab: Tab = TABS.includes(tabParam as Tab) ? (tabParam as Tab) : "schema";
  const snapshotParam = searchParams.get("snapshot");
  const snapshot =
    snapshotParam !== null && snapshotParam !== "" ? Number(snapshotParam) : undefined;

  const enabled = Boolean(catalog && namespace && table);
  const tableQuery = useQuery({
    queryKey: ["table", catalog, namespace, table, snapshot ?? "head"],
    queryFn: () => getTable(catalog!, namespace!, table!, snapshot),
    enabled,
  });
  if (!catalog || !namespace || !table) return null;

  const setParam = (key: string, value: string | undefined) => {
    setSearchParams(
      (prev) => {
        const next = new URLSearchParams(prev);
        if (value === undefined) next.delete(key);
        else next.set(key, value);
        return next;
      },
      { replace: true },
    );
  };

  return (
    <section>
      <h2>
        {table} <span className="subtle">table</span>
        {snapshot !== undefined && (
          <span className="badge time-travel">@ snapshot {snapshot}</span>
        )}
      </h2>
      {tableQuery.isError ? (
        <ErrorBox error={tableQuery.error} />
      ) : tableQuery.isPending ? (
        <SkeletonBlock />
      ) : (
        <StatsHeader table={tableQuery.data} />
      )}
      <div className="tab-bar">
        <div role="tablist" className="tabs">
          {TABS.map((t) => (
            <button
              key={t}
              role="tab"
              aria-selected={tab === t}
              className={tab === t ? "tab active" : "tab"}
              onClick={() => setParam("tab", t === "schema" ? undefined : t)}
            >
              {t}
            </button>
          ))}
        </div>
        <SnapshotSelector
          snapshot={snapshot}
          onChange={(s) => setParam("snapshot", s?.toString())}
        />
      </div>
      {tab === "schema" &&
        (tableQuery.isSuccess ? <SchemaTab table={tableQuery.data} /> : null)}
      {tab === "files" && (
        <FilesTab
          catalog={catalog}
          namespace={namespace}
          table={table}
          snapshot={snapshot}
        />
      )}
      {tab === "scan" && (
        <ScanTab
          catalog={catalog}
          namespace={namespace}
          table={table}
          snapshot={snapshot}
        />
      )}
    </section>
  );
}
