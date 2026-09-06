import { useState } from "react";
import { Link, useParams } from "react-router-dom";
import { useQuery } from "@tanstack/react-query";
import { listConsumers } from "../api/client";
import { ErrorBox } from "../components/ErrorBox";
import { SkeletonRows } from "../components/Skeleton";
import { formatTime } from "../lib/format";

export function ConsumersPage() {
  const { catalog } = useParams();
  const [filter, setFilter] = useState("");

  const query = useQuery({
    queryKey: ["consumers", catalog],
    queryFn: () => listConsumers(catalog!),
    enabled: Boolean(catalog),
  });
  if (!catalog) return null;

  const needle = filter.trim().toLowerCase();
  const consumers = (query.data?.consumers ?? []).filter(
    (c) => !needle || c.consumer_id.toLowerCase().includes(needle),
  );

  return (
    <section>
      <h2>
        Consumers <span className="subtle">{catalog}</span>
      </h2>
      <div className="form-row">
        <label>
          filter
          <input
            value={filter}
            onChange={(e) => setFilter(e.target.value)}
            placeholder="consumer id…"
            aria-label="filter consumers"
          />
        </label>
      </div>
      {query.isError && <ErrorBox error={query.error} />}
      {query.isPending && (
        <table className="data-table">
          <SkeletonRows rows={3} cols={5} />
        </table>
      )}
      {query.isSuccess && consumers.length === 0 && (
        <p className="empty">
          {query.data.consumers.length === 0
            ? "No consumer has committed an offset in this catalog."
            : "No consumer matches the filter."}
        </p>
      )}
      {consumers.map((c) => (
        <div key={c.consumer_id} className="consumer-group">
          <h3 className="mono">{c.consumer_id}</h3>
          <table className="data-table">
            <thead>
              <tr>
                <th>table</th>
                <th>table_uuid</th>
                <th className="num">committed_snapshot</th>
                <th>updated_at</th>
              </tr>
            </thead>
            <tbody>
              {c.offsets.map((o) => (
                <tr key={o.table_uuid}>
                  <td>
                    {o.table_name ? (
                      o.table_dropped ? (
                        <>
                          {o.namespace}.{o.table_name}{" "}
                          <span className="badge badge-warn">dropped</span>
                        </>
                      ) : (
                        <Link
                          to={`/catalogs/${encodeURIComponent(catalog)}/namespaces/${encodeURIComponent(o.namespace!)}/tables/${encodeURIComponent(o.table_name)}`}
                        >
                          {o.namespace}.{o.table_name}
                        </Link>
                      )
                    ) : (
                      <span className="subtle">unknown table</span>
                    )}
                  </td>
                  <td className="mono">{o.table_uuid}</td>
                  <td className="num mono">{o.committed_snapshot}</td>
                  <td className="mono">{formatTime(o.updated_at)}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      ))}
    </section>
  );
}
