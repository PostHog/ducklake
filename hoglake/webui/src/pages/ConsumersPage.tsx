import { useState } from "react";
import { useParams, useSearchParams } from "react-router-dom";
import { useQuery } from "@tanstack/react-query";
import { listConsumerOffsets } from "../api/client";
import { ErrorBox } from "../components/ErrorBox";
import { SkeletonRows } from "../components/Skeleton";
import { formatTime } from "../lib/format";

export function ConsumersPage() {
  const { catalog } = useParams();
  const [searchParams, setSearchParams] = useSearchParams();
  const consumer = searchParams.get("consumer") ?? "";
  const [draft, setDraft] = useState(consumer);

  const query = useQuery({
    queryKey: ["consumer-offsets", catalog, consumer],
    queryFn: () => listConsumerOffsets(catalog!, consumer),
    enabled: Boolean(catalog && consumer),
  });
  if (!catalog) return null;

  return (
    <section>
      <h2>
        Consumers <span className="subtle">{catalog}</span>
      </h2>
      <form
        className="inline-form"
        onSubmit={(e) => {
          e.preventDefault();
          const trimmed = draft.trim();
          setSearchParams(trimmed ? { consumer: trimmed } : {}, {
            replace: true,
          });
        }}
      >
        <div className="form-row">
          <label>
            consumer id
            <input
              value={draft}
              onChange={(e) => setDraft(e.target.value)}
              placeholder="viaduck-sink-7"
              aria-label="consumer id"
            />
          </label>
          <button type="submit">Look up</button>
        </div>
      </form>
      {!consumer && (
        <p className="empty">
          Enter a consumer id to list its committed offsets.
        </p>
      )}
      {consumer && query.isError && <ErrorBox error={query.error} />}
      {consumer && !query.isError && (
        <table className="data-table">
          <thead>
            <tr>
              <th>consumer_id</th>
              <th>table_uuid</th>
              <th className="num">committed_snapshot</th>
              <th>updated_at</th>
            </tr>
          </thead>
          {query.isPending ? (
            <SkeletonRows rows={3} cols={4} />
          ) : (
            <tbody>
              {query.data.length === 0 && (
                <tr>
                  <td colSpan={4} className="empty">
                    No offsets for this consumer.
                  </td>
                </tr>
              )}
              {query.data.map((o) => (
                <tr key={o.table_uuid}>
                  <td>{o.consumer_id}</td>
                  <td className="mono">{o.table_uuid}</td>
                  <td className="num mono">{o.committed_snapshot}</td>
                  <td className="mono">{formatTime(o.updated_at)}</td>
                </tr>
              ))}
            </tbody>
          )}
        </table>
      )}
    </section>
  );
}
