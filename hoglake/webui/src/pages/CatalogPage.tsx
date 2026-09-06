import { useState } from "react";
import { Link, useParams } from "react-router-dom";
import {
  useInfiniteQuery,
  useMutation,
  useQuery,
  useQueryClient,
} from "@tanstack/react-query";
import {
  createNamespace,
  getCatalog,
  listNamespaces,
  listSnapshots,
} from "../api/client";
import { addInt64, compareInt64 } from "../api/int64";
import { ErrorBox } from "../components/ErrorBox";
import { SkeletonBlock, SkeletonRows } from "../components/Skeleton";
import { ChangeBadge } from "../components/badges";
import { formatTime } from "../lib/format";
import { identifierError } from "../lib/names";

const SNAPSHOT_PAGE_SIZE = 50;

function CatalogHeader({ catalog }: { catalog: string }) {
  const { data, isPending, isError, error } = useQuery({
    queryKey: ["catalog", catalog],
    queryFn: () => getCatalog(catalog),
  });
  if (isError) return <ErrorBox error={error} />;
  if (isPending) return <SkeletonBlock />;
  return (
    <dl className="stats-header">
      <div>
        <dt>data_path</dt>
        <dd className="mono">{data.data_path}</dd>
      </div>
      <div>
        <dt>head_snapshot_id</dt>
        <dd className="mono">{data.head_snapshot_id}</dd>
      </div>
      <div>
        <dt>schema_version</dt>
        <dd className="mono">{data.schema_version}</dd>
      </div>
    </dl>
  );
}

function CreateNamespaceForm({ catalog }: { catalog: string }) {
  const queryClient = useQueryClient();
  const [name, setName] = useState("");
  const nameError = identifierError(name);
  const mutation = useMutation({
    mutationFn: () => createNamespace(catalog, name),
    onSuccess: () => {
      setName("");
      void queryClient.invalidateQueries({ queryKey: ["namespaces", catalog] });
    },
  });
  return (
    <form
      className="inline-form"
      onSubmit={(e) => {
        e.preventDefault();
        if (nameError) return;
        mutation.mutate();
      }}
    >
      <h3>Create namespace</h3>
      <div className="form-row">
        <label>
          name
          <input
            value={name}
            onChange={(e) => setName(e.target.value)}
            required
            placeholder="events"
            aria-invalid={nameError !== null}
          />
        </label>
        <button type="submit" disabled={mutation.isPending || nameError !== null}>
          {mutation.isPending ? "Creating…" : "Create"}
        </button>
      </div>
      {nameError && <p className="field-error">{nameError}</p>}
      {mutation.isError && <ErrorBox error={mutation.error} />}
    </form>
  );
}

function NamespacesPanel({ catalog }: { catalog: string }) {
  const { data, isPending, isError, error } = useQuery({
    queryKey: ["namespaces", catalog],
    queryFn: () => listNamespaces(catalog),
  });
  return (
    <section className="panel">
      <h2>Namespaces</h2>
      {isError ? (
        <ErrorBox error={error} />
      ) : (
        <table className="data-table">
          <thead>
            <tr>
              <th>name</th>
            </tr>
          </thead>
          {isPending ? (
            <SkeletonRows rows={3} cols={1} />
          ) : (
            <tbody>
              {data.length === 0 && (
                <tr>
                  <td className="empty">No namespaces yet.</td>
                </tr>
              )}
              {data.map((ns) => (
                <tr key={ns.name}>
                  <td>
                    <Link
                      to={`/catalogs/${encodeURIComponent(catalog)}/namespaces/${encodeURIComponent(ns.name)}`}
                    >
                      {ns.name}
                    </Link>
                  </td>
                </tr>
              ))}
            </tbody>
          )}
        </table>
      )}
      <CreateNamespaceForm catalog={catalog} />
    </section>
  );
}

function SnapshotsPanel({ catalog }: { catalog: string }) {
  // The timeline is newest-first: walk DOWN from head+1 with the `before`
  // cursor (descending pages); "Load more" pages older. `after` (ascending)
  // is never combined with `before` — the server 422s that pair.
  const headQuery = useQuery({
    queryKey: ["catalog", catalog],
    queryFn: () => getCatalog(catalog),
  });
  const head = headQuery.data?.head_snapshot_id;
  const query = useInfiniteQuery({
    queryKey: ["snapshots", catalog, head],
    enabled: head !== undefined,
    queryFn: ({ pageParam }) =>
      listSnapshots(catalog, { before: pageParam, limit: SNAPSHOT_PAGE_SIZE }),
    // head+1 in exact int64 arithmetic: head itself must be included.
    initialPageParam: addInt64(head ?? "0", 1),
    getNextPageParam: (lastPage) => {
      if (!lastPage.has_more || lastPage.snapshots.length === 0) return undefined;
      // Pages are descending; the last row is the oldest id fetched so far.
      return lastPage.snapshots[lastPage.snapshots.length - 1].snapshot_id;
    },
  });

  if (headQuery.isError || query.isError) {
    return (
      <section className="panel">
        <h2>Snapshots</h2>
        <ErrorBox error={headQuery.isError ? headQuery.error : query.error} />
      </section>
    );
  }

  const snapshots = (query.data?.pages ?? [])
    .flatMap((p) => p.snapshots)
    .sort((a, b) => compareInt64(b.snapshot_id, a.snapshot_id));

  return (
    <section className="panel">
      <h2>Snapshots</h2>
      <table className="data-table">
        <thead>
          <tr>
            <th className="num">id</th>
            <th>time</th>
            <th>author</th>
            <th>message</th>
            <th>changes</th>
          </tr>
        </thead>
        {headQuery.isPending || query.isPending ? (
          <SkeletonRows rows={5} cols={5} />
        ) : (
          <tbody>
            {snapshots.length === 0 && (
              <tr>
                <td colSpan={5} className="empty">
                  No snapshots.
                </td>
              </tr>
            )}
            {snapshots.map((s) => (
              <tr key={s.snapshot_id}>
                <td className="num mono">{s.snapshot_id}</td>
                <td className="mono">{formatTime(s.snapshot_time)}</td>
                <td>{s.author ?? "—"}</td>
                <td>{s.message ?? "—"}</td>
                <td>
                  {(s.changes ?? []).map((c, i) => (
                    <ChangeBadge key={i} change={c} />
                  ))}
                </td>
              </tr>
            ))}
          </tbody>
        )}
      </table>
      {query.hasNextPage && (
        <button
          type="button"
          className="load-more"
          onClick={() => void query.fetchNextPage()}
          disabled={query.isFetchingNextPage}
        >
          {query.isFetchingNextPage ? "Loading…" : "Load more"}
        </button>
      )}
    </section>
  );
}

export function CatalogPage() {
  const { catalog } = useParams();
  if (!catalog) return null;
  return (
    <section>
      <div className="page-head">
        <h2>{catalog}</h2>
        <Link
          className="side-link"
          to={`/catalogs/${encodeURIComponent(catalog)}/consumers`}
        >
          Consumers
        </Link>
        <Link
          className="side-link"
          to={`/catalogs/${encodeURIComponent(catalog)}/partitions`}
        >
          Compaction debt
        </Link>
      </div>
      <CatalogHeader catalog={catalog} />
      <div className="two-col">
        <NamespacesPanel catalog={catalog} />
        <SnapshotsPanel catalog={catalog} />
      </div>
    </section>
  );
}
