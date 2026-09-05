import { useState } from "react";
import { Link } from "react-router-dom";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { createCatalog, listCatalogs } from "../api/client";
import { ErrorBox } from "../components/ErrorBox";
import { SkeletonRows } from "../components/Skeleton";

function CreateCatalogForm() {
  const queryClient = useQueryClient();
  const [name, setName] = useState("");
  const [dataPath, setDataPath] = useState("");
  const mutation = useMutation({
    mutationFn: () => createCatalog({ name, data_path: dataPath }),
    onSuccess: () => {
      setName("");
      setDataPath("");
      void queryClient.invalidateQueries({ queryKey: ["catalogs"] });
    },
  });

  return (
    <form
      className="inline-form"
      onSubmit={(e) => {
        e.preventDefault();
        mutation.mutate();
      }}
    >
      <h3>Create catalog</h3>
      <div className="form-row">
        <label>
          name
          <input
            value={name}
            onChange={(e) => setName(e.target.value)}
            required
            placeholder="analytics"
          />
        </label>
        <label>
          data_path
          <input
            value={dataPath}
            onChange={(e) => setDataPath(e.target.value)}
            required
            placeholder="s3://bucket/prefix"
          />
        </label>
        <button type="submit" disabled={mutation.isPending}>
          {mutation.isPending ? "Creating…" : "Create"}
        </button>
      </div>
      {mutation.isError && <ErrorBox error={mutation.error} />}
    </form>
  );
}

export function CatalogsPage() {
  const { data, isPending, isError, error } = useQuery({
    queryKey: ["catalogs"],
    queryFn: listCatalogs,
  });

  return (
    <section>
      <h2>Catalogs</h2>
      {isError ? (
        <ErrorBox error={error} />
      ) : (
        <table className="data-table">
          <thead>
            <tr>
              <th>name</th>
              <th>data_path</th>
              <th className="num">head_snapshot_id</th>
              <th className="num">schema_version</th>
            </tr>
          </thead>
          {isPending ? (
            <SkeletonRows rows={4} cols={4} />
          ) : (
            <tbody>
              {data.length === 0 && (
                <tr>
                  <td colSpan={4} className="empty">
                    No catalogs yet.
                  </td>
                </tr>
              )}
              {data.map((c) => (
                <tr key={c.name}>
                  <td>
                    <Link to={`/catalogs/${encodeURIComponent(c.name)}`}>
                      {c.name}
                    </Link>
                  </td>
                  <td className="mono">{c.data_path}</td>
                  <td className="num mono">{c.head_snapshot_id}</td>
                  <td className="num mono">{c.schema_version}</td>
                </tr>
              ))}
            </tbody>
          )}
        </table>
      )}
      <CreateCatalogForm />
    </section>
  );
}
