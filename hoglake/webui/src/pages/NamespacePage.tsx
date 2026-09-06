import { useState } from "react";
import { Link, useParams } from "react-router-dom";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { createTable, listTables } from "../api/client";
import { COLUMN_TYPES, type ColumnType } from "../api/types";
import { ErrorBox } from "../components/ErrorBox";
import { SkeletonRows } from "../components/Skeleton";
import { columnNameError, identifierError } from "../lib/names";

interface ColumnRow {
  name: string;
  type: ColumnType;
  nullable: boolean;
}

const emptyColumn = (): ColumnRow => ({
  name: "",
  type: "string",
  nullable: true,
});

function CreateTableForm({
  catalog,
  namespace,
}: {
  catalog: string;
  namespace: string;
}) {
  const queryClient = useQueryClient();
  const [name, setName] = useState("");
  const [columns, setColumns] = useState<ColumnRow[]>([emptyColumn()]);

  const mutation = useMutation({
    mutationFn: () =>
      createTable(catalog, namespace, {
        name,
        columns: columns.map((c) => ({
          name: c.name,
          type: c.type,
          nullable: c.nullable,
        })),
      }),
    onSuccess: () => {
      setName("");
      setColumns([emptyColumn()]);
      void queryClient.invalidateQueries({
        queryKey: ["tables", catalog, namespace],
      });
    },
  });

  const updateColumn = (i: number, patch: Partial<ColumnRow>) =>
    setColumns((cols) =>
      cols.map((c, idx) => (idx === i ? { ...c, ...patch } : c)),
    );

  // Mirror the server's identifier pattern (422 on violation) client-side;
  // columns additionally refuse the reserved `_hog` prefix.
  const nameError = identifierError(name);
  const columnErrors = columns.map((c) => columnNameError(c.name));
  const hasErrors = nameError !== null || columnErrors.some((e) => e !== null);

  return (
    <form
      className="inline-form"
      onSubmit={(e) => {
        e.preventDefault();
        if (hasErrors) return;
        mutation.mutate();
      }}
    >
      <h3>Create table</h3>
      <div className="form-row">
        <label>
          name
          <input
            value={name}
            onChange={(e) => setName(e.target.value)}
            required
            placeholder="pageviews"
            aria-invalid={nameError !== null}
          />
        </label>
      </div>
      {nameError && <p className="field-error">table name: {nameError}</p>}
      <div className="column-rows">
        {columns.map((col, i) => (
          <div className="form-row column-row" key={i}>
            <label>
              column
              <input
                value={col.name}
                onChange={(e) => updateColumn(i, { name: e.target.value })}
                required
                placeholder={`col_${i + 1}`}
                aria-label={`column ${i + 1} name`}
                aria-invalid={columnErrors[i] !== null}
              />
            </label>
            <label>
              type
              <select
                value={col.type}
                onChange={(e) =>
                  updateColumn(i, { type: e.target.value as ColumnType })
                }
                aria-label={`column ${i + 1} type`}
              >
                {COLUMN_TYPES.map((t) => (
                  <option key={t} value={t}>
                    {t}
                  </option>
                ))}
              </select>
            </label>
            <label className="check">
              <input
                type="checkbox"
                checked={col.nullable}
                onChange={(e) => updateColumn(i, { nullable: e.target.checked })}
                aria-label={`column ${i + 1} nullable`}
              />
              nullable
            </label>
            <button
              type="button"
              className="ghost"
              onClick={() =>
                setColumns((cols) => cols.filter((_, idx) => idx !== i))
              }
              disabled={columns.length === 1}
              aria-label={`remove column ${i + 1}`}
            >
              ×
            </button>
            {columnErrors[i] && (
              <p className="field-error">
                column {i + 1}: {columnErrors[i]}
              </p>
            )}
          </div>
        ))}
      </div>
      <div className="form-row">
        <button
          type="button"
          className="ghost"
          onClick={() => setColumns((cols) => [...cols, emptyColumn()])}
        >
          + Add column
        </button>
        <button type="submit" disabled={mutation.isPending || hasErrors}>
          {mutation.isPending ? "Creating…" : "Create table"}
        </button>
      </div>
      {mutation.isError && <ErrorBox error={mutation.error} />}
    </form>
  );
}

export function NamespacePage() {
  const { catalog, namespace } = useParams();
  const enabled = Boolean(catalog && namespace);
  const { data, isPending, isError, error } = useQuery({
    queryKey: ["tables", catalog, namespace],
    queryFn: () => listTables(catalog!, namespace!),
    enabled,
  });
  if (!catalog || !namespace) return null;

  return (
    <section>
      <h2>
        {namespace} <span className="subtle">namespace</span>
      </h2>
      {isError ? (
        <ErrorBox error={error} />
      ) : (
        <table className="data-table">
          <thead>
            <tr>
              <th>name</th>
              <th>table_uuid</th>
            </tr>
          </thead>
          {isPending ? (
            <SkeletonRows rows={4} cols={2} />
          ) : (
            <tbody>
              {data.length === 0 && (
                <tr>
                  <td colSpan={2} className="empty">
                    No tables in this namespace.
                  </td>
                </tr>
              )}
              {data.map((t) => (
                <tr key={t.table_uuid}>
                  <td>
                    <Link
                      to={`/catalogs/${encodeURIComponent(catalog)}/namespaces/${encodeURIComponent(namespace)}/tables/${encodeURIComponent(t.name)}`}
                    >
                      {t.name}
                    </Link>
                  </td>
                  <td className="mono">{t.table_uuid}</td>
                </tr>
              ))}
            </tbody>
          )}
        </table>
      )}
      <CreateTableForm catalog={catalog} namespace={namespace} />
    </section>
  );
}
