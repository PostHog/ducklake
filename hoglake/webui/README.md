# hoglake webui

Management console for the hoglake control plane: catalogs, namespaces,
tables (schema / files / scan with time travel), the snapshot timeline,
and consumer offsets. Read-heavy by design — v1 exposes create forms for
catalogs, namespaces, and tables, and deliberately no drop/delete
actions.

## Run

The toolchain lives in a flox environment (Node 22):

```sh
cd webui
flox activate          # or prefix every command with `flox activate --`
npm install
npm run dev            # http://localhost:5173
```

The Vite dev server proxies `/v1`, `/healthz`, and `/openapi.yaml` to
the hoglake server at `http://localhost:8080`, so start the server first
and the app fetches same-origin (no CORS involved). Point the proxy at a
different server with `HOGLAKE_API=http://host:port npm run dev`.

## Test / build

```sh
npm test               # vitest + testing-library, fetch mocked by hand
npm run build          # tsc -b && vite build → dist/
```

## Layout

`src/api/` holds the single integration surface: `types.ts` hand-mirrors
the OpenAPI schemas (snake_case passed through untouched) and
`client.ts` is the one typed fetch client, which unwraps the
`ApiError {error, detail}` body into a typed exception every page renders
inline. `src/pages/` has one component per route (catalogs → catalog →
namespace → table, plus per-catalog consumers), `src/components/` the
shared chrome (topbar with `/healthz` polling, breadcrumbs, badges,
skeleton loaders, error boxes), and `src/lib/format.ts` the humanizers
(bytes, counts, partition transforms). Tests in `test/` render each page
against hand-written fixtures shaped exactly like the spec's schemas and
cover both the happy path and an API-error path per page.
