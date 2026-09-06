# hoglake webui

Management console for the hoglake control plane: catalogs, namespaces,
tables (schema / files / scan with time travel), the snapshot timeline
(newest-first, paged down from head via the `before` cursor), consumer
offsets, a per-catalog compaction-debt view, and a server metrics page.
Read-heavy by design — v1 exposes create forms for catalogs,
namespaces, and tables, and deliberately no drop/delete actions.

Notable surfaces beyond the catalog browser:

- **Metrics** (`/metrics` route): one snapshot of the server's
  Prometheus endpoint rendered visually — stat tiles, per-label bars,
  histogram bucket strips. Manual refresh only, no polling.
- **Compaction debt** (`/catalogs/:catalog/partitions`): leaf
  partitions ranked by `debt_score` (= small-file count, the same
  threshold the compactor plans with), with debt bars, filters, and the
  stale-spec-group count — backed by `GET /stats/partitions`.
- **Consumers** (`/catalogs/:catalog/consumers`): every consumer in the
  catalog, grouped, with table names resolved server-side and dropped
  tables badged (offsets outlive drops by design — no more pasting
  uuids to find out what an offset points at).
- **Instance badge**: the topbar shows the operator-configured instance
  name from `GET /v1/info` (`HOGLAKE_INSTANCE_NAME`), so you always
  know which deployment you're looking at; the health dot's tooltip
  explains the readiness semantics.

Int64 wire fields (snapshot ids, row counts, sizes, row-id starts) are
parsed **losslessly**: a raw-text reviver carries every
`integer, format: int64` field as a decimal string end-to-end (never
through a double), so ids above 2^53−1 display and round-trip exactly
(`src/api/int64.ts`).

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
the OpenAPI schemas (snake_case passed through untouched; int64 fields
typed as strings — see `int64.ts`) and `client.ts` is the one typed
fetch client, which parses from raw response text through the int64
reviver and unwraps the `ApiError {error, detail}` body into a typed
exception every page renders inline. `src/pages/` has one component per
route (catalogs → catalog → namespace → table, plus per-catalog
consumers and partitions/compaction-debt, and the top-level metrics
page), `src/components/` the shared chrome (topbar with `/healthz`
polling + readiness tooltip and the instance-name badge, breadcrumbs,
badges, skeleton loaders, error boxes), and `src/lib/format.ts` the
humanizers (bytes, counts, partition transforms). Tests in `test/`
render each page against hand-written fixtures shaped exactly like the
spec's schemas and cover both the happy path and an API-error path per
page.
