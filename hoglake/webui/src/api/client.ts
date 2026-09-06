// The single typed API client. All requests are same-origin; the Vite dev
// server proxies /v1, /healthz and /openapi.yaml to the hoglake server.

import type {
  ApiErrorBody,
  Catalog,
  ConsumerList,
  ConsumerOffset,
  CreateCatalogRequest,
  CreateTableRequest,
  DataFile,
  Int64,
  Namespace,
  PartitionStatsResponse,
  ScanFile,
  SnapshotPage,
  Table,
  TableSummary,
} from "./types";
import { parseInt64Json } from "./int64";

export class ApiError extends Error {
  readonly status: number;
  readonly error: string;
  readonly detail?: string;

  constructor(status: number, body: ApiErrorBody) {
    super(body.detail ? `${body.error}: ${body.detail}` : body.error);
    this.name = "ApiError";
    this.status = status;
    this.error = body.error;
    this.detail = body.detail;
  }
}

const BASE = "/v1";

function seg(s: string): string {
  return encodeURIComponent(s);
}

export function buildUrl(
  path: string,
  query?: Record<string, string | number | undefined>,
): string {
  const params = new URLSearchParams();
  if (query) {
    for (const [k, v] of Object.entries(query)) {
      if (v !== undefined) params.set(k, String(v));
    }
  }
  const qs = params.toString();
  return `${BASE}${path}${qs ? `?${qs}` : ""}`;
}

async function parseError(res: Response): Promise<ApiError> {
  let body: ApiErrorBody = { error: `HTTP ${res.status}` };
  try {
    const json = (await res.json()) as unknown;
    if (json && typeof json === "object" && "error" in json) {
      body = json as ApiErrorBody;
    }
  } catch {
    // Non-JSON error body; keep the status-based message.
  }
  return new ApiError(res.status, body);
}

async function request<T>(url: string, init?: RequestInit): Promise<T> {
  const res = await fetch(url, {
    ...init,
    headers: {
      Accept: "application/json",
      ...(init?.body ? { "Content-Type": "application/json" } : {}),
      ...init?.headers,
    },
  });
  if (!res.ok) throw await parseError(res);
  // Parse from the raw text so int64 fields survive exactly (never rounded
  // through a double). See src/api/int64.ts.
  return parseInt64Json(await res.text()) as T;
}

// -- catalogs ---------------------------------------------------------------

export function listCatalogs(): Promise<Catalog[]> {
  return request(buildUrl("/catalogs"));
}

export function createCatalog(req: CreateCatalogRequest): Promise<Catalog> {
  return request(buildUrl("/catalogs"), {
    method: "POST",
    body: JSON.stringify(req),
  });
}

export function getCatalog(catalog: string): Promise<Catalog> {
  return request(buildUrl(`/catalogs/${seg(catalog)}`));
}

// -- namespaces -------------------------------------------------------------

export function listNamespaces(catalog: string): Promise<Namespace[]> {
  return request(buildUrl(`/catalogs/${seg(catalog)}/namespaces`));
}

export function createNamespace(
  catalog: string,
  name: string,
): Promise<Namespace> {
  return request(buildUrl(`/catalogs/${seg(catalog)}/namespaces`), {
    method: "POST",
    body: JSON.stringify({ name }),
  });
}

// -- tables -----------------------------------------------------------------

export function listTables(
  catalog: string,
  namespace: string,
): Promise<TableSummary[]> {
  return request(
    buildUrl(`/catalogs/${seg(catalog)}/namespaces/${seg(namespace)}/tables`),
  );
}

export function createTable(
  catalog: string,
  namespace: string,
  req: CreateTableRequest,
): Promise<Table> {
  return request(
    buildUrl(`/catalogs/${seg(catalog)}/namespaces/${seg(namespace)}/tables`),
    { method: "POST", body: JSON.stringify(req) },
  );
}

export function getTable(
  catalog: string,
  namespace: string,
  table: string,
  snapshot?: Int64,
): Promise<Table> {
  return request(
    buildUrl(
      `/catalogs/${seg(catalog)}/namespaces/${seg(namespace)}/tables/${seg(table)}`,
      { snapshot },
    ),
  );
}

export function listFiles(
  catalog: string,
  namespace: string,
  table: string,
  snapshot?: Int64,
): Promise<DataFile[]> {
  return request(
    buildUrl(
      `/catalogs/${seg(catalog)}/namespaces/${seg(namespace)}/tables/${seg(table)}/files`,
      { snapshot },
    ),
  );
}

export function planScan(
  catalog: string,
  namespace: string,
  table: string,
  snapshot?: Int64,
): Promise<ScanFile[]> {
  return request(
    buildUrl(
      `/catalogs/${seg(catalog)}/namespaces/${seg(namespace)}/tables/${seg(table)}/scan`,
      { snapshot },
    ),
  );
}

// -- snapshots --------------------------------------------------------------

export function listSnapshots(
  catalog: string,
  opts?: { after?: Int64; before?: Int64; limit?: number },
): Promise<SnapshotPage> {
  // The spec makes `before` (descending pagination) mutually exclusive with
  // a non-zero `after` (server answers 422). Refuse to build such a request.
  if (
    opts?.before !== undefined &&
    opts?.after !== undefined &&
    opts.after !== "0"
  ) {
    throw new Error(
      "listSnapshots: `before` and a non-zero `after` are mutually exclusive",
    );
  }
  return request(
    buildUrl(`/catalogs/${seg(catalog)}/snapshots`, {
      after: opts?.after,
      before: opts?.before,
      limit: opts?.limit,
    }),
  );
}

// -- partition stats ----------------------------------------------------------

export function listPartitionStats(
  catalog: string,
  opts?: { namespace?: string; table?: string; limit?: number },
): Promise<PartitionStatsResponse> {
  return request(
    buildUrl(`/catalogs/${seg(catalog)}/stats/partitions`, {
      namespace: opts?.namespace || undefined,
      table: opts?.table || undefined,
      limit: opts?.limit,
    }),
  );
}

// -- consumers --------------------------------------------------------------

export function listConsumerOffsets(
  catalog: string,
  consumer: string,
): Promise<ConsumerOffset[]> {
  return request(
    buildUrl(`/catalogs/${seg(catalog)}/consumers/${seg(consumer)}/offsets`),
  );
}

export function listConsumers(catalog: string): Promise<ConsumerList> {
  return request(buildUrl(`/catalogs/${seg(catalog)}/consumers`));
}

export function getInstanceInfo(): Promise<{ name?: string }> {
  return request(buildUrl("/info"));
}

// -- metrics ------------------------------------------------------------------

/**
 * Raw Prometheus text exposition from the server's /metrics endpoint (the
 * Vite dev server proxies it alongside /v1). Not JSON — parsed by
 * src/lib/prometheus.ts.
 */
export async function fetchMetricsText(): Promise<string> {
  const res = await fetch("/metrics", { headers: { Accept: "text/plain" } });
  if (!res.ok) {
    throw new Error(`GET /metrics failed: HTTP ${res.status}`);
  }
  return res.text();
}

// -- health -----------------------------------------------------------------

export async function checkHealth(): Promise<boolean> {
  try {
    const res = await fetch("/healthz", { headers: { Accept: "*/*" } });
    return res.ok;
  } catch {
    return false;
  }
}
