import { useQuery } from "@tanstack/react-query";
import { Link, Outlet, useParams } from "react-router-dom";
import { checkHealth, getInstanceInfo } from "../api/client";

function InstanceName() {
  const { data } = useQuery({
    queryKey: ["instance-info"],
    queryFn: getInstanceInfo,
    staleTime: Infinity,
    retry: false,
  });
  if (!data?.name) return null;
  return <span className="instance-name">{data.name}</span>;
}

function HealthIndicator() {
  const { data, isPending } = useQuery({
    queryKey: ["healthz"],
    queryFn: checkHealth,
    refetchInterval: 10_000,
    refetchIntervalInBackground: false,
  });
  const state = isPending ? "unknown" : data ? "ok" : "down";
  const label = isPending ? "checking" : data ? "healthy" : "unreachable";
  const tooltip = isPending
    ? "Polling GET /healthz (every 10s)…"
    : data
      ? "GET /healthz → 200: the server is up and its catalog Postgres " +
        "answered a readiness query (SELECT 1). Polled every 10s."
      : "GET /healthz failed: the server is down, unreachable, or up but " +
        "unable to reach its catalog Postgres (readiness returns 503 on a " +
        "dead pool). Polled every 10s.";
  return (
    <span className={`health health-${state}`} title={tooltip}>
      <span className="health-dot" aria-hidden="true" />
      {label}
    </span>
  );
}

function Breadcrumbs() {
  const { catalog, namespace, table } = useParams();
  return (
    <nav className="breadcrumbs" aria-label="Breadcrumb">
      <Link to="/">catalogs</Link>
      {catalog && (
        <>
          <span className="crumb-sep">/</span>
          <Link to={`/catalogs/${encodeURIComponent(catalog)}`}>{catalog}</Link>
        </>
      )}
      {catalog && namespace && (
        <>
          <span className="crumb-sep">/</span>
          <Link
            to={`/catalogs/${encodeURIComponent(catalog)}/namespaces/${encodeURIComponent(namespace)}`}
          >
            {namespace}
          </Link>
        </>
      )}
      {catalog && namespace && table && (
        <>
          <span className="crumb-sep">/</span>
          <span className="crumb-current">{table}</span>
        </>
      )}
    </nav>
  );
}

export function Layout() {
  return (
    <div className="app">
      <header className="topbar">
        <Link to="/" className="brand">
          hoglake
        </Link>
        <InstanceName />
        <Breadcrumbs />
        <div className="topbar-right">
          <Link to="/metrics">metrics</Link>
          <a href="/openapi.yaml" target="_blank" rel="noreferrer">
            openapi.yaml
          </a>
          <HealthIndicator />
        </div>
      </header>
      <main className="content">
        <Outlet />
      </main>
    </div>
  );
}
