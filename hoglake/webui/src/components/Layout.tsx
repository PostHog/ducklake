import { useQuery } from "@tanstack/react-query";
import { Link, Outlet, useParams } from "react-router-dom";
import { checkHealth } from "../api/client";

function HealthIndicator() {
  const { data, isPending } = useQuery({
    queryKey: ["healthz"],
    queryFn: checkHealth,
    refetchInterval: 10_000,
    refetchIntervalInBackground: false,
  });
  const state = isPending ? "unknown" : data ? "ok" : "down";
  const label = isPending ? "checking" : data ? "healthy" : "unreachable";
  return (
    <span className={`health health-${state}`} title="GET /healthz">
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
        <Breadcrumbs />
        <div className="topbar-right">
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
