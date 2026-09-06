import type { ReactElement } from "react";
import { render } from "@testing-library/react";
import { MemoryRouter } from "react-router-dom";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { vi } from "vitest";
import { App } from "../src/App";

export function jsonResponse(body: unknown, status = 200): Response {
  return new Response(JSON.stringify(body), {
    status,
    headers: { "Content-Type": "application/json" },
  });
}

export type FetchHandler = (
  url: string,
  init?: RequestInit,
) => Response | undefined;

/**
 * Hand-rolled fetch mock. The handler dispatches on the request URL (as the
 * client built it — relative, same-origin). /healthz gets a default 200 so
 * the topbar health poll never fails a page test by accident.
 */
export function mockFetch(handler: FetchHandler) {
  const fn = vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
    const url =
      typeof input === "string"
        ? input
        : input instanceof URL
          ? input.toString()
          : input.url;
    const res = handler(url, init);
    if (res) return res;
    if (url === "/healthz") return new Response("ok", { status: 200 });
    // Topbar instance badge; unnamed by default so page tests are unaffected.
    if (url === "/v1/info") return jsonResponse({});
    throw new Error(`Unhandled fetch: ${init?.method ?? "GET"} ${url}`);
  });
  vi.stubGlobal("fetch", fn);
  return fn;
}

export function renderApp(route: string) {
  const queryClient = new QueryClient({
    defaultOptions: { queries: { retry: false } },
  });
  return renderWithProviders(
    <MemoryRouter initialEntries={[route]}>
      <App />
    </MemoryRouter>,
    queryClient,
  );
}

function renderWithProviders(ui: ReactElement, queryClient: QueryClient) {
  return render(
    <QueryClientProvider client={queryClient}>{ui}</QueryClientProvider>,
  );
}
