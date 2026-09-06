// XSS audit guard. The live server accepts HTML-shaped payloads in
// namespace names, table names, column names, commit authors and commit
// messages (verified 2026-09-05 — only catalog names are pattern-checked).
// The webui renders all of them through JSX text/attribute positions and has
// no dangerouslySetInnerHTML / innerHTML / SVG-string sinks, so React's
// escaping holds everywhere. These tests pin that: if anyone ever introduces
// a raw-HTML rendering path for these fields, they go red.

import { screen, within } from "@testing-library/react";
import { describe, expect, it } from "vitest";
import { catalogsFixture, tablesFixture } from "./fixtures";
import {
  XSS_AUTHOR,
  XSS_MESSAGE,
  XSS_NS,
  hostileNamespacesFixture,
  hostileSnapshotsFixture,
  hostileTableFixture,
} from "./fixtures.adversarial";
import { jsonResponse, mockFetch, renderApp } from "./helpers";

const catBase = "/v1/catalogs/analytics";

function noInjectedMarkup(container: HTMLElement) {
  // If any hostile string were parsed as HTML instead of text, one of these
  // elements would exist somewhere in the document.
  expect(container.querySelector("script")).toBeNull();
  expect(container.querySelector("img")).toBeNull();
  expect(container.querySelector("svg")).toBeNull();
}

describe("hostile strings render as text, never markup", () => {
  it("namespace names and commit author/message on the catalog page", async () => {
    mockFetch((url) => {
      if (url === catBase) return jsonResponse(catalogsFixture[0]);
      if (url === `${catBase}/namespaces`)
        return jsonResponse(hostileNamespacesFixture);
      if (url === `${catBase}/snapshots?before=4212&limit=50`)
        return jsonResponse(hostileSnapshotsFixture);
      return undefined;
    });
    renderApp("/catalogs/analytics");

    // The payloads appear verbatim as text nodes…
    expect(await screen.findByText(XSS_NS)).toBeInTheDocument();
    expect(await screen.findByText(XSS_AUTHOR)).toBeInTheDocument();
    expect(await screen.findByText(XSS_MESSAGE)).toBeInTheDocument();
    // …and were not parsed into elements.
    noInjectedMarkup(document.body);
    // The namespace link URL-encodes the hostile name rather than splicing it.
    const link = screen.getByRole("link", { name: XSS_NS });
    expect(link.getAttribute("href")).toContain(encodeURIComponent(XSS_NS));
  });

  it("table and column names on the table page (incl. title attributes)", async () => {
    const tBase = "/v1/catalogs/analytics/namespaces/qens/tables/t1";
    mockFetch((url) => {
      const [path] = url.split("?");
      if (path === tBase) return jsonResponse(hostileTableFixture);
      return undefined;
    });
    renderApp("/catalogs/analytics/namespaces/qens/tables/t1");

    expect(await screen.findByText("c1 with spaces <b>")).toBeInTheDocument();
    noInjectedMarkup(document.body);
    // CopyButton builds its title by string concat — React sets it via
    // setAttribute, so it stays an attribute value, not markup.
    const copy = screen.getByRole("button", { name: /copy/i });
    expect(copy.getAttribute("title")).toBe("Copy table_uuid");
  });

  it("ApiError detail (server echoes hostile names back) in the error box", async () => {
    const detail =
      "invalid catalog name 'ui-qe-xss-<img src=x onerror=alert(1)>' " +
      "(must match ^[a-z][a-z0-9_-]{0,62}$)";
    mockFetch((url) => {
      if (url === "/v1/catalogs")
        return jsonResponse({ error: "validation", detail }, 422);
      return undefined;
    });
    renderApp("/");

    const alert = await screen.findByRole("alert");
    expect(within(alert).getByText(detail)).toBeInTheDocument();
    noInjectedMarkup(document.body);
  });

  it("hostile table names still produce well-formed row links", async () => {
    const nsBase = "/v1/catalogs/analytics/namespaces/qens/tables";
    mockFetch((url) => {
      if (url === nsBase)
        return jsonResponse([
          { name: "T<script>", table_uuid: tablesFixture[0].table_uuid },
        ]);
      return undefined;
    });
    renderApp("/catalogs/analytics/namespaces/qens");

    const link = await screen.findByRole("link", { name: "T<script>" });
    expect(link.getAttribute("href")).toContain(
      encodeURIComponent("T<script>"),
    );
    noInjectedMarkup(document.body);
  });
});
