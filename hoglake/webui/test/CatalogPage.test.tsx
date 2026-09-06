import { screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { describe, expect, it } from "vitest";
import {
  catalogsFixture,
  conflictError,
  namespacesFixture,
  snapshotsPage1,
  snapshotsPage2,
} from "./fixtures";
import { jsonResponse, mockFetch, renderApp } from "./helpers";

const base = "/v1/catalogs/analytics";

// head_snapshot_id is 4211, so the timeline's first page asks for
// everything below head+1 = 4212, newest first.
function happyHandler(url: string): Response | undefined {
  if (url === base) return jsonResponse(catalogsFixture[0]);
  if (url === `${base}/namespaces`) return jsonResponse(namespacesFixture);
  if (url === `${base}/snapshots?before=4212&limit=50`)
    return jsonResponse(snapshotsPage1);
  if (url === `${base}/snapshots?before=4210&limit=50`)
    return jsonResponse(snapshotsPage2);
  return undefined;
}

describe("CatalogPage", () => {
  it("renders namespaces and the snapshot timeline newest-first with change badges", async () => {
    mockFetch(happyHandler);
    renderApp("/catalogs/analytics");

    expect(await screen.findByText("events")).toBeInTheDocument();
    expect(screen.getByText("sessions")).toBeInTheDocument();

    // Snapshots: head (4211) sorts above 4210.
    expect(await screen.findByText("4210")).toBeInTheDocument();
    const rows = screen.getAllByRole("row");
    const idx4211 = rows.findIndex((r) => r.textContent?.includes("4211"));
    const idx4210 = rows.findIndex((r) => r.textContent?.includes("4210"));
    expect(idx4211).toBeGreaterThan(-1);
    expect(idx4211).toBeLessThan(idx4210);

    expect(screen.getByText("ops")).toBeInTheDocument();
    expect(screen.getByText("create table events.clicks")).toBeInTheDocument();
    expect(screen.getByText("table_created")).toBeInTheDocument();
    expect(screen.getByText("files_added")).toBeInTheDocument();

    // has_more=true → Load more is offered.
    expect(screen.getByRole("button", { name: "Load more" })).toBeInTheDocument();
  });

  it("pages the timeline older via before/limit when Load more is clicked", async () => {
    const fetchMock = mockFetch(happyHandler);
    renderApp("/catalogs/analytics");
    const user = userEvent.setup();

    await user.click(await screen.findByRole("button", { name: "Load more" }));
    expect(await screen.findByText("append 3 files")).toBeInTheDocument();
    expect(fetchMock).toHaveBeenCalledWith(
      `${base}/snapshots?before=4210&limit=50`,
      expect.anything(),
    );
    // No request ever combined the descending cursor with `after`.
    const urls = fetchMock.mock.calls.map((c) => String(c[0]));
    expect(urls.some((u) => u.includes("after="))).toBe(false);
    // Page 2 ends the feed.
    expect(screen.queryByRole("button", { name: "Load more" })).not.toBeInTheDocument();
  });

  it("shows the ApiError detail inline when namespace creation conflicts", async () => {
    mockFetch((url, init) => {
      if (url === `${base}/namespaces` && init?.method === "POST")
        return jsonResponse(conflictError, 409);
      return happyHandler(url);
    });
    renderApp("/catalogs/analytics");
    const user = userEvent.setup();

    await screen.findByText("events");
    await user.type(screen.getByLabelText("name"), "events");
    await user.click(screen.getByRole("button", { name: "Create" }));

    const alert = await screen.findByRole("alert");
    expect(alert).toHaveTextContent("conflict");
    expect(alert).toHaveTextContent("catalog 'analytics' already exists");
  });

  it("rejects an invalid namespace name client-side without a POST", async () => {
    const fetchMock = mockFetch(happyHandler);
    renderApp("/catalogs/analytics");
    const user = userEvent.setup();

    await screen.findByText("events");
    await user.type(screen.getByLabelText("name"), "9starts-with-digit");
    expect(
      await screen.findByText(
        "name must start with a letter or underscore and use only letters, digits, underscore, or hyphen",
      ),
    ).toBeInTheDocument();
    expect(screen.getByRole("button", { name: "Create" })).toBeDisabled();
    const posts = fetchMock.mock.calls.filter(
      (c) => (c[1] as RequestInit | undefined)?.method === "POST",
    );
    expect(posts).toHaveLength(0);
  });

  it("surfaces a server 422 inline if an invalid name slips through", async () => {
    mockFetch((url, init) => {
      if (url === `${base}/namespaces` && init?.method === "POST")
        return jsonResponse(
          {
            error: "validation_failed",
            detail: "name must match ^[A-Za-z_][A-Za-z0-9_-]{0,127}$",
          },
          422,
        );
      return happyHandler(url);
    });
    renderApp("/catalogs/analytics");
    const user = userEvent.setup();

    await screen.findByText("events");
    // Passes the client-side pattern; the server still rejects (e.g. a
    // reserved name or a stricter server build).
    await user.type(screen.getByLabelText("name"), "valid_name");
    await user.click(screen.getByRole("button", { name: "Create" }));

    const alert = await screen.findByRole("alert");
    expect(alert).toHaveTextContent("422");
    expect(alert).toHaveTextContent("validation_failed");
    expect(alert).toHaveTextContent(
      "name must match ^[A-Za-z_][A-Za-z0-9_-]{0,127}$",
    );
  });
});
