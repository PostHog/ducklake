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

function happyHandler(url: string): Response | undefined {
  if (url === base) return jsonResponse(catalogsFixture[0]);
  if (url === `${base}/namespaces`) return jsonResponse(namespacesFixture);
  if (url === `${base}/snapshots?after=0&limit=50`)
    return jsonResponse(snapshotsPage1);
  if (url === `${base}/snapshots?after=4210&limit=50`)
    return jsonResponse(snapshotsPage2);
  return undefined;
}

describe("CatalogPage", () => {
  it("renders namespaces and the snapshot timeline newest-first with change badges", async () => {
    mockFetch(happyHandler);
    renderApp("/catalogs/analytics");

    expect(await screen.findByText("events")).toBeInTheDocument();
    expect(screen.getByText("sessions")).toBeInTheDocument();

    // Snapshots: newest (4210) sorts above 4209.
    expect(await screen.findByText("4210")).toBeInTheDocument();
    const rows = screen.getAllByRole("row");
    const idx4210 = rows.findIndex((r) => r.textContent?.includes("4210"));
    const idx4209 = rows.findIndex((r) => r.textContent?.includes("4209"));
    expect(idx4210).toBeGreaterThan(-1);
    expect(idx4210).toBeLessThan(idx4209);

    expect(screen.getByText("viaduck")).toBeInTheDocument();
    expect(screen.getByText("append 3 files")).toBeInTheDocument();
    expect(screen.getAllByText("files_added").length).toBe(2);
    expect(screen.getByText("rows_appended")).toBeInTheDocument();

    // has_more=true → Load more is offered.
    expect(screen.getByRole("button", { name: "Load more" })).toBeInTheDocument();
  });

  it("pages the timeline via after/limit when Load more is clicked", async () => {
    const fetchMock = mockFetch(happyHandler);
    renderApp("/catalogs/analytics");
    const user = userEvent.setup();

    await user.click(await screen.findByRole("button", { name: "Load more" }));
    expect(await screen.findByText("create table events.clicks")).toBeInTheDocument();
    expect(fetchMock).toHaveBeenCalledWith(
      `${base}/snapshots?after=4210&limit=50`,
      expect.anything(),
    );
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
});
