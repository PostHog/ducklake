// State / pagination / time-travel adversarial tests.

import { screen, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { describe, expect, it, vi } from "vitest";
import { filesFixture, tableFixture } from "./fixtures";
import {
  bigIntCatalogWireBody,
  bigIntSnapshotsPage1WireBody,
} from "./fixtures.adversarial";
import { jsonResponse, mockFetch, renderApp } from "./helpers";

const tBase = "/v1/catalogs/analytics/namespaces/events/tables/pageviews";
const tRoute = "/catalogs/analytics/namespaces/events/tables/pageviews";

function tableHandler(url: string): Response | undefined {
  const [path] = url.split("?");
  if (path === tBase) return jsonResponse(tableFixture);
  if (path === `${tBase}/files`) return jsonResponse(filesFixture);
  return undefined;
}

function rawJson(body: string): Response {
  return new Response(body, {
    status: 200,
    headers: { "Content-Type": "application/json" },
  });
}

describe("time-travel snapshot selector", () => {
  // int64 regression: the typed id must reach the wire verbatim — ids above
  // 2^53 must not be re-rounded through Number on the way out.
  it("requests exactly the snapshot id the operator typed", async () => {
    const fetchMock = mockFetch(tableHandler);
    renderApp(tRoute);
    const user = userEvent.setup();

    await screen.findByText("1,234,567");
    await user.type(screen.getByLabelText("snapshot id"), "9007199254740993");
    await user.click(screen.getByRole("button", { name: "Go" }));

    await screen.findByText(/@ snapshot 9007199254740993/);
    expect(fetchMock).toHaveBeenCalledWith(
      `${tBase}?snapshot=9007199254740993`,
      expect.anything(),
    );
  });

  // A non-numeric ?snapshot URL param (typo, mangled link) is ignored —
  // treated as head — instead of becoming "@ snapshot NaN" plus a
  // ?snapshot=NaN request the server 400s.
  it("ignores a non-numeric ?snapshot URL param", async () => {
    const fetchMock = mockFetch((url) => {
      if (url.includes("snapshot=NaN"))
        return jsonResponse({ error: "bad_request", detail: "snapshot" }, 400);
      return tableHandler(url);
    });
    renderApp(`${tRoute}?snapshot=garbage`);

    await screen.findByRole("heading", { name: /pageviews/ });
    await screen.findByText("1,234,567"); // renders head, not an error
    expect(screen.queryByText(/NaN/)).not.toBeInTheDocument();
    const urls = fetchMock.mock.calls.map((c) => String(c[0]));
    expect(urls.some((u) => u.includes("snapshot=NaN"))).toBe(false);
    expect(urls.some((u) => u.includes("snapshot=garbage"))).toBe(false);
  });

  it("rejects a non-numeric typed snapshot id inline without a request", async () => {
    const fetchMock = mockFetch(tableHandler);
    renderApp(tRoute);
    const user = userEvent.setup();

    await screen.findByText("1,234,567");
    await user.type(screen.getByLabelText("snapshot id"), "12garbage");
    await user.click(screen.getByRole("button", { name: "Go" }));

    expect(
      await screen.findByText("snapshot id must be a non-negative integer"),
    ).toBeInTheDocument();
    const urls = fetchMock.mock.calls.map((c) => String(c[0]));
    expect(urls.some((u) => u.includes("snapshot="))).toBe(false);
  });
});

describe("snapshot timeline pagination", () => {
  // The timeline walks DESCENDING from head+1 via the `before` cursor, so a
  // deep catalog shows head first — never the oldest page dressed up as a
  // newest-first timeline — and `after` is never sent alongside `before`.
  it("shows the newest snapshots first for a deep catalog", async () => {
    const newest50 = Array.from({ length: 50 }, (_, i) => ({
      snapshot_id: String(2002 - i),
      snapshot_time: `2026-09-01T00:00:${String(i % 60).padStart(2, "0")}Z`,
      schema_version: "1",
    }));
    const fetchMock = mockFetch((url) => {
      if (url === "/v1/catalogs/big")
        return jsonResponse({
          name: "big",
          data_path: "s3://x/big",
          head_snapshot_id: "2002",
          schema_version: "2",
        });
      if (url === "/v1/catalogs/big/namespaces") return jsonResponse([]);
      if (url === "/v1/catalogs/big/snapshots?before=2003&limit=50")
        return jsonResponse({ snapshots: newest50, has_more: true });
      return undefined;
    });
    renderApp("/catalogs/big");

    const panel = (
      await screen.findByRole("heading", { name: "Snapshots" })
    ).closest("section")!;
    // The top row of the timeline is head (2002).
    await within(panel).findByText("2002");
    const rows = within(panel).getAllByRole("row");
    expect(rows[1]).toHaveTextContent("2002");
    // Deeper pages are reachable and no request ever carried `after`.
    expect(
      within(panel).getByRole("button", { name: "Load more" }),
    ).toBeInTheDocument();
    const urls = fetchMock.mock.calls.map((c) => String(c[0]));
    expect(urls.some((u) => u.includes("after="))).toBe(false);
  });

  it("pages older with the last id as the next before cursor, stopping on has_more=false", async () => {
    const page1 = {
      snapshots: [
        { snapshot_id: "12", snapshot_time: "2026-09-01T00:00:02Z", schema_version: "1" },
        { snapshot_id: "11", snapshot_time: "2026-09-01T00:00:01Z", schema_version: "1" },
      ],
      has_more: true,
    };
    const page2 = {
      snapshots: [
        { snapshot_id: "10", snapshot_time: "2026-09-01T00:00:00Z", schema_version: "1" },
      ],
      has_more: false,
    };
    const fetchMock = mockFetch((url) => {
      if (url === "/v1/catalogs/scratch")
        return jsonResponse({
          name: "scratch",
          data_path: "s3://x/s",
          head_snapshot_id: "12",
          schema_version: "1",
        });
      if (url === "/v1/catalogs/scratch/namespaces") return jsonResponse([]);
      if (url === "/v1/catalogs/scratch/snapshots?before=13&limit=50")
        return jsonResponse(page1);
      if (url === "/v1/catalogs/scratch/snapshots?before=11&limit=50")
        return jsonResponse(page2);
      return undefined;
    });
    renderApp("/catalogs/scratch");
    const user = userEvent.setup();

    await user.click(await screen.findByRole("button", { name: "Load more" }));
    await screen.findByText("10");
    expect(fetchMock).toHaveBeenCalledWith(
      "/v1/catalogs/scratch/snapshots?before=11&limit=50",
      expect.anything(),
    );
    // has_more=false on the last page: the feed ends.
    expect(
      screen.queryByRole("button", { name: "Load more" }),
    ).not.toBeInTheDocument();
  });

  // int64 regression: head+1 and the page cursor are computed in exact
  // arithmetic — a catalog whose head is above 2^53 pages correctly.
  it("pages a >2^53 catalog head with exact cursors", async () => {
    const fetchMock = mockFetch((url) => {
      if (url === "/v1/catalogs/bigcat") return rawJson(bigIntCatalogWireBody);
      if (url === "/v1/catalogs/bigcat/namespaces") return jsonResponse([]);
      if (
        url ===
        "/v1/catalogs/bigcat/snapshots?before=9007199254740998&limit=50"
      )
        return rawJson(bigIntSnapshotsPage1WireBody);
      if (
        url ===
        "/v1/catalogs/bigcat/snapshots?before=9007199254740995&limit=50"
      )
        return jsonResponse({ snapshots: [], has_more: false });
      return undefined;
    });
    renderApp("/catalogs/bigcat");
    const user = userEvent.setup();

    // Ids render exactly (odd values above 2^53, never the even neighbour).
    const panel = (
      await screen.findByRole("heading", { name: "Snapshots" })
    ).closest("section")!;
    await within(panel).findByText("9007199254740997");
    expect(within(panel).getByText("9007199254740995")).toBeInTheDocument();
    expect(screen.queryByText("9007199254740992")).not.toBeInTheDocument();
    expect(screen.queryByText("9007199254740996")).not.toBeInTheDocument();

    await user.click(screen.getByRole("button", { name: "Load more" }));
    await vi.waitFor(() =>
      expect(fetchMock).toHaveBeenCalledWith(
        "/v1/catalogs/bigcat/snapshots?before=9007199254740995&limit=50",
        expect.anything(),
      ),
    );
  });
});

describe("create-form submission", () => {
  it("does not double-POST when Enter is pressed during a pending create", async () => {
    let posts = 0;
    let release: (() => void) | undefined;
    const gate = new Promise<void>((r) => (release = r));
    const fn = vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
      const url = String(input);
      if (url === "/healthz") return new Response("ok", { status: 200 });
      if (url === "/v1/catalogs" && init?.method === "POST") {
        posts += 1;
        await gate; // hold the first create in flight
        return jsonResponse({
          name: "ui-qe-x",
          data_path: "s3://x",
          head_snapshot_id: "0",
          schema_version: "0",
        });
      }
      if (url === "/v1/catalogs") return jsonResponse([]);
      throw new Error(`Unhandled fetch: ${url}`);
    });
    vi.stubGlobal("fetch", fn);
    renderApp("/");
    const user = userEvent.setup();

    const nameInput = await screen.findByLabelText("name");
    await user.type(nameInput, "ui-qe-x");
    await user.type(screen.getByLabelText("data_path"), "s3://x");
    await user.keyboard("{Enter}"); // submit #1 — now pending
    expect(
      await screen.findByRole("button", { name: "Creating…" }),
    ).toBeInTheDocument();
    await user.click(nameInput);
    await user.keyboard("{Enter}"); // implicit submission while pending
    release?.();
    await vi.waitFor(() =>
      expect(screen.getByRole("button", { name: "Create" })).toBeInTheDocument(),
    );
    expect(posts).toBe(1);
  });
});
