import { describe, expect, it, vi } from "vitest";
import {
  ApiError,
  buildUrl,
  createCatalog,
  getTable,
  listConsumerOffsets,
  listFiles,
  listSnapshots,
  planScan,
} from "../src/api/client";
import {
  catalogsFixture,
  conflictError,
  filesFixture,
  notFoundError,
  scanFixture,
  snapshotsPage1,
  tableFixture,
} from "./fixtures";
import { jsonResponse, mockFetch } from "./helpers";

describe("buildUrl", () => {
  it("prefixes /v1 and omits empty query", () => {
    expect(buildUrl("/catalogs")).toBe("/v1/catalogs");
  });

  it("serializes defined query params and drops undefined ones", () => {
    expect(buildUrl("/catalogs/c/snapshots", { after: 42, limit: undefined })).toBe(
      "/v1/catalogs/c/snapshots?after=42",
    );
  });
});

describe("api client", () => {
  it("hits GET /v1/catalogs and returns the parsed body as-is (snake_case)", async () => {
    const fetchMock = mockFetch((url) =>
      url === "/v1/catalogs" ? jsonResponse(catalogsFixture) : undefined,
    );
    const catalogs = await import("../src/api/client").then((m) =>
      m.listCatalogs(),
    );
    expect(catalogs).toEqual(catalogsFixture);
    expect(catalogs[0].data_path).toBe("s3://hog-lake/analytics");
    expect(catalogs[0].head_snapshot_id).toBe("4211");
    expect(fetchMock).toHaveBeenCalledOnce();
  });

  it("percent-encodes path segments", async () => {
    const fetchMock = mockFetch(() => jsonResponse(tableFixture));
    await getTable("my catalog", "ns/1", "tab#le");
    expect(fetchMock).toHaveBeenCalledWith(
      "/v1/catalogs/my%20catalog/namespaces/ns%2F1/tables/tab%23le",
      expect.anything(),
    );
  });

  it("appends ?snapshot= for time travel on table, files, and scan", async () => {
    const fetchMock = mockFetch((url) => {
      if (url.endsWith("/files?snapshot=4100")) return jsonResponse(filesFixture);
      if (url.endsWith("/scan?snapshot=4100")) return jsonResponse(scanFixture);
      if (url.endsWith("/tables/pageviews?snapshot=4100"))
        return jsonResponse(tableFixture);
      return undefined;
    });
    await getTable("analytics", "events", "pageviews", "4100");
    await listFiles("analytics", "events", "pageviews", "4100");
    await planScan("analytics", "events", "pageviews", "4100");
    expect(fetchMock).toHaveBeenCalledTimes(3);
  });

  it("passes after/limit to the snapshots endpoint", async () => {
    const fetchMock = mockFetch(() => jsonResponse(snapshotsPage1));
    const page = await listSnapshots("analytics", { after: "4208", limit: 50 });
    expect(fetchMock).toHaveBeenCalledWith(
      "/v1/catalogs/analytics/snapshots?after=4208&limit=50",
      expect.anything(),
    );
    expect(page.has_more).toBe(true);
  });

  it("passes before/limit for descending pagination", async () => {
    const fetchMock = mockFetch(() => jsonResponse(snapshotsPage1));
    await listSnapshots("analytics", { before: "4212", limit: 50 });
    expect(fetchMock).toHaveBeenCalledWith(
      "/v1/catalogs/analytics/snapshots?before=4212&limit=50",
      expect.anything(),
    );
  });

  it("refuses to combine before with a non-zero after (server 422)", async () => {
    const fetchMock = mockFetch(() => jsonResponse(snapshotsPage1));
    expect(() =>
      listSnapshots("analytics", { before: "4212", after: "7" }),
    ).toThrow(/mutually exclusive/);
    expect(fetchMock).not.toHaveBeenCalled();
    // after=0 is the spec's "unset" — allowed alongside before.
    await listSnapshots("analytics", { before: "4212", after: "0" });
    expect(fetchMock).toHaveBeenCalledWith(
      "/v1/catalogs/analytics/snapshots?after=0&before=4212",
      expect.anything(),
    );
  });

  it("POSTs snake_case request bodies untouched", async () => {
    const fetchMock = mockFetch(() => jsonResponse(catalogsFixture[0], 201));
    await createCatalog({ name: "analytics", data_path: "s3://hog-lake/analytics" });
    const [url, init] = fetchMock.mock.calls[0];
    expect(url).toBe("/v1/catalogs");
    expect(init?.method).toBe("POST");
    expect(JSON.parse(init?.body as string)).toEqual({
      name: "analytics",
      data_path: "s3://hog-lake/analytics",
    });
  });

  it("unwraps ApiError {error, detail} bodies with the HTTP status", async () => {
    mockFetch(() => jsonResponse(notFoundError, 404));
    const err = await listConsumerOffsets("nope", "c1").catch((e: unknown) => e);
    expect(err).toBeInstanceOf(ApiError);
    const apiErr = err as ApiError;
    expect(apiErr.status).toBe(404);
    expect(apiErr.error).toBe("not_found");
    expect(apiErr.detail).toBe("catalog 'nope' does not exist");
    expect(apiErr.message).toBe("not_found: catalog 'nope' does not exist");
  });

  it("unwraps 409 conflicts", async () => {
    mockFetch(() => jsonResponse(conflictError, 409));
    const err = await createCatalog({
      name: "analytics",
      data_path: "s3://x",
    }).catch((e: unknown) => e);
    expect(err).toBeInstanceOf(ApiError);
    expect((err as ApiError).status).toBe(409);
    expect((err as ApiError).error).toBe("conflict");
  });

  it("survives non-JSON error bodies", async () => {
    vi.stubGlobal(
      "fetch",
      vi.fn(async () => new Response("<html>bad gateway</html>", { status: 502 })),
    );
    const err = await getTable("a", "b", "c").catch((e: unknown) => e);
    expect(err).toBeInstanceOf(ApiError);
    expect((err as ApiError).status).toBe(502);
    expect((err as ApiError).error).toBe("HTTP 502");
  });
});
