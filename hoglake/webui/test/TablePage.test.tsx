import { screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { describe, expect, it } from "vitest";
import { filesFixture, notFoundError, scanFixture, tableFixture } from "./fixtures";
import { bigIntFilesWireBody } from "./fixtures.adversarial";
import { jsonResponse, mockFetch, renderApp } from "./helpers";

const base = "/v1/catalogs/analytics/namespaces/events/tables/pageviews";
const route = "/catalogs/analytics/namespaces/events/tables/pageviews";

function happyHandler(url: string): Response | undefined {
  const [path] = url.split("?");
  if (path === base) return jsonResponse(tableFixture);
  if (path === `${base}/files`) return jsonResponse(filesFixture);
  if (path === `${base}/scan`) return jsonResponse(scanFixture);
  return undefined;
}

describe("TablePage", () => {
  it("renders the stats header and schema tab with partition spec", async () => {
    mockFetch(happyHandler);
    renderApp(route);

    // Stats header, human formatted.
    expect(await screen.findByText("1,234,567")).toBeInTheDocument();
    expect(screen.getByText("42")).toBeInTheDocument();
    expect(screen.getByText("5.0 GiB")).toBeInTheDocument();
    expect(
      screen.getByText("3f2c9c04-8a1b-4c7e-9f10-6d2a5b3e8c71"),
    ).toBeInTheDocument();
    expect(screen.getByRole("button", { name: /copy/i })).toBeInTheDocument();

    // Schema columns.
    expect(screen.getByText("ts")).toBeInTheDocument();
    expect(screen.getByText("timestamptz")).toBeInTheDocument();
    expect(screen.getByText("user_id")).toBeInTheDocument();
    expect(screen.getAllByText("not null")).toHaveLength(2);

    // Partition spec rendering, source field resolved to column names.
    expect(screen.getByText("day(ts)")).toBeInTheDocument();
    expect(screen.getByText("bucket(16, url)")).toBeInTheDocument();
  });

  it("shows data files with color-coded stats_state badges on the Files tab", async () => {
    mockFetch(happyHandler);
    renderApp(route);
    const user = userEvent.setup();

    await user.click(await screen.findByRole("tab", { name: "files" }));

    expect(await screen.findByText("101")).toBeInTheDocument();
    expect(
      screen.getByText("s3://hog-lake/analytics/events/pageviews/data-00101.parquet"),
    ).toBeInTheDocument();
    expect(screen.getByText("500,000")).toBeInTheDocument();

    const provided = screen.getByText("provided");
    const pending = screen.getByText("pending");
    const failed = screen.getByText("failed");
    expect(provided).toHaveClass("stats-provided");
    expect(pending).toHaveClass("stats-pending");
    expect(failed).toHaveClass("stats-failed");

    // partition_values render when present, em-dash when absent.
    expect(screen.getByText("[2026-09-01, 7]")).toBeInTheDocument();
    expect(screen.getByText("[2026-09-02, null]")).toBeInTheDocument();
  });

  it("pairs data files with deletion vectors on the Scan tab", async () => {
    mockFetch(happyHandler);
    renderApp(route);
    const user = userEvent.setup();

    await user.click(await screen.findByRole("tab", { name: "scan" }));

    expect(
      await screen.findByText("s3://hog-lake/analytics/events/pageviews/dv-00900.puffin"),
    ).toBeInTheDocument();
    expect(screen.getByText("1,250")).toBeInTheDocument();
    // The file without a DV shows "none".
    expect(screen.getByText("none")).toBeInTheDocument();
  });

  it("drives ?snapshot through the selector (time travel)", async () => {
    const fetchMock = mockFetch(happyHandler);
    renderApp(route);
    const user = userEvent.setup();

    await screen.findByText("1,234,567");
    await user.type(screen.getByLabelText("snapshot id"), "4100");
    await user.click(screen.getByRole("button", { name: "Go" }));

    expect(await screen.findByText("@ snapshot 4100")).toBeInTheDocument();
    expect(fetchMock).toHaveBeenCalledWith(
      `${base}?snapshot=4100`,
      expect.anything(),
    );

    // Files tab inherits the snapshot.
    await user.click(screen.getByRole("tab", { name: "files" }));
    await screen.findByText("500,000");
    expect(fetchMock).toHaveBeenCalledWith(
      `${base}/files?snapshot=4100`,
      expect.anything(),
    );
  });

  it("renders int64 file values above 2^53 exactly on the Files tab", async () => {
    mockFetch((url) => {
      const [path] = url.split("?");
      if (path === base) return jsonResponse(tableFixture);
      if (path === `${base}/files`)
        return new Response(bigIntFilesWireBody, {
          status: 200,
          headers: { "Content-Type": "application/json" },
        });
      return undefined;
    });
    renderApp(route);
    const user = userEvent.setup();

    await user.click(await screen.findByRole("tab", { name: "files" }));

    // record_count 2^53+1, grouped exactly (odd, not the even neighbour).
    expect(
      await screen.findByText("9,007,199,254,740,993"),
    ).toBeInTheDocument();
    // row_id_start 2^53+3 verbatim — the lineage anchor an operator reads.
    expect(screen.getByText("9007199254740995")).toBeInTheDocument();
    // file_size_bytes 2^62+1: humanized cell, exact value in the tooltip.
    const sizeCell = screen.getByTitle("4611686018427387905");
    expect(sizeCell).toBeInTheDocument();
    expect(sizeCell.textContent).not.toContain("NaN");
  });

  it("shows the 404 detail when the table does not exist", async () => {
    mockFetch((url) =>
      url.startsWith(base)
        ? jsonResponse(
            { error: "not_found", detail: "table 'pageviews' does not exist" },
            404,
          )
        : undefined,
    );
    renderApp(route);
    const alert = await screen.findByRole("alert");
    expect(alert).toHaveTextContent("not_found");
    expect(alert).toHaveTextContent("table 'pageviews' does not exist");
  });

  it("shows an error on the Files tab without killing the page", async () => {
    mockFetch((url) => {
      const [path] = url.split("?");
      if (path === `${base}/files`) return jsonResponse(notFoundError, 404);
      return happyHandler(url);
    });
    renderApp(route);
    const user = userEvent.setup();

    await screen.findByText("1,234,567");
    await user.click(screen.getByRole("tab", { name: "files" }));
    const alert = await screen.findByRole("alert");
    expect(alert).toHaveTextContent("not_found");
    // Stats header still up.
    expect(screen.getByText("1,234,567")).toBeInTheDocument();
  });
});
