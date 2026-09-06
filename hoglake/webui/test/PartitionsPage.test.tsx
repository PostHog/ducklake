import { screen, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { describe, expect, it } from "vitest";
import { partitionStatsFixture } from "./fixtures";
import { jsonResponse, mockFetch, renderApp } from "./helpers";

const baseUrl = "/v1/catalogs/analytics/stats/partitions?limit=50";

describe("PartitionsPage", () => {
  it("renders partitions ranked by debt with rendered partition values", async () => {
    mockFetch((url) =>
      url === baseUrl ? jsonResponse(partitionStatsFixture) : undefined,
    );
    renderApp("/catalogs/analytics/partitions");

    expect(
      await screen.findByText("team_id=42 / month=2026-09"),
    ).toBeInTheDocument();
    expect(screen.getByText("unpartitioned")).toBeInTheDocument();

    // Server order (debt desc) is preserved: pageviews row above clicks row.
    const rows = screen.getAllByRole("row").slice(1); // drop header
    expect(within(rows[0]).getByText("events.pageviews")).toBeInTheDocument();
    expect(within(rows[1]).getByText("events.clicks")).toBeInTheDocument();

    // Columns: files / small files / sizes / DVs / score.
    expect(within(rows[0]).getByText("120")).toBeInTheDocument();
    expect(within(rows[0]).getAllByText("118").length).toBeGreaterThan(0);
    expect(within(rows[0]).getByText("5.0 GiB")).toBeInTheDocument();
    expect(within(rows[0]).getByText("42.7 MiB")).toBeInTheDocument();
    expect(within(rows[0]).getByText("2")).toBeInTheDocument();

    // Debt bar reflects small_file_count vs file_count.
    const meter = within(rows[0]).getByRole("meter");
    expect(meter).toHaveAttribute("aria-valuenow", "98");
    expect(meter).toHaveAttribute(
      "title",
      "118 of 120 files under the small-file threshold",
    );

    // No truncation banner when the server says complete.
    expect(screen.queryByRole("status")).not.toBeInTheDocument();
  });

  it("displays >2^53 byte totals int64-exactly", async () => {
    // Hand-built raw body: total_bytes as a bare JSON number above 2^53,
    // exactly as the server emits it. JSON.stringify cannot produce this.
    const body =
      '{"partitions":[{"namespace":"events","table":"pageviews",' +
      '"table_uuid":"3f2c9c04-8a1b-4c7e-9f10-6d2a5b3e8c71",' +
      '"partition_values":[{"field":"team_id","value":"42"}],' +
      '"spec_id":3,"file_count":120,"small_file_count":118,' +
      '"total_bytes":9007199254740993027,"small_file_bytes":943718400,' +
      '"avg_file_bytes":75059993789508,"dv_count":2,"debt_score":118}],' +
      '"truncated":false}';
    mockFetch((url) =>
      url === baseUrl
        ? new Response(body, {
            status: 200,
            headers: { "Content-Type": "application/json" },
          })
        : undefined,
    );
    renderApp("/catalogs/analytics/partitions");

    // The exact decimal value survives (never rounded through a double) and
    // is exposed as the cell's title alongside the humanized size.
    const cell = await screen.findByTitle("9007199254740993027");
    expect(cell).toBeInTheDocument();
    expect(screen.getByTitle("75059993789508")).toBeInTheDocument();
  });

  it("wires the namespace/table filters into the query params", async () => {
    const filtered = {
      partitions: [partitionStatsFixture.partitions[0]],
      truncated: false,
    };
    const filteredUrl =
      "/v1/catalogs/analytics/stats/partitions?namespace=events&table=pageviews&limit=50";
    const fetchMock = mockFetch((url) => {
      if (url === baseUrl) return jsonResponse(partitionStatsFixture);
      if (url === filteredUrl) return jsonResponse(filtered);
      return undefined;
    });
    renderApp("/catalogs/analytics/partitions");
    await screen.findByText("unpartitioned");

    const user = userEvent.setup();
    await user.type(screen.getByLabelText("namespace filter"), "events");
    await user.type(screen.getByLabelText("table filter"), "pageviews");
    await user.click(screen.getByRole("button", { name: "Apply" }));

    expect(
      await screen.findByText("team_id=42 / month=2026-09"),
    ).toBeInTheDocument();
    expect(screen.queryByText("unpartitioned")).not.toBeInTheDocument();
    expect(fetchMock).toHaveBeenCalledWith(filteredUrl, expect.anything());
  });

  it("shows the truncated banner when the server cut the list", async () => {
    mockFetch((url) =>
      url === baseUrl
        ? jsonResponse({ ...partitionStatsFixture, truncated: true })
        : undefined,
    );
    renderApp("/catalogs/analytics/partitions");

    const banner = await screen.findByRole("status");
    expect(banner).toHaveTextContent("Showing the top 50 partitions by debt");
  });

  it("shows an empty state when nothing owes debt", async () => {
    mockFetch((url) =>
      url === baseUrl
        ? jsonResponse({ partitions: [], truncated: false })
        : undefined,
    );
    renderApp("/catalogs/analytics/partitions");

    expect(
      await screen.findByText(
        "No partitions match — nothing owes compaction debt here.",
      ),
    ).toBeInTheDocument();
  });

  it("surfaces an API error inline", async () => {
    mockFetch((url) =>
      url === baseUrl
        ? jsonResponse(
            { error: "not_found", detail: "catalog 'analytics' does not exist" },
            404,
          )
        : undefined,
    );
    renderApp("/catalogs/analytics/partitions");

    const alert = await screen.findByRole("alert");
    expect(alert).toHaveTextContent("not_found");
  });
});
