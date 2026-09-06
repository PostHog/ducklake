import { screen, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { describe, expect, it } from "vitest";
import { mockFetch, renderApp } from "./helpers";

function textResponse(body: string, status = 200): Response {
  return new Response(body, {
    status,
    headers: { "Content-Type": "text/plain; version=0.0.4" },
  });
}

const exposition = [
  "# HELP hoglake_commits_total Commits by result",
  "# TYPE hoglake_commits_total counter",
  'hoglake_commits_total{catalog="analytics",result="committed"} 34.0',
  'hoglake_commits_total{catalog="analytics",result="conflict"} 1.0',
  "# HELP hoglake_table_count Live tables",
  "# TYPE hoglake_table_count gauge",
  "hoglake_table_count 42.0",
  "# TYPE hoglake_stats_pending_files gauge", // HELP-less family
  "hoglake_stats_pending_files 7.0",
  "# HELP hoglake_commit_lock_wait_seconds Lock wait",
  "# TYPE hoglake_commit_lock_wait_seconds histogram",
  'hoglake_commit_lock_wait_seconds_bucket{le="0.001"} 10',
  'hoglake_commit_lock_wait_seconds_bucket{le="0.01"} 30',
  'hoglake_commit_lock_wait_seconds_bucket{le="+Inf"} 40',
  "hoglake_commit_lock_wait_seconds_count 40",
  "hoglake_commit_lock_wait_seconds_sum 0.2",
  "# HELP jvm_memory_used_bytes Used heap",
  "# TYPE jvm_memory_used_bytes gauge",
  'jvm_memory_used_bytes{area="heap"} 1073741824.0',
  "# TYPE http_seconds summary",
  'http_seconds{route="/v1",quantile="0.5"} 0.01',
  'http_seconds{route="/v1",quantile="0.99"} 0.04',
  "",
].join("\n");

const servedMetrics = (body: string) => (url: string) =>
  url === "/metrics" ? textResponse(body) : undefined;

describe("MetricsPage", () => {
  it("renders labeled families as bars, scalars as stat tiles, others collapsed", async () => {
    mockFetch(servedMetrics(exposition));
    renderApp("/metrics");

    // Labeled counter family → horizontal bars with chips + exact values.
    const commits = (
      await screen.findByText("hoglake_commits_total")
    ).closest("section")!;
    expect(
      within(commits).getAllByText("catalog=analytics").length,
    ).toBeGreaterThan(0);
    expect(within(commits).getByText("result=conflict")).toBeInTheDocument();
    expect(within(commits).getByText("34")).toBeInTheDocument();
    expect(within(commits).getByText("1")).toBeInTheDocument();
    expect(within(commits).getByText("Commits by result")).toBeInTheDocument();

    // Scalar gauge → stat tile with the big value.
    const tile = screen.getByText("hoglake_table_count").closest("section")!;
    expect(within(tile).getByText("42")).toBeInTheDocument();
    expect(tile.className).toContain("stat-tile");
    expect(within(tile).getByText("Live tables")).toBeInTheDocument();

    // HELP-less family still renders.
    expect(screen.getByText("hoglake_stats_pending_files")).toBeInTheDocument();

    // Non-hoglake families are collapsed under "everything else".
    const rest = screen.getByText("everything else (2 families)");
    const details = rest.closest("details")!;
    expect(within(details).getByText("jvm_memory_used_bytes")).toBeInTheDocument();
    expect(within(details).getByText("1.0 GiB")).toBeInTheDocument();
  });

  it("renders histogram buckets as a strip with count and mean", async () => {
    mockFetch(servedMetrics(exposition));
    renderApp("/metrics");

    const hist = (
      await screen.findByText("hoglake_commit_lock_wait_seconds")
    ).closest("section")!;
    const strip = within(hist).getByTestId("bucket-strip");
    // Cumulative 10/30/40 → three per-bucket bars (10, 20, 10).
    expect(strip.children).toHaveLength(3);
    expect(strip.children[1]).toHaveAttribute("title", "≤ 0.01: 20");
    expect(within(hist).getByText("40")).toBeInTheDocument(); // count
    expect(within(hist).getByText("5.00 ms")).toBeInTheDocument(); // mean 0.2/40
  });

  it("renders summary quantiles as labeled bars", async () => {
    mockFetch(servedMetrics(exposition));
    renderApp("/metrics");

    await screen.findByText("hoglake_commits_total");
    const details = screen
      .getByText("everything else (2 families)")
      .closest("details")!;
    const summary = within(details).getByText("http_seconds").closest("section")!;
    expect(within(summary).getByText("q=0.5")).toBeInTheDocument();
    expect(within(summary).getByText("q=0.99")).toBeInTheDocument();
    expect(within(summary).getByText("40.00 ms")).toBeInTheDocument();
    expect(within(summary).getByText("route=/v1", { exact: false })).toBeInTheDocument();
  });

  it("filters families by name", async () => {
    mockFetch(servedMetrics(exposition));
    renderApp("/metrics");
    await screen.findByText("hoglake_commits_total");

    const user = userEvent.setup();
    await user.type(screen.getByLabelText("filter metrics"), "commits");

    expect(screen.getByText("hoglake_commits_total")).toBeInTheDocument();
    expect(screen.queryByText("hoglake_table_count")).not.toBeInTheDocument();
    expect(screen.queryByText(/everything else/)).not.toBeInTheDocument();
  });

  it("fetches once and refetches only via the Refresh button", async () => {
    let calls = 0;
    const fetchMock = mockFetch((url) => {
      if (url !== "/metrics") return undefined;
      calls++;
      return textResponse(
        calls === 1
          ? "# TYPE hoglake_table_count gauge\nhoglake_table_count 42.0\n"
          : "# TYPE hoglake_table_count gauge\nhoglake_table_count 43.0\n",
      );
    });
    renderApp("/metrics");

    expect(await screen.findByText("42")).toBeInTheDocument();
    expect(screen.getByText(/^fetched /)).toBeInTheDocument();

    const user = userEvent.setup();
    await user.click(screen.getByRole("button", { name: "Refresh" }));

    expect(await screen.findByText("43")).toBeInTheDocument();
    const metricCalls = fetchMock.mock.calls.filter(
      ([input]) => String(input) === "/metrics",
    );
    expect(metricCalls).toHaveLength(2);
  });

  it("shows a clear error state when the server is down", async () => {
    mockFetch((url) =>
      url === "/metrics" ? textResponse("boom", 500) : undefined,
    );
    renderApp("/metrics");

    const alert = await screen.findByRole("alert");
    expect(alert).toHaveTextContent("GET /metrics failed: HTTP 500");
  });
});
