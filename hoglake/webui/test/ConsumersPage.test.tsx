import { screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { describe, expect, it } from "vitest";
import { jsonResponse, mockFetch, renderApp } from "./helpers";

const consumersUrl = "/v1/catalogs/analytics/consumers";

const listingFixture = {
  consumers: [
    {
      consumer_id: "hedgerow-events",
      offsets: [
        {
          table_uuid: "3f2c9c04-8a1b-4c7e-9f10-6d2a5b3e8c71",
          committed_snapshot: 4205,
          updated_at: "2026-09-06T10:00:00Z",
          namespace: "ns1",
          table_name: "events",
          table_dropped: false,
        },
        {
          table_uuid: "b0e9d1c2-3a4b-5c6d-7e8f-901234567890",
          committed_snapshot: 4198,
          updated_at: "2026-09-06T09:00:00Z",
          namespace: "ns1",
          table_name: "retired",
          table_dropped: true,
        },
      ],
    },
    {
      consumer_id: "viaduck-sink-7",
      offsets: [
        {
          table_uuid: "11111111-2222-3333-4444-555555555555",
          committed_snapshot: 17,
          updated_at: "2026-09-06T08:00:00Z",
          namespace: "ns2",
          table_name: "raw",
          table_dropped: false,
        },
      ],
    },
  ],
};

describe("ConsumersPage", () => {
  it("lists every consumer with resolved table names on load", async () => {
    const fetchMock = mockFetch((url) =>
      url === consumersUrl ? jsonResponse(listingFixture) : undefined,
    );
    renderApp("/catalogs/analytics/consumers");

    expect(await screen.findByText("hedgerow-events")).toBeInTheDocument();
    expect(screen.getByText("viaduck-sink-7")).toBeInTheDocument();
    // live table renders as a link to the table page
    const link = screen.getByRole("link", { name: "ns1.events" });
    expect(link).toHaveAttribute(
      "href",
      "/catalogs/analytics/namespaces/ns1/tables/events",
    );
    // dropped table keeps its last name, flagged, not a link
    expect(screen.getByText(/ns1\.retired/)).toBeInTheDocument();
    expect(screen.getByText("dropped")).toBeInTheDocument();
    expect(
      screen.queryByRole("link", { name: /retired/ }),
    ).not.toBeInTheDocument();
    expect(screen.getByText("4205")).toBeInTheDocument();
    expect(fetchMock).toHaveBeenCalledWith(consumersUrl, expect.anything());
  });

  it("filters consumers client-side without refetching", async () => {
    const fetchMock = mockFetch((url) =>
      url === consumersUrl ? jsonResponse(listingFixture) : undefined,
    );
    renderApp("/catalogs/analytics/consumers");
    await screen.findByText("hedgerow-events");
    const user = userEvent.setup();

    await user.type(screen.getByLabelText("filter consumers"), "viaduck");
    expect(screen.queryByText("hedgerow-events")).not.toBeInTheDocument();
    expect(screen.getByText("viaduck-sink-7")).toBeInTheDocument();

    await user.clear(screen.getByLabelText("filter consumers"));
    await user.type(screen.getByLabelText("filter consumers"), "nobody");
    expect(
      screen.getByText("No consumer matches the filter."),
    ).toBeInTheDocument();
    expect(
      fetchMock.mock.calls.filter(([u]) => u === consumersUrl).length,
    ).toBe(1);
  });

  it("shows the empty state for a catalog with no consumers", async () => {
    mockFetch((url) =>
      url === consumersUrl ? jsonResponse({ consumers: [] }) : undefined,
    );
    renderApp("/catalogs/analytics/consumers");
    expect(
      await screen.findByText(
        "No consumer has committed an offset in this catalog.",
      ),
    ).toBeInTheDocument();
  });

  it("surfaces an API error", async () => {
    mockFetch((url) =>
      url === consumersUrl
        ? jsonResponse({ error: "not_found", detail: "no catalog" }, 404)
        : undefined,
    );
    renderApp("/catalogs/analytics/consumers");
    const alert = await screen.findByRole("alert");
    expect(alert).toHaveTextContent("not_found");
  });
});
