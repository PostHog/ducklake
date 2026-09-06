import { screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { describe, expect, it } from "vitest";
import { notFoundError, tableFixture, tablesFixture, unprocessableError } from "./fixtures";
import { jsonResponse, mockFetch, renderApp } from "./helpers";

const tablesUrl = "/v1/catalogs/analytics/namespaces/events/tables";

describe("NamespacePage", () => {
  it("lists tables with uuids and offers the create-table form", async () => {
    mockFetch((url) => (url === tablesUrl ? jsonResponse(tablesFixture) : undefined));
    renderApp("/catalogs/analytics/namespaces/events");

    expect(await screen.findByText("pageviews")).toBeInTheDocument();
    expect(
      screen.getByText("3f2c9c04-8a1b-4c7e-9f10-6d2a5b3e8c71"),
    ).toBeInTheDocument();
    expect(screen.getByText("clicks")).toBeInTheDocument();

    // Dynamic column row: name input, 13-type dropdown, nullable toggle.
    expect(screen.getByLabelText("column 1 name")).toBeInTheDocument();
    const typeSelect = screen.getByLabelText("column 1 type");
    expect(typeSelect.querySelectorAll("option")).toHaveLength(13);
    expect(screen.getByLabelText("column 1 nullable")).toBeChecked();
  });

  it("adds and removes column rows dynamically", async () => {
    mockFetch((url) => (url === tablesUrl ? jsonResponse([]) : undefined));
    renderApp("/catalogs/analytics/namespaces/events");
    const user = userEvent.setup();

    await screen.findByText("No tables in this namespace.");
    await user.click(screen.getByRole("button", { name: "+ Add column" }));
    expect(screen.getByLabelText("column 2 name")).toBeInTheDocument();
    await user.click(screen.getByRole("button", { name: "remove column 2" }));
    expect(screen.queryByLabelText("column 2 name")).not.toBeInTheDocument();
  });

  it("POSTs the typed column defs and refreshes the list", async () => {
    let posted: unknown = null;
    const fetchMock = mockFetch((url, init) => {
      if (url === tablesUrl && init?.method === "POST") {
        posted = JSON.parse(init.body as string);
        return jsonResponse(tableFixture, 201);
      }
      if (url === tablesUrl) {
        // After the create lands, the invalidated list refetch sees the table.
        return jsonResponse(
          posted ? [{ name: "pageviews", table_uuid: tableFixture.table_uuid }] : [],
        );
      }
      return undefined;
    });
    renderApp("/catalogs/analytics/namespaces/events");
    const user = userEvent.setup();

    await screen.findByText("No tables in this namespace.");
    await user.type(screen.getByLabelText("name"), "pageviews");
    await user.type(screen.getByLabelText("column 1 name"), "ts");
    await user.selectOptions(screen.getByLabelText("column 1 type"), "timestamptz");
    await user.click(screen.getByLabelText("column 1 nullable"));
    await user.click(screen.getByRole("button", { name: "Create table" }));

    await screen.findByText("pageviews");
    expect(posted).toEqual({
      name: "pageviews",
      columns: [{ name: "ts", type: "timestamptz", nullable: false }],
    });
    expect(fetchMock).toHaveBeenCalledWith(
      tablesUrl,
      expect.objectContaining({ method: "POST" }),
    );
  });

  it("shows a 422 validation error from create-table inline", async () => {
    mockFetch((url, init) => {
      if (url === tablesUrl && init?.method === "POST")
        return jsonResponse(unprocessableError, 422);
      if (url === tablesUrl) return jsonResponse([]);
      return undefined;
    });
    renderApp("/catalogs/analytics/namespaces/events");
    const user = userEvent.setup();

    await screen.findByText("No tables in this namespace.");
    await user.type(screen.getByLabelText("name"), "bad");
    await user.type(screen.getByLabelText("column 1 name"), "x");
    await user.click(screen.getByRole("button", { name: "Create table" }));

    const alert = await screen.findByRole("alert");
    expect(alert).toHaveTextContent("validation_failed");
    expect(alert).toHaveTextContent("unknown field ids in column_stats: [99]");
  });

  it("rejects invalid table and column names client-side without a POST", async () => {
    const fetchMock = mockFetch((url) =>
      url === tablesUrl ? jsonResponse([]) : undefined,
    );
    renderApp("/catalogs/analytics/namespaces/events");
    const user = userEvent.setup();

    await screen.findByText("No tables in this namespace.");
    await user.type(screen.getByLabelText("name"), "9bad-table");
    await user.type(screen.getByLabelText("column 1 name"), "col.dotted");
    expect(await screen.findByText(/table name:/)).toBeInTheDocument();
    expect(await screen.findByText(/column 1:/)).toBeInTheDocument();
    expect(screen.getByRole("button", { name: "Create table" })).toBeDisabled();

    const posts = fetchMock.mock.calls.filter(
      (c) => (c[1] as RequestInit | undefined)?.method === "POST",
    );
    expect(posts).toHaveLength(0);

    // Fixing the names clears the errors and re-enables the form.
    await user.clear(screen.getByLabelText("name"));
    await user.type(screen.getByLabelText("name"), "good_table");
    await user.clear(screen.getByLabelText("column 1 name"));
    await user.type(screen.getByLabelText("column 1 name"), "col_1");
    expect(screen.queryByText(/table name:/)).not.toBeInTheDocument();
    expect(screen.queryByText(/column 1:/)).not.toBeInTheDocument();
    expect(
      screen.getByRole("button", { name: "Create table" }),
    ).toBeEnabled();
  });

  it("refuses reserved _hog column names but not _hog table names", async () => {
    const fetchMock = mockFetch((url) =>
      url === tablesUrl ? jsonResponse([]) : undefined,
    );
    renderApp("/catalogs/analytics/namespaces/events");
    const user = userEvent.setup();

    await screen.findByText("No tables in this namespace.");
    // A _hog-prefixed TABLE name is fine — the reservation is columns-only.
    await user.type(screen.getByLabelText("name"), "_hog_shadow");
    expect(screen.queryByText(/table name:/)).not.toBeInTheDocument();

    await user.type(screen.getByLabelText("column 1 name"), "_hog_row_id");
    expect(await screen.findByText(/column 1:.*reserved prefix/)).toBeInTheDocument();
    expect(screen.getByRole("button", { name: "Create table" })).toBeDisabled();

    // _hogx is refused too (prefix match, not just the exact carrier name).
    await user.clear(screen.getByLabelText("column 1 name"));
    await user.type(screen.getByLabelText("column 1 name"), "_hogx");
    expect(await screen.findByText(/column 1:.*reserved prefix/)).toBeInTheDocument();

    // No POST ever left the form.
    const posts = fetchMock.mock.calls.filter(
      (c) => (c[1] as RequestInit | undefined)?.method === "POST",
    );
    expect(posts).toHaveLength(0);

    // A plain leading underscore is still a valid column name.
    await user.clear(screen.getByLabelText("column 1 name"));
    await user.type(screen.getByLabelText("column 1 name"), "_leading");
    expect(screen.queryByText(/column 1:/)).not.toBeInTheDocument();
    expect(screen.getByRole("button", { name: "Create table" })).toBeEnabled();
  });

  it("surfaces a 404 when the namespace does not exist", async () => {
    mockFetch((url) =>
      url === tablesUrl ? jsonResponse(notFoundError, 404) : undefined,
    );
    renderApp("/catalogs/analytics/namespaces/events");
    const alert = await screen.findByRole("alert");
    expect(alert).toHaveTextContent("not_found");
  });
});
