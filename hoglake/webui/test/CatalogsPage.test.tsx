import { screen } from "@testing-library/react";
import { describe, expect, it } from "vitest";
import { catalogsFixture, notFoundError } from "./fixtures";
import { jsonResponse, mockFetch, renderApp } from "./helpers";

describe("CatalogsPage", () => {
  it("lists catalogs with data_path, head snapshot, and schema version", async () => {
    mockFetch((url) =>
      url === "/v1/catalogs" ? jsonResponse(catalogsFixture) : undefined,
    );
    renderApp("/");
    expect(await screen.findByText("analytics")).toBeInTheDocument();
    expect(screen.getByText("s3://hog-lake/analytics")).toBeInTheDocument();
    expect(screen.getByText("4211")).toBeInTheDocument();
    expect(screen.getByText("scratch")).toBeInTheDocument();
    expect(screen.getByRole("heading", { name: "Create catalog" })).toBeInTheDocument();
  });

  it("shows the empty state when no catalogs exist", async () => {
    mockFetch((url) => (url === "/v1/catalogs" ? jsonResponse([]) : undefined));
    renderApp("/");
    expect(await screen.findByText("No catalogs yet.")).toBeInTheDocument();
  });

  it("surfaces the ApiError detail when the list fails", async () => {
    mockFetch((url) =>
      url === "/v1/catalogs" ? jsonResponse(notFoundError, 404) : undefined,
    );
    renderApp("/");
    const alert = await screen.findByRole("alert");
    expect(alert).toHaveTextContent("not_found");
    expect(alert).toHaveTextContent("catalog 'nope' does not exist");
    expect(alert).toHaveTextContent("404");
  });
});
