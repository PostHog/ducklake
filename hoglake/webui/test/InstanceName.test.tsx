import { screen } from "@testing-library/react";
import { describe, expect, it } from "vitest";
import { jsonResponse, mockFetch, renderApp } from "./helpers";

const catalogsFixture: unknown[] = [];

describe("topbar instance name", () => {
  it("shows the configured instance name as a badge", async () => {
    mockFetch((url) => {
      if (url === "/v1/info") return jsonResponse({ name: "GigaHog" });
      if (url === "/v1/catalogs") return jsonResponse(catalogsFixture);
      return undefined;
    });
    renderApp("/");
    expect(await screen.findByText("GigaHog")).toBeInTheDocument();
  });

  it("renders no badge when the instance is unnamed", async () => {
    mockFetch((url) =>
      url === "/v1/catalogs" ? jsonResponse(catalogsFixture) : undefined,
    );
    renderApp("/");
    expect(await screen.findByText("hoglake")).toBeInTheDocument();
    expect(document.querySelector(".instance-name")).toBeNull();
  });

  it("renders no badge when the info endpoint fails", async () => {
    mockFetch((url) => {
      if (url === "/v1/info")
        return jsonResponse({ error: "nope", detail: "old server" }, 500);
      if (url === "/v1/catalogs") return jsonResponse(catalogsFixture);
      return undefined;
    });
    renderApp("/");
    expect(await screen.findByText("hoglake")).toBeInTheDocument();
    expect(document.querySelector(".instance-name")).toBeNull();
  });
});
