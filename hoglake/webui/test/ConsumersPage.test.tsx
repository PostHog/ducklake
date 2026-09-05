import { screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { describe, expect, it } from "vitest";
import { consumerOffsetsFixture } from "./fixtures";
import { jsonResponse, mockFetch, renderApp } from "./helpers";

const offsetsUrl = "/v1/catalogs/analytics/consumers/viaduck-sink-7/offsets";

describe("ConsumersPage", () => {
  it("prompts for a consumer id before listing anything", async () => {
    mockFetch(() => undefined);
    renderApp("/catalogs/analytics/consumers");
    expect(
      await screen.findByText("Enter a consumer id to list its committed offsets."),
    ).toBeInTheDocument();
  });

  it("looks up a consumer and lists its offsets", async () => {
    const fetchMock = mockFetch((url) =>
      url === offsetsUrl ? jsonResponse(consumerOffsetsFixture) : undefined,
    );
    renderApp("/catalogs/analytics/consumers");
    const user = userEvent.setup();

    await user.type(screen.getByLabelText("consumer id"), "viaduck-sink-7");
    await user.click(screen.getByRole("button", { name: "Look up" }));

    expect(
      await screen.findByText("3f2c9c04-8a1b-4c7e-9f10-6d2a5b3e8c71"),
    ).toBeInTheDocument();
    expect(screen.getByText("4205")).toBeInTheDocument();
    expect(screen.getByText("4198")).toBeInTheDocument();
    expect(screen.getAllByText("viaduck-sink-7").length).toBeGreaterThan(0);
    expect(fetchMock).toHaveBeenCalledWith(offsetsUrl, expect.anything());
  });

  it("surfaces a 404 for an unknown consumer", async () => {
    mockFetch((url) =>
      url === "/v1/catalogs/analytics/consumers/ghost/offsets"
        ? jsonResponse(
            { error: "not_found", detail: "consumer 'ghost' has no offsets" },
            404,
          )
        : undefined,
    );
    renderApp("/catalogs/analytics/consumers");
    const user = userEvent.setup();

    await user.type(screen.getByLabelText("consumer id"), "ghost");
    await user.click(screen.getByRole("button", { name: "Look up" }));

    const alert = await screen.findByRole("alert");
    expect(alert).toHaveTextContent("not_found");
    expect(alert).toHaveTextContent("consumer 'ghost' has no offsets");
  });
});
