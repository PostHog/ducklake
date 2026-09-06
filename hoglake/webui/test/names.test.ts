import { describe, expect, it } from "vitest";
import { columnNameError, identifierError } from "../src/lib/names";

describe("columnNameError", () => {
  it("refuses the reserved _hog prefix for columns", () => {
    expect(columnNameError("_hog_row_id")).toMatch(/reserved prefix/);
    expect(columnNameError("_hogx")).toMatch(/reserved prefix/);
    expect(columnNameError("_hog")).toMatch(/reserved prefix/);
  });

  it("still applies the shared identifier policy", () => {
    expect(columnNameError("col.dotted")).toMatch(/must start with/);
    expect(columnNameError("")).toBeNull(); // blank left to `required`
    expect(columnNameError("ts")).toBeNull();
  });

  it("allows plain leading-underscore columns", () => {
    expect(columnNameError("_leading")).toBeNull();
    expect(columnNameError("_hedgehog")).toBeNull(); // "_he..." is not the "_hog" prefix
  });
});

describe("identifierError (tables/namespaces unaffected)", () => {
  it("allows _hog-prefixed table and namespace names", () => {
    expect(identifierError("_hog_row_id")).toBeNull();
    expect(identifierError("_hogx")).toBeNull();
  });
});
