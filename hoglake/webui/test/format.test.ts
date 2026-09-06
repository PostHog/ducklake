import { describe, expect, it } from "vitest";
import { formatBytes, formatCount } from "../src/lib/format";
import { identifierError } from "../src/lib/names";

describe("formatCount", () => {
  it("groups digits", () => {
    expect(formatCount("0")).toBe("0");
    expect(formatCount("999")).toBe("999");
    expect(formatCount("1234567")).toBe("1,234,567");
  });

  it("is exact above 2^53 (no Number round-trip)", () => {
    expect(formatCount("9007199254740993")).toBe("9,007,199,254,740,993");
    expect(formatCount("4611686018427387905")).toBe(
      "4,611,686,018,427,387,905",
    );
  });

  it("never renders NaN for missing or malformed input", () => {
    expect(formatCount(undefined)).toBe("—");
    expect(formatCount(null)).toBe("—");
    expect(formatCount("")).toBe("—");
    expect(formatCount("NaN")).toBe("—");
    expect(formatCount("12garbage")).toBe("—");
    expect(formatCount(Number.NaN)).toBe("—");
  });
});

describe("formatBytes", () => {
  it("humanizes byte counts", () => {
    expect(formatBytes("512")).toBe("512 B");
    expect(formatBytes("268435456")).toBe("256 MiB");
    expect(formatBytes("5368709120")).toBe("5.0 GiB");
  });

  it("never renders NaN for missing or malformed input", () => {
    expect(formatBytes(undefined)).toBe("—");
    expect(formatBytes(null)).toBe("—");
    expect(formatBytes("pending")).toBe("—");
    expect(formatBytes(Number.NaN)).toBe("—");
  });
});

describe("identifierError (server 422 pattern mirror)", () => {
  it("accepts spec-valid identifiers", () => {
    expect(identifierError("events")).toBeNull();
    expect(identifierError("_x")).toBeNull();
    expect(identifierError("A9-b_c")).toBeNull();
    expect(identifierError("a".repeat(128))).toBeNull(); // max length
    expect(identifierError("")).toBeNull(); // blank is `required`'s job
  });

  it("rejects pattern violations", () => {
    expect(identifierError("9starts")).not.toBeNull();
    expect(identifierError("-lead")).not.toBeNull();
    expect(identifierError("has space")).not.toBeNull();
    expect(identifierError("dotted.name")).not.toBeNull();
    expect(identifierError("ns<script>alert(1)</script>")).not.toBeNull();
    expect(identifierError("a".repeat(129))).not.toBeNull(); // too long
  });
});
