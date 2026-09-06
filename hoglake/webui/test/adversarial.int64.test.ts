// Int64 precision audit.
//
// The OpenAPI spec types snapshot_id, row_id_start, record_count,
// file_size_bytes, head_snapshot_id, committed_snapshot (and more) as
// `integer, format: int64`, and the live server really emits values above
// 2^53 as bare JSON numbers (verified 2026-09-05: a commit with
// record_count=2^53+1 / file_size_bytes=2^62+1 round-trips intact through
// the server). The client therefore parses response bodies from the raw
// text (JSON.parse reviver with source access, src/api/int64.ts) and
// carries every int64 field as its exact decimal string — never through
// Number. These are the pinned regressions.

import { describe, expect, it, vi } from "vitest";
import { listFiles, listSnapshots } from "../src/api/client";
import { addInt64, compareInt64, parseInt64Json } from "../src/api/int64";
import { formatCount } from "../src/lib/format";
import {
  bigIntFilesWireBody,
  bigIntSnapshotsPage1WireBody,
} from "./fixtures.adversarial";

function stubFetchRaw(body: string) {
  const fn = vi.fn(
    async () =>
      new Response(body, {
        status: 200,
        headers: { "Content-Type": "application/json" },
      }),
  );
  vi.stubGlobal("fetch", fn);
  return fn;
}

describe("int64 wire values above 2^53", () => {
  it("record_count survives JSON parsing losslessly", async () => {
    stubFetchRaw(bigIntFilesWireBody);
    const files = await listFiles("c", "ns", "t");
    expect(String(files[0].record_count)).toBe("9007199254740993");
  });

  it("file_size_bytes survives JSON parsing losslessly", async () => {
    stubFetchRaw(bigIntFilesWireBody);
    const files = await listFiles("c", "ns", "t");
    expect(String(files[0].file_size_bytes)).toBe("4611686018427387905");
  });

  // row_id_start is the lineage anchor; 2^53+3 must not round to 2^53+4.
  it("row_id_start survives JSON parsing losslessly", async () => {
    stubFetchRaw(bigIntFilesWireBody);
    const files = await listFiles("c", "ns", "t");
    expect(String(files[0].row_id_start)).toBe("9007199254740995");
  });

  it("formats counts above 2^53 exactly (no toLocaleString double round-trip)", async () => {
    stubFetchRaw(bigIntFilesWireBody);
    const files = await listFiles("c", "ns", "t");
    expect(formatCount(files[0].record_count)).toBe("9,007,199,254,740,993");
  });

  // A snapshot id from a response page must be echoed EXACTLY when used as
  // the next `before` cursor — the whole point of string-typed ids.
  it("echoes a >2^53 snapshot id verbatim as the next before cursor", async () => {
    const fetchMock = stubFetchRaw(bigIntSnapshotsPage1WireBody);
    const page = await listSnapshots("bigcat", {
      before: addInt64("9007199254740997", 1), // head+1, exact
      limit: 50,
    });
    expect(fetchMock).toHaveBeenCalledWith(
      "/v1/catalogs/bigcat/snapshots?before=9007199254740998&limit=50",
      expect.anything(),
    );
    const cursor = page.snapshots[page.snapshots.length - 1].snapshot_id;
    expect(cursor).toBe("9007199254740995");
    await listSnapshots("bigcat", { before: cursor, limit: 50 });
    expect(fetchMock).toHaveBeenLastCalledWith(
      "/v1/catalogs/bigcat/snapshots?before=9007199254740995&limit=50",
      expect.anything(),
    );
  });

  // Documents the exact corruption mode being defended against: bare
  // JSON.parse rounds to the nearest double, so odd values above 2^53
  // become even neighbours. If this ever fails, the JS engine changed.
  it("JSON.parse rounds int64 (the mechanism being pinned)", () => {
    expect(JSON.parse("9007199254740993")).toBe(9007199254740992);
    expect(JSON.parse("4611686018427387905")).toBe(4611686018427387904);
  });
});

describe("parseInt64Json", () => {
  it("stringifies int64 fields, leaves other types alone", () => {
    const parsed = parseInt64Json(
      '{"snapshot_id":9007199254740993,"ordinal":3,"name":"x",' +
        '"nullable":true,"record_count":"7","object_id":null}',
    ) as Record<string, unknown>;
    expect(parsed.snapshot_id).toBe("9007199254740993");
    expect(parsed.ordinal).toBe(3); // int32 field stays a number
    expect(parsed.name).toBe("x");
    expect(parsed.nullable).toBe(true);
    expect(parsed.record_count).toBe("7"); // already-string passes through
    expect(parsed.object_id).toBeNull();
  });

  it("does not rewrite int64-looking text inside string values", () => {
    const parsed = parseInt64Json(
      '{"message":"saw {\\"record_count\\":9007199254740993} upstream"}',
    ) as Record<string, unknown>;
    expect(parsed.message).toBe(
      'saw {"record_count":9007199254740993} upstream',
    );
  });
});

describe("int64 string helpers", () => {
  it("addInt64 is exact above 2^53", () => {
    expect(addInt64("9007199254740993", 1)).toBe("9007199254740994");
    expect(addInt64("4611686018427387905", 1)).toBe("4611686018427387906");
  });

  it("compareInt64 orders numerically, not lexically", () => {
    expect(compareInt64("9", "10")).toBe(-1);
    expect(compareInt64("9007199254740993", "9007199254740992")).toBe(1);
    expect(compareInt64("5", "5")).toBe(0);
  });
});
