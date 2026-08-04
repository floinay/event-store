import { describe, expect, it } from "vitest";
import { trafficClassForClient } from "../../apps/event-store-service/src/index.js";

describe("critical append authorization", () => {
  it("keeps ordinary appends on the standard WAL budget", () => {
    expect(trafficClassForClient(false, undefined, [])).toBe("standard");
  });

  it("rejects a caller that merely sets critical=true", () => {
    expect(() => trafficClassForClient(true, "untrusted", ["writer"])).toThrow(
      "trusted mTLS",
    );
  });

  it("permits the dedicated mTLS client identity", () => {
    expect(trafficClassForClient(true, "writer", ["writer"])).toBe("critical");
  });
});
