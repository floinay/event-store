import { describe, expect, it } from "vitest";
import { recordAppendFailure } from "../../apps/event-store-service/src/index.js";

describe("append availability metrics", () => {
  it("excludes expected-revision conflicts from availability failures", () => {
    const metrics = {
      appendFailureCount: 0,
      appendConflictCount: 0,
      appendUnknownOutcomeCount: 0,
    };
    recordAppendFailure(metrics, { code: "40001" });
    expect(metrics).toEqual({
      appendFailureCount: 0,
      appendConflictCount: 1,
      appendUnknownOutcomeCount: 0,
    });
  });
});
