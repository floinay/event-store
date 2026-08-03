import { describe, expect, it } from "vitest";
import * as grpc from "@grpc/grpc-js";
import { errorFrom } from "../../apps/event-store-service/dist/index.js";

describe("gRPC error mapping", () => {
  it("maps request validation to INVALID_ARGUMENT", () => {
    expect(errorFrom(new SyntaxError("invalid JSON")).code).toBe(
      grpc.status.INVALID_ARGUMENT,
    );
  });

  it("maps append connection loss to commit_outcome_unknown", () => {
    const error = errorFrom(
      Object.assign(new Error("connection lost"), { code: "08006" }),
      "019fc9c9-84d4-754c-ba77-8a8a9d9c586a",
      true,
    );
    expect(error.code).toBe(grpc.status.UNKNOWN);
    expect(error.message).toContain("commit_outcome_unknown");
    expect(JSON.parse(error.details)).toMatchObject({
      code: "commit_outcome_unknown",
      requestId: "019fc9c9-84d4-754c-ba77-8a8a9d9c586a",
    });
  });

  it("includes the current revision in an optimistic conflict detail", () => {
    const error = errorFrom(
      Object.assign(new Error("expected revision 3, actual revision 4"), {
        code: "40001",
      }),
      "019fc9c9-84d4-754c-ba77-8a8a9d9c586a",
    );
    expect(error.code).toBe(grpc.status.ABORTED);
    expect(JSON.parse(error.details)).toMatchObject({
      code: "expected_revision_conflict",
      actualRevision: "4",
    });
  });
});
