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
  });
});
