import { describe, expect, it } from "vitest";
import { ProjectionFailureReporter } from "@event-store/projection-runtime";
import type { Pool } from "pg";

describe("projection failure reporter", () => {
  it("persists a deterministic diagnostic identity for a malformed record", async () => {
    const calls: unknown[][] = [];
    const pool = {
      on: () => undefined,
      query: async (...args: unknown[]) => {
        calls.push(args);
        return { rows: [] };
      },
    } as unknown as Pool;
    const reporter = new ProjectionFailureReporter(pool, {
      name: "orders-read-model",
      generationId: "019fc9c9-84d4-754c-ba77-8a8a9d9c586a",
    });
    const record = {
      topic: "event-store.events.v1",
      partition: 7,
      offset: 42n,
      key: "invalid",
      headers: {},
      value: Buffer.from("not-json"),
    };

    await reporter.record(
      record,
      { rawBase64: record.value.toString("base64") },
      new Error("invalid envelope"),
      8,
    );
    await reporter.record(
      record,
      { rawBase64: record.value.toString("base64") },
      new Error("invalid envelope"),
      8,
    );

    const firstInsert = calls[0]?.[1] as unknown[];
    const secondInsert = calls[2]?.[1] as unknown[];
    expect(firstInsert?.[2]).toMatch(
      /^[0-9a-f]{8}-[0-9a-f]{4}-5[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/,
    );
    expect(firstInsert?.[3]).toMatch(/^[0-9a-f]{64}$/);
    expect(secondInsert?.[2]).toBe(firstInsert?.[2]);
    expect(secondInsert?.[3]).toBe(firstInsert?.[3]);
  });
});
