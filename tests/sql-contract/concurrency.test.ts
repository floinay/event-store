import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { uuidv7 } from "@event-store/contracts";
import { PostgresEventStore } from "@event-store/postgres-store";
import { EventStoreStack } from "../fixtures/event-store-stack.js";
import type { Pool } from "pg";

const suite = process.env.RUN_STRESS === "true" ? describe : describe.skip;

suite("append concurrency", () => {
  const stack = new EventStoreStack();
  let pool: Pool;
  let store: PostgresEventStore;
  beforeAll(async () => {
    await stack.start();
    pool = await stack.pool();
    store = new PostgresEventStore(pool);
  }, 180_000);
  afterAll(async () => {
    await pool?.end();
    await stack.stop();
  }, 60_000);

  it("allows exactly one of 1,000 expected-revision contenders", async () => {
    const aggregateId = uuidv7();
    const outcomes = await Promise.allSettled(
      Array.from({ length: 1_000 }, async () => {
        const requestId = uuidv7();
        return store.append({
          producerService: "orders-command",
          namespace: "orders",
          aggregateType: "Order",
          aggregateId,
          requestId,
          expectedRevision: { kind: "exact", revision: 0n },
          context: {
            requestId,
            correlationId: uuidv7(),
            causationId: null,
            actor: { kind: "user", subjectRef: "usr_1" },
          },
          events: [
            {
              eventName: "order.created",
              schemaVersion: 1,
              occurredAt: "2026-08-04T10:12:18.120Z",
              payload: { orderRef: requestId },
            },
          ],
        });
      }),
    );
    expect(
      outcomes.filter((outcome) => outcome.status === "fulfilled"),
    ).toHaveLength(1);
    expect(
      outcomes.filter((outcome) => outcome.status === "rejected"),
    ).toHaveLength(999);
    expect(await store.getStreamHead("orders", "Order", aggregateId)).toBe(1n);
  }, 120_000);
});
