import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { uuidv7 } from "@event-store/contracts";
import { PostgresEventStore } from "@event-store/postgres-store";
import { EventStoreStack } from "../fixtures/event-store-stack.js";
import type { Pool } from "pg";

const suite = process.env.RUN_STRESS === "true" ? describe : describe.skip;

function appendInput(requestId: string, aggregateId: string, orderRef: string) {
  return {
    producerService: "orders-command",
    namespace: "orders",
    aggregateType: "Order",
    aggregateId,
    requestId,
    expectedRevision: { kind: "no_stream" as const },
    context: {
      requestId,
      correlationId: uuidv7(),
      causationId: null,
      actor: { kind: "user" as const, subjectRef: "usr_1" },
    },
    events: [
      {
        eventName: "order.created",
        schemaVersion: 1,
        occurredAt: "2026-08-04T10:12:18.120Z",
        payload: { orderRef },
      },
    ],
  };
}

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

  it("returns byte-equivalent results for 1,000 identical request IDs", async () => {
    const requestId = uuidv7();
    const aggregateId = uuidv7();
    const input = appendInput(requestId, aggregateId, "idempotent");
    const outcomes = await Promise.all(
      Array.from({ length: 1_000 }, () => store.append(input)),
    );
    expect(new Set(outcomes.map((result) => JSON.stringify(result)))).toEqual(
      new Set([JSON.stringify(outcomes[0])]),
    );
    expect(await store.getStreamHead("orders", "Order", aggregateId)).toBe(1n);
  }, 180_000);

  it("commits exactly one body for 100 mixed requests with one request ID", async () => {
    const requestId = uuidv7();
    const aggregateId = uuidv7();
    const first = appendInput(requestId, aggregateId, "first-body");
    const second = appendInput(requestId, aggregateId, "second-body");
    const outcomes = await Promise.allSettled(
      Array.from({ length: 100 }, (_, index) =>
        store.append(index % 2 === 0 ? first : second).then(
          () => (index % 2 === 0 ? "first" : "second"),
          (error) =>
            Promise.reject({
              body: index % 2 === 0 ? "first" : "second",
              error,
            }),
        ),
      ),
    );
    const committedBodies = new Set(
      outcomes
        .filter(
          (outcome): outcome is PromiseFulfilledResult<"first" | "second"> =>
            outcome.status === "fulfilled",
        )
        .map((outcome) => outcome.value),
    );
    expect(committedBodies.size).toBe(1);
    const committedBody = [...committedBodies][0]!;
    for (const outcome of outcomes) {
      if (outcome.status === "fulfilled")
        expect(outcome.value).toBe(committedBody);
      else
        expect(outcome.reason).toMatchObject({
          body: committedBody === "first" ? "second" : "first",
        });
    }
    expect(await store.getStreamHead("orders", "Order", aggregateId)).toBe(1n);
  }, 120_000);
});
