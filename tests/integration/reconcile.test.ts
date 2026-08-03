import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { uuidv7 } from "@event-store/contracts";
import { PostgresEventStore } from "@event-store/postgres-store";
import { reconcile } from "@event-store/reconcile";
import { EventStoreStack } from "../fixtures/event-store-stack.js";
import type { Pool } from "pg";

const suite = process.env.RUN_INTEGRATION === "true" ? describe : describe.skip;

suite("reconciliation", () => {
  const stack = new EventStoreStack();
  let pool: Pool;
  beforeAll(async () => {
    await stack.start();
    pool = await stack.pool();
  }, 180_000);
  afterAll(async () => {
    await pool?.end();
    await stack.stop();
  }, 60_000);

  it("verifies restored stream continuity and envelope hashes", async () => {
    const store = new PostgresEventStore(pool);
    const requestId = uuidv7();
    await store.append({
      producerService: "orders-command",
      namespace: "orders",
      aggregateType: "Order",
      aggregateId: uuidv7(),
      requestId,
      expectedRevision: { kind: "no_stream" },
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
          payload: { orderRef: "o1" },
        },
      ],
    });
    await expect(reconcile(stack.databaseUrl)).resolves.toMatchObject({
      count: "1",
      revisionGaps: "0",
      envelopeHashMismatches: "0",
    });
  });
});
