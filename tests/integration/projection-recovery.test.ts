import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { z } from "zod";
import { uuidv7 } from "@event-store/contracts";
import { PostgresEventStore } from "@event-store/postgres-store";
import {
  createProjectionEventTransformer,
  KafkaProjectionRunner,
  ProjectionCheckpointStore,
  ProjectionFailureReporter,
  ProjectionPayloadSchemas,
  ProjectionTransactionRunner,
} from "@event-store/projection-runtime";
import { UpcasterRegistry } from "@event-store/upcasting";
import { EventStoreStack } from "../fixtures/event-store-stack.js";
import type { Pool } from "pg";

const suite = process.env.RUN_INTEGRATION === "true" ? describe : describe.skip;

async function eventually(check: () => Promise<boolean>, timeoutMs = 30_000) {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (await check()) return;
    await new Promise((resolve) => setTimeout(resolve, 100));
  }
  throw new Error(`condition did not become true within ${timeoutMs}ms`);
}

suite("projection recovery", () => {
  const stack = new EventStoreStack();
  let pool: Pool;
  let store: PostgresEventStore;
  beforeAll(async () => {
    await stack.start({ cdc: true });
    pool = await stack.pool();
    store = new PostgresEventStore(pool);
  }, 180_000);
  afterAll(async () => {
    await pool?.end();
    await stack.stop();
  }, 60_000);

  it("rebuilds a new generation from retained Kafka records", async () => {
    const aggregateId = uuidv7();
    const requestId = uuidv7();
    const generationId = uuidv7();
    const projectionName = "recovery";
    await pool.query(
      "CREATE SCHEMA projection_recovery; CREATE TABLE projection_recovery.events(event_id uuid PRIMARY KEY)",
    );
    await pool.query(
      "INSERT INTO projection_runtime.generations(projection_name,generation_id,status,created_at) VALUES ($1,$2,'building',clock_timestamp())",
      [projectionName, generationId],
    );
    await store.append({
      producerService: "orders-command",
      namespace: "orders",
      aggregateType: "Order",
      aggregateId,
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
    const upcasters = new UpcasterRegistry();
    upcasters.setCurrentVersion("order.created", 1);
    const schemas = new ProjectionPayloadSchemas();
    schemas.register(
      "order.created",
      1,
      z.object({ orderRef: z.string() }).strict(),
    );
    const identity = { name: projectionName, generationId };
    const runner = new KafkaProjectionRunner(
      {
        brokers: [stack.kafkaBroker()],
        groupId: `projection-recovery-${uuidv7()}`,
        topic: "event-store.events.v1",
      },
      new ProjectionTransactionRunner(
        pool,
        identity,
        createProjectionEventTransformer(upcasters, schemas),
      ),
      async (client, event) => {
        await client.query(
          "INSERT INTO projection_recovery.events(event_id) VALUES ($1)",
          [event.eventId],
        );
      },
      new ProjectionCheckpointStore(pool, identity),
      new ProjectionFailureReporter(pool, identity),
    );
    const consumer = await runner.start();
    await eventually(async () => {
      const result = await pool.query<{ count: number }>(
        "SELECT count(*)::int AS count FROM projection_recovery.events",
      );
      return result.rows[0]?.count === 1;
    });
    await consumer.disconnect();
    expect(
      (
        await pool.query(
          "SELECT next_offset::text FROM projection_runtime.checkpoints WHERE projection_name=$1 AND generation_id=$2",
          [projectionName, generationId],
        )
      ).rows[0]?.next_offset,
    ).toBe("1");
  }, 60_000);
});
