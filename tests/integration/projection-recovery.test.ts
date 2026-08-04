import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { z } from "zod";
import { KafkaJS } from "@confluentinc/kafka-javascript";
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

  it("processes every same-partition record in a Kafka batch", async () => {
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
        {
          eventName: "order.created",
          schemaVersion: 1,
          occurredAt: "2026-08-04T10:12:18.121Z",
          payload: { orderRef: "o2" },
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
      return result.rows[0]?.count === 2;
    });
    await consumer.disconnect();
    expect(
      (
        await pool.query(
          "SELECT max(next_offset)::text AS next_offset FROM projection_runtime.checkpoints WHERE projection_name=$1 AND generation_id=$2 AND last_event_id IS NOT NULL",
          [projectionName, generationId],
        )
      ).rows[0]?.next_offset,
    ).toBe("2");
    expect(
      (
        await pool.query(
          "SELECT count(*)::int AS count FROM projection_runtime.checkpoints WHERE projection_name=$1 AND generation_id=$2",
          [projectionName, generationId],
        )
      ).rows[0]?.count,
    ).toBe(24);
  }, 60_000);

  it("sends a poisoned projection event to DLQ after eight durable retries", async () => {
    const aggregateId = uuidv7();
    const requestId = uuidv7();
    const generationId = uuidv7();
    const projectionName = "poison";
    await pool.query(
      "INSERT INTO projection_runtime.generations(projection_name,generation_id,status,created_at) VALUES ($1,$2,'building',clock_timestamp())",
      [projectionName, generationId],
    );
    const appended = await store.append({
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
        actor: { kind: "user", subjectRef: "usr_poison" },
      },
      events: [
        {
          eventName: "order.created",
          schemaVersion: 1,
          occurredAt: "2026-08-04T10:12:18.120Z",
          payload: { orderRef: "poison" },
        },
      ],
    });
    const eventId = appended.events[0]?.eventId;
    if (eventId === undefined)
      throw new Error("append omitted poison event ID");
    const kafka = new KafkaJS.Kafka({
      kafkaJS: { brokers: [stack.kafkaBroker()] },
    });
    const dlq = kafka.consumer({
      kafkaJS: {
        groupId: `poison-dlq-${uuidv7()}`,
        autoCommit: false,
        fromBeginning: true,
        readUncommitted: false,
      },
    });
    await dlq.connect();
    await dlq.subscribe({
      topics: ["event-store.projection-dlq.v1"],
      replace: true,
    });
    let resolveDlq!: () => void;
    const published = new Promise<void>((resolve, reject) => {
      resolveDlq = resolve;
      setTimeout(
        () => reject(new Error("poison event was not published to DLQ")),
        70_000,
      );
    });
    await dlq.run({
      eachMessage: async ({ message }) => {
        const value = JSON.parse(message.value?.toString() ?? "{}") as {
          envelope?: { eventId?: string };
        };
        if (value.envelope?.eventId === eventId) resolveDlq();
      },
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
        groupId: `poison-${uuidv7()}`,
        topic: "event-store.events.v1",
      },
      new ProjectionTransactionRunner(
        pool,
        identity,
        createProjectionEventTransformer(upcasters, schemas),
      ),
      async () => {
        throw new Error("poison handler");
      },
      new ProjectionCheckpointStore(pool, identity),
      new ProjectionFailureReporter(pool, identity),
    );
    const consumer = await runner.start();
    try {
      await published;
      await expect(
        pool.query<{ attempt_count: number; dlq_published_at: string | null }>(
          "SELECT attempt_count,dlq_published_at FROM projection_runtime.failures WHERE projection_name=$1 AND generation_id=$2 AND event_id=$3",
          [projectionName, generationId, eventId],
        ),
      ).resolves.toMatchObject({
        rows: [{ attempt_count: 8, dlq_published_at: expect.any(String) }],
      });
    } finally {
      await consumer.disconnect().catch(() => undefined);
      await dlq.disconnect().catch(() => undefined);
    }
  }, 120_000);
});
