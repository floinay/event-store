import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { createHash } from "node:crypto";
import { KafkaJS } from "@confluentinc/kafka-javascript";
import { canonicalJson, partitionKey, uuidv7 } from "@event-store/contracts";
import {
  KafkaProjectionRunner,
  ProjectionCheckpointStore,
  ProjectionCrashError,
  ProjectionFailureReporter,
  ProjectionTransactionRunner,
} from "@event-store/projection-runtime";
import { EventStoreStack } from "../fixtures/event-store-stack.js";
import type { Pool } from "pg";

const suite = process.env.RUN_INTEGRATION === "true" ? describe : describe.skip;

async function eventually(check: () => Promise<boolean>): Promise<void> {
  const deadline = Date.now() + 30_000;
  while (Date.now() < deadline) {
    if (await check()) return;
    await new Promise((resolve) => setTimeout(resolve, 100));
  }
  throw new Error("condition did not become true within 30 seconds");
}

suite("projection consumer Kafka crash recovery", () => {
  const stack = new EventStoreStack();
  let pool: Pool;

  beforeAll(async () => {
    await stack.start({ cdc: true });
    pool = await stack.pool();
    await pool.query(
      "CREATE SCHEMA consumer_kafka_crash; CREATE TABLE consumer_kafka_crash.events(projection_name text NOT NULL,event_id uuid NOT NULL,PRIMARY KEY(projection_name,event_id))",
    );
  }, 180_000);
  afterAll(async () => {
    await pool?.end();
    await stack.stop();
  }, 60_000);

  it.each([
    ["after_kafka_poll", 1],
    ["before_kafka_offset_commit", 1],
    ["after_kafka_offset_commit", 2],
  ] as const)(
    "restarts safely after %s",
    async (point, eventCount) => {
      const kafka = new KafkaJS.Kafka({
        kafkaJS: { brokers: [stack.kafkaBroker()] },
      });
      const topic = `consumer-crash-${uuidv7()}`;
      const admin = kafka.admin();
      await admin.connect();
      await admin.createTopics({
        topics: [{ topic, numPartitions: 1, replicationFactor: 1 }],
      });
      await admin.disconnect();
      const generationId = uuidv7();
      const projectionName = `crash-${point}`;
      const identity = { name: projectionName, generationId };
      await pool.query(
        "INSERT INTO projection_runtime.generations(projection_name,generation_id,status,created_at) VALUES ($1,$2,'building',clock_timestamp())",
        [projectionName, generationId],
      );
      let reached!: () => void;
      const crashed = new Promise<void>((resolve) => {
        reached = resolve;
      });
      const makeRunner = (crash = false) =>
        new KafkaProjectionRunner(
          {
            brokers: [stack.kafkaBroker()],
            groupId: `consumer-crash-${generationId}`,
            topic,
          },
          new ProjectionTransactionRunner(pool, identity, (event) => event),
          async (client, event) => {
            await client.query(
              "INSERT INTO consumer_kafka_crash.events(projection_name,event_id) VALUES ($1,$2)",
              [projectionName, event.eventId],
            );
          },
          new ProjectionCheckpointStore(pool, identity),
          new ProjectionFailureReporter(pool, identity),
          undefined,
          crash
            ? {
                hit: (hit) => {
                  if (hit === point) {
                    reached();
                    throw new ProjectionCrashError(`crash at ${point}`);
                  }
                },
              }
            : undefined,
        );
      let consumer = await makeRunner(true).start();
      const producer = kafka.producer({
        kafkaJS: { idempotent: true, acks: -1 },
      });
      await producer.connect();
      const aggregateId = uuidv7();
      const events = Array.from({ length: eventCount }, (_, index) => ({
        eventId: uuidv7(),
        namespace: "orders",
        aggregateType: "Order",
        aggregateId,
        streamRevision: String(index + 1),
        eventNumber: String(index + 1),
        eventName: "order.created",
        schemaVersion: 1,
        occurredAt: "2026-08-04T10:12:18.120Z",
        recordedAt: "2026-08-04T10:12:18.120Z",
        producerService: "orders-command",
        context: {
          requestId: uuidv7(),
          correlationId: uuidv7(),
          causationId: null,
          actor: { kind: "service" as const, subjectRef: "consumer-crash" },
        },
        payload: { point, index },
      }));
      try {
        await producer.send({
          topic,
          messages: events.map((event) => {
            const value = canonicalJson(event);
            return {
              key: partitionKey(event),
              value,
              headers: {
                id: event.eventId,
                type: event.eventName,
                envelopeHash: createHash("sha256").update(value).digest("hex"),
                namespace: event.namespace,
                aggregateType: event.aggregateType,
                streamRevision: event.streamRevision,
              },
            };
          }),
        });
        await crashed;
        await consumer.disconnect().catch(() => undefined);
        consumer = await makeRunner().start();
        await eventually(async () => {
          const result = await pool.query<{ count: number }>(
            "SELECT count(*)::int AS count FROM consumer_kafka_crash.events WHERE projection_name=$1",
            [projectionName],
          );
          return result.rows[0]?.count === eventCount;
        });
        const checkpoint = await new ProjectionCheckpointStore(
          pool,
          identity,
        ).nextOffset(topic, 0);
        expect(checkpoint).toBe(BigInt(eventCount));
      } finally {
        await producer.disconnect().catch(() => undefined);
        await consumer.disconnect().catch(() => undefined);
      }
    },
    90_000,
  );
});
