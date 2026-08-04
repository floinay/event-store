import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { KafkaJS } from "@confluentinc/kafka-javascript";
import { uuidv7 } from "@event-store/contracts";
import { PostgresEventStore } from "@event-store/postgres-store";
import { EventStoreStack } from "../fixtures/event-store-stack.js";
import type { Pool } from "pg";

const suite = process.env.RUN_INTEGRATION === "true" ? describe : describe.skip;

suite("Connect to Kafka network recovery", () => {
  const stack = new EventStoreStack();
  let pool: Pool;

  beforeAll(async () => {
    await stack.start({ cdc: true, toxiproxy: true, connectKafkaProxy: true });
    pool = await stack.pool();
  }, 180_000);
  afterAll(async () => {
    await pool?.end();
    await stack.stop();
  }, 60_000);

  it("replays an acknowledged append after a Connect-to-Kafka split heals", async () => {
    const kafka = new KafkaJS.Kafka({
      kafkaJS: { brokers: [stack.kafkaBroker()] },
    });
    const consumer = kafka.consumer({
      kafkaJS: {
        groupId: `connect-kafka-recovery-${uuidv7()}`,
        autoCommit: false,
        fromBeginning: true,
        readUncommitted: false,
      },
    });
    await consumer.connect();
    await consumer.subscribe({ topics: ["event-store.events.v1"], replace: true });
    const requestId = uuidv7();
    const delivered = new Promise<void>((resolve, reject) => {
      const timeout = setTimeout(
        () => reject(new Error("event was lost after Connect-to-Kafka recovery")),
        30_000,
      );
      void consumer.run({
        eachMessage: async ({ message }) => {
          const event = JSON.parse(message.value?.toString() ?? "{}") as {
            context?: { requestId?: string };
          };
          if (event.context?.requestId === requestId) {
            clearTimeout(timeout);
            resolve();
          }
        },
      });
    });
    await stack.setConnectKafkaEnabled(false);
    await new PostgresEventStore(pool).append({
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
          payload: { orderRef: "connect-kafka-split" },
        },
      ],
    });
    await expect(
      Promise.race([
        delivered.then(() => false),
        new Promise<true>((resolve) => setTimeout(() => resolve(true), 1_000)),
      ]),
    ).resolves.toBe(true);
    await stack.setConnectKafkaEnabled(true);
    await expect(delivered).resolves.toBeUndefined();
    await consumer.disconnect();
  }, 60_000);
});
