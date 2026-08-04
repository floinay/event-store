import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { KafkaJS } from "@confluentinc/kafka-javascript";
import { uuidv7 } from "@event-store/contracts";
import { PostgresEventStore } from "@event-store/postgres-store";
import { EventStoreStack } from "../fixtures/event-store-stack.js";
import type { Pool } from "pg";

const suite =
  process.env.RUN_LONG_RECOVERY === "true" ? describe : describe.skip;
const outageMs = Number(process.env.CONNECT_OUTAGE_MS ?? 300_000);

suite("five-minute Connect outage recovery", () => {
  const stack = new EventStoreStack();
  let pool: Pool;

  beforeAll(async () => {
    if (!Number.isInteger(outageMs) || outageMs < 1)
      throw new Error("CONNECT_OUTAGE_MS must be a positive integer");
    await stack.start({ cdc: true });
    pool = await stack.pool();
  }, 180_000);
  afterAll(async () => {
    await pool?.end();
    await stack.stop();
  }, 60_000);

  it("delivers every acknowledged event after Connect resumes", async () => {
    const kafka = new KafkaJS.Kafka({
      kafkaJS: { brokers: [stack.kafkaBroker()] },
    });
    const consumer = kafka.consumer({
      kafkaJS: {
        groupId: `connect-long-outage-${uuidv7()}`,
        autoCommit: false,
        fromBeginning: true,
        readUncommitted: false,
      },
    });
    const requestIds = Array.from({ length: 20 }, () => uuidv7());
    const awaiting = new Set(requestIds);
    let delivered!: () => void;
    const allDelivered = new Promise<void>((resolve, reject) => {
      delivered = resolve;
      setTimeout(
        () =>
          reject(
            new Error(
              `Connect recovery lost ${awaiting.size} acknowledged event(s)`,
            ),
          ),
        outageMs + 60_000,
      );
    });
    await consumer.connect();
    await consumer.subscribe({
      topics: ["event-store.events.v1"],
      replace: true,
    });
    await consumer.run({
      eachMessage: async ({ message }) => {
        const envelope = JSON.parse(message.value?.toString() ?? "{}") as {
          context?: { requestId?: string };
        };
        const requestId = envelope.context?.requestId;
        if (requestId !== undefined && awaiting.delete(requestId)) {
          if (awaiting.size === 0) delivered();
        }
      },
    });
    try {
      await stack.stopConnect();
      await Promise.all(
        requestIds.map((requestId) =>
          new PostgresEventStore(pool).append({
            producerService: "connect-long-outage-test",
            namespace: "orders",
            aggregateType: "Order",
            aggregateId: uuidv7(),
            requestId,
            expectedRevision: { kind: "no_stream" },
            context: {
              requestId,
              correlationId: uuidv7(),
              causationId: null,
              actor: {
                kind: "service",
                subjectRef: "connect-long-outage-test",
              },
            },
            events: [
              {
                eventName: "order.created",
                schemaVersion: 1,
                occurredAt: "2026-08-04T10:12:18.120Z",
                payload: { recovery: "five-minute-connect-outage" },
              },
            ],
          }),
        ),
      );
      // This is the required outage interval, not a readiness wait.
      await new Promise((resolve) => setTimeout(resolve, outageMs));
      expect(awaiting.size).toBe(requestIds.length);
    } finally {
      await stack.startConnect();
    }
    await expect(allDelivered).resolves.toBeUndefined();
    await consumer.disconnect();
  }, outageMs + 150_000);
});
