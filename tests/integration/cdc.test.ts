import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { KafkaJS } from "@confluentinc/kafka-javascript";
import { uuidv7 } from "@event-store/contracts";
import { PostgresEventStore } from "@event-store/postgres-store";
import { EventStoreStack } from "../fixtures/event-store-stack.js";
import type { Pool } from "pg";

const suite = process.env.RUN_INTEGRATION === "true" ? describe : describe.skip;

suite("Debezium CDC", () => {
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

  it("publishes the committed canonical envelope only after append", async () => {
    const kafka = new KafkaJS.Kafka({
      kafkaJS: { brokers: [stack.kafkaBroker()] },
    });
    const consumer = kafka.consumer({
      kafkaJS: {
        groupId: `cdc-smoke-${uuidv7()}`,
        autoCommit: false,
        fromBeginning: true,
      },
    });
    await consumer.connect();
    await consumer.subscribe({
      topics: ["event-store.events.v1"],
      replace: true,
    });
    const received = new Promise<{
      key: string;
      value: string;
      headers: Record<string, string>;
    }>((resolve, reject) => {
      const timer = setTimeout(() => {
        void stack
          .cdcDiagnostic()
          .then((diagnostic) =>
            reject(
              new Error(
                `timed out waiting for CDC event: ${JSON.stringify(diagnostic)}`,
              ),
            ),
          );
      }, 30_000);
      void consumer.run({
        eachMessage: async ({ message }) => {
          clearTimeout(timer);
          resolve({
            key: message.key?.toString() ?? "",
            value: message.value?.toString() ?? "",
            headers: Object.fromEntries(
              Object.entries(message.headers ?? {}).map(([key, value]) => [
                key,
                Array.isArray(value)
                  ? (value[0]?.toString() ?? "")
                  : (value?.toString() ?? ""),
              ]),
            ),
          });
        },
      });
    });
    const requestId = uuidv7();
    const aggregateId = uuidv7();
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
    const event = await received;
    await consumer.disconnect();
    const envelope = JSON.parse(event.value) as {
      eventId: string;
      aggregateId: string;
      streamRevision: string;
      eventName: string;
    };
    expect(event.key).toBe(`orders|Order|${aggregateId}`);
    if (envelope.aggregateId === undefined)
      throw new Error(
        `EventRouter value: ${event.value}; headers: ${JSON.stringify(event.headers)}`,
      );
    expect(envelope.aggregateId).toBe(aggregateId);
    expect(envelope.streamRevision).toBe("1");
    expect(envelope.eventName).toBe("order.created");
    if (event.headers.id === undefined)
      throw new Error(`EventRouter headers: ${JSON.stringify(event.headers)}`);
    expect(event.headers.id).toBe(envelope.eventId);
    expect(event.headers.type).toBe("order.created");
  }, 60_000);
});
