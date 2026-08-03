import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { KafkaJS } from "@confluentinc/kafka-javascript";
import { uuidv7 } from "@event-store/contracts";
import { PostgresEventStore } from "@event-store/postgres-store";
import { EventStoreStack } from "../fixtures/event-store-stack.js";
import type { Pool } from "pg";

const suite = process.env.RUN_LATENCY === "true" ? describe : describe.skip;

suite("PostgreSQL commit to Kafka consumer latency", () => {
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

  it("keeps p50 at or below 50ms in the dedicated low-latency environment", async () => {
    const committed = new Map<string, number>();
    const observed = new Map<string, number>();
    const kafka = new KafkaJS.Kafka({
      kafkaJS: { brokers: [stack.kafkaBroker()] },
    });
    const consumer = kafka.consumer({
      kafkaJS: {
        groupId: `latency-${uuidv7()}`,
        autoCommit: false,
        fromBeginning: true,
      },
    });
    await consumer.connect();
    await consumer.subscribe({
      topics: ["event-store.events.v1"],
      replace: true,
    });
    const warmupRequestId = uuidv7();
    let warmupObserved!: () => void;
    const warmup = new Promise<void>((resolve) => {
      warmupObserved = resolve;
    });
    const received = new Promise<void>((resolve, reject) => {
      const deadline = setTimeout(
        () => reject(new Error("timed out waiting for latency samples")),
        30_000,
      );
      void consumer.run({
        eachMessage: async ({ message }) => {
          const event = JSON.parse(message.value?.toString() ?? "{}") as {
            context?: { requestId?: string };
          };
          if (event.context?.requestId === warmupRequestId) warmupObserved();
          else if (event.context?.requestId !== undefined)
            observed.set(event.context.requestId, performance.now());
          if (observed.size === 20) {
            clearTimeout(deadline);
            resolve();
          }
        },
      });
    });
    await store.append({
      producerService: "latency-probe",
      namespace: "latency",
      aggregateType: "Probe",
      aggregateId: uuidv7(),
      requestId: warmupRequestId,
      expectedRevision: { kind: "no_stream" },
      context: {
        requestId: warmupRequestId,
        correlationId: uuidv7(),
        causationId: null,
        actor: { kind: "service", subjectRef: "latency-probe" },
      },
      events: [
        {
          eventName: "probe.appended",
          schemaVersion: 1,
          occurredAt: new Date().toISOString(),
          payload: { warmup: true },
        },
      ],
    });
    await warmup;
    for (let index = 0; index < 20; index += 1) {
      const requestId = uuidv7();
      const aggregateId = uuidv7();
      await store.append({
        producerService: "latency-probe",
        namespace: "latency",
        aggregateType: "Probe",
        aggregateId,
        requestId,
        expectedRevision: { kind: "no_stream" },
        context: {
          requestId,
          correlationId: uuidv7(),
          causationId: null,
          actor: { kind: "service", subjectRef: "latency-probe" },
        },
        events: [
          {
            eventName: "probe.appended",
            schemaVersion: 1,
            occurredAt: new Date().toISOString(),
            payload: { index },
          },
        ],
      });
      committed.set(requestId, performance.now());
    }
    await received;
    await consumer.disconnect();
    const samples = [...committed]
      .map(([eventId, committedAt]) => observed.get(eventId)! - committedAt)
      .sort((left, right) => left - right);
    expect(samples[Math.floor(samples.length / 2)]).toBeLessThanOrEqual(50);
  }, 90_000);
});
