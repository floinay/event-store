import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { KafkaJS } from "@confluentinc/kafka-javascript";
import { uuidv7 } from "@event-store/contracts";
import { PostgresEventStore } from "@event-store/postgres-store";
import { EventStoreStack } from "../fixtures/event-store-stack.js";
import type { Pool } from "pg";

const suite = process.env.RUN_LATENCY === "true" ? describe : describe.skip;
const sampleCount = Number(process.env.LATENCY_SAMPLE_COUNT ?? 100);

function percentile(samples: readonly number[], quantile: number): number {
  return samples[
    Math.min(samples.length - 1, Math.ceil(samples.length * quantile) - 1)
  ]!;
}

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

  it("keeps PostgreSQL-commit-to-consumer p50 at or below 50ms", async () => {
    if (!Number.isInteger(sampleCount) || sampleCount < 100)
      throw new Error(
        "LATENCY_SAMPLE_COUNT must be an integer of at least 100",
      );
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
    let received!: () => void;
    const samplesReceived = new Promise<void>((resolve, reject) => {
      received = resolve;
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
          if (
            committed.size === sampleCount &&
            [...committed.keys()].every((requestId) => observed.has(requestId))
          ) {
            clearTimeout(deadline);
            received();
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
    for (let index = 0; index < sampleCount; index += 1) {
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
      if (
        committed.size === sampleCount &&
        [...committed.keys()].every((id) => observed.has(id))
      )
        received();
    }
    await samplesReceived;
    await consumer.disconnect();
    const samples = [...committed]
      .map(([eventId, committedAt]) => observed.get(eventId)! - committedAt)
      .sort((left, right) => left - right);
    const metrics = {
      samples: samples.length,
      p50: percentile(samples, 0.5),
      p95: percentile(samples, 0.95),
      p99: percentile(samples, 0.99),
      p999: percentile(samples, 0.999),
    };
    console.info(`CDC latency metrics: ${JSON.stringify(metrics)}`);
    expect(metrics.samples).toBe(sampleCount);
    expect(metrics.p50).toBeLessThanOrEqual(50);
    expect(metrics.p95).toBeGreaterThanOrEqual(metrics.p50);
    expect(metrics.p99).toBeGreaterThanOrEqual(metrics.p95);
    expect(metrics.p999).toBeGreaterThanOrEqual(metrics.p99);
  }, 180_000);
});
