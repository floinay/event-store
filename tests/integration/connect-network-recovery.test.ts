import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { KafkaJS } from "@confluentinc/kafka-javascript";
import { uuidv7 } from "@event-store/contracts";
import { PostgresEventStore } from "@event-store/postgres-store";
import { EventStoreStack } from "../fixtures/event-store-stack.js";
import type { Pool } from "pg";

const suite = process.env.RUN_INTEGRATION === "true" ? describe : describe.skip;

suite("PG to Connect network recovery", () => {
  const stack = new EventStoreStack();
  let pool: Pool;
  beforeAll(async () => {
    await stack.start({ cdc: true, toxiproxy: true });
    pool = await stack.pool();
  }, 180_000);
  afterAll(async () => {
    await pool?.end();
    await stack.stop();
  }, 60_000);

  it("delivers an acknowledged event after a PG-to-Connect split heals", async () => {
    const kafka = new KafkaJS.Kafka({
      kafkaJS: { brokers: [stack.kafkaBroker()] },
    });
    const consumer = kafka.consumer({
      kafkaJS: {
        groupId: `split-recovery-${uuidv7()}`,
        autoCommit: false,
        fromBeginning: true,
      },
    });
    await consumer.connect();
    await consumer.subscribe({
      topics: ["event-store.events.v1"],
      replace: true,
    });
    const requestId = uuidv7();
    const delivered = new Promise<void>((resolve, reject) => {
      const timeout = setTimeout(
        () => reject(new Error("event was lost after network recovery")),
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
    await stack.setPostgresConnectEnabled(false);
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
          payload: { orderRef: "o1" },
        },
      ],
    });
    await expect(
      Promise.race([
        delivered.then(() => false),
        new Promise<true>((resolve) => setTimeout(() => resolve(true), 1_000)),
      ]),
    ).resolves.toBe(true);
    await stack.setPostgresConnectEnabled(true);
    await expect(delivered).resolves.toBeUndefined();
    await consumer.disconnect();
  }, 60_000);

  it("retains acknowledged events through latency and bandwidth faults", async () => {
    const kafka = new KafkaJS.Kafka({
      kafkaJS: { brokers: [stack.kafkaBroker()] },
    });
    const consumer = kafka.consumer({
      kafkaJS: {
        groupId: `network-profiles-${uuidv7()}`,
        autoCommit: false,
        fromBeginning: true,
        readUncommitted: false,
      },
    });
    const expected = new Set<string>();
    const received = new Set<string>();
    let complete!: () => void;
    let fail!: (error: Error) => void;
    const delivered = new Promise<void>((resolve, reject) => {
      complete = resolve;
      fail = reject;
    });
    const timeout = setTimeout(
      () => fail(new Error("CDC lost an event under a network profile")),
      60_000,
    );
    await consumer.connect();
    await consumer.subscribe({
      topics: ["event-store.events.v1"],
      replace: true,
    });
    await consumer.run({
      eachMessage: async ({ message }) => {
        const event = JSON.parse(message.value?.toString() ?? "{}") as {
          context?: { requestId?: string };
        };
        const requestId = event.context?.requestId;
        if (requestId !== undefined && expected.has(requestId)) {
          received.add(requestId);
          if (received.size === expected.size) complete();
        }
      },
    });
    const append = async (label: string, payload = "ok"): Promise<string> => {
      const requestId = uuidv7();
      expected.add(requestId);
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
          actor: { kind: "user", subjectRef: "usr_network" },
        },
        events: [
          {
            eventName: "order.created",
            schemaVersion: 1,
            occurredAt: "2026-08-04T10:12:18.120Z",
            payload: { label, payload },
          },
        ],
      });
      return requestId;
    };
    const waitFor = async (requestId: string): Promise<void> => {
      const deadline = Date.now() + 30_000;
      while (!received.has(requestId)) {
        if (Date.now() >= deadline)
          throw new Error(
            `CDC did not deliver ${requestId} under network fault`,
          );
        await new Promise((resolve) => setTimeout(resolve, 25));
      }
    };
    try {
      for (const latency of [20, 50, 100]) {
        const toxic = await stack.addPostgresConnectLatency(latency);
        try {
          await waitFor(await append(`latency-${latency}`));
        } finally {
          await toxic.remove();
        }
      }
      const bandwidth = await stack.addPostgresConnectBandwidthLimit(1024);
      try {
        await waitFor(await append("bandwidth-1mib", "x".repeat(512 * 1024)));
      } finally {
        await bandwidth.remove();
      }
      if (received.size === expected.size) complete();
      await delivered;
      expect(received).toEqual(expected);
    } finally {
      clearTimeout(timeout);
      await consumer.disconnect();
    }
  }, 90_000);
});
