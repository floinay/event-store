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
    await consumer.subscribe({
      topics: ["event-store.events.v1"],
      replace: true,
    });
    const requestId = uuidv7();
    const delivered = new Promise<void>((resolve, reject) => {
      const timeout = setTimeout(
        () =>
          reject(new Error("event was lost after Connect-to-Kafka recovery")),
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

  it("replays WAL after Connect is killed with Kafka delivery blocked", async () => {
    const kafka = new KafkaJS.Kafka({
      kafkaJS: { brokers: [stack.kafkaBroker()] },
    });
    const consumer = kafka.consumer({
      kafkaJS: {
        groupId: `connect-kafka-crash-${uuidv7()}`,
        autoCommit: false,
        fromBeginning: true,
        readUncommitted: false,
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
        () => reject(new Error("event was lost after Connect crash recovery")),
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
    try {
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
            payload: { orderRef: "connect-kafka-crash" },
          },
        ],
      });
      await expect(
        Promise.race([
          delivered.then(() => false),
          new Promise<true>((resolve) =>
            setTimeout(() => resolve(true), 1_000),
          ),
        ]),
      ).resolves.toBe(true);
      await stack.crashConnect();
    } finally {
      await stack.setConnectKafkaEnabled(true);
    }
    await stack.startConnect();
    await expect(delivered).resolves.toBeUndefined();
    await consumer.disconnect();
  }, 90_000);

  it("delivers pending WAL after a Kafka broker restart", async () => {
    const kafka = new KafkaJS.Kafka({
      kafkaJS: { brokers: [stack.kafkaBroker()] },
    });
    const consumer = kafka.consumer({
      kafkaJS: {
        groupId: `kafka-broker-crash-${uuidv7()}`,
        autoCommit: false,
        fromBeginning: true,
        readUncommitted: false,
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
        () => reject(new Error("event was lost after Kafka broker restart")),
        45_000,
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
    try {
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
            payload: { orderRef: "kafka-broker-crash" },
          },
        ],
      });
      await expect(
        Promise.race([
          delivered.then(() => false),
          new Promise<true>((resolve) =>
            setTimeout(() => resolve(true), 1_000),
          ),
        ]),
      ).resolves.toBe(true);
      await stack.restartKafka();
    } finally {
      await stack.setConnectKafkaEnabled(true);
    }
    await expect(delivered).resolves.toBeUndefined();
    await consumer.disconnect();
  }, 90_000);

  it("hides an aborted Connect transaction from read_committed before replay", async () => {
    const kafka = new KafkaJS.Kafka({
      kafkaJS: { brokers: [stack.kafkaBroker()] },
    });
    const requestId = uuidv7();
    const raw = kafka.consumer({
      kafkaJS: {
        groupId: `connect-eos-raw-${uuidv7()}`,
        autoCommit: false,
        fromBeginning: true,
        readUncommitted: true,
      },
    });
    const committed = kafka.consumer({
      kafkaJS: {
        groupId: `connect-eos-committed-${uuidv7()}`,
        autoCommit: false,
        fromBeginning: true,
        readUncommitted: false,
      },
    });
    await Promise.all([raw.connect(), committed.connect()]);
    await Promise.all(
      [raw, committed].map((consumer) =>
        consumer.subscribe({
          topics: ["event-store.events.v1"],
          replace: true,
        }),
      ),
    );
    let rawSeen!: (offset: bigint) => void;
    const rawRecord = new Promise<bigint>((resolve, reject) => {
      rawSeen = resolve;
      setTimeout(
        () =>
          reject(new Error("read_uncommitted did not observe Connect record")),
        30_000,
      );
    });
    let committedSeen!: (offset: bigint) => void;
    const committedRecord = new Promise<bigint>((resolve, reject) => {
      committedSeen = resolve;
      setTimeout(
        () =>
          reject(new Error("read_committed did not receive replayed record")),
        60_000,
      );
    });
    await raw.run({
      eachMessage: async ({ message }) => {
        const event = JSON.parse(message.value?.toString() ?? "{}") as {
          context?: { requestId?: string };
        };
        if (event.context?.requestId === requestId)
          rawSeen(BigInt(message.offset));
      },
    });
    await committed.run({
      eachMessage: async ({ message }) => {
        const event = JSON.parse(message.value?.toString() ?? "{}") as {
          context?: { requestId?: string };
        };
        if (event.context?.requestId === requestId)
          committedSeen(BigInt(message.offset));
      },
    });
    const latency = await stack.addConnectKafkaLatency(10_000);
    try {
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
          actor: { kind: "user", subjectRef: "usr_eos" },
        },
        events: [
          {
            eventName: "order.created",
            schemaVersion: 1,
            occurredAt: "2026-08-04T10:12:18.120Z",
            payload: { orderRef: "connect-eos-abort" },
          },
        ],
      });
      const abortedOffset = await rawRecord;
      await expect(
        Promise.race([
          committedRecord.then(() => false),
          new Promise<true>((resolve) =>
            setTimeout(() => resolve(true), 1_000),
          ),
        ]),
      ).resolves.toBe(true);
      await stack.crashConnect();
      await latency.remove();
      await stack.startConnect();
      const replayedOffset = await committedRecord;
      expect(replayedOffset).toBeGreaterThan(abortedOffset);
    } finally {
      await latency.remove().catch(() => undefined);
      await raw.disconnect().catch(() => undefined);
      await committed.disconnect().catch(() => undefined);
    }
  }, 120_000);
});
