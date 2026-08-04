import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { KafkaJS } from "@confluentinc/kafka-javascript";
import { createHash } from "node:crypto";
import { canonicalJson, uuidv7 } from "@event-store/contracts";
import { PostgresEventStore } from "@event-store/postgres-store";
import { EventStoreStack } from "../fixtures/event-store-stack.js";
import type { Pool, PoolClient } from "pg";

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
          payload: { orderRef: "o1", "😀": "2", "�": "1" },
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
      context: { causationId: string | null };
    };
    expect(event.key).toBe(`orders|Order|${aggregateId}`);
    if (envelope.aggregateId === undefined)
      throw new Error(
        `EventRouter value: ${event.value}; headers: ${JSON.stringify(event.headers)}`,
      );
    expect(envelope.aggregateId).toBe(aggregateId);
    expect(envelope).toEqual(
      (await store.readStream("orders", "Order", aggregateId))[0],
    );
    expect(envelope.streamRevision).toBe("1");
    expect(envelope.eventName).toBe("order.created");
    expect(envelope.context.causationId).toBeNull();
    if (event.headers.id === undefined)
      throw new Error(`EventRouter headers: ${JSON.stringify(event.headers)}`);
    expect(event.headers.id).toBe(envelope.eventId);
    expect(event.headers.type).toBe("order.created");
    expect(event.headers.envelopeHash).toBe(
      createHash("sha256").update(canonicalJson(envelope)).digest("hex"),
    );
  }, 60_000);

  it("delivers every batch event in revision order and loses none of 100 parallel streams", async () => {
    const kafka = new KafkaJS.Kafka({
      kafkaJS: { brokers: [stack.kafkaBroker()] },
    });
    const consumer = kafka.consumer({
      kafkaJS: {
        groupId: `cdc-parallel-${uuidv7()}`,
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
    const seen = new Set<string>();
    const order: string[] = [];
    let expected = new Set<string>();
    let received!: () => void;
    let rejected!: (error: Error) => void;
    const allReceived = new Promise<void>((resolve, reject) => {
      received = resolve;
      rejected = reject;
    });
    const timeout = setTimeout(
      () =>
        rejected(new Error("CDC lost an event from the parallel-stream run")),
      60_000,
    );
    const complete = (): void => {
      if (
        expected.size > 0 &&
        [...expected].every((eventId) => seen.has(eventId))
      )
        received();
    };
    await consumer.run({
      eachMessage: async ({ message }) => {
        const eventId = message.headers?.id;
        const id = Array.isArray(eventId)
          ? eventId[0]?.toString()
          : eventId?.toString();
        if (id !== undefined) {
          seen.add(id);
          order.push(id);
          complete();
        }
      },
    });
    try {
      const batchRequestId = uuidv7();
      const batchAggregateId = uuidv7();
      const batch = await store.append({
        producerService: "orders-command",
        namespace: "orders",
        aggregateType: "Order",
        aggregateId: batchAggregateId,
        requestId: batchRequestId,
        expectedRevision: { kind: "no_stream" },
        context: {
          requestId: batchRequestId,
          correlationId: uuidv7(),
          causationId: null,
          actor: { kind: "user", subjectRef: "usr_parallel" },
        },
        events: Array.from({ length: 3 }, (_, index) => ({
          eventName: "order.created",
          schemaVersion: 1,
          occurredAt: "2026-08-04T10:12:18.120Z",
          payload: { orderRef: `batch-${index}` },
        })),
      });
      const parallel = await Promise.all(
        Array.from({ length: 100 }, async (_, index) => {
          const requestId = uuidv7();
          return store.append({
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
              actor: { kind: "user", subjectRef: "usr_parallel" },
            },
            events: [
              {
                eventName: "order.created",
                schemaVersion: 1,
                occurredAt: "2026-08-04T10:12:18.120Z",
                payload: { orderRef: `parallel-${index}` },
              },
            ],
          });
        }),
      );
      const batchEventIds = batch.events.map((event) => event.eventId);
      expected = new Set([
        ...batchEventIds,
        ...parallel.map((result) => result.events[0]!.eventId),
      ]);
      complete();
      await allReceived;
      expect(
        order.filter((eventId) => batchEventIds.includes(eventId)),
      ).toEqual(batchEventIds);
      expect([...expected].filter((eventId) => seen.has(eventId))).toHaveLength(
        103,
      );
    } finally {
      clearTimeout(timeout);
      await consumer.disconnect();
    }
  }, 90_000);

  it("never publishes an uncommitted or rolled-back append", async () => {
    const kafka = new KafkaJS.Kafka({
      kafkaJS: { brokers: [stack.kafkaBroker()] },
    });
    const consumer = kafka.consumer({
      kafkaJS: {
        groupId: `cdc-transaction-${uuidv7()}`,
        autoCommit: false,
        fromBeginning: true,
        readUncommitted: false,
      },
    });
    const seen = new Set<string>();
    const waiters = new Map<string, () => void>();
    await consumer.connect();
    await consumer.subscribe({
      topics: ["event-store.events.v1"],
      replace: true,
    });
    await consumer.run({
      eachMessage: async ({ message }) => {
        const header = message.headers?.id;
        const eventId = Array.isArray(header)
          ? header[0]?.toString()
          : header?.toString();
        if (eventId === undefined) return;
        seen.add(eventId);
        waiters.get(eventId)?.();
      },
    });
    const waitFor = (eventId: string): Promise<void> => {
      if (seen.has(eventId)) return Promise.resolve();
      return new Promise((resolve, reject) => {
        const timeout = setTimeout(
          () =>
            reject(new Error(`CDC did not publish committed event ${eventId}`)),
          30_000,
        );
        waiters.set(eventId, () => {
          clearTimeout(timeout);
          waiters.delete(eventId);
          resolve();
        });
      });
    };
    const appendInTransaction = async (): Promise<{
      client: Awaited<ReturnType<typeof pool.connect>>;
      eventId: string;
    }> => {
      const client = await pool.connect();
      const requestId = uuidv7();
      try {
        await client.query("BEGIN");
        const result = await client.query<{
          append_v1: { events: { eventId: string }[] };
        }>(
          "SELECT event_store.append_v1($1,$2,$3,$4,$5,$6,$7,$8::jsonb,$9::jsonb)",
          [
            "orders-command",
            "orders",
            "Order",
            uuidv7(),
            requestId,
            "no_stream",
            null,
            JSON.stringify([
              {
                eventName: "order.created",
                schemaVersion: 1,
                occurredAt: "2026-08-04T10:12:18.120Z",
                payload: { orderRef: requestId },
              },
            ]),
            JSON.stringify({
              correlationId: uuidv7(),
              causationId: null,
              actor: { kind: "user", subjectRef: "usr_transaction" },
            }),
          ],
        );
        const eventId = result.rows[0]?.append_v1.events[0]?.eventId;
        if (eventId === undefined) throw new Error("append returned no event");
        return { client, eventId };
      } catch (error) {
        await client.query("ROLLBACK").catch(() => undefined);
        client.release();
        throw error;
      }
    };
    const appendBarrier = async (phase: string): Promise<string> => {
      const requestId = uuidv7();
      const result = await store.append({
        producerService: "orders-command",
        namespace: "orders",
        aggregateType: "Barrier",
        aggregateId: uuidv7(),
        requestId,
        expectedRevision: { kind: "no_stream" },
        context: {
          requestId,
          correlationId: uuidv7(),
          causationId: null,
          actor: { kind: "system", subjectRef: "cdc-transaction-test" },
        },
        events: [
          {
            eventName: "cdc.barrier",
            schemaVersion: 1,
            occurredAt: "2026-08-04T10:12:18.120Z",
            payload: { phase },
          },
        ],
      });
      return result.events[0]!.eventId;
    };
    try {
      const uncommitted = await appendInTransaction();
      await waitFor(await appendBarrier("while-uncommitted"));
      expect(seen.has(uncommitted.eventId)).toBe(false);
      await uncommitted.client.query("COMMIT");
      uncommitted.client.release();
      await waitFor(uncommitted.eventId);
      const rolledBack = await appendInTransaction();
      await rolledBack.client.query("ROLLBACK");
      rolledBack.client.release();
      await waitFor(await appendBarrier("after-rollback"));
      expect(seen.has(rolledBack.eventId)).toBe(false);
    } finally {
      await consumer.disconnect();
    }
  }, 90_000);

  it("delivers both transactions when event-number allocation precedes commit order", async () => {
    const kafka = new KafkaJS.Kafka({
      kafkaJS: { brokers: [stack.kafkaBroker()] },
    });
    const consumer = kafka.consumer({
      kafkaJS: {
        groupId: `cdc-commit-order-${uuidv7()}`,
        autoCommit: false,
        fromBeginning: true,
        readUncommitted: false,
      },
    });
    const seen = new Set<string>();
    const waiters = new Map<string, () => void>();
    await consumer.connect();
    await consumer.subscribe({
      topics: ["event-store.events.v1"],
      replace: true,
    });
    await consumer.run({
      eachMessage: async ({ message }) => {
        const id = message.headers?.id;
        const eventId = Array.isArray(id) ? id[0]?.toString() : id?.toString();
        if (eventId === undefined) return;
        seen.add(eventId);
        waiters.get(eventId)?.();
      },
    });
    const waitFor = (eventId: string): Promise<void> => {
      if (seen.has(eventId)) return Promise.resolve();
      return new Promise((resolve, reject) => {
        const timeout = setTimeout(
          () => reject(new Error(`CDC did not deliver ${eventId}`)),
          30_000,
        );
        waiters.set(eventId, () => {
          clearTimeout(timeout);
          waiters.delete(eventId);
          resolve();
        });
      });
    };
    const append = async (client: Pool | PoolClient, requestId: string) => {
      const result = await client.query<{
        append_v1: { events: { eventId: string; eventNumber: string }[] };
      }>(
        "SELECT event_store.append_v1($1,$2,$3,$4,$5,$6,$7,$8::jsonb,$9::jsonb)",
        [
          "orders-command",
          "orders",
          "Order",
          uuidv7(),
          requestId,
          "no_stream",
          null,
          JSON.stringify([
            {
              eventName: "order.created",
              schemaVersion: 1,
              occurredAt: "2026-08-04T10:12:18.120Z",
              payload: { orderRef: requestId },
            },
          ]),
          JSON.stringify({
            correlationId: uuidv7(),
            causationId: null,
            actor: { kind: "user", subjectRef: "usr_commit_order" },
          }),
        ],
      );
      const event = result.rows[0]?.append_v1.events[0];
      if (event === undefined) throw new Error("append returned no event");
      return event;
    };
    const first = await pool.connect();
    const second = await pool.connect();
    try {
      const firstRequestId = uuidv7();
      const secondRequestId = uuidv7();
      await first.query("BEGIN");
      const allocatedFirst = await append(first, firstRequestId);
      const committedSecond = await append(second, secondRequestId);
      expect(BigInt(allocatedFirst.eventNumber)).toBeLessThan(
        BigInt(committedSecond.eventNumber),
      );
      await waitFor(committedSecond.eventId);
      expect(seen.has(allocatedFirst.eventId)).toBe(false);
      await first.query("COMMIT");
      await Promise.all([
        waitFor(allocatedFirst.eventId),
        waitFor(committedSecond.eventId),
      ]);
    } finally {
      await first.query("ROLLBACK").catch(() => undefined);
      first.release();
      second.release();
      await consumer.disconnect();
    }
  }, 90_000);
});
