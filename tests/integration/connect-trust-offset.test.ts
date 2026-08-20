import { afterAll, beforeAll, describe, it } from "vitest";
import { KafkaJS } from "@confluentinc/kafka-javascript";
import { uuidv7 } from "@event-store/contracts";
import { PostgresEventStore } from "@event-store/postgres-store";
import { EventStoreStack } from "../fixtures/event-store-stack.js";
import type { Pool } from "pg";

const suite = process.env.RUN_INTEGRATION === "true" ? describe : describe.skip;

async function eventually(
  condition: () => Promise<boolean>,
  message: string,
): Promise<void> {
  const deadline = Date.now() + 60_000;
  while (Date.now() < deadline) {
    if (await condition()) return;
    await new Promise((resolve) => setTimeout(resolve, 200));
  }
  throw new Error(message);
}

suite("Debezium trust_offset", () => {
  const stack = new EventStoreStack();
  let pool: Pool;

  beforeAll(async () => {
    await stack.start({ cdc: true });
    pool = await stack.pool();
  }, 180_000);
  afterAll(async () => {
    await pool?.end();
    await stack.stop();
  }, 60_000);

  it("fails closed when the durable Connect offset is behind confirmed_flush_lsn", async () => {
    const kafka = new KafkaJS.Kafka({
      kafkaJS: { brokers: [stack.kafkaBroker()] },
    });
    const offsets = new Map<string, { partition: number; values: string[] }>();
    const offsetConsumer = kafka.consumer({
      kafkaJS: {
        groupId: `trust-offset-observer-${uuidv7()}`,
        autoCommit: false,
        fromBeginning: true,
        readUncommitted: false,
      },
    });
    const producer = kafka.producer({ kafkaJS: { acks: -1 } });
    try {
      await offsetConsumer.connect();
      await offsetConsumer.subscribe({
        topics: ["_connect-event-store-offsets"],
        replace: true,
      });
      await offsetConsumer.run({
        eachMessage: async ({ partition, message }) => {
          if (message.key === null || message.value === null) return;
          const key = message.key.toString();
          const value = message.value.toString();
          const state = offsets.get(key) ?? { partition, values: [] };
          if (state.values.at(-1) !== value) state.values.push(value);
          offsets.set(key, state);
        },
      });
      const append = async (): Promise<string> => {
        const requestId = uuidv7();
        await new PostgresEventStore(pool).append({
          producerService: "trust-offset-test",
          namespace: "orders",
          aggregateType: "Order",
          aggregateId: uuidv7(),
          requestId,
          expectedRevision: { kind: "no_stream" },
          context: {
            requestId,
            correlationId: uuidv7(),
            causationId: null,
            actor: { kind: "service", subjectRef: "trust-offset-test" },
          },
          events: [
            {
              eventName: "order.created",
              schemaVersion: 1,
              occurredAt: "2026-08-04T10:12:18.120Z",
              payload: { scenario: "stored-offset-behind-slot" },
            },
          ],
        });
        const lsn = await pool.query<{ lsn: string }>(
          "SELECT pg_current_wal_lsn()::text AS lsn",
        );
        const value = lsn.rows[0]?.lsn;
        if (value === undefined)
          throw new Error("could not read append WAL LSN");
        return value;
      };
      await append();
      await eventually(
        async () => offsets.size > 0,
        "Kafka Connect did not persist the initial source offset",
      );
      const [offsetKey, offsetState] = [...offsets.entries()][0] ?? [];
      const staleOffset = offsetState?.values[0];
      if (
        offsetKey === undefined ||
        offsetState === undefined ||
        staleOffset === undefined
      )
        throw new Error("could not identify the initial Connect source offset");
      const secondAppendLsn = await append();
      await eventually(
        async () =>
          offsets
            .get(offsetKey)
            ?.values.some((candidate) => candidate !== staleOffset) === true,
        "Kafka Connect did not advance the persisted source offset",
      );
      await stack.waitForLiveCdcCaughtUp(secondAppendLsn);

      // Keep the offsets, but prevent the restarted worker from auto-starting
      // this connector before its offset backing store has finished loading.
      const deletion = await fetch(
        `${stack.connectUrl}/connectors/event-store-live`,
        { method: "DELETE" },
      );
      if (!deletion.ok)
        throw new Error(`connector deletion failed: ${await deletion.text()}`);
      await stack.stopConnect();
      await producer.connect();
      await producer.send({
        topic: "_connect-event-store-offsets",
        messages: [
          {
            key: offsetKey,
            value: staleOffset,
            partition: offsetState.partition,
          },
        ],
      });
      await eventually(
        async () => offsets.get(offsetKey)?.values.at(-1) === staleOffset,
        "the stale Connect offset was not durably visible before restart",
      );
      await producer.disconnect();
      await stack.startConnect(undefined, false);
      await eventually(async () => {
        const status = (await fetch(
          `${stack.connectUrl}/connectors/event-store-live/status`,
        ).then((response) => response.json())) as {
          tasks?: { state?: string }[];
        };
        return status.tasks?.[0]?.state === "FAILED";
      }, "trust_offset did not fail the task with a stale stored offset");
    } finally {
      await producer.disconnect().catch(() => undefined);
      await offsetConsumer.disconnect().catch(() => undefined);
    }
  }, 150_000);
});
