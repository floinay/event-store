import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { createHash } from "node:crypto";
import { canonicalJson, uuidv7 } from "@event-store/contracts";
import { KafkaJS } from "@confluentinc/kafka-javascript";
import {
  KafkaProjectionRunner,
  ProjectionCheckpointStore,
  ProjectionFailureReporter,
  ProjectionTransactionRunner,
  type ProjectionIdentity,
} from "@event-store/projection-runtime";
import {
  appendReplayBarriers,
  ensureReplayTopic,
  kafkaDefaultPartition,
  ReplayCoordinator,
  replayConnectorConfig,
  replayTopicName,
} from "@event-store/replay";
import { PostgresEventStore } from "@event-store/postgres-store";
import { EventStoreStack } from "../fixtures/event-store-stack.js";
import type { Pool } from "pg";

const suite = process.env.RUN_INTEGRATION === "true" ? describe : describe.skip;

async function eventually(check: () => Promise<boolean>): Promise<void> {
  const deadline = Date.now() + 60_000;
  while (Date.now() < deadline) {
    if (await check()) return;
    await new Promise((resolve) => setTimeout(resolve, 100));
  }
  throw new Error("condition did not become true within 60 seconds");
}

async function projectionChecksum(
  pool: Pool,
  generationId: string,
): Promise<string> {
  const rows = await pool.query<{ event_id: string; payload: string }>(
    `SELECT event_id::text,payload::text FROM replay_projection.events
     WHERE generation_id=$1 ORDER BY event_id`,
    [generationId],
  );
  return createHash("sha256")
    .update(rows.rows.map((row) => `${row.event_id}:${row.payload}`).join("\n"))
    .digest("hex");
}

async function fullFoldChecksum(pool: Pool): Promise<string> {
  const rows = await pool.query<{ event_id: string; payload: string }>(
    `SELECT event_id::text,event_envelope->>'payload' AS payload
     FROM event_store.events ORDER BY event_id`,
  );
  return createHash("sha256")
    .update(rows.rows.map((row) => `${row.event_id}:${row.payload}`).join("\n"))
    .digest("hex");
}

function projectionRunner(
  pool: Pool,
  identity: ProjectionIdentity,
  topic: string,
  groupId: string,
  broker: string,
): KafkaProjectionRunner {
  return new KafkaProjectionRunner(
    { brokers: [broker], groupId, topic },
    new ProjectionTransactionRunner(pool, identity, (event) => event),
    async (client, event) => {
      await client.query(
        `INSERT INTO replay_projection.events(generation_id,event_id,payload)
         VALUES ($1,$2,$3::jsonb) ON CONFLICT DO NOTHING`,
        [identity.generationId, event.eventId, JSON.stringify(event.payload)],
      );
    },
    new ProjectionCheckpointStore(pool, identity),
    new ProjectionFailureReporter(pool, identity),
  );
}

suite("replay coordinator", () => {
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

  it("refuses barriers without a committed replay-consumer offset", async () => {
    const identity = {
      projectionName: "orders",
      generationId: uuidv7(),
      replayId: "orders-aug-2026",
    };
    const coordinator = new ReplayCoordinator(
      pool,
      "http://unused",
      [stack.kafkaBroker()],
      1,
    );
    await coordinator.createGeneration(identity);
    await ensureReplayTopic([stack.kafkaBroker()], identity.replayId, 1);
    await expect(
      coordinator.activate(identity, {
        kafkaLag: async () => 1n,
        consumerGroupId: `replay-lagged-${uuidv7()}`,
        checksums: async () => ({ expected: "same", actual: "same" }),
      }),
    ).rejects.toThrow("replay Kafka lag is 1");
    await expect(
      coordinator.activate(identity, {
        kafkaLag: async () => 0n,
        consumerGroupId: `replay-empty-${uuidv7()}`,
        checksums: async () => ({ expected: "same", actual: "same" }),
      }),
    ).rejects.toThrow("all 24 replay barriers");
    const barriers = await appendReplayBarriers(
      new PostgresEventStore(pool),
      identity.replayId,
    );
    for (const barrier of barriers) {
      const row = await pool.query<{ event_envelope: unknown }>(
        "SELECT event_envelope FROM event_store.events WHERE event_id=$1",
        [barrier.eventId],
      );
      await coordinator.recordBarrier(identity, {
        topic: replayTopicName(identity.replayId),
        partition: barrier.partition,
        offset: barrier.partition,
        value: JSON.stringify(row.rows[0]?.event_envelope),
      });
    }
    await expect(
      coordinator.activate(identity, {
        kafkaLag: async () => 0n,
        consumerGroupId: `replay-empty-${uuidv7()}`,
        checksums: async () => ({ expected: "same", actual: "same" }),
      }),
    ).rejects.toThrow("was not consumed and committed");
  });

  it("appends one default-partitioner-verified barrier for every partition", async () => {
    const barriers = await appendReplayBarriers(
      new PostgresEventStore(pool),
      "orders-aug-2026",
    );
    expect(barriers).toHaveLength(24);
    expect(new Set(barriers.map((barrier) => barrier.partition)).size).toBe(24);
    for (const barrier of barriers)
      expect(
        kafkaDefaultPartition(`system|Barrier|${barrier.aggregateId}`),
      ).toBe(barrier.partition);
  });

  it("delivers and records all replay barriers through a temporary connector", async () => {
    const identity = {
      projectionName: "orders-replay",
      generationId: uuidv7(),
      replayId: `orders-${uuidv7().replaceAll("-", "").slice(-12)}`,
    };
    const coordinator = new ReplayCoordinator(
      pool,
      stack.connectUrl,
      [stack.kafkaBroker()],
      1,
    );
    await coordinator.createGeneration(identity);
    const replayProjectionIdentity: ProjectionIdentity = {
      name: identity.projectionName,
      generationId: identity.generationId,
    };
    await pool.query(
      "CREATE SCHEMA replay_projection; CREATE TABLE replay_projection.events(generation_id uuid NOT NULL,event_id uuid NOT NULL,payload jsonb NOT NULL,PRIMARY KEY(generation_id,event_id))",
    );
    await ensureReplayTopic([stack.kafkaBroker()], identity.replayId, 1);
    const replayProjection = await projectionRunner(
      pool,
      replayProjectionIdentity,
      replayTopicName(identity.replayId),
      `replay-projection-${uuidv7()}`,
      stack.kafkaBroker(),
    ).start();
    await coordinator.deployConnector(
      identity,
      replayConnectorConfig(identity.replayId, {
        hostname: "postgres",
        port: 5432,
        user: "event_store_cdc",
        password: "cdc",
        dbname: "event_store",
      }),
    );
    const kafka = new KafkaJS.Kafka({
      kafkaJS: { brokers: [stack.kafkaBroker()] },
    });
    const consumerGroupId = `replay-barriers-${uuidv7()}`;
    const consumer = kafka.consumer({
      kafkaJS: {
        groupId: consumerGroupId,
        autoCommit: false,
        fromBeginning: true,
      },
    });
    try {
      await consumer.connect();
      await consumer.subscribe({
        topics: [replayTopicName(identity.replayId)],
        replace: true,
      });
      const barriers = await appendReplayBarriers(
        new PostgresEventStore(pool),
        identity.replayId,
      );
      const barrierByEventId = new Map(
        barriers.map((barrier) => [barrier.eventId, barrier]),
      );
      const received = new Set<string>();
      await new Promise<void>((resolve, reject) => {
        const timeout = setTimeout(
          () => reject(new Error("timed out waiting for replay barriers")),
          60_000,
        );
        let quiet: ReturnType<typeof setTimeout> | undefined;
        void consumer.run({
          eachMessage: async ({ message, partition }) => {
            const value = message.value?.toString() ?? "";
            const event = JSON.parse(value || "{}") as {
              eventId?: string;
            };
            const barrier =
              event.eventId === undefined
                ? undefined
                : barrierByEventId.get(event.eventId);
            if (barrier !== undefined && !received.has(barrier.eventId)) {
              const stored = await pool.query<{ event_envelope: unknown }>(
                "SELECT event_envelope FROM event_store.events WHERE event_id=$1",
                [barrier.eventId],
              );
              expect(value).toBe(canonicalJson(stored.rows[0]?.event_envelope));
              expect(partition).toBe(barrier.partition);
              await coordinator.recordBarrier(identity, {
                topic: replayTopicName(identity.replayId),
                partition,
                offset: message.offset,
                value: message.value ?? Buffer.alloc(0),
              });
              received.add(barrier.eventId);
            }
            await consumer.commitOffsets([
              {
                topic: replayTopicName(identity.replayId),
                partition,
                offset: (BigInt(message.offset) + 1n).toString(),
              },
            ]);
            if (received.size === barriers.length) {
              if (quiet !== undefined) clearTimeout(quiet);
              quiet = setTimeout(() => {
                clearTimeout(timeout);
                resolve();
              }, 1_000);
            }
          },
        });
      });
      await consumer.disconnect();
      expect(
        (
          await pool.query<{ count: number }>(
            "SELECT count(*)::int AS count FROM projection_runtime.replay_barriers WHERE projection_name=$1 AND generation_id=$2",
            [identity.projectionName, identity.generationId],
          )
        ).rows[0]?.count,
      ).toBe(24);
      const expectedEvents = await pool.query<{ count: number }>(
        "SELECT count(*)::int AS count FROM event_store.events",
      );
      await eventually(async () => {
        const replay = await pool.query<{ count: number }>(
          "SELECT count(*)::int AS count FROM replay_projection.events WHERE generation_id=$1",
          [identity.generationId],
        );
        return (
          expectedEvents.rows[0]?.count !== undefined &&
          expectedEvents.rows[0]?.count > 0 &&
          expectedEvents.rows[0]?.count === replay.rows[0]?.count
        );
      });
      await coordinator.activate(identity, {
        kafkaLag: async () => 0n,
        consumerGroupId,
        checksums: async () => ({
          expected: await fullFoldChecksum(pool),
          actual: await projectionChecksum(pool, identity.generationId),
        }),
      });
    } finally {
      await consumer.disconnect().catch(() => undefined);
      await replayProjection.disconnect().catch(() => undefined);
      await coordinator.teardown(identity).catch(() => undefined);
    }
  }, 120_000);
});
