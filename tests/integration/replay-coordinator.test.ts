import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { uuidv7 } from "@event-store/contracts";
import { KafkaJS } from "@confluentinc/kafka-javascript";
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
          const event = JSON.parse(message.value?.toString() ?? "{}") as {
            eventId?: string;
          };
          const barrier =
            event.eventId === undefined
              ? undefined
              : barrierByEventId.get(event.eventId);
          if (barrier !== undefined && !received.has(barrier.eventId)) {
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
    await coordinator.activate(identity, {
      kafkaLag: async () => 0n,
      consumerGroupId,
      checksums: async () => ({ expected: "same", actual: "same" }),
    });
    await coordinator.teardown(identity);
  }, 120_000);
});
