import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { uuidv7 } from "@event-store/contracts";
import {
  appendReplayBarriers,
  kafkaDefaultPartition,
  ReplayCoordinator,
} from "@event-store/replay";
import { PostgresEventStore } from "@event-store/postgres-store";
import { EventStoreStack } from "../fixtures/event-store-stack.js";
import type { Pool } from "pg";

const suite = process.env.RUN_INTEGRATION === "true" ? describe : describe.skip;

suite("replay coordinator", () => {
  const stack = new EventStoreStack();
  let pool: Pool;
  beforeAll(async () => {
    await stack.start();
    pool = await stack.pool();
  }, 180_000);
  afterAll(async () => {
    await pool?.end();
    await stack.stop();
  }, 60_000);

  it("activates only consumer-verified, checksum-matching barriers", async () => {
    const identity = {
      projectionName: "orders",
      generationId: uuidv7(),
      replayId: "orders-aug-2026",
    };
    const coordinator = new ReplayCoordinator(pool, "http://unused");
    await coordinator.createGeneration(identity);
    await expect(
      coordinator.activate(identity, {
        kafkaLag: 0n,
        expectedChecksum: "same",
        actualChecksum: "same",
      }),
    ).rejects.toThrow("all 24 replay barriers");
    const barriers = await appendReplayBarriers(
      new PostgresEventStore(pool),
      identity.replayId,
    );
    for (const barrier of barriers)
      await coordinator.recordBarrier(
        identity,
        barrier.partition,
        barrier.eventId,
      );
    await coordinator.activate(identity, {
      kafkaLag: 0n,
      expectedChecksum: "same",
      actualChecksum: "same",
    });
    expect(
      (
        await pool.query(
          "SELECT status FROM projection_runtime.generations WHERE projection_name=$1 AND generation_id=$2",
          [identity.projectionName, identity.generationId],
        )
      ).rows[0]?.status,
    ).toBe("active");
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
});
