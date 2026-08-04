import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { z } from "zod";
import { KafkaJS } from "@confluentinc/kafka-javascript";
import { canonicalJson, uuidv7 } from "@event-store/contracts";
import { PostgresEventStore } from "@event-store/postgres-store";
import {
  createProjectionEventTransformer,
  KafkaProjectionRunner,
  ProjectionCheckpointStore,
  ProjectionFailureReporter,
  ProjectionPayloadSchemas,
  ProjectionTransactionRunner,
} from "@event-store/projection-runtime";
import {
  RecoveryCutoverCoordinator,
  appendReplayBarriers,
  recoverySlotName,
  startRecovery,
} from "@event-store/replay";
import { UpcasterRegistry } from "@event-store/upcasting";
import { EventStoreStack } from "../fixtures/event-store-stack.js";
import type { Pool } from "pg";

const suite = process.env.RUN_INTEGRATION === "true" ? describe : describe.skip;

suite("logical slot-loss recovery", () => {
  const stack = new EventStoreStack();
  let pool: Pool;
  let store: PostgresEventStore;
  let projectionConsumer: KafkaJS.Consumer;

  beforeAll(async () => {
    await stack.start({ cdc: true });
    pool = await stack.pool();
    store = new PostgresEventStore(pool, 8n * 1024n ** 3n);
  }, 180_000);
  afterAll(async () => {
    await projectionConsumer?.disconnect().catch(() => undefined);
    await pool?.end();
    await stack.stop();
  }, 60_000);

  it("resumes recovery start after slot and connector creation", async () => {
    const recoveryId = `resume-${uuidv7().replaceAll("-", "").slice(-12)}`;
    const options = {
      identity: {
        projectionName: "slot-loss-recovery-resume",
        generationId: uuidv7(),
        replayId: recoveryId,
      },
      coordinatorDatabaseUrl: stack.databaseUrl,
      appendDatabaseUrl: stack.databaseUrl,
      connectorUrl: stack.connectUrl,
      connectorDatabase: {
        hostname: "postgres",
        port: 5432,
        user: "event_store_cdc",
        password: "cdc",
        dbname: "event_store",
      },
    };
    const first = await startRecovery(options);
    const second = await startRecovery(options);
    expect(second).toEqual(first);
    await expect(
      pool.query<{ count: number }>(
        `SELECT count(*)::int AS count FROM pg_replication_slots
          WHERE slot_name=$1 AND failover AND NOT temporary
            AND invalidation_reason IS NULL`,
        [recoverySlotName(recoveryId)],
      ),
    ).resolves.toMatchObject({ rows: [{ count: 1 }] });
  }, 90_000);

  it("keeps writes closed until a real projection deduplicates, catches up, and reconciles recovery", async () => {
    const projectionName = "slot-loss-recovery";
    const generationId = uuidv7();
    const consumerGroupId = `slot-loss-projection-${uuidv7()}`;
    await pool.query(
      "CREATE SCHEMA recovery_model; CREATE TABLE recovery_model.events(event_id uuid PRIMARY KEY)",
    );
    await pool.query(
      `INSERT INTO projection_runtime.generations(projection_name,generation_id,status,created_at)
       VALUES ($1,$2,'building',clock_timestamp())`,
      [projectionName, generationId],
    );
    const upcasters = new UpcasterRegistry();
    upcasters.setCurrentVersion("recovery.appended", 1);
    upcasters.setCurrentVersion("system.replaybarrier", 1);
    const schemas = new ProjectionPayloadSchemas();
    schemas.register(
      "recovery.appended",
      1,
      z.object({ marker: z.string() }).strict(),
    );
    schemas.register(
      "system.replaybarrier",
      1,
      z.object({ replayId: z.string(), partition: z.string() }).strict(),
    );
    const identity = { name: projectionName, generationId };
    const beforeRequestId = uuidv7();
    await append(store, beforeRequestId, "before-slot-loss");

    const paused = await fetch(
      `${stack.connectUrl}/connectors/event-store-live/pause`,
      { method: "PUT" },
    );
    expect(paused.ok).toBe(true);
    await eventually(async () => {
      const slot = await pool.query<{ active: boolean }>(
        "SELECT active FROM pg_replication_slots WHERE slot_name='event_store_live'",
      );
      return slot.rows[0]?.active === false;
    });
    await pool.query("SELECT pg_drop_replication_slot('event_store_live')");
    await expect(
      append(store, uuidv7(), "rejected-while-slot-missing"),
    ).rejects.toMatchObject({ code: "P0001" });

    const recoverySlot = `event_store_recovery_${uuidv7().replaceAll("-", "_")}`;
    await pool.query(
      "SELECT pg_create_logical_replication_slot($1, 'pgoutput', false, false, true)",
      [recoverySlot],
    );
    const recoveryId = `loss-${uuidv7().replaceAll("-", "").slice(-12)}`;
    const recoveryConnector = await stack.createSnapshotRecoveryConnector(
      recoveryId,
      recoverySlot,
    );
    await eventually(async () => {
      const slot = await pool.query<{ active: boolean }>(
        "SELECT active FROM pg_replication_slots WHERE slot_name=$1",
        [recoverySlot],
      );
      return slot.rows[0]?.active === true;
    });
    await expect(
      pool.query("SELECT event_store.activate_recovery_cdc_slot($1,$2,$3)", [
        recoverySlot,
        recoveryConnector,
        (8n * 1024n ** 3n).toString(),
      ]),
    ).rejects.toMatchObject({ code: "P0001" });

    // Start from the recovery connector's snapshot, then crash at the durable
    // checkpoint boundary before barriers are written.
    projectionConsumer = await startRecoveryProjection(
      stack,
      pool,
      identity,
      consumerGroupId,
      upcasters,
      schemas,
    );
    await eventually(async () => {
      const failures = await pool.query<{ error_detail: { message: string } }>(
        `SELECT error_detail FROM projection_runtime.failures
         WHERE projection_name=$1 AND generation_id=$2`,
        [projectionName, generationId],
      );
      if (failures.rows.length > 0)
        throw new Error(
          `recovery projection failed: ${failures.rows
            .map((row) => row.error_detail.message)
            .join(", ")}`,
        );
      const result = await pool.query<{ count: number }>(
        "SELECT count(*)::int AS count FROM recovery_model.events",
      );
      return result.rows[0]?.count === 1;
    });
    await projectionConsumer.disconnect();
    projectionConsumer = await startRecoveryProjection(
      stack,
      pool,
      identity,
      consumerGroupId,
      upcasters,
      schemas,
    );
    const barriers = await appendReplayBarriers(store, recoveryId);
    await expect(
      pool.query(
        "SELECT count(*)::int AS count FROM event_store.events WHERE event_name='system.replaybarrier' AND event_envelope->'payload'->>'replayId'=$1",
        [recoveryId],
      ),
    ).resolves.toMatchObject({ rows: [{ count: barriers.length }] });
    await observeBarrierTransport(
      stack,
      pool,
      recoverySlot,
      recoveryConnector,
      barriers.map((barrier) => barrier.eventId),
    );
    await eventually(async () => {
      const failures = await pool.query<{ error_detail: { message: string } }>(
        `SELECT error_detail FROM projection_runtime.failures
         WHERE projection_name=$1 AND generation_id=$2`,
        [projectionName, generationId],
      );
      if (failures.rows.length > 0)
        throw new Error(
          `recovery projection failed: ${failures.rows
            .map((row) => row.error_detail.message)
            .join(", ")}`,
        );
      const result = await pool.query<{ count: number }>(
        `SELECT count(*)::int AS count FROM projection_runtime.inbox i
         JOIN event_store.events e ON e.event_id=i.event_id
         WHERE i.projection_name=$1 AND i.generation_id=$2
           AND e.event_name='system.replaybarrier'
           AND e.event_envelope->'payload'->>'replayId'=$3`,
        [projectionName, generationId, recoveryId],
      );
      return result.rows[0]?.count === barriers.length;
    }, 90_000);
    await eventually(async () =>
      recoveryBarriersHaveNoLogicalLag(
        pool,
        projectionName,
        generationId,
        recoveryId,
      ),
    );
    await new RecoveryCutoverCoordinator(pool, stack.connectUrl, [
      stack.kafkaBroker(),
    ]).activate(recoverySlot, recoveryConnector, 8n * 1024n ** 3n, {
      projectionName,
      generationId,
      replayId: recoveryId,
      consumerGroupId,
      kafkaLag: async () =>
        (await recoveryBarriersHaveNoLogicalLag(
          pool,
          projectionName,
          generationId,
          recoveryId,
        ))
          ? 0n
          : 1n,
    });
    await expect(
      fetch(`${stack.connectUrl}/connectors/event-store-live`),
    ).resolves.toMatchObject({ status: 404 });
    await append(store, uuidv7(), "after-slot-recovery");
    await eventually(async () => {
      const missing = await pool.query<{ count: number }>(
        `SELECT count(*)::int AS count FROM event_store.events e
         WHERE NOT EXISTS (
           SELECT 1 FROM projection_runtime.inbox i
            WHERE i.projection_name=$1 AND i.generation_id=$2 AND i.event_id=e.event_id
         )`,
        [projectionName, generationId],
      );
      return missing.rows[0]?.count === 0;
    });
    expect(
      (
        await pool.query<{ count: number }>(
          "SELECT count(*)::int AS count FROM recovery_model.events",
        )
      ).rows[0]?.count,
    ).toBe(26);
  }, 180_000);
});

async function recoveryBarriersHaveNoLogicalLag(
  pool: Pool,
  projectionName: string,
  generationId: string,
  recoveryId: string,
): Promise<boolean> {
  // Kafka high watermarks include transaction-control batches hidden from
  // read_committed consumers. A barrier in every partition is a logical tail:
  // inbox plus a later durable checkpoint proves zero readable-record lag.
  const result = await pool.query<{ count: number }>(
    `SELECT count(*)::int AS count
       FROM event_store.events e
       JOIN projection_runtime.inbox i ON i.event_id=e.event_id
       JOIN projection_runtime.checkpoints c
         ON c.projection_name=i.projection_name
        AND c.generation_id=i.generation_id
        AND c.topic_name=i.topic_name
        AND c.partition_no=i.partition_no
      WHERE i.projection_name=$1 AND i.generation_id=$2
        AND e.event_name='system.replaybarrier'
        AND e.event_envelope->'payload'->>'replayId'=$3
        AND c.next_offset > i.kafka_offset`,
    [projectionName, generationId, recoveryId],
  );
  return result.rows[0]?.count === 24;
}

async function startRecoveryProjection(
  stack: EventStoreStack,
  pool: Pool,
  identity: { name: string; generationId: string },
  groupId: string,
  upcasters: UpcasterRegistry,
  schemas: ProjectionPayloadSchemas,
): Promise<KafkaJS.Consumer> {
  return new KafkaProjectionRunner(
    {
      brokers: [stack.kafkaBroker()],
      groupId,
      topic: "event-store.events.v1",
    },
    new ProjectionTransactionRunner(
      pool,
      identity,
      createProjectionEventTransformer(upcasters, schemas),
    ),
    async (client, event) => {
      await client.query(
        "INSERT INTO recovery_model.events(event_id) VALUES ($1) ON CONFLICT DO NOTHING",
        [event.eventId],
      );
    },
    new ProjectionCheckpointStore(pool, identity),
    new ProjectionFailureReporter(pool, identity),
  ).start();
}

async function observeBarrierTransport(
  stack: EventStoreStack,
  pool: Pool,
  recoverySlot: string,
  connectorName: string,
  eventIds: readonly string[],
): Promise<void> {
  const kafka = new KafkaJS.Kafka({
    kafkaJS: { brokers: [stack.kafkaBroker()] },
  });
  const consumer = kafka.consumer({
    kafkaJS: {
      groupId: `slot-loss-transport-${uuidv7()}`,
      autoCommit: false,
      fromBeginning: true,
      readUncommitted: false,
    },
  });
  const expected = new Set(eventIds);
  const observed = new Set<string>();
  let received = 0;
  await consumer.connect();
  await consumer.subscribe({
    topics: ["event-store.events.v1"],
    replace: true,
  });
  try {
    await new Promise<void>((resolve, reject) => {
      const timeout = setTimeout(() => {
        const admin = kafka.admin();
        void Promise.all([
          fetch(`${stack.connectUrl}/connectors/${connectorName}/status`).then(
            (response) => response.text(),
          ),
          pool.query(
            `SELECT active, restart_lsn::text, confirmed_flush_lsn::text,
                    pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn)::bigint
                       AS unconfirmed_wal_bytes
               FROM pg_replication_slots
              WHERE slot_name=$1`,
            [recoverySlot],
          ),
          admin.connect().then(async () => {
            try {
              return await admin.fetchTopicOffsets("event-store.events.v1");
            } finally {
              await admin.disconnect();
            }
          }),
        ])
          .then(([status, slot, offsets]) =>
            reject(
              new Error(
                `recovery connector did not deliver all barriers; received ${received}, observed ${observed.size}/${expected.size}; slot ${JSON.stringify(slot.rows[0] ?? null)}; offsets ${JSON.stringify(offsets)}; status ${status}`,
              ),
            ),
          )
          .catch(reject);
      }, 60_000);
      void consumer.run({
        eachMessage: async ({ message }) => {
          received += 1;
          const value = message.value?.toString() ?? "";
          const event = JSON.parse(value || "{}") as {
            eventId?: string;
            recordedAt?: string;
          };
          if (event.eventId !== undefined && expected.has(event.eventId)) {
            try {
              expect(value).toBe(canonicalJson(event));
              expect(message.timestamp).toBe(
                String(Date.parse(event.recordedAt!)),
              );
            } catch (error) {
              clearTimeout(timeout);
              reject(error);
              return;
            }
            observed.add(event.eventId);
          }
          if (observed.size === expected.size) {
            clearTimeout(timeout);
            resolve();
          }
        },
      });
    });
  } finally {
    await consumer.disconnect();
  }
}

async function append(
  store: PostgresEventStore,
  requestId: string,
  marker: string,
): Promise<void> {
  await store.append({
    producerService: "slot-recovery-test",
    namespace: "recovery",
    aggregateType: "Slot",
    aggregateId: uuidv7(),
    requestId,
    expectedRevision: { kind: "no_stream" },
    context: {
      requestId,
      correlationId: uuidv7(),
      causationId: null,
      actor: { kind: "system", subjectRef: "slot-recovery-test" },
    },
    events: [
      {
        eventName: "recovery.appended",
        schemaVersion: 1,
        occurredAt: new Date().toISOString(),
        payload: { marker },
      },
    ],
  });
}

async function eventually(
  condition: () => boolean | Promise<boolean>,
  timeoutMs = 60_000,
): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (await condition()) return;
    await new Promise((resolve) => setTimeout(resolve, 100));
  }
  throw new Error("timed out waiting for slot-loss recovery condition");
}
