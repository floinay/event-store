import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { z } from "zod";
import { Pool } from "pg";
import { uuidv7 } from "@event-store/contracts";
import { PostgresEventStore } from "@event-store/postgres-store";
import {
  createProjectionEventTransformer,
  KafkaProjectionRunner,
  ProjectionCheckpointStore,
  ProjectionFailureReporter,
  ProjectionPayloadSchemas,
  ProjectionTransactionRunner,
} from "@event-store/projection-runtime";
import { UpcasterRegistry } from "@event-store/upcasting";
import { EventStoreStack } from "../fixtures/event-store-stack.js";

const suite = process.env.RUN_INTEGRATION === "true" ? describe : describe.skip;

async function eventually(check: () => Promise<boolean>): Promise<void> {
  const deadline = Date.now() + 30_000;
  while (Date.now() < deadline) {
    if (await check()) return;
    await new Promise((resolve) => setTimeout(resolve, 100));
  }
  throw new Error("condition did not become true within 30s");
}

suite("projection consumer PostgreSQL recovery", () => {
  const stack = new EventStoreStack();
  let pool: Pool;
  let consumerPool: Pool;
  beforeAll(async () => {
    await stack.start({ cdc: true, toxiproxy: true });
    pool = await stack.pool();
    consumerPool = new Pool({ connectionString: stack.consumerDatabaseUrl });
  }, 180_000);
  afterAll(async () => {
    await consumerPool?.end();
    await pool?.end();
    await stack.stop();
  }, 60_000);

  it("replays an acknowledged event after consumer-to-PostgreSQL split and restart", async () => {
    const generationId = uuidv7();
    const projectionName = "consumer-db-split";
    await pool.query(
      "CREATE SCHEMA consumer_split; CREATE TABLE consumer_split.events(event_id uuid PRIMARY KEY)",
    );
    await pool.query(
      "INSERT INTO projection_runtime.generations(projection_name,generation_id,status,created_at) VALUES ($1,$2,'building',clock_timestamp())",
      [projectionName, generationId],
    );
    const identity = { name: projectionName, generationId };
    const upcasters = new UpcasterRegistry();
    upcasters.setCurrentVersion("order.created", 1);
    const schemas = new ProjectionPayloadSchemas();
    schemas.register("order.created", 1, z.object({}).passthrough());
    let databaseConnectionReached!: () => void;
    const databaseConnection = new Promise<void>((resolve) => {
      databaseConnectionReached = resolve;
    });
    let releaseDatabaseConnection!: () => void;
    const databaseConnectionGate = new Promise<void>((resolve) => {
      releaseDatabaseConnection = resolve;
    });
    const runner = (holdBeforeDatabaseConnection = false) =>
      new KafkaProjectionRunner(
        {
          brokers: [stack.kafkaBroker()],
          groupId: `consumer-split-${generationId}`,
          topic: "event-store.events.v1",
        },
        new ProjectionTransactionRunner(
          consumerPool,
          identity,
          createProjectionEventTransformer(upcasters, schemas),
          holdBeforeDatabaseConnection
            ? {
                hit: async (point) => {
                  if (point === "before_database_connection") {
                    databaseConnectionReached();
                    await databaseConnectionGate;
                  }
                },
              }
            : undefined,
        ),
        async (client, event) => {
          await client.query(
            "INSERT INTO consumer_split.events(event_id) VALUES ($1)",
            [event.eventId],
          );
        },
        new ProjectionCheckpointStore(consumerPool, identity),
        new ProjectionFailureReporter(consumerPool, identity),
      );
    const first = await runner(true).start();
    const requestId = uuidv7();
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
          actor: { kind: "user", subjectRef: "consumer-split" },
        },
        events: [
          {
            eventName: "order.created",
            schemaVersion: 1,
            occurredAt: "2026-08-04T10:12:18.120Z",
            payload: {},
          },
        ],
      });
      await databaseConnection;
      await stack.setConsumerPostgresEnabled(false);
      releaseDatabaseConnection();
      const probe = new Pool({ connectionString: stack.consumerDatabaseUrl });
      try {
        await expect(probe.query("SELECT 1")).rejects.toThrow();
      } finally {
        await probe.end().catch(() => undefined);
      }
    } finally {
      await first.disconnect().catch(() => undefined);
      await stack.setConsumerPostgresEnabled(true);
    }
    const restarted = await runner().start();
    try {
      await eventually(
        async () =>
          (
            await pool.query<{ count: number }>(
              "SELECT count(*)::int AS count FROM consumer_split.events",
            )
          ).rows[0]?.count === 1,
      );
    } finally {
      await restarted.disconnect();
    }
  }, 90_000);
});
