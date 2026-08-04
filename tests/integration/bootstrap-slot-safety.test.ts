import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { readFile } from "node:fs/promises";
import { PostgresEventStore } from "@event-store/postgres-store";
import { uuidv7 } from "@event-store/contracts";
import { bootstrap } from "../../tools/bootstrap/src/index.js";
import { EventStoreStack } from "../fixtures/event-store-stack.js";
import type { Pool } from "pg";

const suite = process.env.RUN_INTEGRATION === "true" ? describe : describe.skip;

suite("CDC bootstrap slot safety", () => {
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

  it("refuses to recreate a missing live slot after acknowledged events", async () => {
    const requestId = uuidv7();
    const connectUrl = stack.connectUrl;
    await new PostgresEventStore(pool).append({
      producerService: "bootstrap-test",
      namespace: "orders",
      aggregateType: "Order",
      aggregateId: uuidv7(),
      requestId,
      expectedRevision: { kind: "no_stream" },
      context: {
        requestId,
        correlationId: uuidv7(),
        causationId: null,
        actor: { kind: "service", subjectRef: "bootstrap-test" },
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
    await stack.stopConnect();
    const deadline = Date.now() + 30_000;
    while (Date.now() < deadline) {
      const slot = await pool.query<{ active: boolean }>(
        "SELECT active FROM pg_replication_slots WHERE slot_name='event_store_live'",
      );
      if (slot.rows[0]?.active === false) break;
      await new Promise((resolve) => setTimeout(resolve, 100));
    }
    await pool.query("SELECT pg_drop_replication_slot('event_store_live')");
    const connector = JSON.parse(
      await readFile("deploy/connector/event-store-live.json", "utf8"),
    ) as { name: string; config: Record<string, string> };
    const topics = [
      { name: "event-store.events.v1", partitions: 24 },
      { name: "_connect-event-store-configs", partitions: 1 },
      { name: "_connect-event-store-offsets", partitions: 25 },
      { name: "_connect-event-store-status", partitions: 5 },
    ].map((topic) => ({ ...topic, replicationFactor: 1, configs: {} }));

    await expect(
      bootstrap({
        migrationDatabaseUrl: stack.databaseUrl,
        replicationDatabaseUrl: stack.databaseUrl,
        brokers: [stack.kafkaBroker()],
        connectUrl,
        connectorName: connector.name,
        connectorConfig: connector.config,
        topics,
        walBudgetBytes: 8n * 1024n ** 3n,
      }),
    ).rejects.toThrow("missing after events exist");
    await expect(
      pool.query(
        "SELECT 1 FROM pg_replication_slots WHERE slot_name='event_store_live'",
      ),
    ).resolves.toMatchObject({ rows: [] });
  }, 90_000);
});
