import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { randomUUID } from "node:crypto";
import { Pool } from "pg";
import { migrate } from "@event-store/migrate";
import { canonicalJson } from "@event-store/contracts";
import { PostgresEventStore } from "@event-store/postgres-store";
import { EventStoreStack } from "../fixtures/event-store-stack.js";

const suite = process.env.RUN_INTEGRATION === "true" ? describe : describe.skip;
const uuidv7 = () => randomUUID().replace(/^(.{14})./, "$17");

suite("database upgrade from the preceding release", () => {
  const stack = new EventStoreStack();
  let pool: Pool;
  let store: PostgresEventStore;

  beforeAll(async () => {
    await stack.start({ migrate: false });
    await migrate(stack.databaseUrl, true, {
      upToMigration: "046_add_cdc_kafka_timestamp.sql",
    });
    pool = await stack.pool();
    await pool.query(
      "UPDATE event_store.runtime_config SET append_admission_enabled=false WHERE singleton",
    );
    store = new PostgresEventStore(pool);
  }, 180_000);
  afterAll(async () => {
    await pool?.end();
    await stack.stop();
  }, 60_000);

  it("upgrades retained events and keeps new canonical Kafka payloads", async () => {
    const aggregateId = uuidv7();
    const firstRequestId = uuidv7();
    const append = (
      requestId: string,
      expectedRevision: 0 | 1,
      marker: string,
    ) =>
      store.append({
        producerService: "orders-command",
        namespace: "orders",
        aggregateType: "Order",
        aggregateId,
        requestId,
        expectedRevision:
          expectedRevision === 0
            ? { kind: "no_stream" }
            : { kind: "exact", revision: 1n },
        context: {
          requestId,
          correlationId: uuidv7(),
          causationId: null,
          actor: { kind: "service", subjectRef: "upgrade-test" },
        },
        events: [
          {
            eventName: "order.created",
            schemaVersion: 1,
            occurredAt: "2026-08-04T10:12:18.120Z",
            payload: { marker },
          },
        ],
      });

    const first = await append(firstRequestId, 0, "before-upgrade");
    await expect(
      pool.query(
        "SELECT 1 FROM information_schema.columns WHERE table_schema='event_store' AND table_name='events' AND column_name='event_envelope_kafka'",
      ),
    ).resolves.toMatchObject({ rows: [] });

    await migrate(stack.databaseUrl, true);
    const second = await append(uuidv7(), 1, "after-upgrade");
    const events = await pool.query<{
      event_id: string;
      event_envelope: Record<string, unknown>;
      event_envelope_kafka: string;
    }>(
      "SELECT event_id::text,event_envelope,event_envelope_kafka FROM event_store.events WHERE event_id = ANY($1::uuid[]) ORDER BY event_number",
      [[first.events[0]!.eventId, second.events[0]!.eventId]],
    );
    expect(events.rows).toHaveLength(2);
    for (const event of events.rows)
      expect(event.event_envelope_kafka).toBe(
        canonicalJson(event.event_envelope),
      );
  });
});
