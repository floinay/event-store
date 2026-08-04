import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { uuidv7 } from "@event-store/contracts";
import { PostgresEventStore } from "@event-store/postgres-store";
import { EventStoreStack } from "../fixtures/event-store-stack.js";
import { Pool } from "pg";

const suite = process.env.RUN_INTEGRATION === "true" ? describe : describe.skip;

suite("PITR recovery", () => {
  const stack = new EventStoreStack();
  let source: Pool;
  let restored: { databaseUrl: string; stop: () => Promise<void> } | undefined;

  beforeAll(async () => {
    await stack.start();
    source = await stack.pool();
  }, 180_000);
  afterAll(async () => {
    await restored?.stop();
    await source?.end();
    await stack.stop();
  }, 60_000);

  it("restores archived WAL to a target time and creates a new logical slot", async () => {
    const backup = await stack.createPitrBaseBackup();
    const store = new PostgresEventStore(source);
    const aggregateId = uuidv7();
    const beforeRequestId = uuidv7();
    await append(store, aggregateId, beforeRequestId, "before-target", "no_stream");
    const targetTime = (
      await source.query<{ target_time: string }>(
        "SELECT clock_timestamp()::text AS target_time",
      )
    ).rows[0]?.target_time;
    if (targetTime === undefined) throw new Error("could not read PITR target time");
    await append(store, aggregateId, uuidv7(), "after-target", "exact");

    restored = await stack.restorePitr(backup, targetTime);
    const restoredPool = new Pool({ connectionString: restored.databaseUrl });
    try {
      const events = await restoredPool.query<{
        event_envelope: { payload: { marker: string } };
      }>("SELECT event_envelope FROM event_store.events ORDER BY event_number");
      expect(events.rows.map((row) => row.event_envelope.payload.marker)).toEqual([
        "before-target",
      ]);
      const slot = `event_store_pitr_${uuidv7().replaceAll("-", "_")}`;
      await restoredPool.query(
        "SELECT * FROM pg_create_logical_replication_slot($1, 'pgoutput', false, false, true)",
        [slot],
      );
      await expect(
        restoredPool.query(
          "SELECT failover FROM pg_replication_slots WHERE slot_name=$1",
          [slot],
        ),
      ).resolves.toMatchObject({ rows: [{ failover: true }] });
    } finally {
      await restoredPool.end();
    }
  }, 180_000);
});

async function append(
  store: PostgresEventStore,
  aggregateId: string,
  requestId: string,
  marker: string,
  expected: "no_stream" | "exact",
): Promise<void> {
  await store.append({
    producerService: "pitr-test",
    namespace: "pitr",
    aggregateType: "Probe",
    aggregateId,
    requestId,
    expectedRevision:
      expected === "no_stream" ? { kind: "no_stream" } : { kind: "exact", revision: 1n },
    context: {
      requestId,
      correlationId: uuidv7(),
      causationId: null,
      actor: { kind: "system", subjectRef: "pitr-test" },
    },
    events: [
      {
        eventName: "pitr.appended",
        schemaVersion: 1,
        occurredAt: new Date().toISOString(),
        payload: { marker },
      },
    ],
  });
}
