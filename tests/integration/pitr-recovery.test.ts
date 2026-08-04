import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { uuidv7 } from "@event-store/contracts";
import { PostgresEventStore } from "@event-store/postgres-store";
import { EventStoreStack } from "../fixtures/event-store-stack.js";
import { KafkaJS } from "@confluentinc/kafka-javascript";
import { Pool } from "pg";

const suite = process.env.RUN_INTEGRATION === "true" ? describe : describe.skip;

suite("PITR recovery", () => {
  const stack = new EventStoreStack();
  let source: Pool;
  let restored: { databaseUrl: string; stop: () => Promise<void> } | undefined;

  beforeAll(async () => {
    await stack.start({ cdc: true });
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
      const restoredEventIds = await restoredPool.query<{ event_id: string }>(
        "SELECT event_id FROM event_store.events ORDER BY event_number",
      );
      const kafka = new KafkaJS.Kafka({
        kafkaJS: { brokers: [stack.kafkaBroker()] },
      });
      const consumer = kafka.consumer({
        kafkaJS: {
          groupId: `pitr-replay-${uuidv7()}`,
          autoCommit: false,
          fromBeginning: true,
        },
      });
      const deliveries = new Map<string, number>();
      await consumer.connect();
      await consumer.subscribe({ topics: ["event-store.events.v1"], replace: true });
      const replayed = new Promise<void>((resolve, reject) => {
        const timeout = setTimeout(
          () => reject(new Error("restored CDC snapshot did not replay target events")),
          60_000,
        );
        void consumer.run({
          eachMessage: async ({ message }) => {
            const event = JSON.parse(message.value?.toString() ?? "{}") as {
              eventId?: string;
            };
            if (event.eventId === undefined) return;
            deliveries.set(event.eventId, (deliveries.get(event.eventId) ?? 0) + 1);
            if (
              restoredEventIds.rows.every(
                ({ event_id }) => (deliveries.get(event_id) ?? 0) >= 2,
              )
            ) {
              clearTimeout(timeout);
              resolve();
            }
          },
        });
      });
      await stack.createSnapshotRecoveryConnector(
        `pitr-${uuidv7().slice(0, 8)}`,
        slot,
        "pitr-restored",
      );
      await replayed;
      await consumer.disconnect();
      expect(restoredEventIds.rows).toHaveLength(1);
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
