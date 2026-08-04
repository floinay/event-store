import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { KafkaJS } from "@confluentinc/kafka-javascript";
import { uuidv7 } from "@event-store/contracts";
import { PostgresEventStore } from "@event-store/postgres-store";
import { EventStoreStack } from "../fixtures/event-store-stack.js";
import type { Pool } from "pg";

const suite = process.env.RUN_INTEGRATION === "true" ? describe : describe.skip;

suite("logical slot-loss recovery", () => {
  const stack = new EventStoreStack();
  let pool: Pool;
  let store: PostgresEventStore;

  beforeAll(async () => {
    await stack.start({ cdc: true });
    pool = await stack.pool();
    store = new PostgresEventStore(pool, 8n * 1024n ** 3n);
  }, 180_000);
  afterAll(async () => {
    await pool?.end();
    await stack.stop();
  }, 60_000);

  it("fails closed, snapshots the lost slot history, and resumes on a new live slot", async () => {
    const kafka = new KafkaJS.Kafka({
      kafkaJS: { brokers: [stack.kafkaBroker()] },
    });
    const consumer = kafka.consumer({
      kafkaJS: {
        groupId: `slot-loss-${uuidv7()}`,
        autoCommit: false,
        fromBeginning: true,
      },
    });
    const beforeRequestId = uuidv7();
    const afterRequestId = uuidv7();
    const deliveries = new Map<string, number>();
    let notify!: () => void;
    const observed = new Promise<void>((resolve) => {
      notify = resolve;
    });
    await consumer.connect();
    await consumer.subscribe({ topics: ["event-store.events.v1"], replace: true });
    await consumer.run({
      eachMessage: async ({ message }) => {
        const event = JSON.parse(message.value?.toString() ?? "{}") as {
          context?: { requestId?: string };
        };
        const requestId = event.context?.requestId;
        if (requestId === undefined) return;
        deliveries.set(requestId, (deliveries.get(requestId) ?? 0) + 1);
        if (
          (deliveries.get(beforeRequestId) ?? 0) >= 2 &&
          (deliveries.get(afterRequestId) ?? 0) >= 1
        )
          notify();
      },
    });

    await append(store, beforeRequestId, "before-slot-loss");
    await eventually(() => (deliveries.get(beforeRequestId) ?? 0) === 1);

    await stack.deleteConnector("event-store-live");
    await eventually(async () => {
      const slot = await pool.query<{ active: boolean }>(
        "SELECT active FROM pg_replication_slots WHERE slot_name='event_store_live'",
      );
      return slot.rows[0]?.active === false;
    });
    await pool.query("SELECT pg_drop_replication_slot('event_store_live')");
    await expect(append(store, uuidv7(), "rejected-while-slot-missing")).rejects.toMatchObject({
      code: "P0001",
    });

    const recoverySlot = `event_store_recovery_${uuidv7().replaceAll("-", "_")}`;
    await pool.query(
      "SELECT pg_create_logical_replication_slot($1, 'pgoutput', false, false, true)",
      [recoverySlot],
    );
    const recoveryId = `loss-${uuidv7().slice(0, 8)}`;
    const recoveryConnector = await stack.createSnapshotRecoveryConnector(
      recoveryId,
      recoverySlot,
    );
    await eventually(() => (deliveries.get(beforeRequestId) ?? 0) >= 2);
    await eventually(async () => {
      const slot = await pool.query<{ active: boolean }>(
        "SELECT active FROM pg_replication_slots WHERE slot_name=$1",
        [recoverySlot],
      );
      return slot.rows[0]?.active === true;
    });
    await pool.query(
      "SELECT event_store.activate_recovery_cdc_slot($1,$2,$3)",
      [recoverySlot, recoveryConnector, (8n * 1024n ** 3n).toString()],
    );
    await append(store, afterRequestId, "after-slot-recovery");
    await observed;
    await consumer.disconnect();

    expect(deliveries.get(beforeRequestId)).toBeGreaterThanOrEqual(2);
    expect(deliveries.get(afterRequestId)).toBeGreaterThanOrEqual(1);
    await expect(
      pool.query(
        "SELECT cdc_slot_name,cdc_connector_name FROM event_store.runtime_config WHERE singleton",
      ),
    ).resolves.toMatchObject({
      rows: [{ cdc_slot_name: recoverySlot, cdc_connector_name: recoveryConnector }],
    });
    await pool.query("SELECT event_store.enable_append_admission($1)", [
      (8n * 1024n ** 3n).toString(),
    ]);
    await expect(
      pool.query(
        "SELECT cdc_slot_name,cdc_connector_name FROM event_store.runtime_config WHERE singleton",
      ),
    ).resolves.toMatchObject({
      rows: [{ cdc_slot_name: recoverySlot, cdc_connector_name: recoveryConnector }],
    });
    await expect(
      fetch(`${stack.connectUrl}/connectors/${recoveryConnector}/status`),
    ).resolves.toMatchObject({ status: 200 });
  }, 180_000);
});

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
