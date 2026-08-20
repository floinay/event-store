import { afterAll, beforeAll, describe, it } from "vitest";
import { uuidv7 } from "@event-store/contracts";
import { PostgresEventStore } from "@event-store/postgres-store";
import { EventStoreStack } from "../fixtures/event-store-stack.js";
import type { Pool } from "pg";

const suite = process.env.RUN_INTEGRATION === "true" ? describe : describe.skip;

async function eventually(
  condition: () => Promise<boolean>,
  message: string,
): Promise<void> {
  const deadline = Date.now() + 60_000;
  while (Date.now() < deadline) {
    if (await condition()) return;
    await new Promise((resolve) => setTimeout(resolve, 200));
  }
  throw new Error(message);
}

suite("Debezium trust_slot", () => {
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

  it("resumes from the durable slot when the Connect offset is behind", async () => {
    type ConnectorOffset = {
      partition: Record<string, unknown>;
      offset: Record<string, unknown>;
    };
    const readOffsets = async (): Promise<ConnectorOffset[]> => {
      const response = await fetch(
        `${stack.connectUrl}/connectors/event-store-live/offsets`,
      );
      if (!response.ok)
        throw new Error(`offset read failed: ${await response.text()}`);
      const body = (await response.json()) as { offsets?: ConnectorOffset[] };
      return body.offsets ?? [];
    };
    const append = async (): Promise<string> => {
      const requestId = uuidv7();
      await new PostgresEventStore(pool).append({
        producerService: "trust-offset-test",
        namespace: "orders",
        aggregateType: "Order",
        aggregateId: uuidv7(),
        requestId,
        expectedRevision: { kind: "no_stream" },
        context: {
          requestId,
          correlationId: uuidv7(),
          causationId: null,
          actor: { kind: "service", subjectRef: "trust-offset-test" },
        },
        events: [
          {
            eventName: "order.created",
            schemaVersion: 1,
            occurredAt: "2026-08-04T10:12:18.120Z",
            payload: { scenario: "stored-offset-behind-slot" },
          },
        ],
      });
      const lsn = await pool.query<{ lsn: string }>(
        "SELECT pg_current_wal_lsn()::text AS lsn",
      );
      const value = lsn.rows[0]?.lsn;
      if (value === undefined) throw new Error("could not read append WAL LSN");
      return value;
    };

    await append();
    let initialOffset: ConnectorOffset | undefined;
    await eventually(async () => {
      initialOffset = (await readOffsets())[0];
      return initialOffset !== undefined;
    }, "Kafka Connect did not persist the initial source offset");
    if (initialOffset === undefined)
      throw new Error("could not identify the initial Connect source offset");
    const staleOffset = initialOffset;
    const secondAppendLsn = await append();
    await stack.waitForLiveCdcCaughtUp(secondAppendLsn);

    const stop = await fetch(
      `${stack.connectUrl}/connectors/event-store-live/stop`,
      { method: "PUT" },
    );
    if (!stop.ok)
      throw new Error(`connector stop failed: ${await stop.text()}`);
    await eventually(async () => {
      const status = (await fetch(
        `${stack.connectUrl}/connectors/event-store-live/status`,
      ).then((response) => response.json())) as {
        connector?: { state?: string };
        tasks?: unknown[];
      };
      return (
        status.connector?.state === "STOPPED" && status.tasks?.length === 0
      );
    }, "connector did not stop before its offsets were altered");

    const alteration = await fetch(
      `${stack.connectUrl}/connectors/event-store-live/offsets`,
      {
        method: "PATCH",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ offsets: [staleOffset] }),
      },
    );
    if (!alteration.ok)
      throw new Error(`offset alteration failed: ${await alteration.text()}`);
    await eventually(
      async () =>
        JSON.stringify((await readOffsets())[0]) ===
        JSON.stringify(staleOffset),
      "Kafka Connect did not persist the stale source offset",
    );

    const resume = await fetch(
      `${stack.connectUrl}/connectors/event-store-live/resume`,
      { method: "PUT" },
    );
    if (!resume.ok)
      throw new Error(`connector resume failed: ${await resume.text()}`);
    const recoveryLsn = await append();
    await stack.waitForLiveCdcCaughtUp(recoveryLsn);
    await eventually(
      async () =>
        JSON.stringify((await readOffsets())[0]) !==
        JSON.stringify(staleOffset),
      "trust_slot did not advance the stale source offset",
    );
  }, 150_000);
});
