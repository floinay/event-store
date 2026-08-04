import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { uuidv7 } from "@event-store/contracts";
import { PostgresEventStore } from "@event-store/postgres-store";
import { reconcile } from "@event-store/reconcile";
import { EventStoreStack } from "../fixtures/event-store-stack.js";
import type { Pool } from "pg";

const suite = process.env.RUN_INTEGRATION === "true" ? describe : describe.skip;

suite("reconciliation", () => {
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

  it("verifies restored stream continuity and envelope hashes", async () => {
    const store = new PostgresEventStore(pool);
    const requestId = uuidv7();
    await store.append({
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
        actor: { kind: "user", subjectRef: "usr_1" },
      },
      events: [
        {
          eventName: "order.created",
          schemaVersion: 1,
          occurredAt: "2026-08-04T10:12:18.120Z",
          payload: { orderRef: "o1" },
        },
      ],
    });
    const event = await pool.query<{
      event_id: string;
      envelope_sha256: string;
    }>(
      "SELECT event_id::text,envelope_sha256 FROM event_store.events WHERE request_id=$1",
      [requestId],
    );
    const projectionName = "reconcile";
    const generationId = uuidv7();
    await pool.query(
      "INSERT INTO projection_runtime.generations(projection_name,generation_id,status,created_at) VALUES ($1,$2,'building',clock_timestamp())",
      [projectionName, generationId],
    );
    await pool.query(
      "INSERT INTO projection_runtime.inbox(projection_name,generation_id,event_id,envelope_sha256,topic_name,partition_no,kafka_offset,processed_at) VALUES ($1,$2,$3,$4,'event-store.events.v1',0,0,clock_timestamp())",
      [
        projectionName,
        generationId,
        event.rows[0]!.event_id,
        event.rows[0]!.envelope_sha256,
      ],
    );
    await expect(
      reconcile(stack.databaseUrl, { projectionName, generationId }),
    ).resolves.toMatchObject({
      count: "1",
      revisionGaps: "0",
      envelopeHashMismatches: "0",
      missingProjectionEvents: "0",
      unknownProjectionEvents: "0",
    });
    await pool.query(
      "INSERT INTO projection_runtime.inbox(projection_name,generation_id,event_id,envelope_sha256,topic_name,partition_no,kafka_offset,processed_at) VALUES ($1,$2,$3,$4,'event-store.events.v1',0,1,clock_timestamp())",
      [projectionName, generationId, uuidv7(), "0".repeat(64)],
    );
    await store.append({
      producerService: "orders-command",
      namespace: "orders",
      aggregateType: "Order",
      aggregateId: uuidv7(),
      requestId: uuidv7(),
      expectedRevision: { kind: "no_stream" },
      context: {
        requestId: uuidv7(),
        correlationId: uuidv7(),
        causationId: null,
        actor: { kind: "user", subjectRef: "usr_1" },
      },
      events: [
        {
          eventName: "order.created",
          schemaVersion: 1,
          occurredAt: "2026-08-04T10:12:18.120Z",
          payload: { orderRef: "missing" },
        },
      ],
    });
    await expect(
      reconcile(stack.databaseUrl, { projectionName, generationId }),
    ).resolves.toMatchObject({
      missingProjectionEvents: "1",
      unknownProjectionEvents: "1",
    });
  });
});
