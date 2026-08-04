import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { createHash } from "node:crypto";
import { canonicalJson, uuidv7 } from "@event-store/contracts";
import { PostgresEventStore } from "@event-store/postgres-store";
import {
  ProjectionGapError,
  ProjectionTransactionRunner,
} from "@event-store/projection-runtime";
import { EventStoreStack } from "../fixtures/event-store-stack.js";
import type { Pool } from "pg";

const suite = process.env.RUN_INTEGRATION === "true" ? describe : describe.skip;
const sleep = (milliseconds: number): Promise<void> =>
  new Promise((resolve) => setTimeout(resolve, milliseconds));

suite("projection crash boundary", () => {
  const stack = new EventStoreStack();
  let pool: Pool;
  let store: PostgresEventStore;
  beforeAll(async () => {
    await stack.start();
    pool = await stack.pool();
    store = new PostgresEventStore(pool);
  }, 180_000);
  afterAll(async () => {
    await pool?.end();
    await stack.stop();
  }, 60_000);

  it("rolls back inbox, model and checkpoint before retrying the same record", async () => {
    const generationId = uuidv7();
    const aggregateId = uuidv7();
    const requestId = uuidv7();
    await pool.query(
      "CREATE SCHEMA projection_test; CREATE TABLE projection_test.events(event_id uuid PRIMARY KEY)",
    );
    await pool.query(
      "INSERT INTO projection_runtime.generations(projection_name,generation_id,status,created_at) VALUES ('test',$1,'building',clock_timestamp())",
      [generationId],
    );
    await store.append({
      producerService: "orders-command",
      namespace: "orders",
      aggregateType: "Order",
      aggregateId,
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
    const event = (await store.readStream("orders", "Order", aggregateId))[0]!;
    const value = canonicalJson(event);
    const hash = createHash("sha256").update(value).digest("hex");
    const record = {
      topic: "event-store.events.v1",
      partition: 0,
      offset: 0n,
      key: `orders|Order|${aggregateId}`,
      value,
      headers: {
        id: event.eventId,
        type: event.eventName,
        envelopeHash: hash,
        namespace: event.namespace,
        aggregateType: event.aggregateType,
        streamRevision: event.streamRevision,
      },
    };
    const runner = new ProjectionTransactionRunner(
      pool,
      { name: "test", generationId },
      (stored) => stored,
    );
    await expect(
      runner.process(record, async (client, stored) => {
        await client.query(
          "INSERT INTO projection_test.events(event_id) VALUES ($1)",
          [stored.eventId],
        );
        throw new Error("crash before commit");
      }),
    ).rejects.toThrow("crash before commit");
    expect(
      (
        await pool.query(
          "SELECT count(*)::int AS count FROM projection_runtime.inbox",
        )
      ).rows[0]?.count,
    ).toBe(0);
    expect(
      (
        await pool.query(
          "SELECT count(*)::int AS count FROM projection_test.events",
        )
      ).rows[0]?.count,
    ).toBe(0);
    await runner.process(record, async (client, stored) => {
      await client.query(
        "INSERT INTO projection_test.events(event_id) VALUES ($1)",
        [stored.eventId],
      );
    });
    expect(
      (
        await pool.query(
          "SELECT count(*)::int AS count FROM projection_runtime.inbox",
        )
      ).rows[0]?.count,
    ).toBe(1);
    expect(
      (
        await pool.query(
          "SELECT next_offset::text FROM projection_runtime.checkpoints",
        )
      ).rows[0]?.next_offset,
    ).toBe("1");
    await expect(
      runner.process({ ...record, offset: 2n }, async () => undefined),
    ).rejects.toBeInstanceOf(ProjectionGapError);
    expect(
      (
        await pool.query(
          "SELECT next_offset::text FROM projection_runtime.checkpoints",
        )
      ).rows[0]?.next_offset,
    ).toBe("1");
  });

  it("cancels a blocked projection SQL transaction at its configured deadline", async () => {
    const generationId = uuidv7();
    const aggregateId = uuidv7();
    const requestId = uuidv7();
    await pool.query(
      "INSERT INTO projection_runtime.generations(projection_name,generation_id,status,created_at) VALUES ('timeout',$1,'building',clock_timestamp())",
      [generationId],
    );
    await store.append({
      producerService: "orders-command",
      namespace: "orders",
      aggregateType: "Order",
      aggregateId,
      requestId,
      expectedRevision: { kind: "no_stream" },
      context: {
        requestId,
        correlationId: uuidv7(),
        causationId: null,
        actor: { kind: "user", subjectRef: "usr_timeout" },
      },
      events: [
        {
          eventName: "order.created",
          schemaVersion: 1,
          occurredAt: "2026-08-04T10:12:18.120Z",
          payload: { orderRef: "timeout" },
        },
      ],
    });
    const event = (await store.readStream("orders", "Order", aggregateId))[0]!;
    const value = canonicalJson(event);
    const record = {
      topic: "event-store.events.v1",
      partition: 0,
      offset: 0n,
      key: `orders|Order|${aggregateId}`,
      value,
      headers: {
        id: event.eventId,
        type: event.eventName,
        envelopeHash: createHash("sha256").update(value).digest("hex"),
        namespace: event.namespace,
        aggregateType: event.aggregateType,
        streamRevision: event.streamRevision,
      },
    };
    const runner = new ProjectionTransactionRunner(
      pool,
      { name: "timeout", generationId },
      (stored) => stored,
    );
    await expect(
      runner.process(
        record,
        (client) => client.query("SELECT pg_sleep(1)").then(() => undefined),
        {
          transactionTimeoutMs: 50,
        },
      ),
    ).rejects.toMatchObject({
      code: expect.stringMatching(/^(25P04|projection_handler_timeout)$/),
    });
    expect(
      await runner.process(record, async () => undefined, {
        transactionTimeoutMs: 1_000,
      }),
    ).toBe("processed");
  });

  it("prevents a timed-out handler from writing after its connection is released", async () => {
    const generationId = uuidv7();
    const aggregateId = uuidv7();
    const requestId = uuidv7();
    await pool.query(
      "INSERT INTO projection_runtime.generations(projection_name,generation_id,status,created_at) VALUES ('handler-timeout',$1,'building',clock_timestamp())",
      [generationId],
    );
    await store.append({
      producerService: "orders-command",
      namespace: "orders",
      aggregateType: "Order",
      aggregateId,
      requestId,
      expectedRevision: { kind: "no_stream" },
      context: {
        requestId,
        correlationId: uuidv7(),
        causationId: null,
        actor: { kind: "user", subjectRef: "usr_handler_timeout" },
      },
      events: [
        {
          eventName: "order.created",
          schemaVersion: 1,
          occurredAt: "2026-08-04T10:12:18.120Z",
          payload: { orderRef: "handler-timeout" },
        },
      ],
    });
    const event = (await store.readStream("orders", "Order", aggregateId))[0]!;
    const value = canonicalJson(event);
    const runner = new ProjectionTransactionRunner(
      pool,
      { name: "handler-timeout", generationId },
      (stored) => stored,
    );
    await expect(
      runner.process(
        {
          topic: "event-store.events.v1",
          partition: 0,
          offset: 0n,
          key: `orders|Order|${aggregateId}`,
          value,
          headers: {
            id: event.eventId,
            type: event.eventName,
            envelopeHash: createHash("sha256").update(value).digest("hex"),
            namespace: event.namespace,
            aggregateType: event.aggregateType,
            streamRevision: event.streamRevision,
          },
        },
        async (client, stored) => {
          await sleep(100);
          await client.query(
            "INSERT INTO projection_test.events(event_id) VALUES ($1)",
            [stored.eventId],
          );
        },
        { transactionTimeoutMs: 50 },
      ),
    ).rejects.toMatchObject({ code: "projection_handler_timeout" });
    await sleep(150);
    expect(
      (
        await pool.query(
          "SELECT count(*)::int AS count FROM projection_test.events WHERE event_id=$1",
          [event.eventId],
        )
      ).rows[0]?.count,
    ).toBe(0);
  });
});
