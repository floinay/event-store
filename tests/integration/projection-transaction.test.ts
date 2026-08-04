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

  it("checkpoints the CDC probe without requiring a projection schema or handler", async () => {
    const generationId = uuidv7();
    const eventId = uuidv7();
    const requestId = uuidv7();
    const event = {
      eventId,
      namespace: "system",
      aggregateType: "CdcLatencyProbe",
      aggregateId: uuidv7(),
      streamRevision: "1",
      eventNumber: "1",
      eventName: "system.cdc.latency.probe",
      schemaVersion: 1,
      occurredAt: "2026-08-04T10:12:18.120Z",
      recordedAt: "2026-08-04T10:12:18.120Z",
      producerService: "event-store-latency-probe",
      context: {
        requestId,
        correlationId: uuidv7(),
        causationId: null,
        actor: {
          kind: "service" as const,
          subjectRef: "event-store-latency-probe",
        },
      },
      payload: {},
    };
    await pool.query(
      "INSERT INTO projection_runtime.generations(projection_name,generation_id,status,created_at) VALUES ('probe-control',$1,'building',clock_timestamp())",
      [generationId],
    );
    const value = canonicalJson(event);
    const record = {
      topic: "event-store.events.v1",
      partition: 0,
      offset: 0n,
      key: `system|CdcLatencyProbe|${event.aggregateId}`,
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
      { name: "probe-control", generationId },
      () => {
        throw new Error("CDC probe must not require a projection schema");
      },
    );
    let handlerCalls = 0;
    await expect(
      runner.process(record, async () => {
        handlerCalls += 1;
      }),
    ).resolves.toBe("processed");
    expect(handlerCalls).toBe(0);
    await expect(
      pool.query(
        "SELECT next_offset::text, last_event_id::text FROM projection_runtime.checkpoints WHERE projection_name='probe-control' AND generation_id=$1",
        [generationId],
      ),
    ).resolves.toMatchObject({
      rows: [{ next_offset: "1", last_event_id: eventId }],
    });
    await expect(
      pool.query(
        "SELECT count(*)::int AS count FROM projection_runtime.inbox WHERE projection_name='probe-control' AND generation_id=$1",
        [generationId],
      ),
    ).resolves.toMatchObject({ rows: [{ count: 1 }] });
  });

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
      runner.process(record, async () => {
        throw new Error("duplicate Kafka delivery reached the projection");
      }),
    ).resolves.toBe("duplicate");
    expect(
      (
        await pool.query(
          "SELECT count(*)::int AS count FROM projection_test.events",
        )
      ).rows[0]?.count,
    ).toBe(1);
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

  it("exposes deterministic crash barriers at every PostgreSQL boundary", async () => {
    await pool.query(
      "CREATE SCHEMA projection_barrier; CREATE TABLE projection_barrier.events(generation_id uuid NOT NULL,event_id uuid NOT NULL,PRIMARY KEY(generation_id,event_id))",
    );
    for (const point of [
      "after_inbox_insert",
      "after_read_model_mutation",
      "after_checkpoint_update",
      "after_database_commit",
    ] as const) {
      const generationId = uuidv7();
      const aggregateId = uuidv7();
      const requestId = uuidv7();
      await pool.query(
        "INSERT INTO projection_runtime.generations(projection_name,generation_id,status,created_at) VALUES ($1,$2,'building',clock_timestamp())",
        [`barrier-${point}`, generationId],
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
          actor: { kind: "user", subjectRef: "usr_barrier" },
        },
        events: [
          {
            eventName: "order.created",
            schemaVersion: 1,
            occurredAt: "2026-08-04T10:12:18.120Z",
            payload: { point },
          },
        ],
      });
      const event = (
        await store.readStream("orders", "Order", aggregateId)
      )[0]!;
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
        { name: `barrier-${point}`, generationId },
        (stored) => stored,
        {
          hit: (hit) => {
            if (hit === point) throw new Error(`crash at ${hit}`);
          },
        },
      );
      await expect(
        runner.process(record, async (client, stored) => {
          await client.query(
            "INSERT INTO projection_barrier.events(generation_id,event_id) VALUES ($1,$2)",
            [generationId, stored.eventId],
          );
        }),
      ).rejects.toThrow(`crash at ${point}`);
      const [inbox, checkpoint, model] = await Promise.all([
        pool.query<{ count: number }>(
          "SELECT count(*)::int AS count FROM projection_runtime.inbox WHERE projection_name=$1 AND generation_id=$2",
          [`barrier-${point}`, generationId],
        ),
        pool.query<{ count: number }>(
          "SELECT count(*)::int AS count FROM projection_runtime.checkpoints WHERE projection_name=$1 AND generation_id=$2",
          [`barrier-${point}`, generationId],
        ),
        pool.query<{ count: number }>(
          "SELECT count(*)::int AS count FROM projection_barrier.events WHERE generation_id=$1",
          [generationId],
        ),
      ]);
      const expected = point === "after_database_commit" ? 1 : 0;
      expect(inbox.rows[0]?.count).toBe(expected);
      expect(checkpoint.rows[0]?.count).toBe(expected);
      expect(model.rows[0]?.count).toBe(expected);
    }
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

  it("retains projection failures for thirty days", async () => {
    const generationId = uuidv7();
    await pool.query(
      "INSERT INTO projection_runtime.generations(projection_name,generation_id,status,created_at) VALUES ('retention',$1,'building',clock_timestamp())",
      [generationId],
    );
    const insert = (offset: number, age: string) =>
      pool.query(
        `INSERT INTO projection_runtime.failures(projection_name,generation_id,event_id,envelope_sha256,topic_name,partition_no,kafka_offset,attempt_count,error_code,error_detail,envelope,first_failed_at,last_failed_at)
         VALUES ('retention',$1,$2,$3,'event-store.events.v1',0,$4,1,'test','{}','{}',clock_timestamp()-$5::interval,clock_timestamp()-$5::interval)`,
        [generationId, uuidv7(), "0".repeat(64), offset, age],
      );
    await insert(0, "31 days");
    await insert(1, "29 days");
    await expect(
      pool.query("SELECT projection_runtime.prune_failures() AS deleted"),
    ).resolves.toMatchObject({ rows: [{ deleted: "1" }] });
    await expect(
      pool.query<{ count: number }>(
        "SELECT count(*)::int AS count FROM projection_runtime.failures WHERE projection_name='retention' AND generation_id=$1",
        [generationId],
      ),
    ).resolves.toMatchObject({ rows: [{ count: 1 }] });
  });
});
