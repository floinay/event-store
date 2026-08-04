import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { createHash, randomUUID } from "node:crypto";
import { EventStoreStack } from "../fixtures/event-store-stack.js";
import { PostgresEventStore } from "@event-store/postgres-store";
import { canonicalJson } from "@event-store/contracts";
import type { Pool } from "pg";

const enabled = process.env.RUN_INTEGRATION === "true";
const suite = enabled ? describe : describe.skip;
const id = () => randomUUID().replace(/^(.{14})./, "$17");

suite("append SQL contract", () => {
  const stack = new EventStoreStack();
  let store: PostgresEventStore;
  let pool: Pool;
  beforeAll(async () => {
    await stack.start();
    pool = await stack.pool();
    store = new PostgresEventStore(pool);
  }, 180_000);
  afterAll(async () => {
    await pool?.end();
    await stack.stop();
  }, 60_000);
  it("assigns contiguous revisions and replays an idempotent request", async () => {
    const aggregateId = id();
    const requestId = id();
    const correlationId = id();
    const input = {
      producerService: "orders-command",
      namespace: "orders",
      aggregateType: "Order",
      aggregateId,
      requestId,
      expectedRevision: { kind: "no_stream" as const },
      context: {
        requestId,
        correlationId,
        causationId: null,
        actor: { kind: "user" as const, subjectRef: "usr_1" },
      },
      events: [
        {
          eventName: "order.created",
          schemaVersion: 1,
          occurredAt: "2026-08-04T10:12:18.120Z",
          payload: { orderRef: "o1" },
        },
        {
          eventName: "order.confirmed",
          schemaVersion: 1,
          occurredAt: "2026-08-04T10:12:19.120Z",
          payload: {},
        },
      ],
    };
    const first = await store.append(input);
    const second = await store.append(input);
    expect(first).toEqual(second);
    expect(first.currentRevision).toBe("2");
    expect(
      (await store.readStream("orders", "Order", aggregateId)).map(
        (event) => event.streamRevision,
      ),
    ).toEqual(["1", "2"]);
  });

  it("rejects a changed body for an already acknowledged requestId", async () => {
    const aggregateId = id();
    const requestId = id();
    const input = appendInput(aggregateId, requestId, [{ orderRef: "first" }]);
    await store.append(input);
    await expect(
      store.append({
        ...input,
        events: [
          {
            ...input.events[0]!,
            payload: { orderRef: "changed" },
          },
        ],
      }),
    ).rejects.toMatchObject({ code: "23505" });
  });

  it("accepts a 100-event batch and rejects 101 events atomically", async () => {
    const accepted = appendInput(
      id(),
      id(),
      Array.from({ length: 100 }, (_, index) => ({ index: String(index) })),
    );
    expect((await store.append(accepted)).events).toHaveLength(100);
    const rejected = appendInput(
      id(),
      id(),
      Array.from({ length: 101 }, (_, index) => ({ index: String(index) })),
    );
    await expect(store.append(rejected)).rejects.toMatchObject({
      code: "22023",
    });
    expect(
      await store.getStreamHead(
        rejected.namespace,
        rejected.aggregateType,
        rejected.aggregateId,
      ),
    ).toBeUndefined();
  });

  it("owns SECURITY DEFINER functions by event_store_owner", async () => {
    const owner = await pool.query<{ owner: string }>(
      "SELECT pg_get_userbyid(proowner) AS owner FROM pg_proc WHERE pronamespace='event_store'::regnamespace AND proname='append_v1'",
    );
    expect(owner.rows[0]?.owner).toBe("event_store_owner");
  });

  it("does not expose append implementation functions to PUBLIC or CDC", async () => {
    const privileges = await pool.query<{
      public_append: boolean;
      cdc_append: boolean;
      app_append: boolean;
      public_unchecked: boolean;
    }>(
      `SELECT
         NOT EXISTS (
           SELECT 1 FROM pg_proc AS proc
           CROSS JOIN LATERAL aclexplode(coalesce(proc.proacl, acldefault('f', proc.proowner))) AS acl
           WHERE proc.oid='event_store.append_v1(text,text,text,uuid,uuid,text,bigint,jsonb,jsonb)'::regprocedure
             AND acl.grantee=0 AND acl.privilege_type='EXECUTE'
         ) AS public_append,
         has_function_privilege('event_store_cdc','event_store.append_v1(text,text,text,uuid,uuid,text,bigint,jsonb,jsonb)','EXECUTE') AS cdc_append,
         has_function_privilege('event_store_app','event_store.append_v1(text,text,text,uuid,uuid,text,bigint,jsonb,jsonb)','EXECUTE') AS app_append,
         NOT EXISTS (
           SELECT 1 FROM pg_proc AS proc
           CROSS JOIN LATERAL aclexplode(coalesce(proc.proacl, acldefault('f', proc.proowner))) AS acl
           WHERE proc.oid='event_store.append_v1_unchecked(text,text,text,uuid,uuid,text,bigint,jsonb,jsonb)'::regprocedure
             AND acl.grantee=0 AND acl.privilege_type='EXECUTE'
         ) AS public_unchecked`,
    );
    expect(privileges.rows[0]).toEqual({
      public_append: true,
      cdc_append: false,
      app_append: true,
      public_unchecked: true,
    });
  });

  it("denies direct event-table writes and preserves event immutability", async () => {
    const client = await pool.connect();
    try {
      await client.query("SET ROLE event_store_app");
      await expect(
        client.query(
          "INSERT INTO event_store.events(event_number,event_id) VALUES (1,$1)",
          [id()],
        ),
      ).rejects.toMatchObject({ code: "42501" });
    } finally {
      await client.query("RESET ROLE").catch(() => undefined);
      client.release();
    }
    const event = await pool.query<{ event_id: string }>(
      "SELECT event_id FROM event_store.events LIMIT 1",
    );
    await expect(
      pool.query(
        "UPDATE event_store.events SET event_name='order.changed' WHERE event_id=$1",
        [event.rows[0]?.event_id],
      ),
    ).rejects.toMatchObject({ code: "55000" });
  });

  it("rejects a direct SQL append with an invalid canonical context", async () => {
    const error = await pool
      .query(
        "SELECT event_store.append_v1($1,$2,$3,$4,$5,$6,$7,$8::jsonb,$9::jsonb)",
        [
          "orders-command",
          "orders",
          "Order",
          id(),
          id(),
          "no_stream",
          null,
          JSON.stringify([
            {
              eventName: "order.created",
              schemaVersion: 1,
              occurredAt: "2026-08-04T10:12:18.120Z",
              payload: {},
            },
          ]),
          JSON.stringify({ actor: { kind: "user", subjectRef: "usr_1" } }),
        ],
      )
      .then(
        () => undefined,
        (reason: unknown) => reason as { code?: string },
      );
    expect(error?.code).toBe("22023");
  });

  it("rejects direct SQL PII fields before the event is stored", async () => {
    for (const key of [
      "email",
      "customer_email",
      "customerEmail",
      "token",
      "phoneNumber",
      "telephoneNumber",
      "socialSecurityNumber",
      "birthDate",
      "bankAccount",
      "accountNumber",
      "iban",
    ])
      await expect(
        pool.query(
          "SELECT event_store.append_v1($1,$2,$3,$4,$5,$6,$7,$8::jsonb,$9::jsonb)",
          [
            "orders-command",
            "orders",
            "Order",
            id(),
            id(),
            "no_stream",
            null,
            JSON.stringify([
              {
                eventName: "order.created",
                schemaVersion: 1,
                occurredAt: "2026-08-04T10:12:18.120Z",
                payload: { [key]: "person@example.com" },
              },
            ]),
            JSON.stringify({
              correlationId: id(),
              actor: { kind: "user", subjectRef: "usr_1" },
            }),
          ],
        ),
      ).rejects.toMatchObject({ code: "22023" });
  });

  it("rejects direct SQL numbers that cannot preserve the consumer hash", async () => {
    const common = [
      "orders-command",
      "orders",
      "Order",
      id(),
      id(),
      "no_stream",
      null,
    ];
    const context = JSON.stringify({
      correlationId: id(),
      actor: { kind: "user", subjectRef: "usr_1" },
    });
    for (const numericLiteral of ["9007199254740993", "1e400"]) {
      await expect(
        pool.query(
          "SELECT event_store.append_v1($1,$2,$3,$4,$5,$6,$7,$8::jsonb,$9::jsonb)",
          [
            ...common.slice(0, 3),
            id(),
            id(),
            ...common.slice(5),
            `[{"eventName":"order.created","schemaVersion":1,"occurredAt":"2026-08-04T10:12:18.120Z","payload":{"value":${numericLiteral}}}]`,
            context,
          ],
        ),
      ).rejects.toMatchObject({ code: "22023" });
    }
  });

  it("normalizes an omitted causationId to null in a direct SQL append", async () => {
    const requestId = id();
    const appended = await pool.query<{
      append_v1: { events: { eventId: string }[] };
    }>(
      "SELECT event_store.append_v1($1,$2,$3,$4,$5,$6,$7,$8::jsonb,$9::jsonb)",
      [
        "orders-command",
        "orders",
        "Order",
        id(),
        requestId,
        "no_stream",
        null,
        JSON.stringify([
          {
            eventName: "order.created",
            schemaVersion: 1,
            occurredAt: "2026-08-04T10:12:18.120Z",
            payload: {},
          },
        ]),
        JSON.stringify({
          correlationId: id(),
          actor: { kind: "user", subjectRef: "usr_1" },
        }),
      ],
    );
    const eventId = appended.rows[0]?.append_v1.events[0]?.eventId;
    if (eventId === undefined)
      throw new Error("direct append did not return eventId");
    const result = await pool.query<{ event_envelope: { context: unknown } }>(
      "SELECT event_envelope FROM event_store.events WHERE event_id=$1",
      [eventId],
    );
    expect(result.rows[0]?.event_envelope.context).toMatchObject({
      causationId: null,
    });
  });

  it("accepts JSON numbers and hashes their canonical PostgreSQL form", async () => {
    const aggregateId = id();
    const requestId = id();
    const appended = await store.append({
      producerService: "orders-command",
      namespace: "orders",
      aggregateType: "Order",
      aggregateId,
      requestId,
      expectedRevision: { kind: "no_stream" },
      context: {
        requestId,
        correlationId: id(),
        causationId: null,
        actor: { kind: "user", subjectRef: "usr_1" },
      },
      events: [
        {
          eventName: "order.created",
          schemaVersion: 1,
          occurredAt: "2026-08-04T10:12:18.120Z",
          payload: { small: 1e-7, large: 1e21, price: 1.23 },
        },
      ],
    });
    const eventId = appended.events[0]?.eventId;
    const envelope = await pool.query<{
      event_envelope: Record<string, unknown>;
      envelope_sha256: string;
    }>(
      "SELECT event_envelope,envelope_sha256 FROM event_store.events WHERE event_id=$1",
      [eventId],
    );
    const row = envelope.rows[0];
    expect(row).toBeDefined();
    expect(row?.envelope_sha256).toBe(
      createHash("sha256")
        .update(canonicalJson(row?.event_envelope))
        .digest("hex"),
    );
  });

  it("fails closed when a failover candidate has no synchronized logical slot", async () => {
    await expect(
      pool.query(
        "SELECT event_store.assert_failover_candidate('event_store_missing')",
      ),
    ).rejects.toMatchObject({ code: "P0001" });
  });

  it("rejects append admission for a non-failover CDC slot", async () => {
    const original = await pool.query<{ cdc_slot_name: string }>(
      "SELECT cdc_slot_name FROM event_store.runtime_config WHERE singleton",
    );
    const slotName = "event_store_nonfailover_test";
    await pool.query(
      "SELECT pg_create_logical_replication_slot($1, 'pgoutput')",
      [slotName],
    );
    try {
      await pool.query(
        "UPDATE event_store.runtime_config SET cdc_slot_name=$1 WHERE singleton",
        [slotName],
      );
      await expect(
        pool.query("SELECT event_store.assert_append_cdc_ready(8589934592)"),
      ).rejects.toMatchObject({ code: "P0001" });
    } finally {
      await pool.query(
        "UPDATE event_store.runtime_config SET cdc_slot_name=$1 WHERE singleton",
        [original.rows[0]?.cdc_slot_name ?? "event_store_live"],
      );
      await pool.query("SELECT pg_drop_replication_slot($1)", [slotName]);
    }
  });
});

function appendInput(
  aggregateId: string,
  requestId: string,
  payloads: readonly Record<string, string>[],
) {
  return {
    producerService: "orders-command",
    namespace: "orders",
    aggregateType: "Order",
    aggregateId,
    requestId,
    expectedRevision: { kind: "no_stream" as const },
    context: {
      requestId,
      correlationId: id(),
      causationId: null,
      actor: { kind: "user" as const, subjectRef: "usr_1" },
    },
    events: payloads.map((payload) => ({
      eventName: "order.created",
      schemaVersion: 1,
      occurredAt: "2026-08-04T10:12:18.120Z",
      payload,
    })),
  };
}
