import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { randomUUID } from "node:crypto";
import { EventStoreStack } from "../fixtures/event-store-stack.js";
import { PostgresEventStore } from "@event-store/postgres-store";
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
});
