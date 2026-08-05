import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { createHash, randomUUID } from "node:crypto";
import { EventStoreStack } from "../fixtures/event-store-stack.js";
import { PostgresEventStore } from "@event-store/postgres-store";
import { canonicalJson } from "@event-store/contracts";
import type { Pool, PoolClient } from "pg";

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

  it("keeps every canonical envelope field equal to its indexed scalar", async () => {
    const input = appendInput(id(), id(), [{ orderRef: "scalar-columns" }]);
    const appended = await store.append(input);
    const eventId = appended.events[0]?.eventId;
    const row = await pool.query<{
      event_envelope: Record<string, unknown>;
      event_id: string;
      namespace: string;
      aggregate_type: string;
      aggregate_id: string;
      stream_revision: string;
      event_number: string;
      event_name: string;
      schema_version: number;
      occurred_at: string;
      recorded_at: string;
      producer_service: string;
    }>(
      `SELECT event_envelope,event_id::text,namespace,aggregate_type,aggregate_id::text,
              stream_revision::text,event_number::text,event_name,schema_version,
              occurred_at::text,recorded_at::text,producer_service
         FROM event_store.events WHERE event_id=$1`,
      [eventId],
    );
    const event = row.rows[0];
    if (event === undefined) throw new Error("appended event was not stored");
    expect(event.event_envelope).toMatchObject({
      eventId: event.event_id,
      namespace: event.namespace,
      aggregateType: event.aggregate_type,
      aggregateId: event.aggregate_id,
      streamRevision: event.stream_revision,
      eventNumber: event.event_number,
      eventName: event.event_name,
      schemaVersion: event.schema_version,
      producerService: event.producer_service,
    });
    expect(new Date(String(event.event_envelope.occurredAt)).getTime()).toBe(
      new Date(event.occurred_at).getTime(),
    );
    expect(new Date(String(event.event_envelope.recordedAt)).getTime()).toBe(
      new Date(event.recorded_at).getTime(),
    );
  });

  it("rolls back stream, event, and idempotency rows together", async () => {
    const input = appendInput(id(), id(), [{ orderRef: "rolled-back" }]);
    const client = await pool.connect();
    try {
      await client.query("BEGIN");
      await client.query(
        "SELECT event_store.append_v1($1,$2,$3,$4,$5,$6,$7,$8::jsonb,$9::jsonb)",
        [
          input.producerService,
          input.namespace,
          input.aggregateType,
          input.aggregateId,
          input.requestId,
          input.expectedRevision.kind,
          null,
          JSON.stringify(input.events),
          JSON.stringify(input.context),
        ],
      );
      await client.query("ROLLBACK");
    } finally {
      client.release();
    }
    await expect(
      pool.query(
        `SELECT
           (SELECT count(*) FROM event_store.streams WHERE aggregate_id=$1)::int AS streams,
           (SELECT count(*) FROM event_store.events WHERE aggregate_id=$1)::int AS events,
           (SELECT count(*) FROM event_store.append_requests WHERE request_id=$2)::int AS requests`,
        [input.aggregateId, input.requestId],
      ),
    ).resolves.toMatchObject({
      rows: [{ streams: 0, events: 0, requests: 0 }],
    });
  });

  it("accepts exactly 1 MiB and rejects one byte more", async () => {
    const accepted = appendInput(id(), id(), [{ data: "" }]);
    const base = await pool.query<{ bytes: string }>(
      "SELECT (octet_length($1::jsonb::text) + octet_length($2::jsonb::text))::text AS bytes",
      [JSON.stringify(accepted.events), JSON.stringify(accepted.context)],
    );
    const bytes = Number(base.rows[0]?.bytes);
    if (!Number.isSafeInteger(bytes)) throw new Error("could not size append");
    accepted.events[0]!.payload.data = "x".repeat(1_048_576 - bytes);
    await expect(store.append(accepted)).resolves.toMatchObject({
      currentRevision: "1",
    });
    const rejected = appendInput(id(), id(), [{ data: "" }]);
    rejected.events[0]!.payload.data = "x".repeat(1_048_577 - bytes);
    await expect(store.append(rejected)).rejects.toMatchObject({
      code: "22001",
    });
  });

  it("makes snapshots idempotent and rejects a conflicting state hash", async () => {
    const aggregateId = id();
    for (let batch = 0; batch < 10; batch += 1) {
      const input = appendInput(
        aggregateId,
        id(),
        Array.from({ length: 100 }, (_, index) => ({
          sequence: String(batch * 100 + index),
        })),
      );
      if (batch > 0)
        input.expectedRevision = {
          kind: "exact",
          revision: BigInt(batch * 100),
        };
      await store.append(input);
    }
    const reducerVersion = createHash("sha256")
      .update("orders-v1")
      .digest("hex");
    const state = { applied: 1_000 };
    const snapshot = {
      namespace: "orders",
      aggregateType: "Order",
      aggregateId,
      revision: 1_000n,
      reducerVersion,
      stateSchemaVersion: 1,
      state,
    };
    await store.putSnapshot(snapshot);
    await store.putSnapshot(snapshot);
    await expect(
      pool.query(
        "SELECT event_store.put_snapshot_v1($1,$2,$3,$4,$5,$6,$7::jsonb,$8)",
        [
          "orders",
          "Order",
          aggregateId,
          "1000",
          reducerVersion,
          1,
          JSON.stringify(state),
          Buffer.alloc(32, 1),
        ],
      ),
    ).rejects.toMatchObject({ code: "XX001" });
    await expect(
      pool.query(
        "SELECT count(*)::int AS count FROM event_store.snapshots WHERE namespace='orders' AND aggregate_type='Order' AND aggregate_id=$1",
        [aggregateId],
      ),
    ).resolves.toMatchObject({ rows: [{ count: 1 }] });
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

  it("rolls back every append write when an event in a batch fails", async () => {
    const input = appendInput(id(), id(), [
      { index: "1" },
      { index: "2" },
      { index: "3" },
    ]);
    try {
      await pool.query(`
        CREATE FUNCTION event_store.test_fail_second_batch_event() RETURNS trigger
        LANGUAGE plpgsql AS $$
        BEGIN
          IF NEW.request_id = '${input.requestId}'::uuid
             AND NEW.request_event_no = 2 THEN
            RAISE EXCEPTION 'injected batch failure' USING ERRCODE = 'P0001';
          END IF;
          RETURN NEW;
        END $$;
        CREATE TRIGGER test_fail_second_batch_event
        BEFORE INSERT ON event_store.events
        FOR EACH ROW EXECUTE FUNCTION event_store.test_fail_second_batch_event();
      `);
      await expect(store.append(input)).rejects.toMatchObject({
        code: "P0001",
      });
      await expect(
        pool.query(
          `SELECT
             (SELECT count(*) FROM event_store.streams WHERE aggregate_id=$1)::int AS streams,
             (SELECT count(*) FROM event_store.events WHERE aggregate_id=$1)::int AS events,
             (SELECT count(*) FROM event_store.append_requests WHERE request_id=$2)::int AS requests`,
          [input.aggregateId, input.requestId],
        ),
      ).resolves.toMatchObject({
        rows: [{ streams: 0, events: 0, requests: 0 }],
      });
    } finally {
      await pool
        .query(
          "DROP TRIGGER IF EXISTS test_fail_second_batch_event ON event_store.events; DROP FUNCTION IF EXISTS event_store.test_fail_second_batch_event()",
        )
        .catch(() => undefined);
    }
    await expect(store.append(input)).resolves.toMatchObject({
      currentRevision: "3",
    });
  });

  it("replays the canonical result after losing the response after commit", async () => {
    const input = appendInput(id(), id(), [{ orderRef: "response-loss" }]);
    let loseResponse = true;
    const responseLossPool = {
      connect: async (): Promise<PoolClient> => {
        const client = await pool.connect();
        return new Proxy(client, {
          get(target, property, receiver) {
            if (property !== "query")
              return Reflect.get(target, property, receiver);
            return async (...args: unknown[]) => {
              const result = await (
                target.query as (...values: never[]) => unknown
              )(...(args as never[]));
              if (
                loseResponse &&
                typeof args[0] === "string" &&
                args[0].includes("event_store.append_v1")
              ) {
                loseResponse = false;
                throw Object.assign(
                  new Error("connection reset after commit"),
                  {
                    code: "ECONNRESET",
                  },
                );
              }
              return result;
            };
          },
        }) as PoolClient;
      },
    } as Pool;
    await expect(
      new PostgresEventStore(responseLossPool).append(input),
    ).rejects.toMatchObject({
      code: "ECONNRESET",
    });
    const replay = await store.append(input);
    expect(replay.currentRevision).toBe("1");
    await expect(
      pool.query<{ events: number; requests: number }>(
        `SELECT
           (SELECT count(*) FROM event_store.events WHERE request_id=$1)::int AS events,
           (SELECT count(*) FROM event_store.append_requests WHERE request_id=$1)::int AS requests`,
        [input.requestId],
      ),
    ).resolves.toMatchObject({ rows: [{ events: 1, requests: 1 }] });
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
    const seeded = await store.append(
      appendInput(id(), id(), [{ orderRef: "immutable" }]),
    );
    const eventId = seeded.events[0]?.eventId;
    if (eventId === undefined)
      throw new Error("append omitted immutable event");
    await expect(
      pool.query(
        "UPDATE event_store.events SET event_name='order.changed' WHERE event_id=$1",
        [eventId],
      ),
    ).rejects.toMatchObject({ code: "55000" });
    await expect(
      pool.query("DELETE FROM event_store.events WHERE event_id=$1", [eventId]),
    ).rejects.toMatchObject({ code: "55000" });
  });

  it("rejects no-stream and exact-revision writes against an existing stream", async () => {
    const aggregateId = id();
    await store.append(appendInput(aggregateId, id(), [{ orderRef: "first" }]));
    await expect(
      store.append(appendInput(aggregateId, id(), [{ orderRef: "again" }])),
    ).rejects.toMatchObject({ code: "40001" });
    const stale = appendInput(aggregateId, id(), [{ orderRef: "stale" }]);
    await expect(
      store.append({
        ...stale,
        expectedRevision: { kind: "exact", revision: 0n },
      }),
    ).rejects.toMatchObject({ code: "40001" });
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

  it("rejects direct SQL PII fields before a snapshot is stored", async () => {
    await expect(
      pool.query(
        "SELECT event_store.put_snapshot_v1($1,$2,$3,$4,$5,$6,$7::jsonb,$8)",
        [
          "orders",
          "Order",
          id(),
          "1000",
          "a".repeat(64),
          1,
          JSON.stringify({ email: "person@example.com" }),
          Buffer.alloc(32),
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

  it("durably rejects appends when delivery health closes CDC admission", async () => {
    await pool.query(
      "UPDATE event_store.runtime_config SET append_admission_enabled=true, cdc_delivery_healthy=false WHERE singleton",
    );
    try {
      await expect(
        store.append(appendInput(id(), id(), [{ orderRef: "delivery-fence" }])),
      ).rejects.toMatchObject({ code: "P0001" });
    } finally {
      // This suite intentionally runs without CDC; restore storage-only mode.
      await pool.query(
        "UPDATE event_store.runtime_config SET append_admission_enabled=false WHERE singleton",
      );
    }
  });

  it("restricts recovery CDC cutover to the CDC principal", async () => {
    await expect(
      pool.query<{ public_can_execute: boolean }>(
        "SELECT has_function_privilege('public', 'event_store.activate_recovery_cdc_slot(text,text,bigint)', 'EXECUTE') AS public_can_execute",
      ),
    ).resolves.toMatchObject({ rows: [{ public_can_execute: false }] });
  });

  it("allows runtime principals to read the current promotion timeline", async () => {
    await expect(
      pool.query<{
        app_can_execute: boolean;
        cdc_can_execute: boolean;
        public_can_execute: boolean;
      }>(
        `SELECT has_function_privilege('event_store_app', 'event_store.current_timeline_id()', 'EXECUTE') AS app_can_execute,
                has_function_privilege('event_store_cdc', 'event_store.current_timeline_id()', 'EXECUTE') AS cdc_can_execute,
                has_function_privilege('public', 'event_store.current_timeline_id()', 'EXECUTE') AS public_can_execute`,
      ),
    ).resolves.toMatchObject({
      rows: [
        {
          app_can_execute: true,
          cdc_can_execute: true,
          public_can_execute: false,
        },
      ],
    });
  });

  it("rejects a recovery proof sampled on another promotion timeline", async () => {
    const timeline = await pool.query<{ timeline_id: number }>(
      "SELECT event_store.current_timeline_id() AS timeline_id",
    );
    await expect(
      pool.query(
        "SELECT event_store.verify_recovery_cdc_cutover($1,$2,$3,$4::uuid,$5,$6,$7::bigint)",
        [
          "event_store_recovery_timeline_test",
          "event-store-recovery-timeline-test",
          "timeline-proof",
          id(),
          "timeline-proof",
          "timeline-proof-consumer",
          (timeline.rows[0]?.timeline_id ?? 0) + 1,
        ],
      ),
    ).rejects.toMatchObject({ code: "P0001" });
  });

  it("requires event-id reconciliation after a delivery incident", async () => {
    await pool.query(
      "UPDATE event_store.runtime_config SET cdc_delivery_healthy=true,cdc_reconciliation_required=false WHERE singleton",
    );
    await pool.query("SELECT event_store.set_cdc_delivery_health(false)");
    try {
      const timeline = await pool.query<{ timeline_id: number }>(
        "SELECT event_store.current_timeline_id() AS timeline_id",
      );
      await expect(
        pool.query(
          "SELECT event_store.set_cdc_delivery_health_on_timeline($1)",
          [timeline.rows[0]?.timeline_id],
        ),
      ).rejects.toMatchObject({ code: "P0001" });
    } finally {
      await pool.query(
        "UPDATE event_store.runtime_config SET cdc_reconciliation_required=false WHERE singleton",
      );
    }
  });

  it("requires reconciliation when a service starts while delivery is already fenced", async () => {
    await pool.query(
      "UPDATE event_store.runtime_config SET cdc_delivery_healthy=false,cdc_reconciliation_required=false WHERE singleton",
    );
    await pool.query(
      "SELECT event_store.close_cdc_delivery_health_for_restart()",
    );
    await pool.query("SELECT event_store.set_cdc_delivery_health(false)");
    await expect(
      pool.query<{ cdc_reconciliation_required: boolean }>(
        "SELECT cdc_reconciliation_required FROM event_store.runtime_config WHERE singleton",
      ),
    ).resolves.toMatchObject({ rows: [{ cdc_reconciliation_required: true }] });
    await pool.query(
      "UPDATE event_store.runtime_config SET cdc_reconciliation_required=false WHERE singleton",
    );
  });

  it("turns a failed first check after restart into a reconciliation incident", async () => {
    await pool.query(
      "UPDATE event_store.runtime_config SET cdc_delivery_healthy=true,cdc_reconciliation_required=false WHERE singleton",
    );
    await pool.query(
      "SELECT event_store.close_cdc_delivery_health_for_restart()",
    );
    await pool.query("SELECT event_store.set_cdc_delivery_health(false)");
    await expect(
      pool.query<{ cdc_reconciliation_required: boolean }>(
        "SELECT cdc_reconciliation_required FROM event_store.runtime_config WHERE singleton",
      ),
    ).resolves.toMatchObject({ rows: [{ cdc_reconciliation_required: true }] });
    await pool.query(
      "UPDATE event_store.runtime_config SET cdc_reconciliation_required=false,cdc_delivery_startup_pending=false WHERE singleton",
    );
  });

  it("fences a health failure that waits behind completed reconciliation", async () => {
    const original = await pool.query<{
      cdc_delivery_healthy: boolean;
      cdc_delivery_startup_pending: boolean;
      cdc_reconciliation_required: boolean;
      cdc_delivery_incident_epoch: string;
      cdc_reconciled_incident_epoch: string | null;
    }>(
      `SELECT cdc_delivery_healthy,cdc_delivery_startup_pending,cdc_reconciliation_required,
              cdc_delivery_incident_epoch,cdc_reconciled_incident_epoch
         FROM event_store.runtime_config WHERE singleton`,
    );
    const blocker = await pool.connect();
    const failedHealthCheck = await pool.connect();
    try {
      await pool.query(
        `UPDATE event_store.runtime_config
            SET cdc_delivery_healthy=false,cdc_delivery_startup_pending=false,
                cdc_reconciliation_required=false,cdc_delivery_incident_epoch=41,
                cdc_reconciled_incident_epoch=41
          WHERE singleton`,
      );
      await blocker.query("BEGIN");
      await blocker.query(
        "SELECT 1 FROM event_store.runtime_config WHERE singleton FOR UPDATE",
      );
      let completed = false;
      const failure = failedHealthCheck
        .query("SELECT event_store.set_cdc_delivery_health(false)")
        .finally(() => {
          completed = true;
        });
      await new Promise((resolve) => setTimeout(resolve, 50));
      expect(completed).toBe(false);
      await blocker.query("COMMIT");
      await failure;
      await expect(
        pool.query<{
          cdc_reconciliation_required: boolean;
          cdc_delivery_incident_epoch: string;
        }>(
          `SELECT cdc_reconciliation_required,cdc_delivery_incident_epoch
             FROM event_store.runtime_config WHERE singleton`,
        ),
      ).resolves.toMatchObject({
        rows: [
          {
            cdc_reconciliation_required: true,
            cdc_delivery_incident_epoch: "42",
          },
        ],
      });
    } finally {
      await blocker.query("ROLLBACK").catch(() => undefined);
      blocker.release();
      failedHealthCheck.release();
      const row = original.rows[0];
      if (row !== undefined)
        await pool.query(
          `UPDATE event_store.runtime_config
              SET cdc_delivery_healthy=$1,cdc_delivery_startup_pending=$2,
                  cdc_reconciliation_required=$3,cdc_delivery_incident_epoch=$4,
                  cdc_reconciled_incident_epoch=$5
            WHERE singleton`,
          [
            row.cdc_delivery_healthy,
            row.cdc_delivery_startup_pending,
            row.cdc_reconciliation_required,
            row.cdc_delivery_incident_epoch,
            row.cdc_reconciled_incident_epoch,
          ],
        );
    }
  });

  it("serializes delivery fencing behind every append entry point", async () => {
    const original = await pool.query<{
      append_admission_enabled: boolean;
      cdc_delivery_healthy: boolean;
      cdc_delivery_startup_pending: boolean;
      cdc_reconciliation_required: boolean;
      cdc_delivery_incident_epoch: string;
      cdc_reconciled_incident_epoch: string | null;
    }>(
      `SELECT append_admission_enabled,cdc_delivery_healthy,cdc_delivery_startup_pending,
              cdc_reconciliation_required,cdc_delivery_incident_epoch,cdc_reconciled_incident_epoch
         FROM event_store.runtime_config WHERE singleton`,
    );
    const appender = await pool.connect();
    const healthCheck = await pool.connect();
    try {
      await pool.query(
        `UPDATE event_store.runtime_config
            SET append_admission_enabled=false,cdc_delivery_healthy=true,
                cdc_delivery_startup_pending=false,cdc_reconciliation_required=false,
                cdc_reconciled_incident_epoch=cdc_delivery_incident_epoch
          WHERE singleton`,
      );
      const input = appendInput(id(), id(), [{ orderRef: "fence-lock" }]);
      await appender.query("BEGIN");
      await appender.query(
        "SELECT event_store.append_v1($1,$2,$3,$4,$5,$6,$7,$8::jsonb,$9::jsonb)",
        [
          input.producerService,
          input.namespace,
          input.aggregateType,
          input.aggregateId,
          input.requestId,
          input.expectedRevision.kind,
          null,
          JSON.stringify(input.events),
          JSON.stringify(input.context),
        ],
      );
      let completed = false;
      const closeDelivery = healthCheck
        .query("SELECT event_store.set_cdc_delivery_health(false)")
        .finally(() => {
          completed = true;
        });
      await new Promise((resolve) => setTimeout(resolve, 50));
      expect(completed).toBe(false);
      await appender.query("COMMIT");
      await closeDelivery;
      await expect(
        pool.query<{ cdc_reconciliation_required: boolean }>(
          "SELECT cdc_reconciliation_required FROM event_store.runtime_config WHERE singleton",
        ),
      ).resolves.toMatchObject({
        rows: [{ cdc_reconciliation_required: true }],
      });
      await pool.query(
        `UPDATE event_store.runtime_config
            SET cdc_delivery_healthy=true,cdc_reconciliation_required=false,
                cdc_reconciled_incident_epoch=cdc_delivery_incident_epoch
          WHERE singleton`,
      );
      await appender.query("BEGIN");
      await appender.query(
        "SELECT event_store.append_recovery_barrier($1,$2,$3,$4)",
        ["fence-lock", 0, id(), id()],
      );
      completed = false;
      const closeBarrierDelivery = healthCheck
        .query("SELECT event_store.set_cdc_delivery_health(false)")
        .finally(() => {
          completed = true;
        });
      await new Promise((resolve) => setTimeout(resolve, 50));
      expect(completed).toBe(false);
      await appender.query("ROLLBACK");
      await closeBarrierDelivery;
      const definitions = await pool.query<{ definition: string }>(
        `SELECT pg_get_functiondef(proc.oid) AS definition
           FROM pg_proc proc
          WHERE proc.oid = ANY (ARRAY[
            'event_store.append_v1(text,text,text,uuid,uuid,text,bigint,jsonb,jsonb)'::regprocedure,
            'event_store.append_v1_critical(text,text,text,uuid,uuid,text,bigint,jsonb,jsonb)'::regprocedure,
            'event_store.append_cdc_latency_probe(uuid)'::regprocedure,
            'event_store.append_recovery_barrier(text,integer,uuid,uuid)'::regprocedure
          ])`,
      );
      expect(definitions.rows).toHaveLength(4);
      expect(
        definitions.rows.every((row) =>
          /runtime_config WHERE singleton FOR SHARE/.test(row.definition),
        ),
      ).toBe(true);
    } finally {
      await appender.query("ROLLBACK").catch(() => undefined);
      appender.release();
      healthCheck.release();
      const row = original.rows[0];
      if (row !== undefined)
        await pool.query(
          `UPDATE event_store.runtime_config
              SET append_admission_enabled=$1,cdc_delivery_healthy=$2,
                  cdc_delivery_startup_pending=$3,cdc_reconciliation_required=$4,
                  cdc_delivery_incident_epoch=$5,cdc_reconciled_incident_epoch=$6
            WHERE singleton`,
          [
            row.append_admission_enabled,
            row.cdc_delivery_healthy,
            row.cdc_delivery_startup_pending,
            row.cdc_reconciliation_required,
            row.cdc_delivery_incident_epoch,
            row.cdc_reconciled_incident_epoch,
          ],
        );
    }
  });

  it("does not let bootstrap reopen an unreconciled delivery incident", async () => {
    const original = await pool.query<{
      append_admission_enabled: boolean;
      cdc_bootstrap_complete: boolean;
      cdc_delivery_healthy: boolean;
      cdc_delivery_timeline_id: number | null;
      cdc_reconciliation_required: boolean;
      cdc_delivery_incident_epoch: string;
      cdc_reconciled_incident_epoch: string | null;
      wal_budget_bytes: string;
    }>(
      `SELECT append_admission_enabled,cdc_bootstrap_complete,cdc_delivery_healthy,
              cdc_delivery_timeline_id,cdc_reconciliation_required,
              cdc_delivery_incident_epoch,cdc_reconciled_incident_epoch,wal_budget_bytes
         FROM event_store.runtime_config WHERE singleton`,
    );
    try {
      await pool.query(
        `UPDATE event_store.runtime_config
            SET append_admission_enabled=false,cdc_delivery_healthy=false,
                cdc_reconciliation_required=true,cdc_delivery_incident_epoch=7,
                cdc_reconciled_incident_epoch=6,
                cdc_delivery_timeline_id=event_store.current_timeline_id()
          WHERE singleton`,
      );
      await expect(
        pool.query("SELECT event_store.enable_append_admission(8589934592)"),
      ).rejects.toMatchObject({ code: "P0001" });
    } finally {
      const row = original.rows[0];
      if (row !== undefined)
        await pool.query(
          `UPDATE event_store.runtime_config
              SET append_admission_enabled=$1,cdc_bootstrap_complete=$2,
                  cdc_delivery_healthy=$3,cdc_delivery_timeline_id=$4,
                  cdc_reconciliation_required=$5,cdc_delivery_incident_epoch=$6,
                  cdc_reconciled_incident_epoch=$7,wal_budget_bytes=$8
            WHERE singleton`,
          [
            row.append_admission_enabled,
            row.cdc_bootstrap_complete,
            row.cdc_delivery_healthy,
            row.cdc_delivery_timeline_id,
            row.cdc_reconciliation_required,
            row.cdc_delivery_incident_epoch,
            row.cdc_reconciled_incident_epoch,
            row.wal_budget_bytes,
          ],
        );
    }
  });

  it("makes recovery activation inspect the incident fence under a row lock", async () => {
    const original = await pool.query<{
      cdc_reconciliation_required: boolean;
      cdc_delivery_incident_epoch: string;
      cdc_reconciled_incident_epoch: string | null;
    }>(
      `SELECT cdc_reconciliation_required,cdc_delivery_incident_epoch,
              cdc_reconciled_incident_epoch
         FROM event_store.runtime_config WHERE singleton`,
    );
    try {
      await pool.query(
        `UPDATE event_store.runtime_config
            SET cdc_reconciliation_required=true,cdc_delivery_incident_epoch=7,
                cdc_reconciled_incident_epoch=6
          WHERE singleton`,
      );
      await expect(
        pool.query(
          "SELECT event_store.activate_recovery_cdc_slot('event_store_recovery_fence_test','event-store-recovery-fence-test',8589934592)",
        ),
      ).rejects.toMatchObject({ code: "P0001" });
    } finally {
      const row = original.rows[0];
      if (row !== undefined)
        await pool.query(
          `UPDATE event_store.runtime_config
              SET cdc_reconciliation_required=$1,cdc_delivery_incident_epoch=$2,
                  cdc_reconciled_incident_epoch=$3
            WHERE singleton`,
          [
            row.cdc_reconciliation_required,
            row.cdc_delivery_incident_epoch,
            row.cdc_reconciled_incident_epoch,
          ],
        );
    }
    const definition = await pool.query<{ definition: string }>(
      `SELECT pg_get_functiondef(
         'event_store.activate_recovery_cdc_slot(text,text,bigint)'::regprocedure
       ) AS definition`,
    );
    expect(definition.rows[0]?.definition).toContain(
      "runtime_config WHERE singleton FOR UPDATE",
    );
    expect(definition.rows[0]?.definition).toContain(
      "recovery activation requires delivery incident reconciliation",
    );
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
