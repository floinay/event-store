import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { spawn, type ChildProcess } from "node:child_process";
import { once } from "node:events";
import { KafkaJS } from "@confluentinc/kafka-javascript";
import * as grpc from "@grpc/grpc-js";
import * as protoLoader from "@grpc/proto-loader";
import { join } from "node:path";
import { uuidv7 } from "@event-store/contracts";
import { startServer } from "../../apps/event-store-service/dist/index.js";
import { EventStoreStack } from "../fixtures/event-store-stack.js";

const suite = process.env.RUN_INTEGRATION === "true" ? describe : describe.skip;

interface AppendClient extends grpc.Client {
  AppendToStream(
    request: Record<string, unknown>,
    callback: (
      error: grpc.ServiceError | null,
      response?: Record<string, unknown>,
    ) => void,
  ): void;
}

suite("gRPC to CDC", () => {
  const stack = new EventStoreStack();
  let server: grpc.Server;
  let client: AppendClient;
  beforeAll(async () => {
    await stack.start({ cdc: true, toxiproxy: true, connectKafkaProxy: true });
    process.env.DATABASE_URL = stack.databaseUrl;
    process.env.PRODUCER_SERVICE = "orders-command";
    process.env.CDC_WAL_BUDGET_BYTES = String(8 * 1024 ** 3);
    process.env.GRPC_LISTEN_ADDRESS = "127.0.0.1:50061";
    process.env.HTTP_LISTEN_ADDRESS = "127.0.0.1:50161";
    process.env.CONNECT_URL = stack.connectUrl;
    process.env.GRPC_ALLOW_INSECURE = "true";
    server = await startServer();
    const definition = protoLoader.loadSync(
      join(process.cwd(), "packages/contracts/proto/event_store.proto"),
      {
        keepCase: true,
        longs: String,
        enums: String,
        defaults: false,
        oneofs: true,
      },
    );
    const service = (
      grpc.loadPackageDefinition(definition) as unknown as {
        eventstore: {
          v1: { EventStoreService: grpc.ServiceClientConstructor };
        };
      }
    ).eventstore.v1.EventStoreService;
    client = new service(
      "127.0.0.1:50061",
      grpc.credentials.createInsecure(),
    ) as AppendClient;
  }, 180_000);
  afterAll(async () => {
    client?.close();
    await new Promise<void>((resolve) => server?.tryShutdown(() => resolve()));
    await stack.stop();
  }, 60_000);

  it("acknowledges the durable gRPC append and delivers its event through CDC", async () => {
    const kafka = new KafkaJS.Kafka({
      kafkaJS: { brokers: [stack.kafkaBroker()] },
    });
    const consumer = kafka.consumer({
      kafkaJS: {
        groupId: `grpc-cdc-${uuidv7()}`,
        autoCommit: false,
        fromBeginning: true,
      },
    });
    await consumer.connect();
    await consumer.subscribe({
      topics: ["event-store.events.v1"],
      replace: true,
    });
    const aggregateId = uuidv7();
    const requestId = uuidv7();
    const received = new Promise<Record<string, unknown>>((resolve, reject) => {
      const timeout = setTimeout(
        () => reject(new Error("timed out waiting for gRPC CDC event")),
        30_000,
      );
      void consumer.run({
        eachMessage: async ({ message }) => {
          const event = JSON.parse(message.value?.toString() ?? "{}") as Record<
            string,
            unknown
          >;
          if (
            (event.context as { requestId?: string } | undefined)?.requestId ===
            requestId
          ) {
            clearTimeout(timeout);
            resolve(event);
          }
        },
      });
    });
    await stack.setConnectKafkaEnabled(false);
    const response = await new Promise<Record<string, unknown>>(
      (resolve, reject) => {
        client.AppendToStream(
          {
            request_id: requestId,
            namespace: "orders",
            aggregate_type: "Order",
            aggregate_id: aggregateId,
            expected_revision: { no_stream: {} },
            context: {
              correlation_id: uuidv7(),
              actor_json: Buffer.from(
                JSON.stringify({ kind: "user", subjectRef: "usr_1" }),
              ),
            },
            events: [
              {
                event_name: "order.created",
                schema_version: 1,
                occurred_at: "2026-08-04T10:12:18.120Z",
                payload_json: Buffer.from(JSON.stringify({ orderRef: "o1" })),
              },
            ],
          },
          (error, value) =>
            error === null ? resolve(value ?? {}) : reject(error),
        );
      },
    );
    expect(response).toBeDefined();
    expect(response).toMatchObject({
      request_id: requestId,
      previous_revision: "0",
      current_revision: "1",
    });
    expect(typeof response.recorded_at).toBe("string");
    expect(response.events).toMatchObject([
      { stream_revision: "1", event_number: expect.any(String) },
    ]);
    await stack.setConnectKafkaEnabled(true);
    await expect(received).resolves.toMatchObject({
      aggregateId,
      eventName: "order.created",
    });
    await consumer.disconnect();
  }, 60_000);

  it("exposes CDC-gated readiness and append metrics", async () => {
    await expect(fetch("http://127.0.0.1:50161/readyz")).resolves.toMatchObject(
      {
        status: 200,
      },
    );
    const metrics = await fetch("http://127.0.0.1:50161/metrics").then(
      (result) => result.text(),
    );
    expect(metrics).toContain("event_store_append_total");
    expect(metrics).toContain("event_store_append_conflicts_total");
    expect(metrics).toContain("event_store_append_unknown_outcomes_total");
    expect(metrics).toContain("event_store_db_commit_duration_seconds_sum");
    expect(metrics).toContain("event_store_db_pool_waiting");
  });

  it("measures SQL completion to pre-handler consumer receipt", async () => {
    const previous = {
      grpc: process.env.GRPC_LISTEN_ADDRESS,
      http: process.env.HTTP_LISTEN_ADDRESS,
      brokers: process.env.KAFKA_BROKERS,
      probeInterval: process.env.CDC_LATENCY_PROBE_INTERVAL_MS,
    };
    process.env.GRPC_LISTEN_ADDRESS = "127.0.0.1:50064";
    process.env.HTTP_LISTEN_ADDRESS = "127.0.0.1:50164";
    process.env.KAFKA_BROKERS = stack.kafkaBroker();
    process.env.CDC_LATENCY_PROBE_INTERVAL_MS = "1000";
    const probeServer = await startServer();
    try {
      const deadline = Date.now() + 30_000;
      let metrics = "";
      while (Date.now() < deadline) {
        metrics = await fetch("http://127.0.0.1:50164/metrics").then((result) =>
          result.text(),
        );
        if (
          /event_store_cdc_commit_to_consumer_latency_seconds_count [1-9]/.test(
            metrics,
          )
        )
          break;
        await new Promise((resolve) => setTimeout(resolve, 100));
      }
      expect(metrics).toContain("event_store_cdc_latency_probe_up 1");
      expect(metrics).toMatch(
        /event_store_cdc_commit_to_consumer_latency_seconds_count [1-9]/,
      );
      await stack.setConnectKafkaEnabled(false);
      const unavailableDeadline = Date.now() + 10_000;
      while (Date.now() < unavailableDeadline) {
        metrics = await fetch("http://127.0.0.1:50164/metrics").then((result) =>
          result.text(),
        );
        if (metrics.includes("event_store_cdc_latency_probe_up 0")) break;
        await new Promise((resolve) => setTimeout(resolve, 100));
      }
      expect(metrics).toContain("event_store_cdc_latency_probe_up 0");
      await expect(
        new Promise<Record<string, unknown>>((resolve, reject) =>
          client.AppendToStream(
            {
              request_id: uuidv7(),
              namespace: "orders",
              aggregate_type: "Order",
              aggregate_id: uuidv7(),
              expected_revision: { no_stream: {} },
              context: {
                correlation_id: uuidv7(),
                actor_json: Buffer.from(
                  JSON.stringify({ kind: "user", subjectRef: "usr_probe" }),
                ),
              },
              events: [
                {
                  event_name: "order.created",
                  schema_version: 1,
                  occurred_at: "2026-08-04T10:12:18.120Z",
                  payload_json: Buffer.from(JSON.stringify({ orderRef: "o3" })),
                },
              ],
            },
            (error, value) =>
              error === null ? resolve(value ?? {}) : reject(error),
          ),
        ),
      ).rejects.toMatchObject({ code: grpc.status.RESOURCE_EXHAUSTED });
    } finally {
      await stack.setConnectKafkaEnabled(true);
      await new Promise<void>((resolve) => probeServer.tryShutdown(resolve));
      const recoveryDeadline = Date.now() + 30_000;
      while (Date.now() < recoveryDeadline) {
        const readiness = await fetch("http://127.0.0.1:50161/readyz");
        if (readiness.status === 200) break;
        await new Promise((resolve) => setTimeout(resolve, 100));
      }
      await expect(
        fetch("http://127.0.0.1:50161/readyz"),
      ).resolves.toMatchObject({
        status: 200,
      });
      for (const [name, value] of Object.entries(previous)) {
        const key =
          name === "grpc"
            ? "GRPC_LISTEN_ADDRESS"
            : name === "http"
              ? "HTTP_LISTEN_ADDRESS"
              : name === "brokers"
                ? "KAFKA_BROKERS"
                : "CDC_LATENCY_PROBE_INTERVAL_MS";
        if (value === undefined) delete process.env[key];
        else process.env[key] = value;
      }
    }
  }, 60_000);

  it.each([
    "before_sql",
    "after_postgres_commit",
    "before_grpc_response_write",
  ] as const)(
    "recovers an idempotent append after %s",
    async (crashPoint) => {
      let crashOnce = true;
      const previousAddress = process.env.GRPC_LISTEN_ADDRESS;
      const previousHealthAddress = process.env.HTTP_LISTEN_ADDRESS;
      process.env.GRPC_LISTEN_ADDRESS = "127.0.0.1:50063";
      delete process.env.HTTP_LISTEN_ADDRESS;
      const crashServer = await startServer({
        hit: (point) => {
          if (point !== crashPoint || !crashOnce) return;
          crashOnce = false;
          throw Object.assign(new Error(`test crash at ${point}`), {
            code: "ECONNRESET",
            appendDispatched: true,
          });
        },
      });
      const definition = protoLoader.loadSync(
        join(process.cwd(), "packages/contracts/proto/event_store.proto"),
        { keepCase: true, longs: String, enums: String, defaults: false },
      );
      const service = (
        grpc.loadPackageDefinition(definition) as unknown as {
          eventstore: {
            v1: { EventStoreService: grpc.ServiceClientConstructor };
          };
        }
      ).eventstore.v1.EventStoreService;
      const crashClient = new service(
        "127.0.0.1:50063",
        grpc.credentials.createInsecure(),
      ) as AppendClient;
      const requestId = uuidv7();
      const request = {
        request_id: requestId,
        namespace: "orders",
        aggregate_type: "Order",
        aggregate_id: uuidv7(),
        expected_revision: { no_stream: {} },
        context: {
          correlation_id: uuidv7(),
          actor_json: Buffer.from(
            JSON.stringify({ kind: "user", subjectRef: "usr_crash" }),
          ),
        },
        events: [
          {
            event_name: "order.created",
            schema_version: 1,
            occurred_at: "2026-08-04T10:12:18.120Z",
            payload_json: Buffer.from(JSON.stringify({ orderRef: "retry" })),
          },
        ],
      };
      const append = () =>
        new Promise<Record<string, unknown>>((resolve, reject) =>
          crashClient.AppendToStream(request, (error, value) =>
            error === null ? resolve(value ?? {}) : reject(error),
          ),
        );
      try {
        await expect(append()).rejects.toMatchObject({
          code: grpc.status.UNKNOWN,
        });
        await expect(append()).resolves.toMatchObject({
          request_id: requestId,
        });
        const pool = await stack.pool();
        try {
          await expect(
            pool.query<{ count: number }>(
              "SELECT count(*)::int AS count FROM event_store.events WHERE request_id=$1",
              [requestId],
            ),
          ).resolves.toMatchObject({ rows: [{ count: 1 }] });
        } finally {
          await pool.end();
        }
      } finally {
        crashClient.close();
        await new Promise<void>((resolve) => crashServer.tryShutdown(resolve));
        process.env.GRPC_LISTEN_ADDRESS = previousAddress;
        process.env.HTTP_LISTEN_ADDRESS = previousHealthAddress;
      }
    },
    60_000,
  );

  it.each([
    "before_sql",
    "after_postgres_commit",
    "before_grpc_response_write",
  ] as const)(
    "retries exactly once after a real service SIGKILL at %s",
    async (crashPoint) => {
      const address = "127.0.0.1:50064";
      const definition = protoLoader.loadSync(
        join(process.cwd(), "packages/contracts/proto/event_store.proto"),
        { keepCase: true, longs: String, enums: String, defaults: false },
      );
      const service = (
        grpc.loadPackageDefinition(definition) as unknown as {
          eventstore: {
            v1: { EventStoreService: grpc.ServiceClientConstructor };
          };
        }
      ).eventstore.v1.EventStoreService;
      const startChild = (crashPoint?: string): ChildProcess =>
        spawn(
          process.execPath,
          [join(process.cwd(), "apps/event-store-service/dist/index.js")],
          {
            env: {
              ...process.env,
              NODE_ENV: "test",
              GRPC_LISTEN_ADDRESS: address,
              HTTP_LISTEN_ADDRESS: undefined,
              ...(crashPoint === undefined
                ? {}
                : { EVENT_STORE_TEST_CRASH_POINT: crashPoint }),
            },
            stdio: "ignore",
          },
        );
      const call = (
        appendClient: AppendClient,
        request: Record<string, unknown>,
      ) =>
        new Promise<Record<string, unknown>>((resolve, reject) =>
          appendClient.AppendToStream(request, (error, response) =>
            error === null ? resolve(response ?? {}) : reject(error),
          ),
        );
      const requestId = uuidv7();
      const request = {
        request_id: requestId,
        namespace: "orders",
        aggregate_type: "Order",
        aggregate_id: uuidv7(),
        expected_revision: { no_stream: {} },
        context: {
          correlation_id: uuidv7(),
          actor_json: Buffer.from(
            JSON.stringify({ kind: "user", subjectRef: "usr_process_crash" }),
          ),
        },
        events: [
          {
            event_name: "order.created",
            schema_version: 1,
            occurred_at: "2026-08-04T10:12:18.120Z",
            payload_json: Buffer.from(JSON.stringify({ orderRef: "process" })),
          },
        ],
      };
      let child = startChild(crashPoint);
      let appendClient = new service(
        address,
        grpc.credentials.createInsecure(),
      ) as AppendClient;
      try {
        await new Promise<void>((resolve, reject) =>
          appendClient.waitForReady(Date.now() + 30_000, (error) =>
            error === null || error === undefined ? resolve() : reject(error),
          ),
        );
        const exited = once(child, "exit");
        await expect(call(appendClient, request)).rejects.toBeDefined();
        const [code, signal] = await exited;
        expect(code).toBeNull();
        expect(signal).toBe("SIGKILL");
        appendClient.close();
        child = startChild();
        appendClient = new service(
          address,
          grpc.credentials.createInsecure(),
        ) as AppendClient;
        await new Promise<void>((resolve, reject) =>
          appendClient.waitForReady(Date.now() + 30_000, (error) =>
            error === null || error === undefined ? resolve() : reject(error),
          ),
        );
        await expect(call(appendClient, request)).resolves.toMatchObject({
          request_id: requestId,
        });
        const pool = await stack.pool();
        try {
          await expect(
            pool.query<{ count: number }>(
              "SELECT count(*)::int AS count FROM event_store.events WHERE request_id=$1",
              [requestId],
            ),
          ).resolves.toMatchObject({ rows: [{ count: 1 }] });
        } finally {
          await pool.end();
        }
      } finally {
        appendClient.close();
        if (child.exitCode === null) {
          const exited = once(child, "exit");
          child.kill("SIGKILL");
          await exited;
        }
      }
    },
    90_000,
  );

  it("rolls back when the service dies while append_v1 holds the stream lock", async () => {
    const address = "127.0.0.1:50065";
    const pool = await stack.pool();
    const holder = await pool.connect();
    const barrierClass = 71_029;
    const barrierKey = 1;
    const definition = protoLoader.loadSync(
      join(process.cwd(), "packages/contracts/proto/event_store.proto"),
      { keepCase: true, longs: String, enums: String, defaults: false },
    );
    const service = (
      grpc.loadPackageDefinition(definition) as unknown as {
        eventstore: {
          v1: { EventStoreService: grpc.ServiceClientConstructor };
        };
      }
    ).eventstore.v1.EventStoreService;
    const requestId = uuidv7();
    const request = {
      request_id: requestId,
      namespace: "orders",
      aggregate_type: "Order",
      aggregate_id: uuidv7(),
      expected_revision: { no_stream: {} },
      context: {
        correlation_id: uuidv7(),
        actor_json: Buffer.from(
          JSON.stringify({ kind: "user", subjectRef: "usr_stream_lock" }),
        ),
      },
      events: [
        {
          event_name: "order.created",
          schema_version: 1,
          occurred_at: "2026-08-04T10:12:18.120Z",
          payload_json: Buffer.from(JSON.stringify({ orderRef: "lock" })),
        },
      ],
    };
    const call = (appendClient: AppendClient) =>
      new Promise<Record<string, unknown>>((resolve, reject) =>
        appendClient.AppendToStream(request, (error, response) =>
          error === null ? resolve(response ?? {}) : reject(error),
        ),
      );
    let child: ChildProcess | undefined;
    let appendClient: AppendClient | undefined;
    try {
      await holder.query("SELECT pg_advisory_lock($1,$2)", [
        barrierClass,
        barrierKey,
      ]);
      await pool.query(`
        CREATE FUNCTION event_store.test_hold_stream_lock() RETURNS trigger
        LANGUAGE plpgsql AS $$
        BEGIN
          PERFORM pg_advisory_xact_lock(${barrierClass}, ${barrierKey});
          RETURN NEW;
        END $$;
        CREATE TRIGGER test_hold_stream_lock
        BEFORE INSERT ON event_store.events
        FOR EACH ROW EXECUTE FUNCTION event_store.test_hold_stream_lock();
      `);
      child = spawn(
        process.execPath,
        [join(process.cwd(), "apps/event-store-service/dist/index.js")],
        {
          env: {
            ...process.env,
            GRPC_LISTEN_ADDRESS: address,
            HTTP_LISTEN_ADDRESS: undefined,
          },
          stdio: "ignore",
        },
      );
      appendClient = new service(
        address,
        grpc.credentials.createInsecure(),
      ) as AppendClient;
      await new Promise<void>((resolve, reject) =>
        appendClient!.waitForReady(Date.now() + 30_000, (error) =>
          error === null || error === undefined ? resolve() : reject(error),
        ),
      );
      const pending = call(appendClient).then(
        () => "acknowledged" as const,
        () => "connection_dropped" as const,
      );
      const deadline = Date.now() + 15_000;
      while (Date.now() < deadline) {
        const waiting = await pool.query<{ count: number }>(
          `SELECT count(*)::int AS count FROM pg_stat_activity
             WHERE datname=current_database()
               AND wait_event_type='Lock' AND wait_event='advisory'
               AND query LIKE '%append_v1%'`,
        );
        if ((waiting.rows[0]?.count ?? 0) > 0) break;
        await new Promise((resolve) => setTimeout(resolve, 50));
      }
      await expect(
        pool.query<{ count: number }>(
          "SELECT count(*)::int AS count FROM pg_stat_activity WHERE datname=current_database() AND wait_event_type='Lock' AND wait_event='advisory' AND query LIKE '%append_v1%'",
        ),
      ).resolves.toMatchObject({ rows: [{ count: 1 }] });
      const exited = once(child, "exit");
      child.kill("SIGKILL");
      await exited;
      await holder.query("SELECT pg_advisory_unlock($1,$2)", [
        barrierClass,
        barrierKey,
      ]);
      await expect(pending).resolves.toBe("connection_dropped");
      await expect(
        pool.query<{ count: number }>(
          "SELECT count(*)::int AS count FROM event_store.events WHERE request_id=$1",
          [requestId],
        ),
      ).resolves.toMatchObject({ rows: [{ count: 0 }] });
      child = spawn(
        process.execPath,
        [join(process.cwd(), "apps/event-store-service/dist/index.js")],
        {
          env: {
            ...process.env,
            GRPC_LISTEN_ADDRESS: address,
            HTTP_LISTEN_ADDRESS: undefined,
          },
          stdio: "ignore",
        },
      );
      appendClient.close();
      appendClient = new service(
        address,
        grpc.credentials.createInsecure(),
      ) as AppendClient;
      await new Promise<void>((resolve, reject) =>
        appendClient!.waitForReady(Date.now() + 30_000, (error) =>
          error === null || error === undefined ? resolve() : reject(error),
        ),
      );
      await expect(call(appendClient)).resolves.toMatchObject({
        request_id: requestId,
      });
    } finally {
      appendClient?.close();
      if (child?.exitCode === null) {
        const exited = once(child, "exit");
        child.kill("SIGKILL");
        await exited;
      }
      await holder
        .query("SELECT pg_advisory_unlock($1,$2)", [barrierClass, barrierKey])
        .catch(() => undefined);
      await pool
        .query(
          "DROP TRIGGER IF EXISTS test_hold_stream_lock ON event_store.events; DROP FUNCTION IF EXISTS event_store.test_hold_stream_lock()",
        )
        .catch(() => undefined);
      holder.release();
      await pool.end();
    }
  }, 90_000);

  it("durably closes appends after a Connect crash and reopens only after delivery recovers", async () => {
    await stack.stopConnect();
    const pool = await stack.pool();
    const directRequestId = uuidv7();
    const grpcRequestId = uuidv7();
    const kafka = new KafkaJS.Kafka({
      kafkaJS: { brokers: [stack.kafkaBroker()] },
    });
    const consumer = kafka.consumer({
      kafkaJS: {
        groupId: `connect-crash-${uuidv7()}`,
        autoCommit: false,
        fromBeginning: true,
      },
    });
    try {
      const deadline = Date.now() + 30_000;
      while (Date.now() < deadline) {
        const slot = await pool.query<{ active: boolean }>(
          "SELECT active FROM pg_replication_slots WHERE slot_name='event_store_live'",
        );
        if (slot.rows[0]?.active === false) break;
        await new Promise((resolve) => setTimeout(resolve, 100));
      }
      const admissionDeadline = Date.now() + 30_000;
      while (Date.now() < admissionDeadline) {
        const admission = await pool.query<{ cdc_delivery_healthy: boolean }>(
          "SELECT cdc_delivery_healthy FROM event_store.runtime_config WHERE singleton",
        );
        if (admission.rows[0]?.cdc_delivery_healthy === false) break;
        await new Promise((resolve) => setTimeout(resolve, 100));
      }
      await expect(
        pool.query<{ cdc_delivery_healthy: boolean }>(
          "SELECT cdc_delivery_healthy FROM event_store.runtime_config WHERE singleton",
        ),
      ).resolves.toMatchObject({ rows: [{ cdc_delivery_healthy: false }] });
      await consumer.connect();
      await consumer.subscribe({
        topics: ["event-store.events.v1"],
        replace: true,
      });
      const delivered = new Promise<void>((resolve, reject) => {
        const seen = new Set<string>();
        const timeout = setTimeout(
          () =>
            reject(
              new Error(
                "acknowledged events were not delivered after Connect restart",
              ),
            ),
          30_000,
        );
        void consumer.run({
          eachMessage: async ({ message }) => {
            const event = JSON.parse(message.value?.toString() ?? "{}") as {
              context?: { requestId?: string };
            };
            const requestId = event.context?.requestId;
            if (requestId === directRequestId || requestId === grpcRequestId)
              seen.add(requestId);
            if (seen.size === 2) {
              clearTimeout(timeout);
              resolve();
            }
          },
        });
      });
      const direct = await pool.connect();
      try {
        await direct.query("SET ROLE event_store_app");
        await expect(
          direct.query(
            "SELECT event_store.append_v1($1,$2,$3,$4,$5,$6,$7,$8::jsonb,$9::jsonb)",
            [
              "orders-command",
              "orders",
              "Order",
              uuidv7(),
              directRequestId,
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
                correlationId: uuidv7(),
                causationId: null,
                actor: { kind: "user", subjectRef: "usr_1" },
              }),
            ],
          ),
        ).rejects.toMatchObject({ code: "P0001" });
      } finally {
        await direct.query("RESET ROLE").catch(() => undefined);
        direct.release();
      }
      const appendGrpc = () =>
        new Promise<Record<string, unknown>>((resolve, reject) =>
          client.AppendToStream(
            {
              request_id: grpcRequestId,
              namespace: "orders",
              aggregate_type: "Order",
              aggregate_id: uuidv7(),
              expected_revision: { no_stream: {} },
              context: {
                correlation_id: uuidv7(),
                actor_json: Buffer.from(
                  JSON.stringify({ kind: "user", subjectRef: "usr_1" }),
                ),
              },
              events: [
                {
                  event_name: "order.created",
                  schema_version: 1,
                  occurred_at: "2026-08-04T10:12:18.120Z",
                  payload_json: Buffer.from(JSON.stringify({ orderRef: "o2" })),
                },
              ],
            },
            (error, value) =>
              error === null ? resolve(value ?? {}) : reject(error),
          ),
        );
      await expect(appendGrpc()).rejects.toMatchObject({
        code: grpc.status.RESOURCE_EXHAUSTED,
      });
      await stack.restartConnect();
      const readyDeadline = Date.now() + 30_000;
      while (Date.now() < readyDeadline) {
        const readiness = await fetch("http://127.0.0.1:50161/readyz");
        if (readiness.status === 200) break;
        await new Promise((resolve) => setTimeout(resolve, 100));
      }
      await expect(
        fetch("http://127.0.0.1:50161/readyz"),
      ).resolves.toMatchObject({
        status: 200,
      });
      const directAfterRecovery = await pool.connect();
      try {
        await directAfterRecovery.query("SET ROLE event_store_app");
        await expect(
          directAfterRecovery.query(
            "SELECT event_store.append_v1($1,$2,$3,$4,$5,$6,$7,$8::jsonb,$9::jsonb)",
            [
              "orders-command",
              "orders",
              "Order",
              uuidv7(),
              directRequestId,
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
                correlationId: uuidv7(),
                causationId: null,
                actor: { kind: "user", subjectRef: "usr_1" },
              }),
            ],
          ),
        ).resolves.toBeDefined();
      } finally {
        await directAfterRecovery.query("RESET ROLE").catch(() => undefined);
        directAfterRecovery.release();
      }
      const response = await appendGrpc();
      expect(response).toBeDefined();
      await expect(delivered).resolves.toBeUndefined();
    } finally {
      await consumer.disconnect().catch(() => undefined);
      await pool.end();
    }
  }, 60_000);
});
