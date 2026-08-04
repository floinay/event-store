import { afterAll, beforeAll, describe, expect, it } from "vitest";
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
    await stack.start({ cdc: true, toxiproxy: true });
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
    await stack.setPostgresConnectEnabled(false);
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
    await stack.setPostgresConnectEnabled(true);
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
  });

  it("recovers an idempotent append after a crash boundary past PostgreSQL commit", async () => {
    let crashOnce = true;
    const previousAddress = process.env.GRPC_LISTEN_ADDRESS;
    const previousHealthAddress = process.env.HTTP_LISTEN_ADDRESS;
    process.env.GRPC_LISTEN_ADDRESS = "127.0.0.1:50063";
    delete process.env.HTTP_LISTEN_ADDRESS;
    const crashServer = await startServer({
      hit: (point) => {
        if (point !== "after_postgres_commit" || !crashOnce) return;
        crashOnce = false;
        throw Object.assign(new Error("test crash after PostgreSQL commit"), {
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
      await expect(append()).resolves.toMatchObject({ request_id: requestId });
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
  }, 60_000);

  it("keeps a bounded durable append window after a Connect process crash", async () => {
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
      await expect(
        fetch("http://127.0.0.1:50161/readyz"),
      ).resolves.toMatchObject({
        status: 503,
      });
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
        ).resolves.toBeDefined();
      } finally {
        await direct.query("RESET ROLE").catch(() => undefined);
        direct.release();
      }
      const response = await new Promise<Record<string, unknown>>(
        (resolve, reject) =>
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
      expect(response).toBeDefined();
      await stack.restartConnect();
      await expect(delivered).resolves.toBeUndefined();
    } finally {
      await consumer.disconnect().catch(() => undefined);
      await pool.end();
    }
  }, 60_000);
});
