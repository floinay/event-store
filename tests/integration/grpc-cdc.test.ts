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
    await stack.start({ cdc: true });
    process.env.DATABASE_URL = stack.databaseUrl;
    process.env.PRODUCER_SERVICE = "orders-command";
    process.env.GRPC_LISTEN_ADDRESS = "127.0.0.1:50061";
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
    expect(response.current_revision ?? response.currentRevision).toBe("1");
    await expect(received).resolves.toMatchObject({
      aggregateId,
      eventName: "order.created",
    });
    await consumer.disconnect();
  }, 60_000);
});
