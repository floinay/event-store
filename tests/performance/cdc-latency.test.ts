import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { KafkaJS } from "@confluentinc/kafka-javascript";
import * as grpc from "@grpc/grpc-js";
import * as protoLoader from "@grpc/proto-loader";
import { join } from "node:path";
import { uuidv7 } from "@event-store/contracts";
import { startServer } from "../../apps/event-store-service/dist/index.js";
import { EventStoreStack } from "../fixtures/event-store-stack.js";

const suite = process.env.RUN_LATENCY === "true" ? describe : describe.skip;
const sampleCount = Number(process.env.LATENCY_SAMPLE_COUNT ?? 100);

interface AppendClient extends grpc.Client {
  AppendToStream(
    request: Record<string, unknown>,
    callback: (error: grpc.ServiceError | null) => void,
  ): grpc.ClientUnaryCall;
}

function percentile(samples: readonly number[], quantile: number): number {
  return samples[
    Math.min(samples.length - 1, Math.ceil(samples.length * quantile) - 1)
  ]!;
}

suite("PostgreSQL commit to Kafka consumer latency", () => {
  const stack = new EventStoreStack();
  let server: grpc.Server;
  let client: AppendClient;
  beforeAll(async () => {
    await stack.start({ cdc: true });
    process.env.DATABASE_URL = stack.databaseUrl;
    process.env.PRODUCER_SERVICE = "latency-probe";
    process.env.CDC_WAL_BUDGET_BYTES = String(8 * 1024 ** 3);
    process.env.GRPC_LISTEN_ADDRESS = "127.0.0.1:50062";
    process.env.GRPC_ALLOW_INSECURE = "true";
    server = await startServer();
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
    client = new service(
      "127.0.0.1:50062",
      grpc.credentials.createInsecure(),
    ) as AppendClient;
  }, 180_000);
  afterAll(async () => {
    client?.close();
    await new Promise<void>((resolve) => server?.tryShutdown(() => resolve()));
    await stack.stop();
  }, 60_000);

  it("meets the PostgreSQL-commit-to-consumer latency SLO", async () => {
    if (!Number.isInteger(sampleCount) || sampleCount < 100)
      throw new Error(
        "LATENCY_SAMPLE_COUNT must be an integer of at least 100",
      );
    const committed = new Map<string, number>();
    const observed = new Map<string, number>();
    const kafka = new KafkaJS.Kafka({
      kafkaJS: { brokers: [stack.kafkaBroker()] },
    });
    const consumer = kafka.consumer({
      kafkaJS: {
        groupId: `latency-${uuidv7()}`,
        autoCommit: false,
        fromBeginning: true,
      },
    });
    await consumer.connect();
    await consumer.subscribe({
      topics: ["event-store.events.v1"],
      replace: true,
    });
    const warmupRequestId = uuidv7();
    let warmupObserved!: () => void;
    const warmup = new Promise<void>((resolve) => {
      warmupObserved = resolve;
    });
    let received!: () => void;
    const samplesReceived = new Promise<void>((resolve, reject) => {
      received = resolve;
      const deadline = setTimeout(
        () => reject(new Error("timed out waiting for latency samples")),
        30_000,
      );
      void consumer.run({
        eachMessage: async ({ message }) => {
          const event = JSON.parse(message.value?.toString() ?? "{}") as {
            context?: { requestId?: string };
          };
          if (event.context?.requestId === warmupRequestId) warmupObserved();
          else if (
            event.context?.requestId !== undefined &&
            committed.has(event.context.requestId)
          )
            observed.set(event.context.requestId, performance.now());
          if (
            committed.size === sampleCount &&
            [...committed.keys()].every(
              (requestId) =>
                observed.has(requestId) &&
                Number.isFinite(committed.get(requestId)),
            )
          ) {
            clearTimeout(deadline);
            received();
          }
        },
      });
    });
    await append(client, warmupRequestId, uuidv7(), "warmup");
    await warmup;
    for (let index = 0; index < sampleCount; index += 1) {
      const requestId = uuidv7();
      const aggregateId = uuidv7();
      // Register before append so a very fast consumer cannot drop the sample.
      committed.set(requestId, Number.NaN);
      const committedAt = await append(
        client,
        requestId,
        aggregateId,
        String(index),
      );
      committed.set(requestId, committedAt);
      if (
        committed.size === sampleCount &&
        [...committed.keys()].every(
          (id) => observed.has(id) && Number.isFinite(committed.get(id)),
        )
      )
        received();
    }
    await samplesReceived;
    await consumer.disconnect();
    const samples = [...committed]
      .map(([eventId, committedAt]) => observed.get(eventId)! - committedAt)
      .sort((left, right) => left - right);
    const metrics = {
      samples: samples.length,
      p50: percentile(samples, 0.5),
      p95: percentile(samples, 0.95),
      p99: percentile(samples, 0.99),
      p999: percentile(samples, 0.999),
      mean:
        samples.reduce((total, sample) => total + sample, 0) / samples.length,
    };
    console.info(`CDC latency metrics: ${JSON.stringify(metrics)}`);
    expect(metrics.samples).toBe(sampleCount);
    expect(samples.every((sample) => sample >= 0)).toBe(true);
    expect(samples.every((sample) => sample <= 50)).toBe(true);
    expect(metrics.p50).toBeLessThanOrEqual(50);
    expect(metrics.p95).toBeLessThanOrEqual(50);
    expect(metrics.p99).toBeLessThanOrEqual(50);
    expect(metrics.p999).toBeLessThanOrEqual(50);
    expect(metrics.p99).toBeGreaterThanOrEqual(metrics.p95);
    expect(metrics.p999).toBeGreaterThanOrEqual(metrics.p99);
  }, 180_000);
});

function append(
  client: AppendClient,
  requestId: string,
  aggregateId: string,
  index: string,
): Promise<number> {
  return new Promise((resolve, reject) => {
    let committedAt: number | undefined;
    let metadataError: Error | undefined;
    const call = client.AppendToStream(
      {
        request_id: requestId,
        namespace: "latency",
        aggregate_type: "Probe",
        aggregate_id: aggregateId,
        expected_revision: { no_stream: {} },
        context: {
          correlation_id: uuidv7(),
          actor_json: Buffer.from(
            JSON.stringify({ kind: "service", subjectRef: "latency-probe" }),
          ),
        },
        events: [
          {
            event_name: "probe.appended",
            schema_version: 1,
            occurred_at: new Date().toISOString(),
            payload_json: Buffer.from(JSON.stringify({ index })),
          },
        ],
      },
      (error) => {
        if (error !== null) {
          reject(error);
          return;
        }
        if (metadataError !== undefined) {
          reject(metadataError);
          return;
        }
        if (committedAt === undefined) {
          reject(new Error("append acknowledgement omitted SQL commit span"));
          return;
        }
        resolve(committedAt);
      },
    );
    call.once("metadata", (metadata) => {
      const value = metadata.get("x-event-store-commit-monotonic-ms")[0];
      const parsed = Number(value);
      if (!Number.isFinite(parsed))
        metadataError = new Error("invalid SQL commit monotonic span metadata");
      else committedAt = parsed;
    });
  });
}
