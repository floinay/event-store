import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { KafkaJS } from "@confluentinc/kafka-javascript";
import * as grpc from "@grpc/grpc-js";
import * as protoLoader from "@grpc/proto-loader";
import { spawn, type ChildProcess } from "node:child_process";
import { once } from "node:events";
import { join } from "node:path";
import { uuidv7 } from "@event-store/contracts";
import { EventStoreStack } from "../fixtures/event-store-stack.js";
import type { Pool } from "pg";

const suite = process.env.RUN_LATENCY === "true" ? describe : describe.skip;
const sampleCount = Number(process.env.LATENCY_SAMPLE_COUNT ?? 100);
const concurrency = Number(process.env.LATENCY_CONCURRENCY ?? 1);
const durationMs = Number(process.env.LATENCY_DURATION_MS ?? 0);
const maxClockSkewMs = Number(process.env.MAX_CLOCK_SKEW_MS ?? 2);
const appendRate = Number(process.env.LATENCY_APPEND_RATE ?? 0);
const batchSize = Number(process.env.LATENCY_BATCH_SIZE ?? 1);
const payloadBytes = Number(process.env.LATENCY_PAYLOAD_BYTES ?? 128);
const streamCount = Number(process.env.LATENCY_STREAM_COUNT ?? 1_000);
const hotStreamPercent = Number(process.env.LATENCY_HOT_STREAM_PERCENT ?? 0);
const hotStreamCount = Number(process.env.LATENCY_HOT_STREAM_COUNT ?? 10);
const producerCpuWorkMs = Number(process.env.LATENCY_PRODUCER_CPU_WORK_MS ?? 0);
const consumerCpuWorkMs = Number(process.env.LATENCY_CONSUMER_CPU_WORK_MS ?? 0);
const connectKafkaLatencyMs = Number(
  process.env.LATENCY_CONNECT_KAFKA_LATENCY_MS ?? 0,
);
const latencyTestTimeoutMs = Math.max(180_000, durationMs + 60_000);

interface AppendClient extends grpc.Client {
  AppendToStream(
    request: Record<string, unknown>,
    callback: (
      error: grpc.ServiceError | null,
      response?: {
        events?: { event_id?: string; eventId?: string }[];
      },
    ) => void,
  ): grpc.ClientUnaryCall;
}

interface StreamState {
  aggregateId: string;
  nextRevision: bigint;
  tail: Promise<void>;
}

function positiveInteger(name: string, value: number, minimum = 1): void {
  if (!Number.isInteger(value) || value < minimum)
    throw new Error(`${name} must be an integer of at least ${minimum}`);
}

function busyWork(milliseconds: number): void {
  if (milliseconds === 0) return;
  const deadline = performance.now() + milliseconds;
  while (performance.now() < deadline);
}

function delay(milliseconds: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, milliseconds));
}

function percentile(samples: readonly number[], quantile: number): number {
  return samples[
    Math.min(samples.length - 1, Math.ceil(samples.length * quantile) - 1)
  ]!;
}

suite("PostgreSQL commit to Kafka consumer latency", () => {
  const stack = new EventStoreStack();
  let serviceProcess: ChildProcess;
  let client: AppendClient;
  let pool: Pool;
  beforeAll(async () => {
    await stack.start({
      cdc: true,
      toxiproxy: connectKafkaLatencyMs > 0,
      connectKafkaProxy: connectKafkaLatencyMs > 0,
    });
    if (connectKafkaLatencyMs > 0)
      await stack.addConnectKafkaLatency(connectKafkaLatencyMs);
    pool = await stack.pool();
    process.env.DATABASE_URL = stack.databaseUrl;
    process.env.PRODUCER_SERVICE = "latency-probe";
    process.env.CDC_WAL_BUDGET_BYTES = String(8 * 1024 ** 3);
    process.env.GRPC_LISTEN_ADDRESS = "127.0.0.1:50062";
    process.env.GRPC_ALLOW_INSECURE = "true";
    serviceProcess = spawn(
      process.execPath,
      [join(process.cwd(), "apps/event-store-service/dist/index.js")],
      { env: process.env, stdio: "ignore" },
    );
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
    await new Promise<void>((resolve, reject) =>
      client.waitForReady(Date.now() + 30_000, (error) =>
        error === undefined || error === null ? resolve() : reject(error),
      ),
    );
  }, 180_000);
  afterAll(async () => {
    client?.close();
    if (serviceProcess?.exitCode === null) {
      serviceProcess.kill("SIGTERM");
      await once(serviceProcess, "exit");
    }
    await pool?.end();
    await stack.stop();
  }, 60_000);

  it(
    "meets the PostgreSQL-commit-to-consumer latency SLO",
    async () => {
      if (!Number.isInteger(sampleCount) || sampleCount < 100)
        throw new Error(
          "LATENCY_SAMPLE_COUNT must be an integer of at least 100",
        );
      if (!Number.isInteger(concurrency) || concurrency < 1)
        throw new Error("LATENCY_CONCURRENCY must be a positive integer");
      if (!Number.isFinite(durationMs) || durationMs < 0)
        throw new Error("LATENCY_DURATION_MS must be a non-negative number");
      if (!Number.isFinite(maxClockSkewMs) || maxClockSkewMs < 0)
        throw new Error("MAX_CLOCK_SKEW_MS must be a non-negative number");
      if (!Number.isFinite(appendRate) || appendRate < 0)
        throw new Error("LATENCY_APPEND_RATE must be a non-negative number");
      positiveInteger("LATENCY_BATCH_SIZE", batchSize);
      positiveInteger("LATENCY_PAYLOAD_BYTES", payloadBytes, 0);
      positiveInteger("LATENCY_STREAM_COUNT", streamCount);
      if (
        !Number.isFinite(hotStreamPercent) ||
        hotStreamPercent < 0 ||
        hotStreamPercent > 100
      )
        throw new Error("LATENCY_HOT_STREAM_PERCENT must be 0..100");
      positiveInteger("LATENCY_HOT_STREAM_COUNT", hotStreamCount);
      if (!Number.isFinite(producerCpuWorkMs) || producerCpuWorkMs < 0)
        throw new Error("LATENCY_PRODUCER_CPU_WORK_MS must be non-negative");
      if (!Number.isFinite(consumerCpuWorkMs) || consumerCpuWorkMs < 0)
        throw new Error("LATENCY_CONSUMER_CPU_WORK_MS must be non-negative");
      if (!Number.isFinite(connectKafkaLatencyMs) || connectKafkaLatencyMs < 0)
        throw new Error(
          "LATENCY_CONNECT_KAFKA_LATENCY_MS must be non-negative",
        );
      if (process.env.RUN_RELEASE_LATENCY === "true" && durationMs < 1_800_000)
        throw new Error("release latency profile requires at least 30 minutes");
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
          readUncommitted: false,
        },
      });
      await consumer.connect();
      await consumer.subscribe({
        topics: ["event-store.events.v1"],
        replace: true,
      });
      const clockOffsetMs = await calibrateClockOffset(pool, maxClockSkewMs);
      let warmupEventId: string | undefined;
      let appendsFinished = false;
      let warmupObserved!: () => void;
      const warmup = new Promise<void>((resolve) => {
        warmupObserved = resolve;
      });
      let received!: () => void;
      const samplesReceived = new Promise<void>((resolve, reject) => {
        received = resolve;
        const deadline = setTimeout(
          () => reject(new Error("timed out waiting for latency samples")),
          Math.max(30_000, durationMs + 30_000),
        );
        void consumer.run({
          eachMessage: async ({ message }) => {
            // Take this span before JSON parsing, schema validation or a handler.
            const receivedAt = Date.now();
            const eventId = headerText(message.headers?.id);
            if (eventId === undefined) return;
            observed.set(eventId, receivedAt);
            if (eventId === warmupEventId) warmupObserved();
            busyWork(consumerCpuWorkMs);
            if (appendsFinished && allSamplesReceived(committed, observed)) {
              clearTimeout(deadline);
              received();
            }
          },
        });
      });
      warmupEventId = (
        await append(client, uuidv7(), uuidv7(), 0n, 1, 0, "warmup")
      ).eventIds[0];
      if (warmupEventId === undefined)
        throw new Error("warmup append omitted its event ID");
      if (observed.has(warmupEventId)) warmupObserved();
      await warmup;
      const streams: StreamState[] = Array.from(
        { length: streamCount },
        () => ({
          aggregateId: uuidv7(),
          nextRevision: 0n,
          tail: Promise.resolve(),
        }),
      );
      const deadline = performance.now() + durationMs;
      let index = 0;
      let nextRateSlot = performance.now();
      const appendOne = async (): Promise<void> => {
        const current = index++;
        if (appendRate > 0) {
          const slot = nextRateSlot;
          nextRateSlot =
            Math.max(nextRateSlot, performance.now()) + 1_000 / appendRate;
          await delay(Math.max(0, slot - performance.now()));
        }
        const useHot = current % 100 < hotStreamPercent;
        const limit = useHot
          ? Math.min(hotStreamCount, streams.length)
          : streams.length;
        const stream = streams[current % limit]!;
        const previous = stream.tail;
        let release!: () => void;
        stream.tail = new Promise<void>((resolve) => {
          release = resolve;
        });
        await previous;
        try {
          busyWork(producerCpuWorkMs);
          const result = await append(
            client,
            uuidv7(),
            stream.aggregateId,
            stream.nextRevision,
            batchSize,
            payloadBytes,
            String(current),
          );
          stream.nextRevision += BigInt(batchSize);
          for (const eventId of result.eventIds)
            committed.set(eventId, result.commitEpochMs);
        } finally {
          release();
        }
      };
      const worker = async (): Promise<void> => {
        while (committed.size < sampleCount || performance.now() < deadline)
          await appendOne();
      };
      await Promise.all(Array.from({ length: concurrency }, worker));
      appendsFinished = true;
      if (allSamplesReceived(committed, observed)) received();
      await samplesReceived;
      await consumer.disconnect();
      const samples = [...committed]
        .map(
          ([eventId, committedAt]) =>
            observed.get(eventId)! - committedAt - clockOffsetMs,
        )
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
      expect(metrics.samples).toBeGreaterThanOrEqual(sampleCount);
      expect(samples.every((sample) => sample >= 0)).toBe(true);
      expect(samples.every((sample) => sample <= 50)).toBe(true);
      expect(metrics.p50).toBeLessThanOrEqual(50);
      expect(metrics.p95).toBeLessThanOrEqual(50);
      expect(metrics.p99).toBeLessThanOrEqual(50);
      expect(metrics.p999).toBeLessThanOrEqual(50);
      expect(metrics.p99).toBeGreaterThanOrEqual(metrics.p95);
      expect(metrics.p999).toBeGreaterThanOrEqual(metrics.p99);
    },
    latencyTestTimeoutMs,
  );
});

function allSamplesReceived(
  committed: ReadonlyMap<string, number>,
  observed: ReadonlyMap<string, number>,
): boolean {
  return (
    committed.size >= sampleCount &&
    [...committed.keys()].every(
      (requestId) =>
        observed.has(requestId) && Number.isFinite(committed.get(requestId)),
    )
  );
}

async function calibrateClockOffset(
  pool: Pool,
  maxClockSkewMs: number,
): Promise<number> {
  const probes = await Promise.all(
    Array.from({ length: 7 }, async () => {
      const started = Date.now();
      const result = await pool.query<{ epoch_ms: string }>(
        "SELECT (extract(epoch FROM clock_timestamp()) * 1000)::text AS epoch_ms",
      );
      const finished = Date.now();
      const databaseEpochMs = Number(result.rows[0]?.epoch_ms);
      if (!Number.isFinite(databaseEpochMs))
        throw new Error("PostgreSQL did not return a measurable wall clock");
      return {
        roundTripMs: finished - started,
        offsetMs: (started + finished) / 2 - databaseEpochMs,
      };
    }),
  );
  const offsets = probes.map((probe) => probe.offsetMs).sort((a, b) => a - b);
  const spread = offsets.at(-1)! - offsets[0]!;
  expect(spread).toBeLessThanOrEqual(maxClockSkewMs * 2 + 2);
  return probes.sort((left, right) => left.roundTripMs - right.roundTripMs)[0]!
    .offsetMs;
}

function append(
  client: AppendClient,
  requestId: string,
  aggregateId: string,
  currentRevision: bigint,
  eventsPerAppend: number,
  bytesPerPayload: number,
  index: string,
): Promise<{ eventIds: string[]; commitEpochMs: number }> {
  return new Promise((resolve, reject) => {
    let commitEpochMs: number | undefined;
    const call = client.AppendToStream(
      {
        request_id: requestId,
        namespace: "latency",
        aggregate_type: "Probe",
        aggregate_id: aggregateId,
        expected_revision:
          currentRevision === 0n
            ? { no_stream: {} }
            : { exact_revision: currentRevision.toString() },
        context: {
          correlation_id: uuidv7(),
          actor_json: Buffer.from(
            JSON.stringify({ kind: "service", subjectRef: "latency-probe" }),
          ),
        },
        events: Array.from({ length: eventsPerAppend }, (_, eventIndex) => ({
          event_name: "probe.appended",
          schema_version: 1,
          occurred_at: new Date().toISOString(),
          payload_json: Buffer.from(
            JSON.stringify({
              index: `${index}:${eventIndex}`,
              data: "x".repeat(bytesPerPayload),
            }),
          ),
        })),
      },
      (error, response) => {
        if (error !== null) {
          reject(error);
          return;
        }
        const eventIds = (response?.events ?? []).map(
          (event) => event.event_id ?? event.eventId,
        );
        if (
          eventIds.length !== eventsPerAppend ||
          eventIds.some((eventId) => eventId === undefined) ||
          commitEpochMs === undefined
        ) {
          reject(
            new Error(
              `append acknowledgement omitted event ID or commit span: ${JSON.stringify(response)}`,
            ),
          );
          return;
        }
        resolve({ eventIds: eventIds as string[], commitEpochMs });
      },
    );
    call.once("metadata", (metadata) => {
      const value = metadata.get("x-event-store-commit-epoch-ms")[0];
      const parsed = Number(value);
      if (Number.isFinite(parsed)) commitEpochMs = parsed;
    });
  });
}

function headerText(
  value: Buffer | Buffer[] | string | string[] | undefined,
): string | undefined {
  const first = Array.isArray(value) ? value[0] : value;
  return first === undefined ? undefined : first.toString();
}
