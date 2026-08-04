import { join } from "node:path";
import { KafkaJS } from "@confluentinc/kafka-javascript";

export function connectQueueRatio(
  metrics: string,
  connectorName: string,
): number {
  const metric = (name: string): number => {
    const line = metrics
      .split("\n")
      .filter(
        (entry) =>
          (entry.startsWith(`${name}{`) || entry.startsWith(`${name} `)) &&
          entry.includes(`connector="${connectorName}"`),
      );
    if (line.length !== 1)
      throw new Error(`missing or ambiguous Connect metric ${name}`);
    const value = Number(line[0]!.trim().split(/\s+/).at(-1));
    if (!Number.isFinite(value) || value < 0)
      throw new Error(`invalid Connect metric ${name}`);
    return value;
  };
  const remaining = metric("event_store_connect_queue_remaining_capacity");
  const total = metric("event_store_connect_queue_total_capacity");
  if (total <= 0) throw new Error("invalid Connect queue total capacity");
  return remaining / total;
}

/** Critical WAL headroom is reserved for explicitly trusted mTLS callers. */
export function trafficClassForClient(
  requestedCritical: boolean,
  peerCommonName: string | undefined,
  trustedCriticalSubjects: readonly string[],
): "standard" | "critical" {
  if (!requestedCritical) return "standard";
  if (
    peerCommonName === undefined ||
    !trustedCriticalSubjects.includes(peerCommonName)
  )
    throw Object.assign(
      new Error("critical appends require a trusted mTLS client identity"),
      { code: grpc.status.PERMISSION_DENIED },
    );
  return "critical";
}

export async function verifyKafkaReadiness(
  brokers: readonly string[],
  topicName: string,
  minInSyncReplicas: number,
): Promise<void> {
  if (brokers.length === 0 || brokers.some((broker) => broker === ""))
    throw new Error("KAFKA_BROKERS must contain at least one broker");
  if (!Number.isInteger(minInSyncReplicas) || minInSyncReplicas < 1)
    throw new Error("KAFKA_LIVE_MIN_ISR must be a positive integer");
  const admin = new KafkaJS.Kafka({
    kafkaJS: { brokers: [...brokers] },
  }).admin();
  await admin.connect();
  try {
    const metadata = await admin.fetchTopicMetadata({ topics: [topicName] });
    const topic = metadata.find((entry) => entry.name === topicName);
    if (topic === undefined || topic.partitions.length === 0)
      throw new Error(`Kafka live topic ${topicName} is unavailable`);
    for (const partition of topic.partitions) {
      if (partition.isr.length < minInSyncReplicas)
        throw new Error(
          `Kafka live topic ${topicName}/${partition.partitionId} has ISR=${partition.isr.length}; requires ${minInSyncReplicas}`,
        );
    }
  } finally {
    await admin.disconnect();
  }
}
import { fileURLToPath } from "node:url";
import { createServer } from "node:http";
import * as grpc from "@grpc/grpc-js";
import * as protoLoader from "@grpc/proto-loader";
import { Pool } from "pg";
import {
  EventContextSchema,
  UuidV7,
  assertNoDirectPii,
  uuidv7,
} from "@event-store/contracts";
import { ZodError } from "zod";
import {
  PostgresEventStore,
  type ExpectedRevision,
} from "@event-store/postgres-store";

const cdcLatencyBucketsSeconds = [0.01, 0.025, 0.05, 0.1, 0.2] as const;

/** Prometheus histogram from SQL completion to consumer receipt. */
export class CommitToConsumerLatencyHistogram {
  private count = 0;
  private sumSeconds = 0;
  private readonly bucketCounts = new Map<number, number>(
    cdcLatencyBucketsSeconds.map((bucket) => [bucket, 0]),
  );

  observe(milliseconds: number): void {
    if (!Number.isFinite(milliseconds) || milliseconds < 0) return;
    this.count += 1;
    this.sumSeconds += milliseconds / 1_000;
    for (const bucket of cdcLatencyBucketsSeconds)
      if (milliseconds / 1_000 <= bucket)
        this.bucketCounts.set(bucket, (this.bucketCounts.get(bucket) ?? 0) + 1);
  }

  prometheus(): string {
    let output =
      "# TYPE event_store_cdc_commit_to_consumer_latency_seconds histogram\n";
    for (const bucket of cdcLatencyBucketsSeconds)
      output += `event_store_cdc_commit_to_consumer_latency_seconds_bucket{le="${bucket}"} ${this.bucketCounts.get(bucket) ?? 0}\n`;
    output +=
      `event_store_cdc_commit_to_consumer_latency_seconds_bucket{le="+Inf"} ${this.count}\n` +
      `event_store_cdc_commit_to_consumer_latency_seconds_sum ${this.sumSeconds}\n` +
      `event_store_cdc_commit_to_consumer_latency_seconds_count ${this.count}\n`;
    return output;
  }
}

interface CdcLatencyProbe {
  up: () => boolean;
  stop: () => Promise<void>;
}

async function startCdcLatencyProbe(
  store: PostgresEventStore,
  brokers: readonly string[],
  topic: string,
  intervalMs: number,
  histogram: CommitToConsumerLatencyHistogram,
): Promise<CdcLatencyProbe> {
  if (!Number.isInteger(intervalMs) || intervalMs < 1_000)
    throw new Error(
      "CDC_LATENCY_PROBE_INTERVAL_MS must be an integer of at least 1000",
    );
  const kafka = new KafkaJS.Kafka({ kafkaJS: { brokers: [...brokers] } });
  const consumer = kafka.consumer({
    kafkaJS: {
      groupId: `event-store-latency-probe-${process.env.HOSTNAME ?? process.pid}`,
      autoCommit: true,
      fromBeginning: false,
      readUncommitted: false,
      allowAutoTopicCreation: false,
    },
  });
  const pending = new Map<string, number>();
  let connected = false;
  let lastMeasurementEpochMs: number | undefined;
  let lastFailureEpochMs: number | undefined;
  const staleAfterMs = intervalMs * 3;
  await consumer.connect();
  await consumer.subscribe({ topics: [topic], replace: true });
  await consumer.run({
    eachMessage: async ({ message }) => {
      // The receive span deliberately precedes JSON validation and handlers.
      const receivedEpochMs = Date.now();
      const id = message.headers?.id;
      const eventId = (Array.isArray(id) ? id[0] : id)?.toString();
      if (eventId === undefined) return;
      const committedEpochMs = pending.get(eventId);
      if (committedEpochMs === undefined) return;
      pending.delete(eventId);
      histogram.observe(receivedEpochMs - committedEpochMs);
      lastMeasurementEpochMs = receivedEpochMs;
      lastFailureEpochMs = undefined;
    },
  });
  connected = true;
  const publish = async (): Promise<void> => {
    const now = new Date().toISOString();
    const requestId = uuidv7();
    const result = await store.append({
      producerService: "event-store-latency-probe",
      requestId,
      namespace: "system",
      aggregateType: "CdcLatencyProbe",
      aggregateId: uuidv7(),
      expectedRevision: { kind: "no_stream" },
      events: [
        {
          eventName: "system.cdc.latency.probe",
          schemaVersion: 1,
          occurredAt: now,
          payload: {},
        },
      ],
      context: {
        requestId,
        correlationId: uuidv7(),
        causationId: null,
        actor: { kind: "service", subjectRef: "event-store-latency-probe" },
        trafficClass: "standard",
      },
    });
    const eventId = result.events[0]?.eventId;
    if (eventId !== undefined && result.commitEpochMs !== undefined)
      pending.set(eventId, result.commitEpochMs);
    const cutoff = Date.now() - 300_000;
    for (const [id, committedAt] of pending)
      if (committedAt < cutoff) {
        pending.delete(id);
        lastFailureEpochMs = Date.now();
      }
  };
  const publishSafely = (): void => {
    void publish().catch(() => {
      lastFailureEpochMs = Date.now();
    });
  };
  const timer = setInterval(publishSafely, intervalMs);
  timer.unref();
  publishSafely();
  return {
    up: () =>
      connected &&
      lastMeasurementEpochMs !== undefined &&
      Date.now() - lastMeasurementEpochMs <= staleAfterMs &&
      lastFailureEpochMs === undefined,
    stop: async () => {
      connected = false;
      clearInterval(timer);
      await consumer.disconnect().catch(() => undefined);
    },
  };
}

type JsonObject = Record<string, unknown>;
interface AppendRequest extends JsonObject {
  request_id: string;
  namespace: string;
  aggregate_type: string;
  aggregate_id: string;
  expected_revision?: { no_stream?: unknown; exact_revision?: string };
  events: {
    event_name: string;
    schema_version: number;
    occurred_at: string;
    payload_json: Buffer;
  }[];
  context: {
    correlation_id: string;
    causation_id?: string;
    actor_json: Buffer;
    traceparent?: string;
    critical?: boolean;
  };
}
interface StreamRequest extends JsonObject {
  namespace: string;
  aggregate_type: string;
  aggregate_id: string;
  from_revision?: string;
}
interface LoadRequest extends StreamRequest {
  reducer_version: string;
}
interface PutSnapshotRequest extends StreamRequest {
  snapshot: {
    snapshot_revision: string;
    reducer_version: string;
    state_schema_version: number;
    state_json: Buffer;
    state_sha256: Buffer;
  };
}

/** Test-only deterministic boundaries; production start-up leaves this absent. */
export type ServiceCrashPoint =
  | "before_sql"
  | "after_postgres_commit"
  | "before_grpc_response_write";

export interface ServiceTestHooks {
  hit(point: ServiceCrashPoint): Promise<void> | void;
}

export function errorFrom(
  error: unknown,
  requestId?: string,
  appendDispatched = false,
): grpc.ServiceError {
  const source = error as { code?: string; message?: string };
  const message = source.message ?? "internal error";
  const sqlCode = source.code;
  const statusCode = (error as { code?: unknown }).code;
  let code = grpc.status.INTERNAL;
  let machineCode = "internal_error";
  if (
    error instanceof ZodError ||
    error instanceof SyntaxError ||
    error instanceof RangeError ||
    sqlCode === "22023" ||
    sqlCode === "22001" ||
    sqlCode === "22003" ||
    sqlCode === "22P02" ||
    sqlCode === "23514"
  )
    [code, machineCode] = [grpc.status.INVALID_ARGUMENT, "validation_failed"];
  else if (sqlCode === "40001")
    [code, machineCode] = [grpc.status.ABORTED, "expected_revision_conflict"];
  else if (sqlCode === "57014")
    [code, machineCode] = [grpc.status.DEADLINE_EXCEEDED, "deadline_exceeded"];
  else if (statusCode === grpc.status.PERMISSION_DENIED)
    [code, machineCode] = [
      grpc.status.PERMISSION_DENIED,
      "critical_append_forbidden",
    ];
  else if (sqlCode === "23505")
    [code, machineCode] = [grpc.status.ALREADY_EXISTS, "idempotency_conflict"];
  else if (sqlCode === "XX001")
    [code, machineCode] = [grpc.status.DATA_LOSS, "snapshot_nondeterminism"];
  else if (sqlCode === "P0002")
    [code, machineCode] = [grpc.status.NOT_FOUND, "stream_not_found"];
  else if (sqlCode === "P0001")
    [code, machineCode] = [
      grpc.status.RESOURCE_EXHAUSTED,
      "cdc_admission_closed",
    ];
  else if (
    appendDispatched &&
    [
      "57P01",
      "57P02",
      "08000",
      "08003",
      "08006",
      "ETIMEDOUT",
      "ECONNRESET",
      "ECONNREFUSED",
    ].includes(sqlCode ?? "")
  )
    [code, machineCode] = [grpc.status.UNKNOWN, "commit_outcome_unknown"];
  else if (
    [
      "57P01",
      "57P02",
      "08000",
      "08003",
      "08006",
      "ETIMEDOUT",
      "ECONNRESET",
      "ECONNREFUSED",
    ].includes(sqlCode ?? "")
  )
    [code, machineCode] = [grpc.status.UNAVAILABLE, "database_unavailable"];
  const actualRevision = /actual revision (\d+)/.exec(message)?.[1];
  const detail = {
    code: machineCode,
    requestId,
    retriable:
      code === grpc.status.UNAVAILABLE ||
      machineCode === "commit_outcome_unknown",
    ...(actualRevision === undefined ? {} : { actualRevision }),
  };
  const result = Object.assign(new Error(JSON.stringify(detail)), {
    code,
  }) as grpc.ServiceError;
  result.details = JSON.stringify(detail);
  return result;
}

function requireUuid(value: string): string {
  return UuidV7.parse(value);
}

function parseState(value: Buffer): Record<string, unknown> {
  const parsed = JSON.parse(value.toString()) as unknown;
  if (parsed === null || Array.isArray(parsed) || typeof parsed !== "object") {
    throw Object.assign(new Error("snapshot state must be an object"), {
      code: "22023",
    });
  }
  assertNoDirectPii(parsed);
  return parsed as Record<string, unknown>;
}

function parseExpected(
  input: AppendRequest["expected_revision"],
): ExpectedRevision {
  if (input?.exact_revision !== undefined)
    return { kind: "exact", revision: BigInt(input.exact_revision) };
  if (input?.no_stream !== undefined) return { kind: "no_stream" };
  throw Object.assign(new Error("expected_revision is required"), {
    code: "22023",
  });
}

function parseAppendRequest(
  request: AppendRequest,
  producerService: string,
  trafficClass: "standard" | "critical",
) {
  requireUuid(request.request_id);
  requireUuid(request.aggregate_id);
  const actor = JSON.parse(request.context.actor_json.toString()) as unknown;
  const context = EventContextSchema.parse({
    requestId: request.request_id,
    correlationId: request.context.correlation_id,
    causationId: request.context.causation_id || null,
    actor,
    traceparent: request.context.traceparent,
    trafficClass,
  });
  return {
    producerService,
    requestId: request.request_id,
    namespace: request.namespace,
    aggregateType: request.aggregate_type,
    aggregateId: request.aggregate_id,
    expectedRevision: parseExpected(request.expected_revision),
    context,
    events: request.events.map((event) => ({
      eventName: event.event_name,
      schemaVersion: event.schema_version,
      occurredAt: event.occurred_at,
      payload: JSON.parse(event.payload_json.toString()) as JsonObject,
    })),
  };
}

export async function startServer(
  testHooks?: ServiceTestHooks,
): Promise<grpc.Server> {
  const url = process.env.DATABASE_URL;
  const producerService = process.env.PRODUCER_SERVICE;
  const address = process.env.GRPC_LISTEN_ADDRESS ?? "0.0.0.0:50051";
  if (url === undefined || producerService === undefined)
    throw new Error("DATABASE_URL and PRODUCER_SERVICE are required");
  const pool = new Pool({
    connectionString: url,
    max: Number(process.env.DB_POOL_SIZE ?? 20),
  });
  const criticalUrl = process.env.CRITICAL_DATABASE_URL;
  const walBudget = process.env.CDC_WAL_BUDGET_BYTES;
  if (
    walBudget === undefined ||
    !/^\d+$/.test(walBudget) ||
    BigInt(walBudget) <= 0n
  )
    throw new Error("CDC_WAL_BUDGET_BYTES must be a positive integer");
  const store = new PostgresEventStore(pool, BigInt(walBudget));
  const criticalPool =
    criticalUrl === undefined
      ? undefined
      : new Pool({ connectionString: criticalUrl });
  const criticalStore =
    criticalPool === undefined
      ? undefined
      : new PostgresEventStore(criticalPool, BigInt(walBudget), true);
  const metrics = {
    appendCount: 0,
    appendFailureCount: 0,
    appendConflictCount: 0,
    appendUnknownOutcomeCount: 0,
    appendDurationSeconds: 0,
    dbCommitDurationSeconds: 0,
  };
  const processCrashPoint = process.env.EVENT_STORE_TEST_CRASH_POINT;
  const hitCrashBoundary = async (point: ServiceCrashPoint): Promise<void> => {
    await testHooks?.hit(point);
    if (process.env.NODE_ENV === "test" && processCrashPoint === point)
      process.kill(process.pid, "SIGKILL");
  };
  const connectUrl = process.env.CONNECT_URL;
  const connectMetricsPort = process.env.CONNECT_METRICS_PORT;
  const kafkaBrokers = process.env.KAFKA_BROKERS?.split(",");
  const kafkaTopic = process.env.KAFKA_LIVE_TOPIC ?? "event-store.events.v1";
  const kafkaMinIsr = Number(process.env.KAFKA_LIVE_MIN_ISR ?? "2");
  const latencyProbeIntervalText = process.env.CDC_LATENCY_PROBE_INTERVAL_MS;
  const latencyHistogram = new CommitToConsumerLatencyHistogram();
  let latencyProbe: CdcLatencyProbe | undefined;
  if (latencyProbeIntervalText !== undefined && kafkaBrokers === undefined)
    throw new Error(
      "KAFKA_BROKERS is required when CDC latency probe is enabled",
    );
  const deliveryHealthIntervalText =
    process.env.CDC_DELIVERY_HEALTH_CHECK_INTERVAL_MS ?? "5000";
  const deliveryHealthIntervalMs = Number(deliveryHealthIntervalText);
  if (
    !/^\d+$/.test(deliveryHealthIntervalText) ||
    !Number.isInteger(deliveryHealthIntervalMs) ||
    deliveryHealthIntervalMs < 1_000
  )
    throw new Error(
      "CDC_DELIVERY_HEALTH_CHECK_INTERVAL_MS must be an integer of at least 1000",
    );
  const assertCdcDeliveryHealthy = async (): Promise<void> => {
    await pool.query("SELECT event_store.assert_cdc_delivery_ready($1)", [
      walBudget,
    ]);
    if (connectUrl !== undefined) {
      const connector = await pool.query<{
        cdc_connector_name: string;
      }>(
        "SELECT cdc_connector_name FROM event_store.runtime_config WHERE singleton",
      );
      const connectorName = connector.rows[0]?.cdc_connector_name;
      if (connectorName === undefined)
        throw new Error("CDC connector ownership is unavailable");
      const status = await fetch(
        `${connectUrl}/connectors/${encodeURIComponent(connectorName)}/status`,
        { signal: AbortSignal.timeout(5_000) },
      );
      const body = (await status.json()) as {
        connector?: { state?: string };
        tasks?: { state?: string; worker_id?: string }[];
      };
      if (
        !status.ok ||
        body.connector?.state !== "RUNNING" ||
        body.tasks?.length !== 1 ||
        body.tasks[0]?.state !== "RUNNING"
      )
        throw new Error("CDC connector is not ready");
      if (connectMetricsPort !== undefined) {
        if (!/^\d+$/.test(connectMetricsPort))
          throw new Error("CONNECT_METRICS_PORT must be numeric");
        const workerId = body.tasks[0]?.worker_id;
        const workerHost = workerId?.replace(/:\d+$/, "");
        if (workerHost === undefined || workerHost === "")
          throw new Error("CDC task worker id is unavailable");
        const metrics = await fetch(
          `http://${workerHost}:${connectMetricsPort}/metrics`,
          { signal: AbortSignal.timeout(5_000) },
        ).then((result) => {
          if (!result.ok) throw new Error("Connect metrics are unavailable");
          return result.text();
        });
        if (connectQueueRatio(metrics, connectorName) < 0.2)
          throw new Error("CDC Connect queue is saturated");
      }
    }
    if (kafkaBrokers !== undefined)
      await verifyKafkaReadiness(kafkaBrokers, kafkaTopic, kafkaMinIsr);
  };
  const reconcileCdcDeliveryAdmission = async (): Promise<void> => {
    try {
      await assertCdcDeliveryHealthy();
      await pool.query("SELECT event_store.set_cdc_delivery_health(true)");
    } catch (error) {
      // This is durable and is checked inside append_v1, so existing gRPC
      // connections cannot continue writing after the next failed probe.
      await pool
        .query("SELECT event_store.set_cdc_delivery_health(false)")
        .catch(() => undefined);
      throw error;
    }
  };
  let deliveryHealthCheckInFlight = false;
  const runDeliveryHealthCheck = (): void => {
    if (deliveryHealthCheckInFlight) return;
    deliveryHealthCheckInFlight = true;
    void reconcileCdcDeliveryAdmission()
      .catch(() => undefined)
      .finally(() => {
        deliveryHealthCheckInFlight = false;
      });
  };
  const healthAddress = process.env.HTTP_LISTEN_ADDRESS;
  const health =
    healthAddress === undefined
      ? undefined
      : createServer((request, response) => {
          void (async () => {
            if (request.url === "/livez") {
              response.writeHead(200).end("ok\n");
              return;
            }
            if (request.url === "/metrics") {
              response
                .writeHead(200, { "content-type": "text/plain; version=0.0.4" })
                .end(
                  `event_store_append_total ${metrics.appendCount}\n` +
                    `event_store_append_failures_total ${metrics.appendFailureCount}\n` +
                    `event_store_append_conflicts_total ${metrics.appendConflictCount}\n` +
                    `event_store_append_unknown_outcomes_total ${metrics.appendUnknownOutcomeCount}\n` +
                    `event_store_append_duration_seconds_sum ${metrics.appendDurationSeconds}\n` +
                    `event_store_db_commit_duration_seconds_sum ${metrics.dbCommitDurationSeconds}\n` +
                    `event_store_db_pool_total ${pool.totalCount}\n` +
                    `event_store_db_pool_idle ${pool.idleCount}\n` +
                    `event_store_db_pool_waiting ${pool.waitingCount}\n` +
                    (latencyProbe === undefined
                      ? ""
                      : latencyHistogram.prometheus() +
                        `event_store_cdc_latency_probe_up ${latencyProbe.up() ? 1 : 0}\n`),
                );
              return;
            }
            if (request.url !== "/readyz") {
              response.writeHead(404).end();
              return;
            }
            // Readiness also drives the durable append fence.  A 503 alone is
            // insufficient because already-open gRPC connections can append.
            await reconcileCdcDeliveryAdmission();
            response.writeHead(200).end("ready\n");
          })().catch(() => response.writeHead(503).end("not ready\n"));
        });
  if (health !== undefined) {
    const [host, portText] = healthAddress!.split(":");
    const port = Number(portText);
    if (host === undefined || !Number.isInteger(port) || port <= 0)
      throw new Error("HTTP_LISTEN_ADDRESS must be host:port");
    await new Promise<void>((resolve, reject) =>
      health.once("error", reject).listen(port, host, resolve),
    );
  }
  if (latencyProbeIntervalText !== undefined)
    latencyProbe = await startCdcLatencyProbe(
      store,
      kafkaBrokers!,
      kafkaTopic,
      Number(latencyProbeIntervalText),
      latencyHistogram,
    );
  const protoPath =
    process.env.PROTO_PATH ??
    join(process.cwd(), "packages/contracts/proto/event_store.proto");
  const definition = protoLoader.loadSync(protoPath, {
    keepCase: true,
    longs: String,
    enums: String,
    defaults: false,
    oneofs: true,
  });
  const loaded = grpc.loadPackageDefinition(definition) as unknown as {
    eventstore: { v1: { EventStoreService: grpc.ServiceClientConstructor } };
  };
  const service = loaded.eventstore.v1.EventStoreService;
  const server = new grpc.Server();
  const trustedCriticalSubjects = (process.env.CRITICAL_CLIENT_SUBJECTS ?? "")
    .split(",")
    .map((subject) => subject.trim())
    .filter((subject) => subject.length > 0);
  server.addService(service.service, {
    AppendToStream: async (
      call: grpc.ServerUnaryCall<AppendRequest, JsonObject>,
      callback: grpc.sendUnaryData<JsonObject>,
    ) => {
      try {
        const started = performance.now();
        const trafficClass = trafficClassForClient(
          call.request.context.critical === true,
          call.getAuthContext().sslPeerCertificate?.subject?.CN,
          trustedCriticalSubjects,
        );
        const appendStore = trafficClass === "critical" ? criticalStore : store;
        if (appendStore === undefined)
          throw Object.assign(
            new Error("critical append database principal is not configured"),
            { code: grpc.status.PERMISSION_DENIED },
          );
        await hitCrashBoundary("before_sql");
        const databaseStarted = performance.now();
        const result = await appendStore.append(
          parseAppendRequest(call.request, producerService, trafficClass),
        );
        metrics.dbCommitDurationSeconds +=
          (performance.now() - databaseStarted) / 1_000;
        await hitCrashBoundary("after_postgres_commit");
        if (result.commitEpochMs !== undefined) {
          const commitMetadata = new grpc.Metadata();
          commitMetadata.set(
            "x-event-store-commit-epoch-ms",
            result.commitEpochMs.toString(),
          );
          call.sendMetadata(commitMetadata);
        }
        await hitCrashBoundary("before_grpc_response_write");
        callback(null, {
          request_id: result.requestId,
          previous_revision: result.previousRevision,
          current_revision: result.currentRevision,
          recorded_at: result.recordedAt,
          events: result.events.map((event) => ({
            event_id: event.eventId,
            event_number: event.eventNumber,
            stream_revision: event.streamRevision,
          })),
          acknowledged_at: new Date().toISOString(),
        });
        metrics.appendCount += 1;
        metrics.appendDurationSeconds += (performance.now() - started) / 1_000;
      } catch (error) {
        metrics.appendFailureCount += 1;
        const source = error as {
          code?: unknown;
          appendDispatched?: unknown;
        };
        if (source.code === "40001") metrics.appendConflictCount += 1;
        if (
          source.appendDispatched === true &&
          (source.code === undefined ||
            /^(08|ECONNRESET|ETIMEDOUT|EPIPE)/.test(String(source.code)))
        )
          metrics.appendUnknownOutcomeCount += 1;
        callback(
          errorFrom(
            error,
            call.request.request_id,
            (error as { appendDispatched?: unknown }).appendDispatched === true,
          ),
        );
      }
    },
    GetStreamHead: async (
      call: grpc.ServerUnaryCall<StreamRequest, JsonObject>,
      callback: grpc.sendUnaryData<JsonObject>,
    ) => {
      try {
        const revision = await store.getStreamHead(
          call.request.namespace,
          call.request.aggregate_type,
          requireUuid(call.request.aggregate_id),
        );
        if (revision === undefined)
          throw Object.assign(new Error("stream not found"), { code: "P0002" });
        callback(null, { current_revision: revision.toString() });
      } catch (error) {
        callback(errorFrom(error));
      }
    },
    ReadStream: async (
      call: grpc.ServerWritableStream<StreamRequest, JsonObject>,
    ) => {
      try {
        const events = await store.readStream(
          call.request.namespace,
          call.request.aggregate_type,
          requireUuid(call.request.aggregate_id),
          BigInt(call.request.from_revision || "1"),
        );
        if (events.length === 0) {
          const head = await store.getStreamHead(
            call.request.namespace,
            call.request.aggregate_type,
            call.request.aggregate_id,
          );
          if (head === undefined)
            throw Object.assign(new Error("stream not found"), {
              code: "P0002",
            });
        }
        for (const event of events)
          call.write({ envelope_json: Buffer.from(JSON.stringify(event)) });
        call.end();
      } catch (error) {
        call.destroy(errorFrom(error));
      }
    },
    LoadAggregate: async (
      call: grpc.ServerWritableStream<LoadRequest, JsonObject>,
    ) => {
      try {
        const frames = await store.loadAggregate(
          call.request.namespace,
          call.request.aggregate_type,
          requireUuid(call.request.aggregate_id),
          call.request.reducer_version,
        );
        for (const frame of frames) {
          if (frame.kind === "info")
            call.write({
              info: { head_revision: frame.headRevision.toString() },
            });
          else if (frame.kind === "event")
            call.write({
              event: {
                envelope_json: Buffer.from(JSON.stringify(frame.event)),
              },
            });
          else
            call.write({
              snapshot: {
                snapshot_revision: frame.snapshot.revision.toString(),
                reducer_version: frame.snapshot.reducerVersion,
                state_schema_version: frame.snapshot.stateSchemaVersion,
                state_json: Buffer.from(JSON.stringify(frame.snapshot.state)),
                state_sha256: frame.snapshot.stateSha256,
              },
            });
        }
        call.end();
      } catch (error) {
        call.destroy(errorFrom(error));
      }
    },
    PutSnapshot: async (
      call: grpc.ServerUnaryCall<PutSnapshotRequest, JsonObject>,
      callback: grpc.sendUnaryData<JsonObject>,
    ) => {
      try {
        const snapshot = call.request.snapshot;
        if (snapshot === undefined)
          throw Object.assign(new Error("snapshot is required"), {
            code: "22023",
          });
        await store.putSnapshot({
          namespace: call.request.namespace,
          aggregateType: call.request.aggregate_type,
          aggregateId: requireUuid(call.request.aggregate_id),
          revision: BigInt(snapshot.snapshot_revision),
          reducerVersion: snapshot.reducer_version,
          stateSchemaVersion: snapshot.state_schema_version,
          state: parseState(snapshot.state_json),
          stateSha256: snapshot.state_sha256,
        });
        callback(null, {});
      } catch (error) {
        callback(errorFrom(error));
      }
    },
  });
  const insecureGrpc = process.env.GRPC_ALLOW_INSECURE === "true";
  if (insecureGrpc && process.env.NODE_ENV === "production")
    throw new Error("GRPC_ALLOW_INSECURE is forbidden in production");
  const credentials = insecureGrpc
    ? grpc.ServerCredentials.createInsecure()
    : (() => {
        const certificate = process.env.GRPC_TLS_CERT_PEM;
        const key = process.env.GRPC_TLS_KEY_PEM;
        const ca = process.env.GRPC_TLS_CA_PEM;
        if (certificate === undefined || key === undefined || ca === undefined)
          throw new Error(
            "mTLS credentials are required; set GRPC_ALLOW_INSECURE=true only for local development",
          );
        return grpc.ServerCredentials.createSsl(
          Buffer.from(ca),
          [
            {
              cert_chain: Buffer.from(certificate),
              private_key: Buffer.from(key),
            },
          ],
          true,
        );
      })();
  await new Promise<void>((resolve, reject) =>
    server.bindAsync(address, credentials, (error) =>
      error === null ? resolve() : reject(error),
    ),
  );
  server.start();
  runDeliveryHealthCheck();
  const deliveryHealthTimer = setInterval(
    runDeliveryHealthCheck,
    deliveryHealthIntervalMs,
  );
  deliveryHealthTimer.unref();
  const grpcShutdown = server.tryShutdown.bind(server);
  server.tryShutdown = (callback) => {
    grpcShutdown(() => {
      void (async () => {
        await new Promise<void>(
          (resolve) => health?.close(() => resolve()) ?? resolve(),
        );
        clearInterval(deliveryHealthTimer);
        await latencyProbe?.stop();
        await pool.end();
        await criticalPool?.end();
      })().finally(callback);
    });
  };
  return server;
}

if (process.argv[1] === fileURLToPath(import.meta.url)) void startServer();
