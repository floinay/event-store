import { join } from "node:path";
import { fileURLToPath } from "node:url";
import * as grpc from "@grpc/grpc-js";
import * as protoLoader from "@grpc/proto-loader";
import { Pool } from "pg";
import { EventContextSchema, UuidV7 } from "@event-store/contracts";
import { ZodError } from "zod";
import {
  PostgresEventStore,
  type ExpectedRevision,
} from "@event-store/postgres-store";

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

export function errorFrom(
  error: unknown,
  requestId?: string,
  appendDispatched = false,
): grpc.ServiceError {
  const source = error as { code?: string; message?: string };
  const message = source.message ?? "internal error";
  const sqlCode = source.code;
  let code = grpc.status.INTERNAL;
  let machineCode = "internal_error";
  if (
    error instanceof ZodError ||
    error instanceof SyntaxError ||
    error instanceof RangeError ||
    sqlCode === "22023" ||
    sqlCode === "22001"
  )
    [code, machineCode] = [grpc.status.INVALID_ARGUMENT, "validation_failed"];
  else if (sqlCode === "40001")
    [code, machineCode] = [grpc.status.ABORTED, "expected_revision_conflict"];
  else if (sqlCode === "23505")
    [code, machineCode] = [grpc.status.ALREADY_EXISTS, "idempotency_conflict"];
  else if (sqlCode === "XX001")
    [code, machineCode] = [grpc.status.DATA_LOSS, "snapshot_nondeterminism"];
  else if (sqlCode === "P0002")
    [code, machineCode] = [grpc.status.NOT_FOUND, "stream_not_found"];
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
    retriable: code === grpc.status.UNAVAILABLE,
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

function parseAppendRequest(request: AppendRequest, producerService: string) {
  requireUuid(request.request_id);
  requireUuid(request.aggregate_id);
  const actor = JSON.parse(request.context.actor_json.toString()) as unknown;
  const context = EventContextSchema.parse({
    requestId: request.request_id,
    correlationId: request.context.correlation_id,
    causationId: request.context.causation_id || null,
    actor,
    traceparent: request.context.traceparent,
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

export async function startServer(): Promise<grpc.Server> {
  const url = process.env.DATABASE_URL;
  const producerService = process.env.PRODUCER_SERVICE;
  const address = process.env.GRPC_LISTEN_ADDRESS ?? "0.0.0.0:50051";
  if (url === undefined || producerService === undefined)
    throw new Error("DATABASE_URL and PRODUCER_SERVICE are required");
  const pool = new Pool({
    connectionString: url,
    max: Number(process.env.DB_POOL_SIZE ?? 20),
  });
  const store = new PostgresEventStore(pool);
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
  server.addService(service.service, {
    AppendToStream: async (
      call: grpc.ServerUnaryCall<AppendRequest, JsonObject>,
      callback: grpc.sendUnaryData<JsonObject>,
    ) => {
      try {
        const result = await store.append(
          parseAppendRequest(call.request, producerService),
        );
        callback(null, {
          ...result,
          acknowledged_at: new Date().toISOString(),
        });
      } catch (error) {
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
  const credentials =
    process.env.GRPC_ALLOW_INSECURE === "true"
      ? grpc.ServerCredentials.createInsecure()
      : (() => {
          const certificate = process.env.GRPC_TLS_CERT_PEM;
          const key = process.env.GRPC_TLS_KEY_PEM;
          const ca = process.env.GRPC_TLS_CA_PEM;
          if (
            certificate === undefined ||
            key === undefined ||
            ca === undefined
          )
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
  return server;
}

if (process.argv[1] === fileURLToPath(import.meta.url)) void startServer();
