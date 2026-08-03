import { createHash } from "node:crypto";
import type { Pool, PoolClient } from "pg";
import { z } from "zod";
import { UpcasterRegistry } from "@event-store/upcasting";
import {
  canonicalJson,
  partitionKey,
  StoredEventSchema,
  type StoredEvent,
} from "@event-store/contracts";

export interface ConsumedRecord {
  topic: string;
  partition: number;
  offset: bigint;
  key: string;
  headers: Readonly<Record<string, string | undefined>>;
  value: Buffer | string;
}

export interface ProjectionIdentity {
  name: string;
  generationId: string;
}
export type ProjectionHandler = (
  client: PoolClient,
  event: StoredEvent,
) => Promise<void>;
export type ProjectionEventTransformer = (event: StoredEvent) => StoredEvent;

export class ProjectionUnknownSchemaError extends Error {
  readonly code = "projection_unknown_schema";
}

/** Event payload schemas are versioned after upcasting, never inferred. */
export class ProjectionPayloadSchemas {
  readonly #schemas = new Map<string, z.ZodType<unknown>>();

  register(
    eventName: string,
    schemaVersion: number,
    schema: z.ZodType<unknown>,
  ): void {
    if (!Number.isInteger(schemaVersion) || schemaVersion < 1)
      throw new TypeError("schemaVersion must be a positive integer");
    const key = `${eventName}@${schemaVersion}`;
    if (this.#schemas.has(key))
      throw new Error(`duplicate projection payload schema: ${key}`);
    this.#schemas.set(key, schema);
  }

  parse(
    eventName: string,
    schemaVersion: number,
    payload: unknown,
  ): Record<string, unknown> {
    const schema = this.#schemas.get(`${eventName}@${schemaVersion}`);
    if (schema === undefined)
      throw new ProjectionUnknownSchemaError(
        `no projection payload schema for ${eventName}@${schemaVersion}`,
      );
    const output = schema.parse(payload);
    if (output === null || typeof output !== "object" || Array.isArray(output))
      throw new TypeError(
        "projection event payload schema must yield an object",
      );
    return output as Record<string, unknown>;
  }
}

export function createProjectionEventTransformer(
  upcasters: UpcasterRegistry,
  schemas: ProjectionPayloadSchemas,
): ProjectionEventTransformer {
  return (event) => {
    const upcasted = upcasters.upcast(
      event.eventName,
      event.schemaVersion,
      event.payload,
    );
    return {
      ...event,
      schemaVersion: upcasted.version,
      payload: schemas.parse(
        event.eventName,
        upcasted.version,
        upcasted.payload,
      ),
    };
  };
}

export class ProjectionGapError extends Error {
  readonly code = "event_gap";
}
export class ProjectionIntegrityError extends Error {
  readonly code = "event_integrity";
}
export class ProjectionRetentionError extends Error {
  readonly code = "projection_rebuild_required";
}

export const projectionRetryDelaysMs = [
  100, 250, 500, 1_000, 2_000, 5_000, 10_000, 30_000,
] as const;

export class ProjectionFailureReporter {
  constructor(
    private readonly pool: Pool,
    private readonly identity: ProjectionIdentity,
  ) {}

  async record(
    record: ConsumedRecord,
    event: StoredEvent,
    error: unknown,
    attemptCount: number,
  ): Promise<void> {
    const detail =
      error instanceof Error
        ? { message: error.message }
        : { message: String(error) };
    await this.pool.query(
      `INSERT INTO projection_runtime.failures(projection_name,generation_id,event_id,envelope_sha256,topic_name,partition_no,kafka_offset,attempt_count,error_code,error_detail,envelope,first_failed_at,last_failed_at)
       VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10::jsonb,$11::jsonb,clock_timestamp(),clock_timestamp())
       ON CONFLICT (projection_name,generation_id,topic_name,partition_no,kafka_offset)
       DO UPDATE SET attempt_count=EXCLUDED.attempt_count,error_code=EXCLUDED.error_code,error_detail=EXCLUDED.error_detail,last_failed_at=EXCLUDED.last_failed_at`,
      [
        this.identity.name,
        this.identity.generationId,
        event.eventId,
        record.headers.envelopeHash,
        record.topic,
        record.partition,
        record.offset.toString(),
        attemptCount,
        error instanceof ProjectionGapError
          ? error.code
          : "projection_handler_failed",
        JSON.stringify(detail),
        JSON.stringify(event),
      ],
    );
  }
}

export class ProjectionCheckpointStore {
  constructor(
    private readonly pool: Pool,
    private readonly identity: ProjectionIdentity,
  ) {}

  async nextOffset(
    topic: string,
    partition: number,
  ): Promise<bigint | undefined> {
    const result = await this.pool.query<{ next_offset: string }>(
      "SELECT next_offset FROM projection_runtime.checkpoints WHERE projection_name=$1 AND generation_id=$2 AND topic_name=$3 AND partition_no=$4",
      [this.identity.name, this.identity.generationId, topic, partition],
    );
    return result.rows[0] === undefined
      ? undefined
      : BigInt(result.rows[0].next_offset);
  }

  async ensureAtLowWatermark(
    topic: string,
    partition: number,
    lowWatermark: bigint,
  ): Promise<bigint> {
    const existing = await this.nextOffset(topic, partition);
    if (existing !== undefined) {
      if (existing < lowWatermark)
        throw new ProjectionRetentionError(
          `checkpoint ${existing} is below Kafka low watermark ${lowWatermark}`,
        );
      return existing;
    }
    await this.pool.query(
      `INSERT INTO projection_runtime.checkpoints(projection_name,generation_id,topic_name,partition_no,next_offset,updated_at)
       VALUES ($1,$2,$3,$4,$5,clock_timestamp()) ON CONFLICT DO NOTHING`,
      [
        this.identity.name,
        this.identity.generationId,
        topic,
        partition,
        lowWatermark.toString(),
      ],
    );
    return (await this.nextOffset(topic, partition)) ?? lowWatermark;
  }
}

/** Commits model mutation, inbox marker and checkpoint in one database transaction. */
export class ProjectionTransactionRunner {
  constructor(
    private readonly pool: Pool,
    private readonly identity: ProjectionIdentity,
    private readonly transform: ProjectionEventTransformer,
  ) {}

  async process(
    record: ConsumedRecord,
    apply: ProjectionHandler,
  ): Promise<"processed" | "duplicate"> {
    const wireValue = record.value.toString();
    const envelope = JSON.parse(wireValue);
    const event = this.transform(StoredEventSchema.parse(envelope));
    const hash = record.headers.envelopeHash;
    if (hash === undefined || !/^[0-9a-f]{64}$/.test(hash))
      throw new ProjectionIntegrityError("missing or invalid envelopeHash");
    if (
      hash !==
      createHash("sha256").update(canonicalJson(envelope)).digest("hex")
    )
      throw new ProjectionIntegrityError("envelopeHash does not match value");
    if (
      record.headers.id !== event.eventId ||
      record.headers.type !== event.eventName ||
      record.headers.namespace !== event.namespace ||
      record.headers.aggregateType !== event.aggregateType ||
      record.headers.streamRevision !== event.streamRevision
    )
      throw new ProjectionIntegrityError(
        "canonical headers do not match envelope",
      );
    if (record.key !== partitionKey(event))
      throw new ProjectionIntegrityError("Kafka key does not match envelope");
    const client = await this.pool.connect();
    try {
      await client.query("BEGIN");
      const checkpoint = await client.query<{ next_offset: string }>(
        `SELECT next_offset FROM projection_runtime.checkpoints WHERE projection_name=$1 AND generation_id=$2 AND topic_name=$3 AND partition_no=$4 FOR UPDATE`,
        [
          this.identity.name,
          this.identity.generationId,
          record.topic,
          record.partition,
        ],
      );
      const nextOffset =
        checkpoint.rows[0] === undefined
          ? record.offset
          : BigInt(checkpoint.rows[0].next_offset);
      if (record.offset > nextOffset)
        throw new ProjectionGapError(
          `expected ${nextOffset}, got ${record.offset}`,
        );
      if (record.offset < nextOffset) {
        await client.query("COMMIT");
        return "duplicate";
      }
      const inserted = await client.query(
        `INSERT INTO projection_runtime.inbox(projection_name,generation_id,event_id,envelope_sha256,topic_name,partition_no,kafka_offset,processed_at)
         VALUES ($1,$2,$3,$4,$5,$6,$7,clock_timestamp()) ON CONFLICT DO NOTHING`,
        [
          this.identity.name,
          this.identity.generationId,
          event.eventId,
          hash,
          record.topic,
          record.partition,
          record.offset.toString(),
        ],
      );
      if (inserted.rowCount === 0) {
        const existing = await client.query<{ envelope_sha256: string }>(
          "SELECT envelope_sha256 FROM projection_runtime.inbox WHERE projection_name=$1 AND generation_id=$2 AND event_id=$3",
          [this.identity.name, this.identity.generationId, event.eventId],
        );
        if (existing.rows[0]?.envelope_sha256 !== hash)
          throw new ProjectionIntegrityError(
            "event id was previously observed with another hash",
          );
      } else await apply(client, event);
      await client.query(
        `INSERT INTO projection_runtime.checkpoints(projection_name,generation_id,topic_name,partition_no,next_offset,last_event_id,updated_at)
         VALUES ($1,$2,$3,$4,$5,$6,clock_timestamp())
         ON CONFLICT (projection_name,generation_id,topic_name,partition_no)
         DO UPDATE SET next_offset=EXCLUDED.next_offset,last_event_id=EXCLUDED.last_event_id,updated_at=EXCLUDED.updated_at`,
        [
          this.identity.name,
          this.identity.generationId,
          record.topic,
          record.partition,
          (record.offset + 1n).toString(),
          event.eventId,
        ],
      );
      await client.query("COMMIT");
      return inserted.rowCount === 1 ? "processed" : "duplicate";
    } catch (error) {
      await client.query("ROLLBACK").catch(() => undefined);
      throw error;
    } finally {
      client.release();
    }
  }
}

export interface OrderedConsumer {
  records(): AsyncIterable<ConsumedRecord>;
  commit(topic: string, partition: number, nextOffset: bigint): Promise<void>;
  pause(topic: string, partition: number): Promise<void>;
}

/** The adapter commits Kafka only after `ProjectionTransactionRunner` has committed PostgreSQL. */
export class OrderedProjectionConsumer {
  constructor(
    private readonly consumer: OrderedConsumer,
    private readonly runner: ProjectionTransactionRunner,
    private readonly apply: ProjectionHandler,
  ) {}

  async run(): Promise<void> {
    for await (const record of this.consumer.records()) {
      try {
        await this.runner.process(record, this.apply);
        await this.consumer.commit(
          record.topic,
          record.partition,
          record.offset + 1n,
        );
      } catch (error) {
        await this.consumer.pause(record.topic, record.partition);
        throw error;
      }
    }
  }
}

export { KafkaProjectionRunner } from "./kafka.js";
export type { KafkaProjectionConfig } from "./kafka.js";
