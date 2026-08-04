import { fileURLToPath } from "node:url";
import type { Pool } from "pg";
import { partitionKey, uuidv7 } from "@event-store/contracts";
import { PostgresEventStore } from "@event-store/postgres-store";

export interface ReplayIdentity {
  projectionName: string;
  generationId: string;
  replayId: string;
}

export interface ReplayReadiness {
  kafkaLag: bigint;
  expectedChecksum: string;
  actualChecksum: string;
}

export interface ReplayBarrier {
  partition: number;
  aggregateId: string;
  eventId: string;
}

const kafkaPartitionCount = 24;

function murmur2(bytes: Uint8Array): number {
  let hash = 0x9747b28c ^ bytes.length;
  let index = 0;
  while (bytes.length - index >= 4) {
    let word =
      bytes[index]! |
      (bytes[index + 1]! << 8) |
      (bytes[index + 2]! << 16) |
      (bytes[index + 3]! << 24);
    word = Math.imul(word, 0x5bd1e995);
    word ^= word >>> 24;
    word = Math.imul(word, 0x5bd1e995);
    hash = Math.imul(hash, 0x5bd1e995) ^ word;
    index += 4;
  }
  switch (bytes.length - index) {
    case 3:
      hash ^= bytes[index + 2]! << 16;
    // fall through
    case 2:
      hash ^= bytes[index + 1]! << 8;
    // fall through
    case 1:
      hash ^= bytes[index]!;
      hash = Math.imul(hash, 0x5bd1e995);
  }
  hash ^= hash >>> 13;
  hash = Math.imul(hash, 0x5bd1e995);
  return (hash ^ (hash >>> 15)) >>> 0;
}

export function kafkaDefaultPartition(key: string): number {
  return (murmur2(Buffer.from(key)) & 0x7fffffff) % kafkaPartitionCount;
}

function barrierAggregateId(partition: number): string {
  for (let attempts = 0; attempts < 10_000; attempts += 1) {
    const aggregateId = uuidv7();
    if (
      kafkaDefaultPartition(
        partitionKey({
          namespace: "system",
          aggregateType: "Barrier",
          aggregateId,
        }),
      ) === partition
    )
      return aggregateId;
  }
  throw new Error(
    `unable to find a Kafka key for replay partition ${partition}`,
  );
}

/** Appends exactly one partition-verified barrier event for each replay partition. */
export async function appendReplayBarriers(
  store: PostgresEventStore,
  replayId: string,
): Promise<ReplayBarrier[]> {
  if (!/^[a-z0-9-]{1,63}$/.test(replayId))
    throw new Error("replayId must be lowercase alphanumeric/hyphen");
  const barriers: ReplayBarrier[] = [];
  for (let partition = 0; partition < kafkaPartitionCount; partition += 1) {
    const aggregateId = barrierAggregateId(partition);
    const requestId = uuidv7();
    const result = await store.append({
      producerService: "replay-coordinator",
      namespace: "system",
      aggregateType: "Barrier",
      aggregateId,
      requestId,
      expectedRevision: { kind: "no_stream" },
      context: {
        requestId,
        correlationId: uuidv7(),
        causationId: null,
        actor: { kind: "system", subjectRef: "replay-coordinator" },
      },
      events: [
        {
          eventName: "system.replaybarrier",
          schemaVersion: 1,
          occurredAt: new Date().toISOString(),
          payload: { replayId, partition: String(partition) },
        },
      ],
    });
    const eventId = result.events[0]?.eventId;
    if (eventId === undefined)
      throw new Error("replay barrier append returned no event");
    barriers.push({ partition, aggregateId, eventId });
  }
  return barriers;
}

/** Coordinates the durable DB half of a temporary connector rebuild. */
export class ReplayCoordinator {
  constructor(
    private readonly pool: Pool,
    private readonly connectorUrl: string,
  ) {}

  async createGeneration(identity: ReplayIdentity): Promise<void> {
    await this.pool.query(
      `INSERT INTO projection_runtime.generations(projection_name,generation_id,status,created_at)
       VALUES ($1,$2,'building',clock_timestamp())`,
      [identity.projectionName, identity.generationId],
    );
  }

  async deployConnector(
    identity: ReplayIdentity,
    config: Record<string, string>,
  ): Promise<void> {
    const response = await fetch(
      `${this.connectorUrl}/connectors/event-store-replay-${identity.replayId}/config`,
      {
        method: "PUT",
        headers: { "content-type": "application/json" },
        body: JSON.stringify(config),
      },
    );
    if (!response.ok)
      throw new Error(
        `replay connector deployment failed: ${await response.text()}`,
      );
  }

  async recordBarrier(
    identity: ReplayIdentity,
    partition: number,
    eventId: string,
  ): Promise<void> {
    if (!Number.isInteger(partition) || partition < 0 || partition >= 24)
      throw new RangeError("replay barrier partition must be 0..23");
    const event = await this.pool.query<{
      partition_key: string;
      event_envelope: { eventName?: string; payload?: unknown };
    }>(
      "SELECT partition_key,event_envelope FROM event_store.events WHERE event_id=$1",
      [eventId],
    );
    const row = event.rows[0];
    const payload = row?.event_envelope.payload as
      | { replayId?: unknown; partition?: unknown }
      | undefined;
    if (
      row === undefined ||
      row.event_envelope.eventName !== "system.replaybarrier" ||
      payload?.replayId !== identity.replayId ||
      payload.partition !== String(partition) ||
      kafkaDefaultPartition(row.partition_key) !== partition
    )
      throw new Error("replay barrier does not match its canonical event");
    await this.pool.query(
      `INSERT INTO projection_runtime.replay_barriers(projection_name,generation_id,partition_no,event_id,processed_at)
       VALUES ($1,$2,$3,$4,clock_timestamp()) ON CONFLICT DO NOTHING`,
      [identity.projectionName, identity.generationId, partition, eventId],
    );
  }

  async activate(
    identity: ReplayIdentity,
    readiness: ReplayReadiness,
  ): Promise<void> {
    if (readiness.kafkaLag !== 0n)
      throw new Error(`replay Kafka lag is ${readiness.kafkaLag}`);
    if (readiness.expectedChecksum !== readiness.actualChecksum)
      throw new Error("replay checksum differs from full fold");
    const client = await this.pool.connect();
    try {
      await client.query("BEGIN");
      const barriers = await client.query<{ count: number }>(
        `SELECT count(*)::int AS count FROM projection_runtime.replay_barriers
         WHERE projection_name=$1 AND generation_id=$2`,
        [identity.projectionName, identity.generationId],
      );
      if (barriers.rows[0]?.count !== 24)
        throw new Error("all 24 replay barriers must be processed");
      const failures = await client.query<{ count: number }>(
        `SELECT count(*)::int AS count FROM projection_runtime.failures
         WHERE projection_name=$1 AND generation_id=$2`,
        [identity.projectionName, identity.generationId],
      );
      if (failures.rows[0]?.count !== 0)
        throw new Error("replay generation has failures");
      await client.query(
        "UPDATE projection_runtime.generations SET status='retired' WHERE projection_name=$1 AND status='active'",
        [identity.projectionName],
      );
      const activated = await client.query(
        `UPDATE projection_runtime.generations SET status='active',activated_at=clock_timestamp()
         WHERE projection_name=$1 AND generation_id=$2 AND status='building'`,
        [identity.projectionName, identity.generationId],
      );
      if (activated.rowCount !== 1)
        throw new Error("replay generation is not in building state");
      await client.query("COMMIT");
    } catch (error) {
      await client.query("ROLLBACK").catch(() => undefined);
      throw error;
    } finally {
      client.release();
    }
  }

  async teardown(identity: ReplayIdentity): Promise<void> {
    const response = await fetch(
      `${this.connectorUrl}/connectors/event-store-replay-${identity.replayId}`,
      { method: "DELETE" },
    );
    if (!response.ok && response.status !== 404)
      throw new Error(
        `replay connector deletion failed: ${await response.text()}`,
      );
    await this.pool.query("SELECT pg_drop_replication_slot($1)", [
      `event_store_replay_${identity.replayId.replaceAll("-", "_")}`,
    ]);
  }
}

export function replayConnectorConfig(
  replayId: string,
  database: {
    hostname: string;
    port: number;
    user: string;
    password: string;
    dbname: string;
  },
): Record<string, string> {
  if (!/^[a-z0-9-]{1,63}$/.test(replayId))
    throw new Error("replayId must be lowercase alphanumeric/hyphen");
  return {
    "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
    "tasks.max": "1",
    "database.hostname": database.hostname,
    "database.port": String(database.port),
    "database.user": database.user,
    "database.password": database.password,
    "database.dbname": database.dbname,
    "topic.prefix": `event-store-replay-${replayId}`,
    "plugin.name": "pgoutput",
    "publication.name": "event_store_events",
    "publication.autocreate.mode": "disabled",
    "slot.name": `event_store_replay_${replayId.replaceAll("-", "_")}`,
    "slot.drop.on.stop": "false",
    "schema.include.list": "event_store",
    "table.include.list": "event_store.events",
    "snapshot.mode": "initial",
    "snapshot.select.statement.overrides": "event_store.events",
    "snapshot.select.statement.overrides.event_store.events":
      "SELECT * FROM event_store.events ORDER BY event_number",
    transforms: "outbox",
    "transforms.outbox.type": "io.debezium.transforms.outbox.EventRouter",
    "transforms.outbox.table.field.event.id": "event_id",
    "transforms.outbox.table.field.event.key": "partition_key",
    "transforms.outbox.table.field.event.type": "event_name",
    "transforms.outbox.table.field.event.payload": "event_envelope",
    "transforms.outbox.route.by.field": "topic_route",
    "transforms.outbox.route.topic.replacement": `event-store.replay.${replayId}.v1`,
    "transforms.outbox.table.expand.json.payload": "false",
    "transforms.outbox.table.op.invalid.behavior": "fatal",
    "transforms.outbox.table.fields.additional.placement":
      "event_id:header:id,event_name:header:type,envelope_sha256:header:envelopeHash,namespace:header:namespace,aggregate_type:header:aggregateType,stream_revision:header:streamRevision",
  };
}

if (process.argv[1] === fileURLToPath(import.meta.url)) {
  const replayId = process.env.REPLAY_ID;
  const raw = process.env.REPLAY_DATABASE_JSON;
  if (replayId === undefined || raw === undefined)
    throw new Error("REPLAY_ID and REPLAY_DATABASE_JSON are required");
  console.log(
    JSON.stringify(
      {
        name: `event-store-replay-${replayId}`,
        config: replayConnectorConfig(
          replayId,
          JSON.parse(raw) as {
            hostname: string;
            port: number;
            user: string;
            password: string;
            dbname: string;
          },
        ),
      },
      null,
      2,
    ),
  );
}
