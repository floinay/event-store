import { fileURLToPath } from "node:url";
import { KafkaJS } from "@confluentinc/kafka-javascript";
import { Pool } from "pg";
import { partitionKey, uuidv7 } from "@event-store/contracts";
import { PostgresEventStore } from "@event-store/postgres-store";

export interface ReplayIdentity {
  projectionName: string;
  generationId: string;
  replayId: string;
}

export interface ReplayVerification {
  /** The replay projection consumer group whose offsets must be caught up. */
  consumerGroupId: string;
  /** Projection-specific deterministic full-fold and rebuilt-model checksums. */
  checksums: () => Promise<{ expected: string; actual: string }>;
}

export interface ReplayBarrier {
  partition: number;
  aggregateId: string;
  eventId: string;
}

export interface ReplayStartOptions {
  identity: ReplayIdentity;
  coordinatorDatabaseUrl: string;
  appendDatabaseUrl: string;
  connectorUrl: string;
  brokers: string[];
  replicationFactor?: number;
  connectorDatabase: Parameters<typeof replayConnectorConfig>[1];
}

const kafkaPartitionCount = 24;

export function replayTopicName(replayId: string): string {
  if (!/^[a-z0-9-]{1,63}$/.test(replayId))
    throw new Error("replayId must be lowercase alphanumeric/hyphen");
  return `event-store.replay.${replayId}.v1`;
}

/** Creates and verifies the fixed partition topology required by replay barriers. */
export async function ensureReplayTopic(
  brokers: string[],
  replayId: string,
  replicationFactor = 3,
): Promise<void> {
  if (brokers.length === 0)
    throw new Error("at least one Kafka broker is required");
  const topic = replayTopicName(replayId);
  const kafka = new KafkaJS.Kafka({ kafkaJS: { brokers } });
  const admin = kafka.admin();
  await admin.connect();
  try {
    await admin.createTopics({
      topics: [
        {
          topic,
          numPartitions: kafkaPartitionCount,
          replicationFactor,
          configEntries: [
            { name: "cleanup.policy", value: "delete" },
            { name: "min.insync.replicas", value: "2" },
          ],
        },
      ],
    });
    const metadata = await admin.fetchTopicMetadata({ topics: [topic] });
    if (metadata[0]?.partitions.length !== kafkaPartitionCount)
      throw new Error(
        `replay topic ${topic} must have ${kafkaPartitionCount} partitions`,
      );
  } finally {
    await admin.disconnect();
  }
}

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
    private readonly brokers: string[],
    private readonly replayReplicationFactor = 3,
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
    await ensureReplayTopic(
      this.brokers,
      identity.replayId,
      this.replayReplicationFactor,
    );
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
    const slotName = `event_store_replay_${identity.replayId.replaceAll("-", "_")}`;
    const deadline = Date.now() + 60_000;
    while (Date.now() < deadline) {
      const [status, slot] = await Promise.all([
        fetch(
          `${this.connectorUrl}/connectors/event-store-replay-${identity.replayId}/status`,
        ).then((result) =>
          result.ok
            ? (result.json() as Promise<{
                connector?: { state?: string };
                tasks?: { state?: string }[];
              }>)
            : undefined,
        ),
        this.pool.query<{ active: boolean }>(
          "SELECT active FROM pg_replication_slots WHERE slot_name=$1",
          [slotName],
        ),
      ]);
      if (
        status?.connector?.state === "RUNNING" &&
        status.tasks?.length === 1 &&
        status.tasks[0]?.state === "RUNNING" &&
        slot.rows[0]?.active === true
      )
        return;
      await new Promise((resolve) => setTimeout(resolve, 250));
    }
    throw new Error(
      `replay connector ${identity.replayId} did not become ready`,
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
    verification: ReplayVerification,
  ): Promise<void> {
    const [kafkaLag, checksums] = await Promise.all([
      this.replayKafkaLag(identity, verification.consumerGroupId),
      verification.checksums(),
    ]);
    if (kafkaLag !== 0n) throw new Error(`replay Kafka lag is ${kafkaLag}`);
    if (checksums.expected !== checksums.actual)
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

  private async replayKafkaLag(
    identity: ReplayIdentity,
    consumerGroupId: string,
  ): Promise<bigint> {
    const topic = replayTopicName(identity.replayId);
    const kafka = new KafkaJS.Kafka({ kafkaJS: { brokers: this.brokers } });
    const admin = kafka.admin();
    await admin.connect();
    try {
      const [endOffsets, groupOffsets] = await Promise.all([
        admin.fetchTopicOffsets(topic, {
          isolationLevel: KafkaJS.IsolationLevel.READ_COMMITTED,
        }),
        admin.fetchOffsets({ groupId: consumerGroupId, topics: [topic] }),
      ]);
      const committed = new Map(
        (groupOffsets[0]?.partitions ?? []).map((partition) => [
          partition.partition,
          BigInt(partition.offset),
        ]),
      );
      return endOffsets.reduce((lag, partition) => {
        const low = BigInt(partition.low);
        const high = BigInt(partition.high);
        if (high < 0n) return lag;
        const offset = committed.get(partition.partition) ?? low;
        // Kafka reports -1 for a group with no committed offset.
        const position = offset < low ? low : offset;
        if (position > high)
          throw new Error(
            `replay consumer offset ${position} is ahead of ${topic}/${partition.partition} high watermark ${high}`,
          );
        return lag + (high - position);
      }, 0n);
    } finally {
      await admin.disconnect();
    }
  }

  async teardown(identity: ReplayIdentity): Promise<void> {
    const slotName = `event_store_replay_${identity.replayId.replaceAll("-", "_")}`;
    const response = await fetch(
      `${this.connectorUrl}/connectors/event-store-replay-${identity.replayId}`,
      { method: "DELETE" },
    );
    if (!response.ok && response.status !== 404)
      throw new Error(
        `replay connector deletion failed: ${await response.text()}`,
      );
    const deadline = Date.now() + 60_000;
    while (Date.now() < deadline) {
      const slot = await this.pool.query<{ active: boolean }>(
        "SELECT active FROM pg_replication_slots WHERE slot_name=$1",
        [slotName],
      );
      if (slot.rows[0]?.active !== true) {
        if (slot.rows[0] !== undefined)
          await this.pool.query("SELECT pg_drop_replication_slot($1)", [
            slotName,
          ]);
        return;
      }
      await new Promise((resolve) => setTimeout(resolve, 250));
    }
    throw new Error(`replay connector did not release slot ${slotName}`);
  }
}

/** Starts the durable half of a rebuild; activation remains deliberately separate. */
export async function startReplay(
  options: ReplayStartOptions,
): Promise<ReplayBarrier[]> {
  const coordinatorPool = new Pool({
    connectionString: options.coordinatorDatabaseUrl,
  });
  const appendPool = new Pool({ connectionString: options.appendDatabaseUrl });
  try {
    const coordinator = new ReplayCoordinator(
      coordinatorPool,
      options.connectorUrl,
      options.brokers,
      options.replicationFactor,
    );
    await coordinator.createGeneration(options.identity);
    await coordinator.deployConnector(
      options.identity,
      replayConnectorConfig(
        options.identity.replayId,
        options.connectorDatabase,
      ),
    );
    return await appendReplayBarriers(
      new PostgresEventStore(appendPool),
      options.identity.replayId,
    );
  } finally {
    await coordinatorPool.end();
    await appendPool.end();
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
    "transforms.outbox.route.topic.replacement": replayTopicName(replayId),
    "transforms.outbox.table.expand.json.payload": "false",
    "transforms.outbox.table.op.invalid.behavior": "fatal",
    "transforms.outbox.table.fields.additional.placement":
      "event_id:header:id,event_name:header:type,envelope_sha256:header:envelopeHash,namespace:header:namespace,aggregate_type:header:aggregateType,stream_revision:header:streamRevision",
  };
}

if (process.argv[1] === fileURLToPath(import.meta.url)) {
  const replayId = process.env.REPLAY_ID;
  const raw = process.env.REPLAY_DATABASE_JSON;
  const coordinatorDatabaseUrl = process.env.REPLAY_COORDINATOR_DATABASE_URL;
  const appendDatabaseUrl = process.env.REPLAY_APPEND_DATABASE_URL;
  const connectorUrl = process.env.CONNECT_URL;
  const brokers = process.env.KAFKA_BROKERS?.split(",");
  const projectionName = process.env.REPLAY_PROJECTION_NAME;
  const generationId = process.env.REPLAY_GENERATION_ID;
  if (
    replayId === undefined ||
    raw === undefined ||
    coordinatorDatabaseUrl === undefined ||
    appendDatabaseUrl === undefined ||
    connectorUrl === undefined ||
    brokers === undefined ||
    projectionName === undefined ||
    generationId === undefined
  )
    throw new Error(
      "REPLAY_ID, REPLAY_DATABASE_JSON, REPLAY_COORDINATOR_DATABASE_URL, REPLAY_APPEND_DATABASE_URL, CONNECT_URL, KAFKA_BROKERS, REPLAY_PROJECTION_NAME and REPLAY_GENERATION_ID are required",
    );
  const barriers = await startReplay({
    identity: { replayId, projectionName, generationId },
    coordinatorDatabaseUrl,
    appendDatabaseUrl,
    connectorUrl,
    brokers,
    connectorDatabase: JSON.parse(raw) as Parameters<
      typeof replayConnectorConfig
    >[1],
  });
  console.log(
    JSON.stringify(
      {
        replayId,
        generationId,
        barriers,
      },
      null,
      2,
    ),
  );
}
