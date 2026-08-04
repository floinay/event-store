import { fileURLToPath } from "node:url";
import { createHash } from "node:crypto";
import { KafkaJS } from "@confluentinc/kafka-javascript";
import { Pool } from "pg";
import { partitionKey } from "@event-store/contracts";
import { PostgresEventStore } from "@event-store/postgres-store";

export interface ReplayIdentity {
  projectionName: string;
  generationId: string;
  replayId: string;
}

export interface ReplayVerification {
  /**
   * Readable-record lag measured by the projection runtime itself. This must
   * account for Kafka transaction-control records, which Admin high watermarks
   * cannot safely use as consumer lag.
   */
  kafkaLag: () => Promise<bigint>;
  /** The replay projection consumer group that committed every barrier. */
  consumerGroupId: string;
  /** Projection-specific deterministic full-fold and rebuilt-model checksums. */
  checksums: () => Promise<{ expected: string; actual: string }>;
}

/**
 * Implemented by the replay projection runtime, not by the operator shell.
 * It supplies the runtime's readable lag and independently-derived model
 * checksums to the activation command.
 */
export interface ReplayVerificationModule {
  createReplayVerification: (
    identity: ReplayIdentity,
  ) => Promise<ReplayVerification> | ReplayVerification;
}

export interface ReplayBarrier {
  partition: number;
  aggregateId: string;
  eventId: string;
}

/** A replay barrier as received by the projection's Kafka consumer. */
export interface ConsumedReplayRecord {
  topic: string;
  partition: number;
  offset: string | bigint;
  value: Buffer | string;
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

export interface RecoveryStartOptions {
  identity: ReplayIdentity;
  coordinatorDatabaseUrl: string;
  appendDatabaseUrl: string;
  connectorUrl: string;
  connectorDatabase: Parameters<typeof recoveryConnectorConfig>[1];
}

/** Runtime-owned evidence required before a recovered CDC slot can take writes. */
export interface RecoveryCutoverVerification {
  projectionName: string;
  generationId: string;
  replayId: string;
  consumerGroupId: string;
  /** Must measure readable records, not raw Kafka high-watermark positions. */
  kafkaLag: () => Promise<bigint>;
}

const kafkaPartitionCount = 24;
const maxReplayIdLength = 44;

function assertReplayId(replayId: string): void {
  if (!new RegExp(`^[a-z0-9-]{1,${maxReplayIdLength}}$`).test(replayId))
    throw new Error(
      `replayId must be lowercase alphanumeric/hyphen and at most ${maxReplayIdLength} characters`,
    );
}

function assertRecoveryId(recoveryId: string): void {
  assertReplayId(recoveryId);
  // `event_store_recovery_` plus the suffix must also satisfy the SQL
  // ownership gate's 50-character suffix and PostgreSQL's 63-byte limit.
  if (recoveryId.length > 41)
    throw new Error("recoveryId must be at most 41 characters");
}

/** PostgreSQL replication-slot names have a 63-byte limit. */
export function replaySlotName(replayId: string): string {
  assertReplayId(replayId);
  return `event_store_replay_${replayId.replaceAll("-", "_")}`;
}

export function replayTopicName(replayId: string): string {
  assertReplayId(replayId);
  return `event-store.replay.${replayId}.v1`;
}

/** PostgreSQL replication-slot names have a 63-byte limit. */
export function recoverySlotName(recoveryId: string): string {
  assertRecoveryId(recoveryId);
  return `event_store_recovery_${recoveryId.replaceAll("-", "_")}`;
}

export function recoveryConnectorName(recoveryId: string): string {
  assertRecoveryId(recoveryId);
  return `event-store-recovery-${recoveryId}`;
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
            {
              name: "min.insync.replicas",
              value: String(Math.min(2, replicationFactor)),
            },
          ],
        },
      ],
    });
    const metadata = await admin.fetchTopicMetadata({ topics: [topic] });
    const partitions = metadata[0]?.partitions;
    if (partitions?.length !== kafkaPartitionCount)
      throw new Error(
        `replay topic ${topic} must have ${kafkaPartitionCount} partitions`,
      );
    const minInSyncReplicas = Math.min(2, replicationFactor);
    for (const partition of partitions) {
      if (partition.replicas.length !== replicationFactor)
        throw new Error(
          `replay topic ${topic}/${partition.partitionId} has RF=${partition.replicas.length}; expected ${replicationFactor}`,
        );
      if (partition.isr.length < minInSyncReplicas)
        throw new Error(
          `replay topic ${topic}/${partition.partitionId} has ISR=${partition.isr.length}; requires ${minInSyncReplicas}`,
        );
    }
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

function barrierAggregateId(replayId: string, partition: number): string {
  for (let attempts = 0; attempts < 10_000; attempts += 1) {
    const aggregateId = stableBarrierId(
      replayId,
      partition,
      `aggregate-${attempts}`,
    );
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

function stableBarrierId(
  replayId: string,
  partition: number,
  kind: string,
): string {
  const bytes = createHash("sha256")
    .update(`${kind}:${replayId}:${partition}`)
    .digest()
    .subarray(0, 16);
  bytes[6] = (bytes[6]! & 0x0f) | 0x70;
  bytes[8] = (bytes[8]! & 0x3f) | 0x80;
  const hex = bytes.toString("hex");
  return `${hex.slice(0, 8)}-${hex.slice(8, 12)}-${hex.slice(12, 16)}-${hex.slice(16, 20)}-${hex.slice(20)}`;
}

/** Appends exactly one partition-verified barrier event for each replay partition. */
export async function appendReplayBarriers(
  store: PostgresEventStore,
  replayId: string,
): Promise<ReplayBarrier[]> {
  assertReplayId(replayId);
  const barriers: ReplayBarrier[] = [];
  for (let partition = 0; partition < kafkaPartitionCount; partition += 1) {
    const aggregateId = barrierAggregateId(replayId, partition);
    const requestId = stableBarrierId(replayId, partition, "request");
    const result = await store.appendRecoveryBarrier(
      replayId,
      partition,
      aggregateId,
      requestId,
    );
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
       VALUES ($1,$2,'building',clock_timestamp())
       ON CONFLICT (projection_name,generation_id) DO NOTHING`,
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
    const slotName = replaySlotName(identity.replayId);
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
    record: ConsumedReplayRecord,
  ): Promise<void> {
    const partition = record.partition;
    if (!Number.isInteger(partition) || partition < 0 || partition >= 24)
      throw new RangeError("replay barrier partition must be 0..23");
    if (record.topic !== replayTopicName(identity.replayId))
      throw new Error("replay barrier came from the wrong Kafka topic");
    let offset: bigint;
    let envelope: { eventId?: unknown };
    try {
      offset = BigInt(record.offset);
      envelope = JSON.parse(record.value.toString()) as { eventId?: unknown };
    } catch {
      throw new Error("replay barrier is not a valid Kafka event record");
    }
    if (offset < 0n || typeof envelope.eventId !== "string")
      throw new Error("replay barrier is not a valid Kafka event record");
    const eventId = envelope.eventId;
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
      `INSERT INTO projection_runtime.replay_barriers(projection_name,generation_id,partition_no,event_id,topic_name,kafka_offset,processed_at)
       VALUES ($1,$2,$3,$4,$5,$6,clock_timestamp()) ON CONFLICT DO NOTHING`,
      [
        identity.projectionName,
        identity.generationId,
        partition,
        eventId,
        record.topic,
        offset.toString(),
      ],
    );
  }

  async activate(
    identity: ReplayIdentity,
    verification: ReplayVerification,
  ): Promise<void> {
    const [kafkaLag, checksums] = await Promise.all([
      verification.kafkaLag(),
      verification.checksums(),
    ]);
    if (kafkaLag !== 0n) throw new Error(`replay Kafka lag is ${kafkaLag}`);
    if (checksums.expected !== checksums.actual)
      throw new Error("replay checksum differs from full fold");
    const client = await this.pool.connect();
    try {
      await client.query("BEGIN");
      const barriers = await client.query<{
        count: number;
        topic_name: string | null;
        partition_no: number;
        kafka_offset: string | null;
      }>(
        `SELECT count(*)::int AS count FROM projection_runtime.replay_barriers
         WHERE projection_name=$1 AND generation_id=$2`,
        [identity.projectionName, identity.generationId],
      );
      if (barriers.rows[0]?.count !== 24)
        throw new Error("all 24 replay barriers must be processed");
      const barrierOffsets = await client.query<{
        topic_name: string | null;
        partition_no: number;
        kafka_offset: string | null;
      }>(
        `SELECT topic_name,partition_no,kafka_offset FROM projection_runtime.replay_barriers
         WHERE projection_name=$1 AND generation_id=$2 ORDER BY partition_no`,
        [identity.projectionName, identity.generationId],
      );
      await this.assertBarrierOffsetsCommitted(
        identity,
        verification.consumerGroupId,
        barrierOffsets.rows,
      );
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

  private async assertBarrierOffsetsCommitted(
    identity: ReplayIdentity,
    consumerGroupId: string,
    barriers: {
      topic_name: string | null;
      partition_no: number;
      kafka_offset: string | null;
    }[],
  ): Promise<void> {
    const topic = replayTopicName(identity.replayId);
    if (
      barriers.length !== kafkaPartitionCount ||
      barriers.some(
        (barrier) =>
          barrier.topic_name !== topic || barrier.kafka_offset === null,
      )
    )
      throw new Error("replay barriers lack Kafka delivery proof");
    const kafka = new KafkaJS.Kafka({ kafkaJS: { brokers: this.brokers } });
    const admin = kafka.admin();
    await admin.connect();
    try {
      const offsets = await admin.fetchOffsets({
        groupId: consumerGroupId,
        topics: [topic],
      });
      const committed = new Map(
        (offsets[0]?.partitions ?? []).map((partition) => [
          partition.partition,
          BigInt(partition.offset),
        ]),
      );
      for (const barrier of barriers) {
        const offset = BigInt(barrier.kafka_offset!);
        const position = committed.get(barrier.partition_no);
        if (position === undefined || position <= offset)
          throw new Error(
            `replay barrier ${barrier.partition_no} was not consumed and committed`,
          );
      }
    } finally {
      await admin.disconnect();
    }
  }

  async teardown(identity: ReplayIdentity): Promise<void> {
    const slotName = replaySlotName(identity.replayId);
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

/**
 * Performs the external half of slot-loss cutover before invoking the
 * database's durable barrier/reconciliation gate. This keeps arbitrary SQL
 * arguments from being treated as proof that Connect and Kafka are healthy.
 */
export class RecoveryCutoverCoordinator {
  constructor(
    private readonly pool: Pool,
    private readonly connectorUrl: string,
    private readonly brokers: string[],
  ) {}

  async activate(
    slotName: string,
    connectorName: string,
    walBudgetBytes: bigint,
    verification: RecoveryCutoverVerification,
  ): Promise<void> {
    if ((await verification.kafkaLag()) !== 0n)
      throw new Error("recovery projection Kafka lag is not zero");
    await Promise.all([
      this.assertConnectorDelivery(slotName, connectorName),
      this.assertConsumerGroup(verification),
    ]);
    const client = await this.pool.connect();
    try {
      await client.query("BEGIN");
      await client.query(
        "SELECT event_store.verify_recovery_cdc_cutover($1,$2,$3,$4,$5,$6)",
        [
          slotName,
          connectorName,
          verification.projectionName,
          verification.generationId,
          verification.replayId,
          verification.consumerGroupId,
        ],
      );
      await client.query(
        "SELECT event_store.activate_recovery_cdc_slot($1,$2,$3)",
        [slotName, connectorName, walBudgetBytes.toString()],
      );
      await client.query("COMMIT");
    } catch (error) {
      await client.query("ROLLBACK").catch(() => undefined);
      throw error;
    } finally {
      client.release();
    }
  }

  private async assertConnectorDelivery(
    slotName: string,
    connectorName: string,
  ): Promise<void> {
    const [statusResponse, configResponse] = await Promise.all([
      fetch(`${this.connectorUrl}/connectors/${connectorName}/status`),
      fetch(`${this.connectorUrl}/connectors/${connectorName}/config`),
    ]);
    if (!statusResponse.ok || !configResponse.ok)
      throw new Error("recovery connector status or config is unavailable");
    const [status, config] = await Promise.all([
      statusResponse.json() as Promise<{
        connector?: { state?: string };
        tasks?: { state?: string }[];
      }>,
      configResponse.json() as Promise<Record<string, string>>,
    ]);
    if (
      status.connector?.state !== "RUNNING" ||
      status.tasks?.length !== 1 ||
      status.tasks[0]?.state !== "RUNNING" ||
      config["connector.class"] !==
        "io.debezium.connector.postgresql.PostgresConnector" ||
      config["slot.name"] !== slotName ||
      config["table.include.list"] !== "event_store.events" ||
      config["slot.drop.on.stop"] !== "false" ||
      config["slot.failover"] !== "true" ||
      config["lsn.flush.mode"] !== "connector" ||
      config["offset.mismatch.strategy"] !== "trust_offset" ||
      config["errors.tolerance"] !== "none" ||
      config["transforms.outbox.table.field.event.payload"] !==
        "event_envelope_kafka" ||
      config["transforms.outbox.route.topic.replacement"] !== "$1.events.v1" ||
      config["transforms.outbox.table.op.invalid.behavior"] !== "fatal" ||
      config["exactly.once.support"] !== "required"
    )
      throw new Error(
        "recovery connector does not prove canonical Kafka delivery",
      );
  }

  private async assertConsumerGroup(
    verification: RecoveryCutoverVerification,
  ): Promise<void> {
    const barriers = await this.pool.query<{
      partition_no: number;
      kafka_offset: string;
    }>(
      `SELECT i.partition_no,i.kafka_offset::text
         FROM projection_runtime.inbox i
         JOIN event_store.events e ON e.event_id=i.event_id
        WHERE i.projection_name=$1 AND i.generation_id=$2
          AND e.event_name='system.replaybarrier'
          AND e.event_envelope->'payload'->>'replayId'=$3`,
      [
        verification.projectionName,
        verification.generationId,
        verification.replayId,
      ],
    );
    if (barriers.rows.length !== kafkaPartitionCount)
      throw new Error("recovery projection has not recorded every barrier");
    const kafka = new KafkaJS.Kafka({ kafkaJS: { brokers: this.brokers } });
    const admin = kafka.admin();
    await admin.connect();
    try {
      const offsets = await admin.fetchOffsets({
        groupId: verification.consumerGroupId,
        topics: ["event-store.events.v1"],
      });
      const partitions = offsets[0]?.partitions ?? [];
      const committed = new Map(
        partitions.map((partition) => [
          partition.partition,
          BigInt(partition.offset),
        ]),
      );
      if (
        barriers.rows.some((barrier) => {
          const offset = committed.get(barrier.partition_no);
          return offset === undefined || offset <= BigInt(barrier.kafka_offset);
        })
      )
        throw new Error(
          "recovery projection consumer group has not committed every barrier",
        );
    } finally {
      await admin.disconnect();
    }
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

/**
 * Rebuilds a lost CDC cursor from a durable table snapshot. The caller must
 * keep append admission closed until `RecoveryCutoverCoordinator.activate()`
 * verifies barriers, the consumer group, and the connector configuration.
 */
export async function startRecovery(
  options: RecoveryStartOptions,
): Promise<ReplayBarrier[]> {
  const coordinatorPool = new Pool({
    connectionString: options.coordinatorDatabaseUrl,
  });
  const appendPool = new Pool({ connectionString: options.appendDatabaseUrl });
  const slotName = recoverySlotName(options.identity.replayId);
  const connectorName = recoveryConnectorName(options.identity.replayId);
  const connectorConfig = recoveryConnectorConfig(
    options.identity.replayId,
    options.connectorDatabase,
  );
  try {
    await ensureRecoverySlot(coordinatorPool, slotName);
    await ensureRecoveryConnector(
      options.connectorUrl,
      connectorName,
      connectorConfig,
    );
    await waitForRecoveryConnector(
      coordinatorPool,
      options.connectorUrl,
      connectorName,
      slotName,
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

async function ensureRecoverySlot(pool: Pool, slotName: string): Promise<void> {
  const existing = await pool.query<{
    plugin: string;
    failover: boolean;
    temporary: boolean;
    invalidation_reason: string | null;
    restart_lsn: string | null;
  }>(
    `SELECT plugin,failover,temporary,invalidation_reason,restart_lsn::text
       FROM pg_replication_slots WHERE slot_name=$1`,
    [slotName],
  );
  if (existing.rows[0] === undefined) {
    try {
      await pool.query(
        "SELECT pg_create_logical_replication_slot($1, 'pgoutput', false, false, true)",
        [slotName],
      );
    } catch (error) {
      if ((error as { code?: string }).code !== "42710") throw error;
    }
  }
  const slot = await pool.query<{
    plugin: string;
    failover: boolean;
    temporary: boolean;
    invalidation_reason: string | null;
    restart_lsn: string | null;
  }>(
    `SELECT plugin,failover,temporary,invalidation_reason,restart_lsn::text
       FROM pg_replication_slots WHERE slot_name=$1`,
    [slotName],
  );
  const value = slot.rows[0];
  if (
    value === undefined ||
    value.plugin !== "pgoutput" ||
    value.failover !== true ||
    value.temporary !== false ||
    value.invalidation_reason !== null ||
    value.restart_lsn === null
  )
    throw new Error("existing recovery slot is not safe to resume");
}

function assertRecoveryConnectorConfig(
  actual: Record<string, string>,
  expected: Record<string, string>,
): void {
  for (const [key, value] of Object.entries(expected))
    if (actual[key] !== value)
      throw new Error("existing recovery connector configuration is unsafe");
}

async function ensureRecoveryConnector(
  connectorUrl: string,
  connectorName: string,
  config: Record<string, string>,
): Promise<void> {
  const existing = await fetch(
    `${connectorUrl}/connectors/${connectorName}/config`,
  );
  if (existing.ok) {
    assertRecoveryConnectorConfig(
      (await existing.json()) as Record<string, string>,
      config,
    );
    return;
  }
  if (existing.status !== 404)
    throw new Error(
      `recovery connector lookup failed: ${await existing.text()}`,
    );
  const created = await fetch(
    `${connectorUrl}/connectors/${connectorName}/config`,
    {
      method: "PUT",
      headers: { "content-type": "application/json" },
      body: JSON.stringify(config),
    },
  );
  if (!created.ok)
    throw new Error(
      `recovery connector creation failed: ${await created.text()}`,
    );
}

async function waitForRecoveryConnector(
  pool: Pool,
  connectorUrl: string,
  connectorName: string,
  slotName: string,
): Promise<void> {
  const deadline = Date.now() + 60_000;
  while (Date.now() < deadline) {
    const [response, slot] = await Promise.all([
      fetch(`${connectorUrl}/connectors/${connectorName}/status`),
      pool.query<{ active: boolean }>(
        "SELECT active FROM pg_replication_slots WHERE slot_name=$1",
        [slotName],
      ),
    ]);
    if (response.ok) {
      const status = (await response.json()) as {
        connector?: { state?: string };
        tasks?: { state?: string }[];
      };
      if (
        status.connector?.state === "RUNNING" &&
        status.tasks?.length === 1 &&
        status.tasks[0]?.state === "RUNNING" &&
        slot.rows[0]?.active === true
      )
        return;
    }
    await new Promise((resolve) => setTimeout(resolve, 200));
  }
  throw new Error(`recovery connector ${connectorName} did not become ready`);
}

/** Recovery CDC streams a full snapshot and then canonical live events. */
export function recoveryConnectorConfig(
  recoveryId: string,
  database: {
    hostname: string;
    port: number;
    user: string;
    password: string;
    dbname: string;
  },
): Record<string, string> {
  assertRecoveryId(recoveryId);
  const prefix = recoveryConnectorName(recoveryId);
  return {
    "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
    "tasks.max": "1",
    "database.hostname": database.hostname,
    "database.port": String(database.port),
    "database.user": database.user,
    "database.password": database.password,
    "database.dbname": database.dbname,
    "topic.prefix": prefix,
    "plugin.name": "pgoutput",
    "publication.name": "event_store_events",
    "publication.autocreate.mode": "disabled",
    "slot.name": recoverySlotName(recoveryId),
    "slot.drop.on.stop": "false",
    "slot.failover": "true",
    "schema.include.list": "event_store",
    "table.include.list": "event_store.events",
    "snapshot.mode": "initial",
    "snapshot.select.statement.overrides": "event_store.events",
    "snapshot.select.statement.overrides.event_store.events":
      "SELECT * FROM event_store.events ORDER BY event_number",
    "poll.interval.ms": "5",
    "lsn.flush.mode": "connector",
    "offset.mismatch.strategy": "trust_offset",
    "exactly.once.support": "required",
    "transaction.boundary": "poll",
    "errors.tolerance": "none",
    predicates: "isCanonicalEvents",
    "predicates.isCanonicalEvents.type":
      "org.apache.kafka.connect.transforms.predicates.TopicNameMatches",
    "predicates.isCanonicalEvents.pattern": `${prefix}\\.event_store\\.events`,
    transforms: "outbox",
    "transforms.outbox.type": "io.debezium.transforms.outbox.EventRouter",
    "transforms.outbox.predicate": "isCanonicalEvents",
    "transforms.outbox.table.field.event.id": "event_id",
    "transforms.outbox.table.field.event.key": "partition_key",
    "transforms.outbox.table.field.event.type": "event_name",
    "transforms.outbox.table.field.event.payload": "event_envelope_kafka",
    "transforms.outbox.table.field.event.timestamp": "recorded_at_kafka",
    "transforms.outbox.route.by.field": "topic_route",
    "transforms.outbox.route.topic.regex": "(.*)",
    "transforms.outbox.route.topic.replacement": "$1.events.v1",
    "transforms.outbox.table.expand.json.payload": "false",
    "transforms.outbox.table.op.invalid.behavior": "fatal",
    "transforms.outbox.table.fields.additional.placement":
      "event_id:header:id,event_name:header:type,envelope_sha256:header:envelopeHash,namespace:header:namespace,aggregate_type:header:aggregateType,stream_revision:header:streamRevision",
  };
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
  assertReplayId(replayId);
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
    "slot.name": replaySlotName(replayId),
    "slot.drop.on.stop": "false",
    "schema.include.list": "event_store",
    "table.include.list": "event_store.events",
    "snapshot.mode": "initial",
    "snapshot.select.statement.overrides": "event_store.events",
    "snapshot.select.statement.overrides.event_store.events":
      "SELECT * FROM event_store.events ORDER BY event_number",
    "exactly.once.support": "required",
    "transaction.boundary": "poll",
    transforms: "outbox",
    "transforms.outbox.type": "io.debezium.transforms.outbox.EventRouter",
    "transforms.outbox.table.field.event.id": "event_id",
    "transforms.outbox.table.field.event.key": "partition_key",
    "transforms.outbox.table.field.event.type": "event_name",
    "transforms.outbox.table.field.event.payload": "event_envelope_kafka",
    "transforms.outbox.table.field.event.timestamp": "recorded_at_kafka",
    "transforms.outbox.route.by.field": "topic_route",
    "transforms.outbox.route.topic.replacement": replayTopicName(replayId),
    "transforms.outbox.table.expand.json.payload": "false",
    "transforms.outbox.table.op.invalid.behavior": "fatal",
    "transforms.outbox.table.fields.additional.placement":
      "event_id:header:id,event_name:header:type,envelope_sha256:header:envelopeHash,namespace:header:namespace,aggregate_type:header:aggregateType,stream_revision:header:streamRevision",
  };
}

export async function loadReplayVerification(
  moduleSpecifier: string,
  identity: ReplayIdentity,
): Promise<ReplayVerification> {
  const module = (await import(
    moduleSpecifier
  )) as Partial<ReplayVerificationModule>;
  if (typeof module.createReplayVerification !== "function")
    throw new Error(
      "REPLAY_VERIFICATION_MODULE must export createReplayVerification(identity)",
    );
  const verification = await module.createReplayVerification(identity);
  if (
    typeof verification.consumerGroupId !== "string" ||
    verification.consumerGroupId.length === 0 ||
    typeof verification.kafkaLag !== "function" ||
    typeof verification.checksums !== "function"
  )
    throw new Error(
      "createReplayVerification(identity) returned an invalid replay verification adapter",
    );
  return verification;
}

if (process.argv[1] === fileURLToPath(import.meta.url)) {
  const action = process.env.REPLAY_ACTION;
  const replayId = process.env.REPLAY_ID;
  const coordinatorDatabaseUrl = process.env.REPLAY_COORDINATOR_DATABASE_URL;
  const connectorUrl = process.env.CONNECT_URL;
  const brokers = process.env.KAFKA_BROKERS?.split(",");
  const projectionName = process.env.REPLAY_PROJECTION_NAME;
  const generationId = process.env.REPLAY_GENERATION_ID;
  if (
    (action !== "start" &&
      action !== "activate" &&
      action !== "recovery-start" &&
      action !== "recovery-activate") ||
    replayId === undefined ||
    coordinatorDatabaseUrl === undefined ||
    connectorUrl === undefined ||
    brokers === undefined ||
    projectionName === undefined ||
    generationId === undefined
  )
    throw new Error(
      "REPLAY_ACTION=start|activate|recovery-start|recovery-activate, REPLAY_ID, REPLAY_COORDINATOR_DATABASE_URL, CONNECT_URL, KAFKA_BROKERS, REPLAY_PROJECTION_NAME and REPLAY_GENERATION_ID are required",
    );
  const identity = { replayId, projectionName, generationId };
  if (action === "start" || action === "recovery-start") {
    const raw = process.env.REPLAY_DATABASE_JSON;
    const appendDatabaseUrl = process.env.REPLAY_APPEND_DATABASE_URL;
    if (raw === undefined || appendDatabaseUrl === undefined)
      throw new Error(
        "REPLAY_DATABASE_JSON and REPLAY_APPEND_DATABASE_URL are required for REPLAY_ACTION=start",
      );
    const options = {
      identity,
      coordinatorDatabaseUrl,
      appendDatabaseUrl,
      connectorUrl,
      brokers,
      connectorDatabase: JSON.parse(raw) as Parameters<
        typeof replayConnectorConfig
      >[1],
    };
    const barriers =
      action === "start"
        ? await startReplay(options)
        : await startRecovery(options);
    console.log(JSON.stringify({ action, ...identity, barriers }, null, 2));
  } else {
    const verificationModule = process.env.REPLAY_VERIFICATION_MODULE;
    if (verificationModule === undefined)
      throw new Error(
        "REPLAY_VERIFICATION_MODULE is required for REPLAY_ACTION=activate",
      );
    const pool = new Pool({ connectionString: coordinatorDatabaseUrl });
    const coordinator = new ReplayCoordinator(pool, connectorUrl, brokers);
    try {
      const verification = await loadReplayVerification(
        verificationModule,
        identity,
      );
      if (action === "activate") {
        await coordinator.activate(identity, verification);
        await coordinator.teardown(identity);
      } else {
        const rawBudget = process.env.RECOVERY_WAL_BUDGET_BYTES;
        if (rawBudget === undefined)
          throw new Error(
            "RECOVERY_WAL_BUDGET_BYTES is required for REPLAY_ACTION=recovery-activate",
          );
        await new RecoveryCutoverCoordinator(
          pool,
          connectorUrl,
          brokers,
        ).activate(
          recoverySlotName(replayId),
          recoveryConnectorName(replayId),
          BigInt(rawBudget),
          {
            projectionName,
            generationId,
            replayId,
            consumerGroupId: verification.consumerGroupId,
            kafkaLag: verification.kafkaLag,
          },
        );
      }
    } finally {
      await pool.end();
    }
    console.log(
      JSON.stringify({ action, ...identity, status: "active" }, null, 2),
    );
  }
}
