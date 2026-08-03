import { fileURLToPath } from "node:url";
import type { Pool } from "pg";

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
