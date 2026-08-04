import { readFile } from "node:fs/promises";
import { fileURLToPath } from "node:url";
import { KafkaJS } from "@confluentinc/kafka-javascript";
import { migrate } from "@event-store/migrate";
import { Client } from "pg";

interface TopicDefinition {
  name: string;
  partitions: number;
  replicationFactor: number;
  configs: Record<string, string>;
}

export interface BootstrapOptions {
  /** Used only for DDL and schema migrations (event_store_migrator). */
  migrationDatabaseUrl: string;
  /** Used only for logical-slot and publication readiness checks (event_store_cdc). */
  replicationDatabaseUrl: string;
  brokers: string[];
  connectUrl: string;
  connectorName: string;
  connectorConfig: Record<string, string>;
  topics: TopicDefinition[];
  walBudgetBytes: bigint;
  createClusterRoles?: boolean;
}

async function waitForConnector(url: string, name: string): Promise<void> {
  const deadline = Date.now() + 60_000;
  while (Date.now() < deadline) {
    const response = await fetch(`${url}/connectors/${name}/status`);
    if (response.ok) {
      const status = (await response.json()) as {
        connector?: { state?: string };
        tasks?: { state?: string }[];
      };
      if (
        status.connector?.state === "RUNNING" &&
        status.tasks?.length === 1 &&
        status.tasks[0]?.state === "RUNNING"
      )
        return;
    }
    await new Promise((resolve) => setTimeout(resolve, 250));
  }
  throw new Error(`connector ${name} did not reach RUNNING within 60s`);
}

async function verifyTopics(
  admin: KafkaJS.Admin,
  topics: TopicDefinition[],
): Promise<void> {
  const metadata = await admin.fetchTopicMetadata({
    topics: topics.map((topic) => topic.name),
  });
  const actual = new Map(metadata.map((topic) => [topic.name, topic]));
  for (const expected of topics) {
    const topic = actual.get(expected.name);
    if (topic === undefined)
      throw new Error(`Kafka topic was not created: ${expected.name}`);
    if (topic.partitions.length !== expected.partitions)
      throw new Error(
        `Kafka topic ${expected.name} has ${topic.partitions.length} partitions; expected ${expected.partitions}`,
      );
    for (const partition of topic.partitions) {
      if (partition.replicas.length !== expected.replicationFactor)
        throw new Error(
          `Kafka topic ${expected.name}/${partition.partitionId} has RF=${partition.replicas.length}; expected ${expected.replicationFactor}`,
        );
      const minInSyncReplicas = Number(expected.configs["min.insync.replicas"]);
      if (
        Number.isInteger(minInSyncReplicas) &&
        partition.isr.length < minInSyncReplicas
      )
        throw new Error(
          `Kafka topic ${expected.name}/${partition.partitionId} has ISR=${partition.isr.length}; requires ${minInSyncReplicas}`,
        );
    }
  }
}

async function verifyCdcReadiness(
  database: Client,
  slotName: string,
): Promise<void> {
  const publication = await database.query<{
    schema_name: string;
    table_name: string;
  }>(
    "SELECT schemaname AS schema_name, tablename AS table_name FROM pg_publication_tables WHERE pubname = 'event_store_events' ORDER BY schemaname, tablename",
  );
  if (
    publication.rows.length !== 1 ||
    publication.rows[0]?.schema_name !== "event_store" ||
    publication.rows[0]?.table_name !== "events"
  )
    throw new Error(
      "event_store_events publication must contain only event_store.events",
    );

  const slot = await database.query<{
    exists: boolean;
    active: boolean;
    failover: boolean;
    invalidationReason: string | null;
  }>(
    'SELECT true AS exists, active, failover, invalidation_reason AS "invalidationReason" FROM pg_replication_slots WHERE slot_name = $1',
    [slotName],
  );
  const row = slot.rows[0];
  if (row === undefined || row.invalidationReason !== null)
    throw new Error(`${slotName} logical replication slot is unavailable`);
  if (!row.active)
    throw new Error(`${slotName} logical replication slot is not active`);
  if (!row.failover)
    throw new Error(
      `${slotName} logical replication slot is not failover-enabled`,
    );
}

export async function bootstrap(options: BootstrapOptions): Promise<void> {
  await migrate(
    options.migrationDatabaseUrl,
    options.createClusterRoles === true,
  );
  const kafka = new KafkaJS.Kafka({ kafkaJS: { brokers: options.brokers } });
  const admin = kafka.admin();
  await admin.connect();
  try {
    await admin.createTopics({
      topics: options.topics.map((topic) => ({
        topic: topic.name,
        numPartitions: topic.partitions,
        replicationFactor: topic.replicationFactor,
        configEntries: Object.entries(topic.configs).map(([name, value]) => ({
          name,
          value,
        })),
      })),
    });
    await verifyTopics(admin, options.topics);
  } finally {
    await admin.disconnect();
  }
  const database = new Client({
    connectionString: options.replicationDatabaseUrl,
  });
  await database.connect();
  try {
    const runtime = await database.query<{
      cdc_slot_name: string;
      cdc_connector_name: string;
    }>(
      "SELECT cdc_slot_name,cdc_connector_name FROM event_store.runtime_config WHERE singleton",
    );
    const cdcSlotName = runtime.rows[0]?.cdc_slot_name ?? "event_store_live";
    const cdcConnectorName =
      runtime.rows[0]?.cdc_connector_name ?? options.connectorName;
    const adoptedRecovery =
      cdcSlotName !== "event_store_live" ||
      cdcConnectorName !== options.connectorName;
    const slot = await database.query<{ exists: boolean }>(
      "SELECT EXISTS(SELECT 1 FROM pg_replication_slots WHERE slot_name=$1) AS exists",
      [cdcSlotName],
    );
    if (!slot.rows[0]?.exists && adoptedRecovery) {
      throw new Error(
        `adopted recovery slot ${cdcSlotName} is missing; refusing to replace it`,
      );
    }
    if (!slot.rows[0]?.exists) {
      try {
        await database.query(
          "SELECT pg_create_logical_replication_slot($1, 'pgoutput', false, false, true)",
          [cdcSlotName],
        );
      } catch (error) {
        if ((error as { code?: string }).code !== "42710") throw error;
      }
    }
  } finally {
    await database.end();
  }
  const ownership = new Client({
    connectionString: options.replicationDatabaseUrl,
  });
  await ownership.connect();
  let cdcSlotName: string;
  let cdcConnectorName: string;
  try {
    const runtime = await ownership.query<{
      cdc_slot_name: string;
      cdc_connector_name: string;
    }>(
      "SELECT cdc_slot_name,cdc_connector_name FROM event_store.runtime_config WHERE singleton",
    );
    cdcSlotName = runtime.rows[0]?.cdc_slot_name ?? "event_store_live";
    cdcConnectorName =
      runtime.rows[0]?.cdc_connector_name ?? options.connectorName;
  } finally {
    await ownership.end();
  }
  if (cdcConnectorName === options.connectorName) {
    const response = await fetch(
      `${options.connectUrl}/connectors/${options.connectorName}/config`,
      {
        method: "PUT",
        headers: { "content-type": "application/json" },
        body: JSON.stringify(options.connectorConfig),
      },
    );
    if (!response.ok)
      throw new Error(
        `connector registration failed: ${await response.text()}`,
      );
  }
  await waitForConnector(options.connectUrl, cdcConnectorName);
  const readinessDatabase = new Client({
    connectionString: options.replicationDatabaseUrl,
  });
  await readinessDatabase.connect();
  try {
    await verifyCdcReadiness(readinessDatabase, cdcSlotName);
    // Append admission is the final rollout step: the logical slot and the
    // selected connector have already proved that they are active.
    await readinessDatabase.query(
      "SELECT event_store.enable_append_admission($1)",
      [options.walBudgetBytes.toString()],
    );
  } finally {
    await readinessDatabase.end();
  }
}

if (process.argv[1] === fileURLToPath(import.meta.url)) {
  const migrationDatabaseUrl = process.env.MIGRATION_DATABASE_URL;
  const replicationDatabaseUrl = process.env.REPLICATION_DATABASE_URL;
  const brokers = process.env.KAFKA_BROKERS?.split(",");
  const connectUrl = process.env.CONNECT_URL;
  const connectorPath = process.env.CONNECTOR_CONFIG_PATH;
  const topicsPath = process.env.TOPICS_CONFIG_PATH;
  const walBudget = process.env.CDC_WAL_BUDGET_BYTES;
  if (
    migrationDatabaseUrl === undefined ||
    replicationDatabaseUrl === undefined ||
    brokers === undefined ||
    connectUrl === undefined ||
    connectorPath === undefined ||
    topicsPath === undefined ||
    walBudget === undefined ||
    !/^\d+$/.test(walBudget) ||
    BigInt(walBudget) <= 0n
  )
    throw new Error(
      "MIGRATION_DATABASE_URL, REPLICATION_DATABASE_URL, KAFKA_BROKERS, CONNECT_URL, CONNECTOR_CONFIG_PATH, TOPICS_CONFIG_PATH and positive CDC_WAL_BUDGET_BYTES are required",
    );
  const connector = JSON.parse(await readFile(connectorPath, "utf8")) as {
    name: string;
    config: Record<string, string>;
  };
  await bootstrap({
    migrationDatabaseUrl,
    replicationDatabaseUrl,
    brokers,
    connectUrl,
    connectorName: connector.name,
    connectorConfig: connector.config,
    topics: JSON.parse(await readFile(topicsPath, "utf8")) as TopicDefinition[],
    walBudgetBytes: BigInt(walBudget),
    createClusterRoles: process.env.MIGRATE_CLUSTER === "true",
  });
}
