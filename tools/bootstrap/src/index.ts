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
  databaseUrl: string;
  brokers: string[];
  connectUrl: string;
  connectorName: string;
  connectorConfig: Record<string, string>;
  topics: TopicDefinition[];
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
        status.tasks !== undefined &&
        status.tasks.length > 0 &&
        status.tasks.every((task) => task.state === "RUNNING")
      )
        return;
    }
    await new Promise((resolve) => setTimeout(resolve, 250));
  }
  throw new Error(`connector ${name} did not reach RUNNING within 60s`);
}

export async function bootstrap(options: BootstrapOptions): Promise<void> {
  await migrate(options.databaseUrl, options.createClusterRoles === true);
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
  } finally {
    await admin.disconnect();
  }
  const database = new Client({ connectionString: options.databaseUrl });
  await database.connect();
  try {
    const slot = await database.query<{ exists: boolean }>(
      "SELECT EXISTS(SELECT 1 FROM pg_replication_slots WHERE slot_name='event_store_live') AS exists",
    );
    if (!slot.rows[0]?.exists)
      await database.query(
        "SELECT pg_create_logical_replication_slot('event_store_live', 'pgoutput', false, false, true)",
      );
  } finally {
    await database.end();
  }
  const response = await fetch(
    `${options.connectUrl}/connectors/${options.connectorName}/config`,
    {
      method: "PUT",
      headers: { "content-type": "application/json" },
      body: JSON.stringify(options.connectorConfig),
    },
  );
  if (!response.ok)
    throw new Error(`connector registration failed: ${await response.text()}`);
  await waitForConnector(options.connectUrl, options.connectorName);
}

if (process.argv[1] === fileURLToPath(import.meta.url)) {
  const databaseUrl = process.env.DATABASE_URL;
  const brokers = process.env.KAFKA_BROKERS?.split(",");
  const connectUrl = process.env.CONNECT_URL;
  const connectorPath = process.env.CONNECTOR_CONFIG_PATH;
  const topicsPath = process.env.TOPICS_CONFIG_PATH;
  if (
    databaseUrl === undefined ||
    brokers === undefined ||
    connectUrl === undefined ||
    connectorPath === undefined ||
    topicsPath === undefined
  )
    throw new Error(
      "DATABASE_URL, KAFKA_BROKERS, CONNECT_URL, CONNECTOR_CONFIG_PATH and TOPICS_CONFIG_PATH are required",
    );
  const connector = JSON.parse(await readFile(connectorPath, "utf8")) as {
    name: string;
    config: Record<string, string>;
  };
  await bootstrap({
    databaseUrl,
    brokers,
    connectUrl,
    connectorName: connector.name,
    connectorConfig: connector.config,
    topics: JSON.parse(await readFile(topicsPath, "utf8")) as TopicDefinition[],
    createClusterRoles: process.env.MIGRATE_CLUSTER === "true",
  });
}
