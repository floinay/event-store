import { fileURLToPath } from "node:url";

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
    "transforms.outbox.table.field.event.payload": "event_envelope",
    "transforms.outbox.route.by.field": "topic_route",
    "transforms.outbox.route.topic.replacement": `event-store.replay.${replayId}.v1`,
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
