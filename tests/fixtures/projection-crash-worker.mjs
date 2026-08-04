import { Pool } from "pg";
import process from "node:process";
import {
  KafkaProjectionRunner,
  ProjectionCheckpointStore,
  ProjectionFailureReporter,
  ProjectionTransactionRunner,
} from "../../packages/projection-runtime/dist/index.js";

const required = (name) => {
  const value = process.env[name];
  if (value === undefined) throw new Error(`${name} is required`);
  return value;
};

const pool = new Pool({ connectionString: required("DATABASE_URL") });
const identity = {
  name: required("PROJECTION_NAME"),
  generationId: required("GENERATION_ID"),
};
const crashPoint = process.env.CRASH_POINT;
const crashBarrier =
  crashPoint === undefined
    ? undefined
    : {
        hit: (point) => {
          if (point === crashPoint) process.kill(process.pid, "SIGKILL");
        },
      };
const runner = new KafkaProjectionRunner(
  {
    brokers: [required("KAFKA_BROKER")],
    groupId: required("GROUP_ID"),
    topic: required("TOPIC"),
  },
  new ProjectionTransactionRunner(
    pool,
    identity,
    (event) => event,
    crashBarrier,
  ),
  async (client, event) => {
    await client.query(
      "INSERT INTO consumer_kafka_crash.events(projection_name,event_id) VALUES ($1,$2) ON CONFLICT DO NOTHING",
      [identity.name, event.eventId],
    );
  },
  new ProjectionCheckpointStore(pool, identity),
  new ProjectionFailureReporter(pool, identity),
  undefined,
  crashBarrier,
);

process.send?.("BOOTED");
const consumer = await runner.start();
process.send?.("READY");

const stop = async () => {
  await consumer.disconnect().catch(() => undefined);
  await pool.end().catch(() => undefined);
  process.exit(0);
};
process.once("SIGTERM", () => void stop());
process.once("SIGINT", () => void stop());
await new Promise(() => undefined);
