import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { createHash } from "node:crypto";
import { spawn, type ChildProcess } from "node:child_process";
import { once } from "node:events";
import { fileURLToPath } from "node:url";
import { KafkaJS } from "@confluentinc/kafka-javascript";
import { canonicalJson, partitionKey, uuidv7 } from "@event-store/contracts";
import {
  KafkaProjectionRunner,
  ProjectionCheckpointStore,
  ProjectionCrashError,
  ProjectionFailureReporter,
  ProjectionTransactionRunner,
} from "@event-store/projection-runtime";
import { EventStoreStack } from "../fixtures/event-store-stack.js";
import type { Pool } from "pg";

const suite = process.env.RUN_INTEGRATION === "true" ? describe : describe.skip;

async function eventually(check: () => Promise<boolean>): Promise<void> {
  const deadline = Date.now() + 60_000;
  while (Date.now() < deadline) {
    if (await check()) return;
    await new Promise((resolve) => setTimeout(resolve, 100));
  }
  throw new Error("condition did not become true within 60 seconds");
}

async function waitForReady(child: ChildProcess): Promise<void> {
  await new Promise<void>((resolve, reject) => {
    let output = "";
    const timeout = setTimeout(
      () =>
        reject(new Error(`projection worker did not become ready: ${output}`)),
      30_000,
    );
    child.on("message", (message) => {
      output += String(message);
      if (message === "BOOTED") {
        clearTimeout(timeout);
        resolve();
      }
    });
    child.stderr?.on("data", (chunk: Buffer) => {
      output += chunk.toString();
    });
    child.once("exit", (code, signal) => {
      clearTimeout(timeout);
      reject(
        new Error(
          `projection worker exited before ready: code=${code ?? "null"} signal=${signal ?? "null"} ${output}`,
        ),
      );
    });
  });
}

async function stopWorker(child: ChildProcess): Promise<void> {
  if (child.exitCode !== null || child.signalCode !== null) return;
  const exited = once(child, "exit");
  child.kill("SIGKILL");
  await exited;
}

suite("projection consumer Kafka crash recovery", () => {
  const stack = new EventStoreStack();
  let pool: Pool;

  beforeAll(async () => {
    await stack.start({ cdc: true });
    pool = await stack.pool();
    await pool.query(
      "CREATE SCHEMA consumer_kafka_crash; CREATE TABLE consumer_kafka_crash.events(projection_name text NOT NULL,event_id uuid NOT NULL,PRIMARY KEY(projection_name,event_id)); CREATE TABLE consumer_kafka_crash.handler_calls(projection_name text NOT NULL,event_id uuid NOT NULL,calls integer NOT NULL,PRIMARY KEY(projection_name,event_id))",
    );
  }, 180_000);
  afterAll(async () => {
    await pool?.end();
    await stack.stop();
  }, 60_000);

  it.each([
    ["after_kafka_poll", 1],
    ["before_database_connection", 1],
    ["after_inbox_insert", 1],
    ["after_read_model_mutation", 1],
    ["after_checkpoint_update", 1],
    ["after_database_commit", 1],
    ["before_kafka_offset_commit", 1],
    ["after_kafka_offset_commit", 2],
  ] as const)(
    "restarts safely after %s",
    async (point, eventCount) => {
      const kafka = new KafkaJS.Kafka({
        kafkaJS: { brokers: [stack.kafkaBroker()] },
      });
      const topic = `consumer-crash-${uuidv7()}`;
      const admin = kafka.admin();
      await admin.connect();
      await admin.createTopics({
        topics: [{ topic, numPartitions: 1, replicationFactor: 1 }],
      });
      await admin.disconnect();
      const generationId = uuidv7();
      const projectionName = `crash-${point}`;
      const identity = { name: projectionName, generationId };
      await pool.query(
        "INSERT INTO projection_runtime.generations(projection_name,generation_id,status,created_at) VALUES ($1,$2,'building',clock_timestamp())",
        [projectionName, generationId],
      );
      let reached!: () => void;
      const crashed = new Promise<void>((resolve) => {
        reached = resolve;
      });
      const makeRunner = (crash = false) => {
        const crashBarrier = crash
          ? {
              hit: (hit: string) => {
                if (hit === point) {
                  reached();
                  throw new ProjectionCrashError(`crash at ${point}`);
                }
              },
            }
          : undefined;
        return new KafkaProjectionRunner(
          {
            brokers: [stack.kafkaBroker()],
            groupId: `consumer-crash-${generationId}`,
            topic,
          },
          new ProjectionTransactionRunner(
            pool,
            identity,
            (event) => event,
            crashBarrier,
          ),
          async (client, event) => {
            await client.query(
              "INSERT INTO consumer_kafka_crash.events(projection_name,event_id) VALUES ($1,$2)",
              [projectionName, event.eventId],
            );
          },
          new ProjectionCheckpointStore(pool, identity),
          new ProjectionFailureReporter(pool, identity),
          undefined,
          crashBarrier,
        );
      };
      let consumer = await makeRunner(true).start();
      const producer = kafka.producer({
        kafkaJS: { idempotent: true, acks: -1 },
      });
      await producer.connect();
      const aggregateId = uuidv7();
      const events = Array.from({ length: eventCount }, (_, index) => ({
        eventId: uuidv7(),
        namespace: "orders",
        aggregateType: "Order",
        aggregateId,
        streamRevision: String(index + 1),
        eventNumber: String(index + 1),
        eventName: "order.created",
        schemaVersion: 1,
        occurredAt: "2026-08-04T10:12:18.120Z",
        recordedAt: "2026-08-04T10:12:18.120Z",
        producerService: "orders-command",
        context: {
          requestId: uuidv7(),
          correlationId: uuidv7(),
          causationId: null,
          actor: { kind: "service" as const, subjectRef: "consumer-crash" },
        },
        payload: { point, index },
      }));
      try {
        await producer.send({
          topic,
          messages: events.map((event) => {
            const value = canonicalJson(event);
            return {
              key: partitionKey(event),
              value,
              headers: {
                id: event.eventId,
                type: event.eventName,
                envelopeHash: createHash("sha256").update(value).digest("hex"),
                namespace: event.namespace,
                aggregateType: event.aggregateType,
                streamRevision: event.streamRevision,
              },
            };
          }),
        });
        await crashed;
        await consumer.disconnect().catch(() => undefined);
        consumer = await makeRunner().start();
        await eventually(async () => {
          const result = await pool.query<{ count: number }>(
            "SELECT count(*)::int AS count FROM consumer_kafka_crash.events WHERE projection_name=$1",
            [projectionName],
          );
          return result.rows[0]?.count === eventCount;
        });
        const checkpoint = await new ProjectionCheckpointStore(
          pool,
          identity,
        ).nextOffset(topic, 0);
        expect(checkpoint).toBe(BigInt(eventCount));
      } finally {
        await producer.disconnect().catch(() => undefined);
        await consumer.disconnect().catch(() => undefined);
      }
    },
    90_000,
  );

  it("refuses startup when Kafka retention removed the durable checkpoint", async () => {
    const kafka = new KafkaJS.Kafka({
      kafkaJS: { brokers: [stack.kafkaBroker()] },
    });
    const topic = `consumer-retention-${uuidv7()}`;
    const admin = kafka.admin();
    const producer = kafka.producer({
      kafkaJS: { idempotent: true, acks: -1 },
    });
    const generationId = uuidv7();
    const identity = { name: `retention-${generationId}`, generationId };
    const event = {
      eventId: uuidv7(),
      namespace: "orders",
      aggregateType: "Order",
      aggregateId: uuidv7(),
      streamRevision: "1",
      eventNumber: "1",
      eventName: "order.created",
      schemaVersion: 1,
      occurredAt: "2026-08-04T10:12:18.120Z",
      recordedAt: "2026-08-04T10:12:18.120Z",
      producerService: "retention-test",
      context: {
        requestId: uuidv7(),
        correlationId: uuidv7(),
        causationId: null,
        actor: { kind: "service" as const, subjectRef: "retention-test" },
      },
      payload: { kind: "retention" },
    };
    const value = canonicalJson(event);
    try {
      await admin.connect();
      await admin.createTopics({
        topics: [{ topic, numPartitions: 1, replicationFactor: 1 }],
      });
      await producer.connect();
      await producer.send({
        topic,
        messages: [
          {
            key: partitionKey(event),
            value,
            headers: {
              id: event.eventId,
              type: event.eventName,
              envelopeHash: createHash("sha256").update(value).digest("hex"),
              namespace: event.namespace,
              aggregateType: event.aggregateType,
              streamRevision: event.streamRevision,
            },
          },
        ],
      });
      await admin.deleteTopicRecords({
        topic,
        partitions: [{ partition: 0, offset: "1" }],
      });
      await eventually(async () => {
        const offsets = await admin.fetchTopicOffsets(topic, {
          isolationLevel: KafkaJS.IsolationLevel.READ_COMMITTED,
        });
        return offsets[0]?.low === "1";
      });
      await pool.query(
        "INSERT INTO projection_runtime.generations(projection_name,generation_id,status,created_at) VALUES ($1,$2,'building',clock_timestamp())",
        [identity.name, identity.generationId],
      );
      await pool.query(
        `INSERT INTO projection_runtime.checkpoints(projection_name,generation_id,topic_name,partition_no,next_offset,updated_at)
         VALUES ($1,$2,$3,0,0,clock_timestamp())`,
        [identity.name, identity.generationId, topic],
      );
      const runner = new KafkaProjectionRunner(
        {
          brokers: [stack.kafkaBroker()],
          groupId: `consumer-retention-${generationId}`,
          topic,
        },
        new ProjectionTransactionRunner(pool, identity, (stored) => stored),
        async () => undefined,
        new ProjectionCheckpointStore(pool, identity),
        new ProjectionFailureReporter(pool, identity),
      );
      await expect(runner.start()).rejects.toThrow("below Kafka low watermark");
    } finally {
      await producer.disconnect().catch(() => undefined);
      await admin.disconnect().catch(() => undefined);
    }
  }, 90_000);

  it("rewinds a Kafka group offset ahead of the durable PostgreSQL checkpoint", async () => {
    const kafka = new KafkaJS.Kafka({
      kafkaJS: { brokers: [stack.kafkaBroker()] },
    });
    const topic = `consumer-offset-ahead-${uuidv7()}`;
    const groupId = `consumer-offset-ahead-${uuidv7()}`;
    const generationId = uuidv7();
    const identity = { name: `offset-ahead-${generationId}`, generationId };
    const admin = kafka.admin();
    const producer = kafka.producer({
      kafkaJS: { idempotent: true, acks: -1 },
    });
    let runnerConsumer: KafkaJS.Consumer | undefined;
    let seedConsumer: KafkaJS.Consumer | undefined;
    try {
      await admin.connect();
      await admin.createTopics({
        topics: [{ topic, numPartitions: 1, replicationFactor: 1 }],
      });
      await producer.connect();
      const events = Array.from({ length: 2 }, (_, index) => {
        const event = {
          eventId: uuidv7(),
          namespace: "orders",
          aggregateType: "Order",
          aggregateId: uuidv7(),
          streamRevision: "1",
          eventNumber: String(index + 1),
          eventName: "order.created",
          schemaVersion: 1,
          occurredAt: "2026-08-04T10:12:18.120Z",
          recordedAt: "2026-08-04T10:12:18.120Z",
          producerService: "offset-ahead-test",
          context: {
            requestId: uuidv7(),
            correlationId: uuidv7(),
            causationId: null,
            actor: { kind: "service" as const, subjectRef: "offset-ahead" },
          },
          payload: { index },
        };
        const value = canonicalJson(event);
        return {
          event,
          message: {
            key: partitionKey(event),
            value,
            headers: {
              id: event.eventId,
              type: event.eventName,
              envelopeHash: createHash("sha256").update(value).digest("hex"),
              namespace: event.namespace,
              aggregateType: event.aggregateType,
              streamRevision: event.streamRevision,
            },
          },
        };
      });
      await producer.send({
        topic,
        messages: events.map(({ message }) => message),
      });
      await pool.query(
        "INSERT INTO projection_runtime.generations(projection_name,generation_id,status,created_at) VALUES ($1,$2,'building',clock_timestamp())",
        [identity.name, identity.generationId],
      );
      // Simulate an old process that committed Kafka farther than the DB
      // transaction/checkpoint it was supposed to commit with.
      seedConsumer = kafka.consumer({
        kafkaJS: { groupId, autoCommit: false, fromBeginning: true },
      });
      await seedConsumer.connect();
      await seedConsumer.subscribe({ topics: [topic], replace: true });
      await seedConsumer.run({ eachBatch: async () => undefined });
      await eventually(async () => seedConsumer!.assignment().length === 1);
      await seedConsumer.commitOffsets([{ topic, partition: 0, offset: "2" }]);
      await seedConsumer.disconnect();
      seedConsumer = undefined;
      const runner = new KafkaProjectionRunner(
        { brokers: [stack.kafkaBroker()], groupId, topic },
        new ProjectionTransactionRunner(pool, identity, (event) => event),
        async (client, event) => {
          await client.query(
            "INSERT INTO consumer_kafka_crash.events(projection_name,event_id) VALUES ($1,$2)",
            [identity.name, event.eventId],
          );
        },
        new ProjectionCheckpointStore(pool, identity),
        new ProjectionFailureReporter(pool, identity),
      );
      runnerConsumer = await runner.start();
      await eventually(async () => {
        const result = await pool.query<{ count: number }>(
          "SELECT count(*)::int AS count FROM consumer_kafka_crash.events WHERE projection_name=$1",
          [identity.name],
        );
        return result.rows[0]?.count === events.length;
      });
      await expect(
        new ProjectionCheckpointStore(pool, identity).nextOffset(topic, 0),
      ).resolves.toBe(2n);
    } finally {
      await runnerConsumer?.disconnect().catch(() => undefined);
      await seedConsumer?.disconnect().catch(() => undefined);
      await producer.disconnect().catch(() => undefined);
      await admin.disconnect().catch(() => undefined);
    }
  }, 90_000);

  it("aborts an open projection transaction during consumer-group rebalance", async () => {
    const kafka = new KafkaJS.Kafka({
      kafkaJS: { brokers: [stack.kafkaBroker()] },
    });
    const topic = `consumer-rebalance-${uuidv7()}`;
    const admin = kafka.admin();
    await admin.connect();
    await admin.createTopics({
      topics: [{ topic, numPartitions: 1, replicationFactor: 1 }],
    });
    await admin.disconnect();
    const generationId = uuidv7();
    const projectionName = `rebalance-${generationId}`;
    const identity = { name: projectionName, generationId };
    await pool.query(
      "INSERT INTO projection_runtime.generations(projection_name,generation_id,status,created_at) VALUES ($1,$2,'building',clock_timestamp())",
      [projectionName, generationId],
    );
    let entered!: () => void;
    let aborted!: () => void;
    const handlerEntered = new Promise<void>((resolve) => {
      entered = resolve;
    });
    const handlerAborted = new Promise<void>((resolve) => {
      aborted = resolve;
    });
    const blockedRunner = new KafkaProjectionRunner(
      {
        brokers: [stack.kafkaBroker()],
        groupId: `consumer-rebalance-${generationId}`,
        topic,
        partitionAssignors: [KafkaJS.PartitionAssignors.range],
      },
      new ProjectionTransactionRunner(pool, identity, (event) => event),
      async (_client, _event, signal) => {
        entered();
        await new Promise<void>((_resolve, reject) => {
          signal.addEventListener(
            "abort",
            () => {
              aborted();
              reject(new Error("projection handler aborted on rebalance"));
            },
            { once: true },
          );
        });
      },
      new ProjectionCheckpointStore(pool, identity),
      new ProjectionFailureReporter(pool, identity),
    );
    const replacementRunner = new KafkaProjectionRunner(
      {
        brokers: [stack.kafkaBroker()],
        groupId: `consumer-rebalance-${generationId}`,
        topic,
        partitionAssignors: [KafkaJS.PartitionAssignors.range],
      },
      new ProjectionTransactionRunner(pool, identity, (event) => event),
      async (client, event) => {
        await client.query(
          "INSERT INTO consumer_kafka_crash.events(projection_name,event_id) VALUES ($1,$2)",
          [projectionName, event.eventId],
        );
        await client.query(
          `INSERT INTO consumer_kafka_crash.handler_calls(projection_name,event_id,calls)
           VALUES ($1,$2,1)
           ON CONFLICT (projection_name,event_id) DO UPDATE SET calls=consumer_kafka_crash.handler_calls.calls+1`,
          [projectionName, event.eventId],
        );
      },
      new ProjectionCheckpointStore(pool, identity),
      new ProjectionFailureReporter(pool, identity),
    );
    const producer = kafka.producer({
      kafkaJS: { idempotent: true, acks: -1 },
    });
    let blockedConsumer: KafkaJS.Consumer | undefined;
    let replacementConsumer: KafkaJS.Consumer | undefined;
    try {
      blockedConsumer = await blockedRunner.start();
      await producer.connect();
      const event = {
        eventId: uuidv7(),
        namespace: "orders",
        aggregateType: "Order",
        aggregateId: uuidv7(),
        streamRevision: "1",
        eventNumber: "1",
        eventName: "order.created",
        schemaVersion: 1,
        occurredAt: "2026-08-04T10:12:18.120Z",
        recordedAt: "2026-08-04T10:12:18.120Z",
        producerService: "orders-command",
        context: {
          requestId: uuidv7(),
          correlationId: uuidv7(),
          causationId: null,
          actor: { kind: "service" as const, subjectRef: "rebalance-test" },
        },
        payload: { kind: "rebalance" },
      };
      const value = canonicalJson(event);
      await producer.send({
        topic,
        messages: [
          {
            key: partitionKey(event),
            value,
            headers: {
              id: event.eventId,
              type: event.eventName,
              envelopeHash: createHash("sha256").update(value).digest("hex"),
              namespace: event.namespace,
              aggregateType: event.aggregateType,
              streamRevision: event.streamRevision,
            },
          },
        ],
      });
      await handlerEntered;
      const replacementStart = replacementRunner.start();
      const started = Date.now();
      // A second member causes an actual eager group rebalance. The original
      // process remains connected: its open DB transaction must be cancelled
      // by the 10-second transaction deadline, then the replacement owns p0.
      await expect(
        Promise.race([
          handlerAborted,
          new Promise<void>((_, reject) =>
            setTimeout(
              () => reject(new Error("rebalance did not abort transaction")),
              12_000,
            ),
          ),
        ]),
      ).resolves.toBeUndefined();
      expect(Date.now() - started).toBeGreaterThanOrEqual(9_500);
      expect(Date.now() - started).toBeLessThanOrEqual(12_000);
      replacementConsumer = await replacementStart;
      await eventually(
        async () =>
          replacementConsumer
            ?.assignment()
            .some(
              (assignment) =>
                assignment.topic === topic && assignment.partition === 0,
            ) ?? false,
      );
      expect(blockedConsumer.assignment()).not.toContainEqual({
        topic,
        partition: 0,
      });
      await eventually(async () => {
        const result = await pool.query<{ count: number }>(
          "SELECT count(*)::int AS count FROM consumer_kafka_crash.events WHERE projection_name=$1",
          [projectionName],
        );
        return result.rows[0]?.count === 1;
      });
      await expect(
        pool.query<{ calls: number }>(
          "SELECT calls FROM consumer_kafka_crash.handler_calls WHERE projection_name=$1 AND event_id=$2",
          [projectionName, event.eventId],
        ),
      ).resolves.toMatchObject({ rows: [{ calls: 1 }] });
      await expect(
        new ProjectionCheckpointStore(pool, identity).nextOffset(topic, 0),
      ).resolves.toBe(1n);
    } finally {
      await producer.disconnect().catch(() => undefined);
      await blockedConsumer?.disconnect().catch(() => undefined);
      await replacementConsumer?.disconnect().catch(() => undefined);
    }
  }, 90_000);

  it.each([
    "after_kafka_poll",
    "before_database_connection",
    "after_inbox_insert",
    "after_read_model_mutation",
    "after_checkpoint_update",
    "after_database_commit",
    "before_kafka_offset_commit",
    "after_kafka_offset_commit",
  ] as const)(
    "recovers after a real process SIGKILL at %s",
    async (point) => {
      const kafka = new KafkaJS.Kafka({
        kafkaJS: { brokers: [stack.kafkaBroker()] },
      });
      const topic = `consumer-process-crash-${uuidv7()}`;
      const admin = kafka.admin();
      await admin.connect();
      await admin.createTopics({
        topics: [{ topic, numPartitions: 1, replicationFactor: 1 }],
      });
      await admin.disconnect();
      const generationId = uuidv7();
      const projectionName = `process-crash-${point}`;
      await pool.query(
        "INSERT INTO projection_runtime.generations(projection_name,generation_id,status,created_at) VALUES ($1,$2,'building',clock_timestamp())",
        [projectionName, generationId],
      );
      const worker = (crashPoint?: string): ChildProcess =>
        spawn(
          process.execPath,
          [
            fileURLToPath(
              new URL(
                "../fixtures/projection-crash-worker.mjs",
                import.meta.url,
              ),
            ),
          ],
          {
            env: {
              ...process.env,
              DATABASE_URL: stack.databaseUrl,
              KAFKA_BROKER: stack.kafkaBroker(),
              TOPIC: topic,
              GROUP_ID: `consumer-process-crash-${generationId}`,
              PROJECTION_NAME: projectionName,
              GENERATION_ID: generationId,
              ...(crashPoint === undefined ? {} : { CRASH_POINT: crashPoint }),
            },
            stdio: ["ignore", "pipe", "pipe", "ipc"],
          },
        );
      const crashing = worker(point);
      const crashingReady = waitForReady(crashing);
      let recovering: ChildProcess | undefined;
      let recoveringReady: Promise<void> | undefined;
      const producer = kafka.producer({
        kafkaJS: { idempotent: true, acks: -1 },
      });
      await producer.connect();
      const aggregateId = uuidv7();
      const event = {
        eventId: uuidv7(),
        namespace: "orders",
        aggregateType: "Order",
        aggregateId,
        streamRevision: "1",
        eventNumber: "1",
        eventName: "order.created",
        schemaVersion: 1,
        occurredAt: "2026-08-04T10:12:18.120Z",
        recordedAt: "2026-08-04T10:12:18.120Z",
        producerService: "orders-command",
        context: {
          requestId: uuidv7(),
          correlationId: uuidv7(),
          causationId: null,
          actor: { kind: "service" as const, subjectRef: "consumer-crash" },
        },
        payload: { point },
      };
      try {
        await crashingReady;
        const value = canonicalJson(event);
        const crashed = once(crashing, "exit");
        await producer.send({
          topic,
          messages: [
            {
              key: partitionKey(event),
              value,
              headers: {
                id: event.eventId,
                type: event.eventName,
                envelopeHash: createHash("sha256").update(value).digest("hex"),
                namespace: event.namespace,
                aggregateType: event.aggregateType,
                streamRevision: event.streamRevision,
              },
            },
          ],
        });
        const [code, signal] = await crashed;
        expect(code).toBeNull();
        expect(signal).toBe("SIGKILL");
        recovering = worker();
        recoveringReady = waitForReady(recovering);
        await recoveringReady;
        await eventually(async () => {
          const result = await pool.query<{ count: number }>(
            "SELECT count(*)::int AS count FROM consumer_kafka_crash.events WHERE projection_name=$1",
            [projectionName],
          );
          return result.rows[0]?.count === 1;
        });
        const checkpoint = await new ProjectionCheckpointStore(pool, {
          name: projectionName,
          generationId,
        }).nextOffset(topic, 0);
        expect(checkpoint).toBe(1n);
        await expect(
          pool.query<{ calls: number }>(
            "SELECT calls FROM consumer_kafka_crash.handler_calls WHERE projection_name=$1 AND event_id=$2",
            [projectionName, event.eventId],
          ),
        ).resolves.toMatchObject({ rows: [{ calls: 1 }] });
      } finally {
        await stopWorker(crashing).catch(() => undefined);
        if (recovering !== undefined)
          await stopWorker(recovering).catch(() => undefined);
        await Promise.race([
          producer.disconnect().catch(() => undefined),
          new Promise<void>((resolve) => setTimeout(resolve, 5_000)),
        ]);
      }
    },
    120_000,
  );
});
