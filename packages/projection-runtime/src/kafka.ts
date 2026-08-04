import { KafkaJS } from "@confluentinc/kafka-javascript";
import type {
  ProjectionCheckpointStore,
  ProjectionFailureReporter,
  ProjectionHandler,
  ProjectionTransactionRunner,
} from "./index.js";
import { projectionRetryDelaysMs } from "./index.js";

export interface KafkaProjectionConfig {
  brokers: string[];
  groupId: string;
  topic: string;
}

function rawEnvelope(value: Buffer): unknown {
  try {
    return JSON.parse(value.toString());
  } catch {
    return { rawBase64: value.toString("base64") };
  }
}

function wait(milliseconds: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, milliseconds));
}

async function withHeartbeats<T>(
  work: () => Promise<T>,
  heartbeat: () => Promise<void>,
): Promise<T> {
  let finished = false;
  let heartbeatError: unknown;
  const pulse = (async () => {
    while (!finished) {
      await wait(1_000);
      if (!finished) {
        try {
          await heartbeat();
        } catch (error) {
          heartbeatError = error;
          return;
        }
      }
    }
  })();
  try {
    const result = await work();
    if (heartbeatError !== undefined) throw heartbeatError;
    return result;
  } finally {
    finished = true;
    await pulse;
  }
}

/** KafkaJS-compatible adapter with manual, post-transaction offset commits. */
export class KafkaProjectionRunner {
  constructor(
    private readonly config: KafkaProjectionConfig,
    private readonly transactionRunner: ProjectionTransactionRunner,
    private readonly apply: ProjectionHandler,
    private readonly checkpointStore: ProjectionCheckpointStore,
    private readonly failureReporter: ProjectionFailureReporter,
    private readonly dlqTopic = "event-store.projection-dlq.v1",
  ) {}

  async start(): Promise<KafkaJS.Consumer> {
    const kafka = new KafkaJS.Kafka({
      kafkaJS: { brokers: this.config.brokers },
    });
    const consumer = kafka.consumer({
      kafkaJS: {
        groupId: this.config.groupId,
        autoCommit: false,
        // Assignment-time database seeks determine the initial position.
        fromBeginning: false,
        readUncommitted: false,
        allowAutoTopicCreation: false,
        minBytes: 1,
        maxWaitTimeInMs: 20,
        sessionTimeout: 30_000,
        heartbeatInterval: 3_000,
        partitionAssignors: [KafkaJS.PartitionAssignors.cooperativeSticky],
      },
    });
    const producer = kafka.producer({
      kafkaJS: { idempotent: true, acks: -1 },
    });
    const admin = kafka.admin();
    await producer.connect();
    await admin.connect();
    await consumer.connect();
    await consumer.subscribe({ topics: [this.config.topic], replace: true });
    let resolveAssigned!: () => void;
    let rejectAssigned!: (error: unknown) => void;
    const assigned = new Promise<void>((resolve, reject) => {
      resolveAssigned = resolve;
      rejectAssigned = reject;
    });
    const expectedOffsets = new Map<string, bigint>();
    const resyncAttempts = new Set<string>();
    const alignPartition = async (
      assignment: { topic: string; partition: number },
    ): Promise<bigint> => {
      const offsets = await admin.fetchTopicOffsets(assignment.topic, {
        isolationLevel: KafkaJS.IsolationLevel.READ_COMMITTED,
      });
      const low = offsets.find(
        (offset) => offset.partition === assignment.partition,
      )?.low;
      if (low === undefined)
        throw new Error(
          `Kafka low watermark is unavailable for ${assignment.topic}/${assignment.partition}`,
        );
      const expected = await this.checkpointStore.ensureAtLowWatermark(
        assignment.topic,
        assignment.partition,
        BigInt(low),
      );
      expectedOffsets.set(`${assignment.topic}/${assignment.partition}`, expected);
      // A previous process can have committed Kafka farther than its durable
      // PostgreSQL checkpoint. Reset the group to the database source of truth
      // before seeking, so a re-assignment cannot skip a durable record.
      await consumer.commitOffsets([
        {
          topic: assignment.topic,
          partition: assignment.partition,
          offset: expected.toString(),
        },
      ]);
      consumer.seek({
        topic: assignment.topic,
        partition: assignment.partition,
        offset: expected.toString(),
      });
      return expected;
    };
    const alignAssignment = async (): Promise<void> => {
      const assignments = consumer
        .assignment()
        .filter((assignment) => assignment.topic === this.config.topic);
      if (assignments.length === 0) return;
      await Promise.all(assignments.map((assignment) => alignPartition(assignment)));
    };
    await consumer.run({
      eachMessage: async ({ topic, partition, message, pause, heartbeat }) => {
        await assigned;
        if (message.key === null || message.value === null) {
          pause();
          throw new Error("Kafka event record requires a key and value");
        }
        const headers = Object.fromEntries(
          Object.entries(message.headers ?? {}).map(([key, value]) => [
            key,
            Array.isArray(value) ? value[0]?.toString() : value?.toString(),
          ]),
        );
        const record = {
          topic,
          partition,
          offset: BigInt(message.offset),
          key: message.key.toString(),
          headers,
          value: message.value,
        };
        if (expectedOffsets.get(`${topic}/${partition}`) === undefined)
          await alignAssignment();
        const persistedOffset = await this.checkpointStore.nextOffset(
          topic,
          partition,
        );
        if (persistedOffset !== undefined)
          expectedOffsets.set(`${topic}/${partition}`, persistedOffset);
        const expected = expectedOffsets.get(`${topic}/${partition}`);
        if (expected === undefined)
          throw new Error(`partition ${topic}/${partition} was not assigned`);
        if (record.offset < expected) {
          await consumer.commitOffsets([
            { topic, partition, offset: expected.toString() },
          ]);
          return;
        }
        if (record.offset > expected) {
          const attempt = `${topic}/${partition}/${expected}`;
          if (!resyncAttempts.has(attempt)) {
            // Rebalance notifications are not exposed by this client API. On
            // the first discontinuity, re-check retention and force the group
            // back to the durable checkpoint before accepting any gap.
            resyncAttempts.add(attempt);
            await alignPartition({ topic, partition });
            return;
          }
          // The forced seek above proved that Kafka skipped only records hidden
          // from a read_committed consumer (transaction-control/aborted data).
          resyncAttempts.delete(attempt);
        }
        let failure: unknown;
        for (const delay of projectionRetryDelaysMs) {
          try {
            // read_committed consumers do not receive Kafka transaction-control
            // batches, so readable record offsets are not necessarily adjacent.
            await withHeartbeats(
              () =>
                this.transactionRunner.process(record, this.apply, {
                  allowReadCommittedOffsetGap: true,
                }),
              heartbeat,
            );
          } catch (error) {
            failure = error;
            await withHeartbeats(() => wait(delay), heartbeat);
            continue;
          }
          for (const commitDelay of projectionRetryDelaysMs) {
            try {
              await consumer.commitOffsets([
                {
                  topic,
                  partition,
                  offset: (BigInt(message.offset) + 1n).toString(),
                },
              ]);
              expectedOffsets.set(
                `${topic}/${partition}`,
                BigInt(message.offset) + 1n,
              );
              return;
            } catch (commitError) {
              failure = commitError;
              await withHeartbeats(() => wait(commitDelay), heartbeat);
            }
          }
          // The DB checkpoint is durable. Retrying this record as a duplicate is
          // safe; it must never be classified as a poisoned business event.
          throw failure;
        }
        try {
          const envelope = rawEnvelope(message.value);
          await this.failureReporter.record(
            record,
            envelope,
            failure,
            projectionRetryDelaysMs.length,
          );
          await producer.send({
            topic: this.dlqTopic,
            messages: [
              {
                key: `${this.config.groupId}|${topic}|${partition}|${message.offset}`,
                value: JSON.stringify({
                  projection: this.config.groupId,
                  topic,
                  partition,
                  offset: message.offset,
                  error:
                    failure instanceof Error
                      ? failure.message
                      : String(failure),
                  envelope,
                }),
              },
            ],
          });
          await this.failureReporter.markDlqPublished(record);
        } finally {
          pause();
        }
        throw failure;
      },
    });
    try {
      const deadline = Date.now() + 30_000;
      while (consumer.assignment().length === 0) {
        if (Date.now() >= deadline)
          throw new Error(
            "Kafka consumer did not receive a partition assignment",
          );
        await wait(20);
      }
      await alignAssignment();
      resolveAssigned();
    } catch (error) {
      rejectAssigned(error);
      await consumer.disconnect().catch(() => undefined);
      await producer.disconnect().catch(() => undefined);
      await admin.disconnect().catch(() => undefined);
      throw error;
    }
    return consumer;
  }
}
