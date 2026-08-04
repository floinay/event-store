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
    const alignAssignment = async (): Promise<void> => {
      const assignments = consumer
        .assignment()
        .filter((assignment) => assignment.topic === this.config.topic);
      if (assignments.length === 0) return;
      const offsets = await admin.fetchTopicOffsets(this.config.topic, {
        isolationLevel: KafkaJS.IsolationLevel.READ_COMMITTED,
      });
      for (const assignment of assignments) {
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
        expectedOffsets.set(
          `${assignment.topic}/${assignment.partition}`,
          expected,
        );
        // A previous process can have committed Kafka farther than its durable
        // PostgreSQL checkpoint (for example, across a crash between the two
        // acknowledgements). Reset the group to the database source of truth
        // before seeking; otherwise the broker assignment may skip a record.
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
      }
    };
    await consumer.run({
      eachMessage: async ({ topic, partition, message, pause }) => {
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
        // With read_committed, a seek to `expected` can legitimately yield a
        // later offset because Kafka hides transaction-control batches. The
        // transaction runner records the delivered offset atomically.
        let failure: unknown;
        for (const delay of projectionRetryDelaysMs) {
          try {
            // read_committed consumers do not receive Kafka transaction-control
            // batches, so readable record offsets are not necessarily adjacent.
            await this.transactionRunner.process(record, this.apply, {
              allowReadCommittedOffsetGap: true,
            });
          } catch (error) {
            failure = error;
            await new Promise((resolve) => setTimeout(resolve, delay));
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
              await new Promise((resolve) => setTimeout(resolve, commitDelay));
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
