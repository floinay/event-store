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
        // A new projection group must see retained records before its first
        // checkpoint is established; checkpoint seeks handle later restarts.
        fromBeginning: true,
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
    await consumer.run({
      eachMessage: async ({ topic, partition, message, pause }) => {
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
        const offsets = await admin.fetchTopicOffsets(topic, {
          isolationLevel: KafkaJS.IsolationLevel.READ_COMMITTED,
        });
        const lowWatermark = offsets.find(
          (offset) => offset.partition === partition,
        )?.low;
        if (lowWatermark === undefined) {
          pause();
          throw new Error(
            `Kafka low watermark is unavailable for ${topic}/${partition}`,
          );
        }
        const expected = await this.checkpointStore.ensureAtLowWatermark(
          topic,
          partition,
          BigInt(lowWatermark),
        );
        if (record.offset !== expected) {
          consumer.seek({ topic, partition, offset: expected.toString() });
          return;
        }
        let failure: unknown;
        for (const delay of projectionRetryDelaysMs) {
          try {
            await this.transactionRunner.process(record, this.apply);
            await consumer.commitOffsets([
              {
                topic,
                partition,
                offset: (BigInt(message.offset) + 1n).toString(),
              },
            ]);
            return;
          } catch (error) {
            failure = error;
            await new Promise((resolve) => setTimeout(resolve, delay));
          }
        }
        try {
          const event = JSON.parse(message.value.toString()) as never;
          await this.failureReporter.record(
            record,
            event,
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
                  envelope: event,
                }),
              },
            ],
          });
        } finally {
          pause();
        }
        throw failure;
      },
    });
    return consumer;
  }
}
