import { KafkaJS } from "@confluentinc/kafka-javascript";
import type {
  ProjectionCrashBarrier,
  ProjectionCheckpointStore,
  ProjectionFailureReporter,
  ProjectionHandler,
  ProjectionMetrics,
  ProjectionTransactionRunner,
} from "./index.js";
import {
  ProjectionCrashError,
  ProjectionHandlerTimeoutError,
  ProjectionRebalanceError,
  projectionRetryDelaysMs,
} from "./index.js";

export interface KafkaProjectionConfig {
  brokers: string[];
  groupId: string;
  topic: string;
  /** Override only when a deployment needs a compatible group assignor. */
  partitionAssignors?: KafkaJS.PartitionAssignors[];
}

function rawEnvelope(value: Buffer | null): unknown {
  if (value === null) return { rawBase64: null };
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
  let stopPulse!: () => void;
  const stopped = new Promise<void>((resolve) => {
    stopPulse = resolve;
  });
  let heartbeatError: unknown;
  const pulse = (async () => {
    while (!finished) {
      await Promise.race([wait(1_000), stopped]);
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
    stopPulse();
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
    private readonly crashBarrier?: ProjectionCrashBarrier,
    private readonly metrics?: ProjectionMetrics,
  ) {}

  async start(): Promise<KafkaJS.Consumer> {
    const kafka = new KafkaJS.Kafka({
      kafkaJS: { brokers: this.config.brokers },
    });
    const consumer = kafka.consumer({
      kafkaJS: {
        groupId: this.config.groupId,
        autoCommit: false,
        // The Confluent compatibility adapter attempts its automatic reset
        // before `assignment()` becomes observable, so it cannot express
        // `auto.offset.reset=error` while this runner performs a manual seek.
        // Earliest is safe: the assignment gate below prevents any record from
        // reaching a handler until the durable PostgreSQL checkpoint has set
        // the exact position. It can replay duplicates, but can never skip.
        fromBeginning: true,
        readUncommitted: false,
        allowAutoTopicCreation: false,
        minBytes: 1,
        maxWaitTimeInMs: 20,
        sessionTimeout: 30_000,
        heartbeatInterval: 3_000,
        partitionAssignors: this.config.partitionAssignors ?? [
          KafkaJS.PartitionAssignors.cooperativeSticky,
        ],
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
    // Startup can fail before eachBatch begins awaiting this gate. Keep the
    // original rejected promise for an active batch, but mark that early
    // rejection handled so a retention refusal is not an unhandled process
    // rejection.
    void assigned.catch(() => undefined);
    const expectedOffsets = new Map<string, bigint>();
    const readCommittedGapAttempts = new Set<string>();
    const alignPartition = async (assignment: {
      topic: string;
      partition: number;
    }): Promise<bigint> => {
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
      expectedOffsets.set(
        `${assignment.topic}/${assignment.partition}`,
        expected,
      );
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
      await Promise.all(
        assignments.map((assignment) => alignPartition(assignment)),
      );
    };
    await consumer.run({
      eachBatchAutoResolve: false,
      eachBatch: async ({
        batch,
        pause,
        heartbeat,
        isRunning,
        isStale,
        resolveOffset,
      }) => {
        const topic = batch.topic;
        const partition = batch.partition;
        const assertCurrentAssignment = (): void => {
          if (!isRunning() || isStale())
            throw new Error(
              `projection assignment was revoked for ${topic}/${partition}`,
            );
        };
        await assigned;
        messageLoop: for (const message of batch.messages) {
          assertCurrentAssignment();
          await this.crashBarrier?.hit("after_kafka_poll");
          const missingKeyOrValue =
            message.key === null || message.value === null;
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
            key: message.key?.toString() ?? "",
            headers,
            value: message.value ?? Buffer.alloc(0),
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
            resolveOffset(message.offset);
            continue;
          }
          if (record.offset > expected) {
            const attempt = `${topic}/${partition}/${expected}/${record.offset}`;
            if (!readCommittedGapAttempts.has(attempt)) {
              const offsets = await admin.fetchTopicOffsets(topic, {
                isolationLevel: KafkaJS.IsolationLevel.READ_COMMITTED,
              });
              const low = offsets.find(
                (offset) => offset.partition === partition,
              )?.low;
              if (low === undefined || BigInt(low) > expected) {
                pause();
                throw new Error(
                  `projection Kafka gap is below the retained range for ${topic}/${partition}: expected ${expected}, low ${low ?? "unknown"}`,
                );
              }
              // Seek to the durable physical offset once. If the same
              // read_committed record is returned again, Kafka has proved that
              // all intervening retained log entries are invisible transaction
              // control or aborted records; a delete-only event topic cannot
              // otherwise have a visible interior gap.
              readCommittedGapAttempts.add(attempt);
              await alignPartition({ topic, partition });
              return;
            }
            readCommittedGapAttempts.delete(attempt);
          }
          let failure: unknown;
          for (const delay of projectionRetryDelaysMs) {
            assertCurrentAssignment();
            try {
              if (missingKeyOrValue)
                throw new Error("Kafka event record requires a key and value");
              const assignmentAbort = new AbortController();
              const assignmentWatcher = setInterval(() => {
                if (!isRunning() || isStale()) assignmentAbort.abort();
              }, 50);
              await withHeartbeats(
                () =>
                  this.transactionRunner.process(record, this.apply, {
                    allowReadCommittedOffsetGap: record.offset > expected,
                    transactionTimeoutMs: 10_000,
                    abortSignal: assignmentAbort.signal,
                  }),
                heartbeat,
              ).finally(() => clearInterval(assignmentWatcher));
            } catch (error) {
              // A revoked assignment or a transaction timeout must return to
              // Kafka immediately. Retrying inside the current eachBatch
              // would retain a stale assignment and postpone rebalance.
              if (
                error instanceof ProjectionCrashError ||
                error instanceof ProjectionHandlerTimeoutError ||
                error instanceof ProjectionRebalanceError
              )
                throw error;
              failure = error;
              await withHeartbeats(() => wait(delay), heartbeat);
              continue;
            }
            for (const commitDelay of projectionRetryDelaysMs) {
              assertCurrentAssignment();
              try {
                await this.crashBarrier?.hit("before_kafka_offset_commit");
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
                resolveOffset(message.offset);
                await this.crashBarrier?.hit("after_kafka_offset_commit");
                continue messageLoop;
              } catch (commitError) {
                if (commitError instanceof ProjectionCrashError)
                  throw commitError;
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
            let publishError: unknown;
            for (const delay of projectionRetryDelaysMs) {
              try {
                await producer.send({
                  topic: this.dlqTopic,
                  messages: [
                    {
                      key: `${this.transactionRunner.projectionIdentity.name}|${this.transactionRunner.projectionIdentity.generationId}|${topic}|${partition}|${message.offset}`,
                      value: JSON.stringify({
                        projectionName:
                          this.transactionRunner.projectionIdentity.name,
                        generationId:
                          this.transactionRunner.projectionIdentity
                            .generationId,
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
                publishError = undefined;
                break;
              } catch (error) {
                publishError = error;
                await withHeartbeats(() => wait(delay), heartbeat);
              }
            }
            if (publishError !== undefined) throw publishError;
          } finally {
            this.metrics?.poisonEvent();
            this.metrics?.pausePartition();
            pause();
          }
          throw failure;
        }
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
    const disconnectConsumer = consumer.disconnect.bind(consumer);
    let disconnected = false;
    consumer.disconnect = async (): Promise<void> => {
      if (disconnected) return;
      disconnected = true;
      await disconnectConsumer().catch(() => undefined);
      await producer.disconnect().catch(() => undefined);
      await admin.disconnect().catch(() => undefined);
    };
    return consumer;
  }
}
