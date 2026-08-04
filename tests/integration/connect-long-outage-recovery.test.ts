import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { KafkaJS } from "@confluentinc/kafka-javascript";
import { uuidv7 } from "@event-store/contracts";
import { PostgresEventStore } from "@event-store/postgres-store";
import { EventStoreStack } from "../fixtures/event-store-stack.js";
import { randomBytes } from "node:crypto";
import { Pool } from "pg";

const suite =
  process.env.RUN_LONG_RECOVERY === "true" ? describe : describe.skip;
const outageMs = Number(process.env.CONNECT_OUTAGE_MS ?? 300_000);
const walBudgetBytes = BigInt(
  process.env.WAL_RECOVERY_BUDGET_BYTES ?? 128 * 1024 * 1024,
);

async function waitFor(
  predicate: () => Promise<boolean>,
  message: string,
  timeoutMs = 90_000,
): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (await predicate()) return;
    await new Promise((resolve) => setTimeout(resolve, 200));
  }
  throw new Error(message);
}

function walFillPayload(): string {
  // PostgreSQL TOAST compression must not turn a nominal 512 KiB event into a
  // few bytes of WAL; this test deliberately consumes the actual WAL budget.
  return randomBytes(384 * 1024).toString("base64url");
}

suite("five-minute Connect outage recovery", () => {
  const stack = new EventStoreStack();
  let pool: Pool;

  beforeAll(async () => {
    if (!Number.isInteger(outageMs) || outageMs < 1)
      throw new Error("CONNECT_OUTAGE_MS must be a positive integer");
    await stack.start({ cdc: true });
    pool = await stack.pool();
  }, 180_000);
  afterAll(async () => {
    await pool?.end();
    await stack.stop();
  }, 60_000);

  it(
    "delivers every acknowledged event after Connect resumes",
    async () => {
      const kafka = new KafkaJS.Kafka({
        kafkaJS: { brokers: [stack.kafkaBroker()] },
      });
      const consumer = kafka.consumer({
        kafkaJS: {
          groupId: `connect-long-outage-${uuidv7()}`,
          autoCommit: false,
          fromBeginning: true,
          readUncommitted: false,
        },
      });
      const requestIds = Array.from({ length: 20 }, () => uuidv7());
      const awaiting = new Set(requestIds);
      let delivered!: () => void;
      const allDelivered = new Promise<void>((resolve, reject) => {
        delivered = resolve;
        setTimeout(
          () =>
            reject(
              new Error(
                `Connect recovery lost ${awaiting.size} acknowledged event(s)`,
              ),
            ),
          outageMs + 60_000,
        );
      });
      await consumer.connect();
      await consumer.subscribe({
        topics: ["event-store.events.v1"],
        replace: true,
      });
      await consumer.run({
        eachMessage: async ({ message }) => {
          const envelope = JSON.parse(message.value?.toString() ?? "{}") as {
            context?: { requestId?: string };
          };
          const requestId = envelope.context?.requestId;
          if (requestId !== undefined && awaiting.delete(requestId)) {
            if (awaiting.size === 0) delivered();
          }
        },
      });
      try {
        await stack.stopConnect();
        await Promise.all(
          requestIds.map((requestId) =>
            new PostgresEventStore(pool).append({
              producerService: "connect-long-outage-test",
              namespace: "orders",
              aggregateType: "Order",
              aggregateId: uuidv7(),
              requestId,
              expectedRevision: { kind: "no_stream" },
              context: {
                requestId,
                correlationId: uuidv7(),
                causationId: null,
                actor: {
                  kind: "service",
                  subjectRef: "connect-long-outage-test",
                },
              },
              events: [
                {
                  eventName: "order.created",
                  schemaVersion: 1,
                  occurredAt: "2026-08-04T10:12:18.120Z",
                  payload: { recovery: "five-minute-connect-outage" },
                },
              ],
            }),
          ),
        );
        // This is the required outage interval, not a readiness wait.
        await new Promise((resolve) => setTimeout(resolve, outageMs));
        expect(awaiting.size).toBe(requestIds.length);
      } finally {
        await stack.startConnect();
      }
      await expect(allDelivered).resolves.toBeUndefined();
      await consumer.disconnect();
    },
    outageMs + 150_000,
  );
});

suite("WAL admission after Kafka delivery outage", () => {
  const stack = new EventStoreStack();
  let pool: Pool;

  const slotWalState = async (): Promise<{
    retained: bigint;
    segmentBytes: bigint;
    restartLsn: string;
    confirmedFlushLsn: string;
  }> => {
    const result = await pool.query<{
      retained_wal_bytes: string;
      wal_segment_bytes: string;
      restart_lsn: string;
      confirmed_flush_lsn: string;
    }>(
      `SELECT pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn)::bigint
         AS retained_wal_bytes,
               pg_size_bytes(current_setting('wal_segment_size'))::bigint
         AS wal_segment_bytes,
               restart_lsn::text,
               confirmed_flush_lsn::text
         FROM pg_replication_slots
        WHERE slot_name='event_store_live'`,
    );
    const row = result.rows[0];
    if (row === undefined)
      throw new Error("event_store_live has no retained WAL measurement");
    return {
      retained: BigInt(row.retained_wal_bytes),
      segmentBytes: BigInt(row.wal_segment_bytes),
      restartLsn: row.restart_lsn,
      confirmedFlushLsn: row.confirmed_flush_lsn,
    };
  };

  beforeAll(async () => {
    if (walBudgetBytes < 1n)
      throw new Error("WAL_RECOVERY_BUDGET_BYTES must be positive");
    await stack.start({
      cdc: true,
      toxiproxy: true,
      connectKafkaProxy: true,
      walBudgetBytes,
      slotWalKeepBytes: walBudgetBytes * 4n,
    });
    pool = await stack.pool();
    await pool.query(
      "ALTER ROLE event_store_critical_app LOGIN PASSWORD 'critical'",
    );
  }, 180_000);
  afterAll(async () => {
    await pool?.end();
    await stack.stop();
  }, 60_000);

  it("warns at 50%, admits only critical writes after 70%, then recovers standard traffic", async () => {
    const kafka = new KafkaJS.Kafka({
      kafkaJS: { brokers: [stack.kafkaBroker()] },
    });
    const consumer = kafka.consumer({
      kafkaJS: {
        groupId: `wal-recovery-${uuidv7()}`,
        autoCommit: false,
        fromBeginning: true,
        readUncommitted: false,
      },
    });
    const expectedEventIds = new Set<string>();
    const observedEventIds = new Set<string>();
    let allDelivered!: () => void;
    let deliveryTimeout!: ReturnType<typeof setTimeout>;
    const delivered = new Promise<void>((resolve, reject) => {
      allDelivered = () => {
        clearTimeout(deliveryTimeout);
        resolve();
      };
      deliveryTimeout = setTimeout(
        () =>
          reject(
            new Error(
              `Kafka recovery lost ${expectedEventIds.size} acknowledged event(s)`,
            ),
          ),
        180_000,
      );
    });
    const expectEvent = (eventId: string): void => {
      expectedEventIds.add(eventId);
      if (observedEventIds.delete(eventId) && expectedEventIds.size === 0)
        allDelivered();
    };
    await consumer.connect();
    await consumer.subscribe({
      topics: ["event-store.events.v1"],
      replace: true,
    });
    await consumer.run({
      eachMessage: async ({ message }) => {
        const header = message.headers?.id;
        const eventId = Array.isArray(header)
          ? header[0]?.toString()
          : header?.toString();
        if (eventId !== undefined) {
          observedEventIds.add(eventId);
          if (expectedEventIds.delete(eventId) && expectedEventIds.size === 0)
            allDelivered();
        }
      },
    });

    const append = async (trafficClass: "standard" | "critical") => {
      const store = new PostgresEventStore(
        pool,
        undefined,
        trafficClass === "critical",
      );
      const requestId = uuidv7();
      const result = await store.append({
        producerService: "wal-recovery-test",
        namespace: "orders",
        aggregateType: "Order",
        aggregateId: uuidv7(),
        requestId,
        expectedRevision: { kind: "no_stream" },
        context: {
          requestId,
          correlationId: uuidv7(),
          causationId: null,
          actor: { kind: "service", subjectRef: "wal-recovery-test" },
          trafficClass,
        },
        events: [
          {
            eventName: "order.created",
            schemaVersion: 1,
            occurredAt: "2026-08-04T10:12:18.120Z",
            payload: { walFill: walFillPayload() },
          },
        ],
      });
      const eventId = result.events[0]?.eventId;
      if (eventId === undefined) throw new Error("append returned no event id");
      expectEvent(eventId);
    };

    const criticalPool = new Pool({
      connectionString: stack.databaseUrl.replace(
        "postgresql://postgres:postgres@",
        "postgresql://event_store_critical_app:critical@",
      ),
    });
    let baseline!: Awaited<ReturnType<typeof slotWalState>>;
    try {
      baseline = await slotWalState();
      await stack.setConnectKafkaEnabled(false);
      while ((await slotWalState()).retained < (walBudgetBytes * 50n) / 100n)
        await append("standard");
      expect((await slotWalState()).retained).toBeGreaterThanOrEqual(
        (walBudgetBytes * 50n) / 100n,
      );

      let standardRejected = false;
      while (!standardRejected) {
        try {
          await append("standard");
        } catch (error) {
          expect(String(error)).toContain(
            "CDC retained WAL exceeds append admission threshold",
          );
          standardRejected = true;
        }
      }
      expect((await slotWalState()).retained).toBeGreaterThanOrEqual(
        (walBudgetBytes * 70n) / 100n,
      );

      const criticalStore = new PostgresEventStore(
        criticalPool,
        undefined,
        true,
      );
      const appendCritical = async () => {
        const requestId = uuidv7();
        const result = await criticalStore.append({
          producerService: "wal-recovery-test",
          namespace: "orders",
          aggregateType: "Order",
          aggregateId: uuidv7(),
          requestId,
          expectedRevision: { kind: "no_stream" },
          context: {
            requestId,
            correlationId: uuidv7(),
            causationId: null,
            actor: { kind: "service", subjectRef: "wal-recovery-test" },
            trafficClass: "critical",
          },
          events: [
            {
              eventName: "order.created",
              schemaVersion: 1,
              occurredAt: "2026-08-04T10:12:18.120Z",
              payload: { walFill: walFillPayload() },
            },
          ],
        });
        const eventId = result.events[0]?.eventId;
        if (eventId === undefined)
          throw new Error("append returned no event id");
        expectEvent(eventId);
      };
      let criticalRejected = false;
      while (!criticalRejected) {
        try {
          await appendCritical();
        } catch (error) {
          expect(String(error)).toContain(
            "CDC retained WAL exceeds append admission threshold",
          );
          criticalRejected = true;
        }
      }
      const peak = await slotWalState();
      expect(peak.retained).toBeGreaterThanOrEqual(
        (walBudgetBytes * 85n) / 100n,
      );
      expect(peak.retained).toBeGreaterThan(baseline.retained);
    } finally {
      await stack.setConnectKafkaEnabled(true);
      await criticalPool.end();
    }

    try {
      await expect(delivered).resolves.toBeUndefined();
      // Restart from the persisted Connect offset after the transport heals.
      // This must send PostgreSQL a fresh standby-status acknowledgement; it
      // must not replay or lose any of the already committed Kafka records.
      await stack.restartConnect();
      // A logical slot advances restart_lsn at a decoded transaction boundary.
      // The recovery barrier is the only narrowly-scoped write allowed while
      // normal admission remains fail-closed; its Kafka delivery proves the
      // connector can establish that boundary without reopening traffic.
      const replayId = `wal-recovery-${uuidv7()}`;
      await Promise.all(
        Array.from({ length: 24 }, async (_, partition) => {
          const barrier = await new PostgresEventStore(
            pool,
          ).appendRecoveryBarrier(replayId, partition, uuidv7(), uuidv7());
          const barrierEventId = barrier.events[0]?.eventId;
          if (barrierEventId === undefined)
            throw new Error("recovery barrier returned no event id");
          expectEvent(barrierEventId);
        }),
      );
      await waitFor(
        async () => expectedEventIds.size === 0,
        "Connect did not deliver every recovery barrier",
      );
      let latestState!: Awaited<ReturnType<typeof slotWalState>>;
      await waitFor(
        async () => {
          latestState = await slotWalState();
          return latestState.retained < (walBudgetBytes * 70n) / 100n;
        },
        "logical slot did not catch up below the standard-admission WAL threshold",
        120_000,
      ).catch((error: unknown) => {
        return pool
          .query<{
            application_name: string;
            state: string;
            xact_start: string | null;
            query: string;
          }>(
            `SELECT application_name, state, xact_start::text, query
               FROM pg_stat_activity
              WHERE datname=current_database()
                AND xact_start IS NOT NULL
              ORDER BY xact_start`,
          )
          .then((activity) => {
            throw new Error(
              `${String(error)}; retained=${latestState.retained}; segment=${latestState.segmentBytes}; restart=${latestState.restartLsn}; confirmed=${latestState.confirmedFlushLsn}; baseline=${baseline.retained}; activeTransactions=${JSON.stringify(activity.rows)}`,
            );
          });
      });
      await expect(append("standard")).resolves.toBeUndefined();
    } finally {
      clearTimeout(deliveryTimeout);
      await consumer.disconnect();
    }
  }, 300_000);
});
