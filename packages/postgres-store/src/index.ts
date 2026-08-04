import { createHash } from "node:crypto";
import type { Pool, PoolClient } from "pg";
import {
  EventContextSchema,
  EventDraftSchema,
  assertNoDirectPii,
  canonicalJson,
  StoredEventSchema,
  type EventContext,
  type EventDraft,
  type StoredEvent,
} from "@event-store/contracts";

export type ExpectedRevision =
  | { kind: "no_stream" }
  | { kind: "exact"; revision: bigint };

export interface AppendInput {
  producerService: string;
  namespace: string;
  aggregateType: string;
  aggregateId: string;
  requestId: string;
  expectedRevision: ExpectedRevision;
  events: readonly EventDraft[];
  context: EventContext;
}

export interface AppendResult {
  requestId: string;
  previousRevision: string;
  currentRevision: string;
  recordedAt: string;
  /** UTC timestamp sampled immediately after PostgreSQL completed append. */
  commitEpochMs: number;
  events: readonly {
    eventId: string;
    streamRevision: string;
    eventNumber: string;
  }[];
}

export type AggregateFrame =
  | { kind: "info"; headRevision: bigint }
  | {
      kind: "snapshot";
      snapshot: {
        revision: bigint;
        reducerVersion: string;
        stateSchemaVersion: number;
        state: Record<string, unknown>;
        stateSha256: Buffer;
      };
    }
  | { kind: "event"; event: StoredEvent };

const appendRetryDelaysMs = [10, 30, 90] as const;

function isRetriableAppendError(error: unknown): boolean {
  const candidate = error as { code?: unknown; message?: unknown };
  if (candidate.code === "40P01") return true;
  if (candidate.code !== "40001") return false;
  const message = String(candidate.message ?? "");
  return !/^(expected revision|stream already exists)/.test(message);
}

function sleep(milliseconds: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, milliseconds));
}

export class PostgresEventStore {
  constructor(
    private readonly pool: Pool,
    private readonly walBudgetBytes?: bigint,
    private readonly useCriticalAppendFunction = false,
  ) {}

  async append(input: AppendInput): Promise<AppendResult> {
    const events = input.events.map((event) => EventDraftSchema.parse(event));
    const context = EventContextSchema.parse(input.context);
    assertNoDirectPii(events);
    assertNoDirectPii(context);
    const expected =
      input.expectedRevision.kind === "no_stream"
        ? null
        : input.expectedRevision.revision.toString();
    for (let attempt = 0; ; attempt += 1) {
      try {
        return await this.withSession(async (client) => {
          if (this.walBudgetBytes !== undefined)
            await client.query(
              "SELECT event_store.assert_append_cdc_ready($1,$2)",
              [
                this.walBudgetBytes.toString(),
                context.trafficClass === "critical",
              ],
            );
          let result: { rows: { append_v1: AppendResult }[] };
          try {
            result = await client.query<{ append_v1: AppendResult }>(
              `SELECT event_store.${this.useCriticalAppendFunction ? "append_v1_critical" : "append_v1"}($1,$2,$3,$4,$5,$6,$7,$8::jsonb,$9::jsonb) AS append_v1`,
              [
                input.producerService,
                input.namespace,
                input.aggregateType,
                input.aggregateId,
                input.requestId,
                input.expectedRevision.kind,
                expected,
                JSON.stringify(events),
                JSON.stringify(context),
              ],
            );
          } catch (error) {
            throw Object.assign(
              error instanceof Error ? error : new Error(String(error)),
              {
                appendDispatched: true,
              },
            );
          }
          // append_v1 is a single autocommitted statement. Taking the span at
          // query completion bounds the PostgreSQL COMMIT-to-consumer path,
          // without including response serialization or gRPC scheduling.
          const commitEpochMs = Date.now();
          const value = result.rows[0]?.append_v1;
          if (value === undefined) throw new Error("append returned no result");
          return { ...value, commitEpochMs };
        });
      } catch (error) {
        const delay = appendRetryDelaysMs[attempt];
        if (delay === undefined || !isRetriableAppendError(error)) throw error;
        await sleep(delay);
      }
    }
  }

  /**
   * Appends the fixed recovery barrier event through the narrowly-scoped
   * database function. It intentionally bypasses general append admission so
   * a fail-closed recovery can prove catch-up before normal writes resume.
   */
  async appendRecoveryBarrier(
    replayId: string,
    partition: number,
    aggregateId: string,
    requestId: string,
  ): Promise<AppendResult> {
    return this.withSession(async (client) => {
      const result = await client.query<{
        append_recovery_barrier: AppendResult;
      }>(
        "SELECT event_store.append_recovery_barrier($1,$2,$3,$4) AS append_recovery_barrier",
        [replayId, partition, aggregateId, requestId],
      );
      const commitEpochMs = Date.now();
      const value = result.rows[0]?.append_recovery_barrier;
      if (value === undefined)
        throw new Error("recovery barrier append returned no result");
      return { ...value, commitEpochMs };
    });
  }

  async getStreamHead(
    namespace: string,
    aggregateType: string,
    aggregateId: string,
  ): Promise<bigint | undefined> {
    return this.withSession(async (client) => {
      const result = await client.query<{ get_stream_head_v1: string | null }>(
        `SELECT event_store.get_stream_head_v1($1,$2,$3)`,
        [namespace, aggregateType, aggregateId],
      );
      const value = result.rows[0]?.get_stream_head_v1;
      return value === null || value === undefined ? undefined : BigInt(value);
    });
  }

  async readStream(
    namespace: string,
    aggregateType: string,
    aggregateId: string,
    fromRevision = 1n,
  ): Promise<StoredEvent[]> {
    return this.withSession(async (client) => {
      const result = await client.query<{ event_envelope: unknown }>(
        `SELECT event_store.read_stream_v1($1,$2,$3,$4) AS event_envelope`,
        [namespace, aggregateType, aggregateId, fromRevision.toString()],
      );
      return result.rows.map((row) =>
        StoredEventSchema.parse(row.event_envelope),
      );
    });
  }

  async loadAggregate(
    namespace: string,
    aggregateType: string,
    aggregateId: string,
    reducerVersion: string,
  ): Promise<AggregateFrame[]> {
    return this.withRepeatableRead(async (client) => {
      const result = await client.query<{ frame_kind: string; frame: unknown }>(
        `SELECT frame_kind, frame FROM event_store.load_aggregate_v1($1,$2,$3,$4)`,
        [namespace, aggregateType, aggregateId, reducerVersion],
      );
      return result.rows.map((row): AggregateFrame => {
        const frame = row.frame as Record<string, unknown>;
        if (row.frame_kind === "info")
          return {
            kind: "info",
            headRevision: BigInt(String(frame.headRevision)),
          };
        if (row.frame_kind === "event")
          return { kind: "event", event: StoredEventSchema.parse(frame) };
        if (row.frame_kind === "snapshot") {
          const state = frame.state as Record<string, unknown>;
          const stateSha256 = Buffer.from(String(frame.stateSha256), "hex");
          const actualHash = createHash("sha256")
            .update(canonicalJson(state))
            .digest();
          if (!actualHash.equals(stateSha256))
            throw Object.assign(
              new Error("snapshot state hash does not match state"),
              { code: "XX001" },
            );
          return {
            kind: "snapshot",
            snapshot: {
              revision: BigInt(String(frame.snapshotRevision)),
              reducerVersion: String(frame.reducerVersion),
              stateSchemaVersion: Number(frame.stateSchemaVersion),
              state,
              stateSha256,
            },
          };
        }
        throw new Error(`unknown aggregate frame ${row.frame_kind}`);
      });
    });
  }

  async putSnapshot(input: {
    namespace: string;
    aggregateType: string;
    aggregateId: string;
    revision: bigint;
    reducerVersion: string;
    stateSchemaVersion: number;
    state: Record<string, unknown>;
    stateSha256?: Buffer;
  }): Promise<void> {
    const computedHash = createHash("sha256")
      .update(canonicalJson(input.state))
      .digest();
    const hash = input.stateSha256 ?? computedHash;
    if (!hash.equals(computedHash))
      throw Object.assign(new Error("snapshot state hash mismatch"), {
        code: "XX001",
      });
    await this.withSession((client) =>
      client
        .query(
          `SELECT event_store.put_snapshot_v1($1,$2,$3,$4,$5,$6,$7::jsonb,$8)`,
          [
            input.namespace,
            input.aggregateType,
            input.aggregateId,
            input.revision.toString(),
            input.reducerVersion,
            input.stateSchemaVersion,
            JSON.stringify(input.state),
            hash,
          ],
        )
        .then(() => undefined),
    );
  }

  private async withSession<T>(
    fn: (client: PoolClient) => Promise<T>,
  ): Promise<T> {
    const client = await this.pool.connect();
    try {
      await client.query(
        `SET synchronous_commit = 'on'; SET statement_timeout = '1500ms'; SET lock_timeout = '750ms'; SET idle_in_transaction_session_timeout = '2000ms'`,
      );
      return await fn(client);
    } finally {
      client.release();
    }
  }

  private async withRepeatableRead<T>(
    fn: (client: PoolClient) => Promise<T>,
  ): Promise<T> {
    const client = await this.pool.connect();
    try {
      await client.query("BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY");
      const result = await fn(client);
      await client.query("COMMIT");
      return result;
    } catch (error) {
      await client.query("ROLLBACK").catch(() => undefined);
      throw error;
    } finally {
      client.release();
    }
  }
}
