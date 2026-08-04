import { describe, expect, it } from "vitest";
import { uuidv7 } from "@event-store/contracts";
import { PostgresEventStore } from "@event-store/postgres-store";

const input = () => {
  const requestId = uuidv7();
  return {
    producerService: "orders-command",
    namespace: "orders",
    aggregateType: "Order",
    aggregateId: uuidv7(),
    requestId,
    expectedRevision: { kind: "no_stream" as const },
    context: {
      requestId,
      correlationId: uuidv7(),
      causationId: null,
      actor: { kind: "user" as const, subjectRef: "usr_1" },
    },
    events: [
      {
        eventName: "order.created",
        schemaVersion: 1,
        occurredAt: "2026-08-04T10:12:18.120Z",
        payload: { orderRef: "o1" },
      },
    ],
  };
};

function poolThatFails(error: Error & { code?: string }, failures: number) {
  let appendCalls = 0;
  const client = {
    query: async (sql: string) => {
      if (!sql.includes("event_store.append_v1")) return { rows: [] };
      appendCalls += 1;
      if (appendCalls <= failures) throw error;
      return {
        rows: [
          {
            append_v1: {
              requestId: "request",
              previousRevision: "0",
              currentRevision: "1",
              recordedAt: "2026-08-04T10:12:18.120Z",
              events: [],
            },
          },
        ],
      };
    },
    release: () => undefined,
  };
  return {
    pool: { connect: async () => client } as never,
    appendCalls: () => appendCalls,
  };
}

describe("PostgresEventStore append retries", () => {
  it("rejects direct PII in snapshot state before opening a database session", async () => {
    const pool = {
      connect: async () => {
        throw new Error("must not connect");
      },
    } as never;
    await expect(
      new PostgresEventStore(pool).putSnapshot({
        namespace: "orders",
        aggregateType: "Order",
        aggregateId: uuidv7(),
        revision: 1000n,
        reducerVersion: "a".repeat(64),
        stateSchemaVersion: 1,
        state: { email: "person@example.com" },
      }),
    ).rejects.toThrow("direct PII");
  });

  it("retries database deadlocks with the identical request", async () => {
    const error = Object.assign(new Error("deadlock detected"), {
      code: "40P01",
    });
    const fake = poolThatFails(error, 2);
    await expect(
      new PostgresEventStore(fake.pool).append(input()),
    ).resolves.toMatchObject({
      currentRevision: "1",
    });
    expect(fake.appendCalls()).toBe(3);
  });

  it("does not retry an optimistic-concurrency conflict", async () => {
    const error = Object.assign(
      new Error("expected revision 3, actual revision 4"),
      {
        code: "40001",
      },
    );
    const fake = poolThatFails(error, 1);
    await expect(
      new PostgresEventStore(fake.pool).append(input()),
    ).rejects.toBe(error);
    expect(fake.appendCalls()).toBe(1);
  });

  it("uses PostgreSQL recordedAt as a conservative CDC latency anchor", async () => {
    const fake = poolThatFails(new Error("append failed"), 0);
    const result = await new PostgresEventStore(fake.pool).append(input());
    expect(result.currentRevision).toBe("1");
    expect(result.commitEpochMs).toBe(
      Date.parse("2026-08-04T10:12:18.120Z"),
    );
    expect(fake.appendCalls()).toBe(1);
  });
});
