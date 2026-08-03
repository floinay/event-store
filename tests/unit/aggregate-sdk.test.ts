import { describe, expect, it } from "vitest";
import { fold, reconstruct } from "@event-store/aggregate-sdk";
import type { StoredEvent } from "@event-store/contracts";

const base = {
  eventId: "0198f99a-9b1c-7000-8000-000000000001",
  namespace: "orders",
  aggregateType: "Order",
  aggregateId: "0198f999-cacf-7000-8000-000000000001",
  eventNumber: "1",
  eventName: "order.created",
  schemaVersion: 1,
  occurredAt: "2026-08-04T10:12:18.120Z",
  recordedAt: "2026-08-04T10:12:18.128Z",
  producerService: "orders-command",
  context: {
    requestId: "0198f99a-1234-7000-8000-000000000001",
    correlationId: "0198f999-aaaa-7000-8000-000000000001",
    causationId: null,
    actor: { kind: "user", subjectRef: "usr_1" },
  },
  payload: {},
} as const;

describe("aggregate reconstruction", () => {
  it("folds tail events and rejects revision gaps", () => {
    const events: StoredEvent[] = [1, 2, 3].map((revision) => ({
      ...base,
      eventId: `0198f99a-9b1c-7000-8000-00000000000${revision}`,
      streamRevision: String(revision),
      eventNumber: String(revision),
    }));
    expect(fold(0, (state, event: number) => state + event, [1, 2, 3])).toBe(6);
    expect(
      reconstruct(
        {
          initial: 0,
          evolve: (state, _: unknown) => state + 1,
          decode: (_) => undefined,
        },
        events,
      ),
    ).toEqual({ state: 3, revision: 3n });
    expect(() =>
      reconstruct(
        {
          initial: 0,
          evolve: (state, _: unknown) => state + 1,
          decode: (_) => undefined,
        },
        [events[0]!, events[2]!],
      ),
    ).toThrow("event_gap");
  });
});
