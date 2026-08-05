import { describe, expect, it } from "vitest";
import fc from "fast-check";
import { fold, reconstruct } from "@event-store/aggregate-sdk";
import type { StoredEvent } from "@event-store/contracts";

type CounterEvent = { readonly delta: number };
type CounterState = { readonly total: number; readonly applied: number };

const initial: CounterState = { total: 0, applied: 0 };
const evolve = (state: CounterState, event: CounterEvent): CounterState => ({
  total: state.total + event.delta,
  applied: state.applied + 1,
});

function storedEvents(events: readonly CounterEvent[]): StoredEvent[] {
  return events.map(
    (event, index) =>
      ({
        streamRevision: String(index + 1),
        payload: event,
      }) as StoredEvent,
  );
}

describe("aggregate snapshot properties", () => {
  it("reconstructs every snapshot boundary to the same evolved state", () => {
    fc.assert(
      fc.property(
        fc.array(
          fc.record({ delta: fc.integer({ min: -1_000, max: 1_000 }) }),
          {
            maxLength: 120,
          },
        ),
        (events) => {
          const full = fold(initial, evolve, events);
          const stored = storedEvents(events);
          for (let boundary = 0; boundary <= events.length; boundary += 1) {
            const snapshotState = fold(
              initial,
              evolve,
              events.slice(0, boundary),
            );
            expect(fold(snapshotState, evolve, events.slice(boundary))).toEqual(
              full,
            );
            expect(
              reconstruct(
                {
                  initial,
                  evolve,
                  decode: (event: StoredEvent) => event.payload as CounterEvent,
                },
                stored.slice(boundary),
                boundary === 0
                  ? undefined
                  : { state: snapshotState, revision: BigInt(boundary) },
              ),
            ).toEqual({ state: full, revision: BigInt(events.length) });
          }
        },
      ),
      { numRuns: 1_000 },
    );
  });
});
