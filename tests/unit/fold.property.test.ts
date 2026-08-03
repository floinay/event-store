import { describe, expect, it } from "vitest";
import fc from "fast-check";
import { fold } from "@event-store/aggregate-sdk";

describe("reducer properties", () => {
  it("is associative across an arbitrary snapshot boundary", () => {
    fc.assert(
      fc.property(
        fc.array(fc.integer({ min: -1_000, max: 1_000 }), { maxLength: 500 }),
        fc.nat(),
        (events, boundary) => {
          const at = boundary % (events.length + 1);
          const full = fold(0, (state, event: number) => state + event, events);
          const snapshotAndTail = fold(
            fold(
              0,
              (state, event: number) => state + event,
              events.slice(0, at),
            ),
            (state, event: number) => state + event,
            events.slice(at),
          );
          expect(snapshotAndTail).toBe(full);
        },
      ),
      { numRuns: 10_000 },
    );
  });
});
