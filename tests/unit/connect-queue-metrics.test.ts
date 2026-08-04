import { describe, expect, it } from "vitest";
import { connectQueueRatio } from "../../apps/event-store-service/src/index.js";

describe("Connect queue metrics", () => {
  it("reads QueueRemainingCapacity and QueueTotalCapacity", () => {
    expect(
      connectQueueRatio(
        "event_store_connect_queue_remaining_capacity{connector=\"live\"} 1638\n" +
          "event_store_connect_queue_total_capacity{connector=\"live\"} 2048\n",
      ),
    ).toBeCloseTo(0.8);
  });

  it("fails closed for absent or invalid metrics", () => {
    expect(() => connectQueueRatio("")).toThrow("missing Connect metric");
    expect(() =>
      connectQueueRatio(
        "event_store_connect_queue_remaining_capacity 1\n" +
          "event_store_connect_queue_total_capacity 0\n",
      ),
    ).toThrow("invalid Connect queue total capacity");
  });
});
