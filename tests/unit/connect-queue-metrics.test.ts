import { describe, expect, it } from "vitest";
import { connectQueueRatio } from "../../apps/event-store-service/src/index.js";

describe("Connect queue metrics", () => {
  it("reads QueueRemainingCapacity and QueueTotalCapacity", () => {
    expect(
      connectQueueRatio(
        "event_store_connect_queue_remaining_capacity{connector=\"live\"} 1638\n" +
          "event_store_connect_queue_total_capacity{connector=\"live\"} 2048\n",
        "live",
      ),
    ).toBeCloseTo(0.8);
  });

  it("fails closed for absent or invalid metrics", () => {
    expect(() => connectQueueRatio("", "live")).toThrow("missing or ambiguous");
    expect(() =>
      connectQueueRatio(
        "event_store_connect_queue_remaining_capacity{connector=\"live\"} 1\n" +
          "event_store_connect_queue_total_capacity{connector=\"live\"} 0\n",
        "live",
      ),
    ).toThrow("invalid Connect queue total capacity");
  });

  it("selects the runtime-owned connector only", () => {
    const metrics =
      "event_store_connect_queue_remaining_capacity{connector=\"live\"} 1600\n" +
      "event_store_connect_queue_total_capacity{connector=\"live\"} 2000\n" +
      "event_store_connect_queue_remaining_capacity{connector=\"recovery\"} 1\n" +
      "event_store_connect_queue_total_capacity{connector=\"recovery\"} 2000\n";
    expect(connectQueueRatio(metrics, "live")).toBeCloseTo(0.8);
  });
});
