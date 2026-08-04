import { readFile } from "node:fs/promises";
import { describe, expect, it } from "vitest";
import { connectQueueRatio } from "../../apps/event-store-service/src/index.js";

describe("Connect queue metrics", () => {
  it("reads QueueRemainingCapacity and QueueTotalCapacity", () => {
    expect(
      connectQueueRatio(
        'event_store_connect_queue_remaining_capacity{connector="live"} 1638\n' +
          'event_store_connect_queue_total_capacity{connector="live"} 2048\n',
        "live",
      ),
    ).toBeCloseTo(0.8);
  });

  it("fails closed for absent or invalid metrics", () => {
    expect(() => connectQueueRatio("", "live")).toThrow("missing or ambiguous");
    expect(() =>
      connectQueueRatio(
        'event_store_connect_queue_remaining_capacity{connector="live"} 1\n' +
          'event_store_connect_queue_total_capacity{connector="live"} 0\n',
        "live",
      ),
    ).toThrow("invalid Connect queue total capacity");
  });

  it("selects the runtime-owned connector only", () => {
    const metrics =
      'event_store_connect_queue_remaining_capacity{connector="live"} 1600\n' +
      'event_store_connect_queue_total_capacity{connector="live"} 2000\n' +
      'event_store_connect_queue_remaining_capacity{connector="recovery"} 1\n' +
      'event_store_connect_queue_total_capacity{connector="recovery"} 2000\n';
    expect(connectQueueRatio(metrics, "live")).toBeCloseTo(0.8);
  });

  it("exports the Debezium streaming health and lag metrics", async () => {
    const config = await readFile(
      new URL(
        "../../deploy/production/connect-metrics-config.yaml",
        import.meta.url,
      ),
      "utf8",
    );
    for (const metric of [
      "event_store_connect_queue_size_bytes",
      "event_store_connect_queue_max_bytes",
      "event_store_connect_milliseconds_behind_source",
      "event_store_connect_milliseconds_behind_source_p95",
      "event_store_connect_milliseconds_behind_source_p99",
      "event_store_connect_source_connected",
      "event_store_connect_committed_transactions_total",
    ])
      expect(config).toContain(metric);
  });

  it("uses the runtime-owned connector name as the Debezium JMX server label", async () => {
    const [connector, runtimeConfig] = await Promise.all([
      readFile("deploy/connector/event-store-live.json", "utf8"),
      readFile(
        "migrations/028_preserve_recovery_connector_ownership.sql",
        "utf8",
      ),
    ]);
    expect(JSON.parse(connector).config["topic.prefix"]).toBe(
      "event-store-live",
    );
    expect(runtimeConfig).toContain("DEFAULT 'event-store-live'");
  });
});
