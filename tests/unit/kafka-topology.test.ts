import { readFile } from "node:fs/promises";
import { describe, expect, it } from "vitest";

interface Topic {
  name: string;
  partitions: number;
  replicationFactor: number;
  configs: Record<string, string>;
}

describe("production Kafka topology", () => {
  it("provisions durable live and Connect internal topics", async () => {
    const topics = JSON.parse(
      await readFile("deploy/kafka/topics.json", "utf8"),
    ) as Topic[];
    const byName = new Map(topics.map((topic) => [topic.name, topic]));
    for (const name of [
      "event-store.events.v1",
      "event-store.projection-dlq.v1",
      "__debezium-heartbeat.event-store-live",
      "_connect-event-store-configs",
      "_connect-event-store-offsets",
      "_connect-event-store-status",
    ]) {
      const topic = byName.get(name);
      expect(topic?.replicationFactor).toBe(3);
      expect(topic?.configs["min.insync.replicas"]).toBe("2");
    }
    expect(
      byName.get("_connect-event-store-offsets")?.configs["cleanup.policy"],
    ).toBe("compact");
  });
});
