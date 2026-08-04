import { describe, expect, it } from "vitest";
import { replayConnectorConfig, replayTopicName } from "@event-store/replay";
import { readFile } from "node:fs/promises";

describe("replay connector", () => {
  it("uses the same canonical envelope and headers as the live connector", () => {
    const config = replayConnectorConfig("aug-2026", {
      hostname: "postgres",
      port: 5432,
      user: "event_store_cdc",
      password: "secret",
      dbname: "event_store",
    });
    expect(config).toMatchObject({
      "transforms.outbox.table.field.event.id": "event_id",
      "transforms.outbox.table.field.event.key": "partition_key",
      "transforms.outbox.table.field.event.type": "event_name",
      "transforms.outbox.table.field.event.payload": "event_envelope",
      "transforms.outbox.table.expand.json.payload": "false",
      "transforms.outbox.route.topic.replacement":
        "event-store.replay.aug-2026.v1",
    });
    expect(
      config["transforms.outbox.table.fields.additional.placement"],
    ).toContain("envelope_sha256:header:envelopeHash");
  });
});

describe("replay topic", () => {
  it("uses the stable, per-replay route topic name", () => {
    expect(replayTopicName("aug-2026")).toBe("event-store.replay.aug-2026.v1");
  });
});

describe("live Connect worker", () => {
  it("keeps the canonical envelope as an unquoted JSON record", async () => {
    const worker = await readFile("deploy/connect/worker.properties", "utf8");
    expect(worker).toContain(
      "value.converter=org.apache.kafka.connect.json.JsonConverter",
    );
  });

  it("places every projection-integrity header in the live connector", async () => {
    const connector = JSON.parse(
      await readFile("deploy/connector/event-store-live.json", "utf8"),
    ) as { config: Record<string, string> };
    const placement =
      connector.config["transforms.outbox.table.fields.additional.placement"];
    expect(placement).toContain("event_id:header:id");
    expect(placement).toContain("event_name:header:type");
    expect(placement).toContain("envelope_sha256:header:envelopeHash");
  });
});
