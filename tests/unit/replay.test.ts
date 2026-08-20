import { describe, expect, it } from "vitest";
import {
  replayConnectorConfig,
  recoveryConnectorConfig,
  recoveryConnectorName,
  recoverySlotName,
  replaySlotName,
  replayTopicName,
} from "@event-store/replay";
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
      "transforms.outbox.table.field.event.payload": "event_envelope_kafka",
      "transforms.outbox.table.field.event.timestamp": "recorded_at_kafka",
      "exactly.once.support": "required",
      "transaction.boundary": "poll",
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

  it("bounds the replay ID to a valid PostgreSQL replication-slot name", () => {
    const replayId = "a".repeat(44);
    expect(replaySlotName(replayId)).toHaveLength(63);
    expect(() => replayTopicName("a".repeat(45))).toThrow(
      "at most 44 characters",
    );
  });
});

describe("slot-loss recovery connector", () => {
  it("uses an isolated failover slot and restores canonical live delivery", () => {
    const config = recoveryConnectorConfig("slot-loss-aug-2026", {
      hostname: "postgres",
      port: 5432,
      user: "event_store_cdc",
      password: "secret",
      dbname: "event_store",
    });
    expect(recoverySlotName("slot-loss-aug-2026")).toBe(
      "event_store_recovery_slot_loss_aug_2026",
    );
    expect(recoveryConnectorName("slot-loss-aug-2026")).toBe(
      "event-store-recovery-slot-loss-aug-2026",
    );
    expect(() => recoverySlotName("a".repeat(42))).toThrow(
      "at most 41 characters",
    );
    expect(config).toMatchObject({
      "slot.drop.on.stop": "false",
      "slot.failover": "true",
      "snapshot.mode": "initial",
      "snapshot.select.statement.overrides": "event_store.events",
      "snapshot.select.statement.overrides.event_store.events":
        "SELECT * FROM event_store.events ORDER BY event_number",
      "lsn.flush.mode": "connector",
      "offset.mismatch.strategy": "trust_slot",
      "errors.tolerance": "none",
      "transforms.outbox.table.field.event.payload": "event_envelope_kafka",
      "transforms.outbox.route.topic.replacement": "$1.events.v1",
      "transforms.outbox.table.op.invalid.behavior": "fatal",
    });
  });
});

describe("live Connect worker", () => {
  it("keeps the canonical envelope as an unquoted JSON record", async () => {
    const worker = await readFile("deploy/connect/worker.properties", "utf8");
    expect(worker).toContain(
      "value.converter=org.apache.kafka.connect.storage.StringConverter",
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
    expect(
      connector.config["transforms.outbox.table.field.event.timestamp"],
    ).toBe("recorded_at_kafka");
    expect(connector.config["time.precision.mode"]).toBeUndefined();
  });
});
