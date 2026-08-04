import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { verifyKafkaReadiness } from "../../apps/event-store-service/src/index.js";
import { EventStoreStack } from "../fixtures/event-store-stack.js";

const suite = process.env.RUN_INTEGRATION === "true" ? describe : describe.skip;

suite("Kafka readiness", () => {
  const stack = new EventStoreStack();
  beforeAll(async () => {
    await stack.start({ cdc: true });
  }, 180_000);
  afterAll(async () => {
    await stack.stop();
  }, 60_000);

  it("fails closed when the live topic cannot satisfy its required ISR", async () => {
    await expect(
      verifyKafkaReadiness([stack.kafkaBroker()], "event-store.events.v1", 1),
    ).resolves.toBeUndefined();
    await expect(
      verifyKafkaReadiness([stack.kafkaBroker()], "event-store.events.v1", 2),
    ).rejects.toThrow("requires 2");
  }, 60_000);
});
