import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { Client } from "pg";
import { migrate } from "@event-store/migrate";
import { EventStoreStack } from "../fixtures/event-store-stack.js";

const suite = process.env.RUN_INTEGRATION === "true" ? describe : describe.skip;

suite("cluster role secret provisioning", () => {
  const stack = new EventStoreStack();

  beforeAll(async () => {
    await stack.start();
  }, 180_000);
  afterAll(async () => {
    await stack.stop();
  }, 60_000);

  it("sets a TCP login password from the role secret", async () => {
    const previous = process.env.EVENT_STORE_ROLE_PASSWORDS_JSON;
    const password = "cdc-'password";
    process.env.EVENT_STORE_ROLE_PASSWORDS_JSON = JSON.stringify({
      event_store_cdc: password,
    });
    try {
      await migrate(stack.databaseUrl, true);
      const url = new URL(stack.databaseUrl);
      url.username = "event_store_cdc";
      url.password = password;
      const client = new Client({ connectionString: url.toString() });
      await client.connect();
      try {
        await expect(client.query("SELECT 1")).resolves.toBeDefined();
      } finally {
        await client.end();
      }
    } finally {
      if (previous === undefined) delete process.env.EVENT_STORE_ROLE_PASSWORDS_JSON;
      else process.env.EVENT_STORE_ROLE_PASSWORDS_JSON = previous;
    }
  });
});
