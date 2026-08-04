import { describe, expect, it, vi } from "vitest";
import { ensureCdcSlot } from "../../tools/bootstrap/src/index.js";
import type { Client } from "pg";

const liveRuntime = {
  cdc_slot_name: "event_store_live",
  cdc_connector_name: "event-store-live",
};

function databaseWith(
  rows: Array<Array<Record<string, unknown>>>,
): Pick<Client, "query"> & { query: ReturnType<typeof vi.fn> } {
  const query = vi.fn(async () => ({ rows: rows.shift() ?? [] }));
  return { query } as unknown as Pick<Client, "query"> & {
    query: ReturnType<typeof vi.fn>;
  };
}

describe("CDC bootstrap slot safety", () => {
  it("creates a failover slot only before the first event", async () => {
    const database = databaseWith([
      [liveRuntime],
      [{ exists: false }],
      [{ exists: false }],
      [],
    ]);

    await expect(
      ensureCdcSlot(database, "event-store-live"),
    ).resolves.toBeUndefined();
    expect(database.query).toHaveBeenLastCalledWith(
      "SELECT pg_create_logical_replication_slot($1, 'pgoutput', false, false, true)",
      ["event_store_live"],
    );
  });

  it("rejects a missing live slot once events exist", async () => {
    const database = databaseWith([
      [liveRuntime],
      [{ exists: false }],
      [{ exists: true }],
    ]);

    await expect(ensureCdcSlot(database, "event-store-live")).rejects.toThrow(
      "missing after events exist",
    );
    expect(database.query).not.toHaveBeenCalledWith(
      expect.stringContaining("pg_create_logical_replication_slot"),
      expect.anything(),
    );
  });

  it("never replaces an adopted recovery slot", async () => {
    const database = databaseWith([
      [
        {
          cdc_slot_name: "event_store_recovery",
          cdc_connector_name: "event-store-recovery",
        },
      ],
      [{ exists: false }],
    ]);

    await expect(ensureCdcSlot(database, "event-store-live")).rejects.toThrow(
      "adopted recovery slot event_store_recovery is missing",
    );
    expect(database.query).not.toHaveBeenCalledWith(
      expect.stringContaining("pg_create_logical_replication_slot"),
      expect.anything(),
    );
  });
});
