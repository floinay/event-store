import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { GenericContainer, Network, Wait } from "testcontainers";
import { Pool } from "pg";
import { migrate } from "@event-store/migrate";
import { reconcile } from "@event-store/reconcile";
import { PostgresEventStore } from "@event-store/postgres-store";
import { uuidv7 } from "@event-store/contracts";

const suite = process.env.RUN_INTEGRATION === "true" ? describe : describe.skip;
const image = "postgres:18.4-bookworm";

async function eventually(
  check: () => Promise<boolean>,
  timeoutMs = 60_000,
): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (await check()) return;
    await new Promise((resolve) => setTimeout(resolve, 200));
  }
  throw new Error(`condition did not become true within ${timeoutMs}ms`);
}

suite("PostgreSQL HA promotion", () => {
  let network: Network;
  let primary: Awaited<ReturnType<GenericContainer["start"]>>;
  let standby: Awaited<ReturnType<GenericContainer["start"]>>;
  let primaryUrl: string;
  let standbyUrl: string;

  beforeAll(async () => {
    network = await new Network().start();
    primary = await new GenericContainer(image)
      .withEnvironment({
        POSTGRES_USER: "postgres",
        POSTGRES_PASSWORD: "postgres",
        POSTGRES_DB: "event_store",
        POSTGRES_HOST_AUTH_METHOD: "trust",
      })
      .withCommand([
        "postgres",
        "-c",
        "wal_level=logical",
        "-c",
        "max_replication_slots=10",
        "-c",
        "max_wal_senders=10",
      ])
      .withNetwork(network)
      .withNetworkAliases("ha-primary")
      .withExposedPorts(5432)
      .withWaitStrategy(
        Wait.forLogMessage(/database system is ready to accept connections/, 2),
      )
      .start();
    primaryUrl = `postgresql://postgres:postgres@${primary.getHost()}:${primary.getMappedPort(5432)}/event_store`;
    await migrate(primaryUrl, true);
    const replicationHba = await primary.exec([
      "bash",
      "-ceu",
      'printf \'\\nhost replication all all trust\\n\' >> "$PGDATA/pg_hba.conf"; gosu postgres pg_ctl reload -D "$PGDATA"',
    ]);
    if (replicationHba.exitCode !== 0)
      throw new Error(
        `could not enable replication HBA: ${replicationHba.stderr}`,
      );
    standby = await new GenericContainer(image)
      .withEntrypoint(["bash"])
      .withCommand([
        "-ceu",
        `until pg_isready -h ha-primary -U postgres; do sleep 1; done
         mkdir -p \"$PGDATA\"
         chown -R postgres:postgres \"$PGDATA\"
         chmod 700 \"$PGDATA\"
         rm -rf \"$PGDATA\"/*
         PGPASSWORD=postgres gosu postgres pg_basebackup -h ha-primary -U postgres -D \"$PGDATA\" -R -X stream -C -S standby_slot
         printf \"primary_conninfo = 'host=ha-primary port=5432 user=postgres password=postgres dbname=event_store application_name=ha-standby'\\nprimary_slot_name = 'standby_slot'\\nhot_standby_feedback = on\\nsync_replication_slots = on\\n\" >> \"$PGDATA/postgresql.auto.conf\"
         exec gosu postgres postgres -D \"$PGDATA\" -c hot_standby=on -c wal_level=logical`,
      ])
      .withNetwork(network)
      .withNetworkAliases("ha-standby")
      .withExposedPorts(5432)
      .withWaitStrategy(
        Wait.forLogMessage(
          /database system is ready to accept (read-only )?connections/,
        ),
      )
      .start();
    standbyUrl = `postgresql://postgres:postgres@${standby.getHost()}:${standby.getMappedPort(5432)}/event_store`;
    await eventually(async () => {
      const pool = new Pool({ connectionString: standbyUrl });
      try {
        return (
          (
            await pool.query<{ recovery: boolean }>(
              "SELECT pg_is_in_recovery() AS recovery",
            )
          ).rows[0]?.recovery === true
        );
      } finally {
        await pool.end();
      }
    });
    const pool = new Pool({ connectionString: primaryUrl });
    try {
      await pool.query(
        "SELECT * FROM pg_create_logical_replication_slot('event_store_live', 'pgoutput', false, false, true)",
      );
    } finally {
      await pool.end();
    }
    await eventually(async () => {
      const pool = new Pool({ connectionString: standbyUrl });
      try {
        const result = await pool.query<{ synced: boolean; failover: boolean }>(
          "SELECT synced, failover FROM pg_replication_slots WHERE slot_name='event_store_live'",
        );
        return (
          result.rows[0]?.synced === true && result.rows[0]?.failover === true
        );
      } finally {
        await pool.end();
      }
    });
    const admission = new Pool({ connectionString: primaryUrl });
    try {
      await admission.query(
        "SELECT event_store.enable_append_admission(8589934592)",
      );
    } finally {
      await admission.end();
    }
  }, 180_000);

  afterAll(async () => {
    await standby?.stop().catch(() => undefined);
    await primary?.stop().catch(() => undefined);
    await network?.stop().catch(() => undefined);
  }, 60_000);

  it("requires synced logical slot before promotion and preserves reconciliation", async () => {
    const pool = new Pool({ connectionString: primaryUrl });
    try {
      const store = new PostgresEventStore(pool);
      const requestId = uuidv7();
      await store.append({
        producerService: "ha-test",
        namespace: "ha",
        aggregateType: "Probe",
        aggregateId: uuidv7(),
        requestId,
        expectedRevision: { kind: "no_stream" },
        context: {
          requestId,
          correlationId: uuidv7(),
          causationId: null,
          actor: { kind: "system", subjectRef: "ha-test" },
        },
        events: [
          {
            eventName: "ha.appended",
            schemaVersion: 1,
            occurredAt: new Date().toISOString(),
            payload: { phase: "before-promotion" },
          },
        ],
      });
    } finally {
      await pool.end();
    }
    await primary.stop();
    const promote = await standby.exec([
      "bash",
      "-ceu",
      'gosu postgres pg_ctl promote -D "$PGDATA"',
    ]);
    expect(promote.exitCode).toBe(0);
    await eventually(async () => {
      const pool = new Pool({ connectionString: standbyUrl });
      try {
        return (
          (
            await pool.query<{ recovery: boolean }>(
              "SELECT pg_is_in_recovery() AS recovery",
            )
          ).rows[0]?.recovery === false
        );
      } finally {
        await pool.end();
      }
    });
    await expect(reconcile(standbyUrl)).resolves.toMatchObject({
      count: "1",
      revisionGaps: "0",
      envelopeHashMismatches: "0",
    });
    const promoted = new Pool({ connectionString: standbyUrl });
    try {
      const slot = await promoted.query<{
        failover: boolean;
        temporary: boolean;
        invalidation_reason: string | null;
      }>(
        "SELECT failover, temporary, invalidation_reason FROM pg_replication_slots WHERE slot_name='event_store_live'",
      );
      expect(slot.rows[0]).toMatchObject({
        failover: true,
        temporary: false,
        invalidation_reason: null,
      });
    } finally {
      await promoted.end();
    }
  }, 90_000);
});
