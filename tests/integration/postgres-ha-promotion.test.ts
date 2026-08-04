import { afterAll, beforeAll, describe, expect, it } from "vitest";
import {
  GenericContainer,
  Network,
  RandomPortGenerator,
  Wait,
} from "testcontainers";
import { Pool } from "pg";
import { KafkaJS } from "@confluentinc/kafka-javascript";
import { migrate } from "@event-store/migrate";
import { reconcile } from "@event-store/reconcile";
import { PostgresEventStore } from "@event-store/postgres-store";
import { canonicalJson, uuidv7 } from "@event-store/contracts";

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
  let kafka: Awaited<ReturnType<GenericContainer["start"]>>;
  let connect: Awaited<ReturnType<GenericContainer["start"]>>;
  let primaryUrl: string;
  let standbyUrl: string;
  let kafkaBroker: string;
  let connectUrl: string;

  const waitForConnector = async (): Promise<void> => {
    await eventually(async () => {
      const status = (await fetch(
        `${connectUrl}/connectors/event-store-live/status`,
      ).then((response) => response.json())) as {
        connector?: { state?: string };
        tasks?: { state?: string }[];
      };
      return (
        status.connector?.state === "RUNNING" &&
        status.tasks?.length === 1 &&
        status.tasks[0]?.state === "RUNNING"
      );
    });
  };

  const configureConnector = async (
    databaseHostname: string,
  ): Promise<void> => {
    const response = await fetch(
      `${connectUrl}/connectors/event-store-live/config`,
      {
        method: "PUT",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({
          "connector.class":
            "io.debezium.connector.postgresql.PostgresConnector",
          "tasks.max": "1",
          "database.hostname": databaseHostname,
          "database.port": "5432",
          "database.user": "event_store_cdc",
          "database.password": "cdc",
          "database.dbname": "event_store",
          "topic.prefix": "event-store-live",
          "plugin.name": "pgoutput",
          "publication.name": "event_store_events",
          "publication.autocreate.mode": "disabled",
          "slot.name": "event_store_live",
          "slot.drop.on.stop": "false",
          "slot.failover": "true",
          "schema.include.list": "event_store",
          "table.include.list": "event_store.events",
          "snapshot.mode": "no_data",
          "poll.interval.ms": "2",
          "max.batch.size": "2048",
          "max.queue.size": "8192",
          "lsn.flush.mode": "connector",
          "offset.mismatch.strategy": "trust_offset",
          "exactly.once.support": "required",
          "transaction.boundary": "poll",
          "errors.tolerance": "none",
          predicates: "isCanonicalEvents",
          "predicates.isCanonicalEvents.type":
            "org.apache.kafka.connect.transforms.predicates.TopicNameMatches",
          "predicates.isCanonicalEvents.pattern":
            "event-store-live\\.event_store\\.events",
          transforms: "outbox",
          "transforms.outbox.type": "io.debezium.transforms.outbox.EventRouter",
          "transforms.outbox.predicate": "isCanonicalEvents",
          "transforms.outbox.table.field.event.id": "event_id",
          "transforms.outbox.table.field.event.key": "partition_key",
          "transforms.outbox.table.field.event.type": "event_name",
          "transforms.outbox.table.field.event.payload": "event_envelope_kafka",
          "transforms.outbox.table.field.event.timestamp": "recorded_at_kafka",
          "transforms.outbox.route.by.field": "topic_route",
          "transforms.outbox.route.topic.regex": "(.*)",
          "transforms.outbox.route.topic.replacement": "$1.events.v1",
          "transforms.outbox.table.expand.json.payload": "false",
          "transforms.outbox.table.op.invalid.behavior": "fatal",
          "transforms.outbox.route.tombstone.on.empty.payload": "false",
          "transforms.outbox.table.fields.additional.placement":
            "event_id:header:id,event_name:header:type,envelope_sha256:header:envelopeHash,namespace:header:namespace,aggregate_type:header:aggregateType,stream_revision:header:streamRevision",
        }),
      },
    );
    if (!response.ok)
      throw new Error(`connector creation failed: ${await response.text()}`);
    await waitForConnector();
  };

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
         mkdir -p "$PGDATA"
         chown -R postgres:postgres "$PGDATA"
         chmod 700 "$PGDATA"
         rm -rf "$PGDATA"/*
         PGPASSWORD=postgres gosu postgres pg_basebackup -h ha-primary -U postgres -D "$PGDATA" -R -X stream -C -S standby_slot
         printf "primary_conninfo = 'host=ha-primary port=5432 user=postgres password=postgres dbname=event_store application_name=ha-standby'\\nprimary_slot_name = 'standby_slot'\\nhot_standby_feedback = on\\nsync_replication_slots = on\\n" >> "$PGDATA/postgresql.auto.conf"
         exec gosu postgres postgres -D "$PGDATA" -c hot_standby=on -c wal_level=logical`,
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
    const unsyncedCandidate = new Pool({ connectionString: standbyUrl });
    try {
      await expect(
        unsyncedCandidate.query(
          "SELECT event_store.assert_failover_candidate('event_store_live')",
        ),
      ).rejects.toMatchObject({ code: "P0001" });
    } finally {
      await unsyncedCandidate.end();
    }
    const pool = new Pool({ connectionString: primaryUrl });
    try {
      await pool.query("ALTER ROLE event_store_cdc PASSWORD 'cdc'");
      await pool.query(
        "ALTER SYSTEM SET synchronous_standby_names = 'FIRST 1 (\"ha-standby\")'",
      );
      await pool.query("SELECT pg_reload_conf()");
      await eventually(async () => {
        const replication = await pool.query<{ sync_state: string }>(
          "SELECT sync_state FROM pg_stat_replication WHERE application_name='ha-standby'",
        );
        return replication.rows[0]?.sync_state === "sync";
      });
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
    const kafkaExternalPort = await new RandomPortGenerator().generatePort();
    kafka = await new GenericContainer("apache/kafka:4.3.1")
      .withEnvironment({
        KAFKA_NODE_ID: "1",
        KAFKA_PROCESS_ROLES: "broker,controller",
        KAFKA_CONTROLLER_QUORUM_VOTERS: "1@kafka:29093",
        KAFKA_LISTENERS:
          "PLAINTEXT://kafka:29092,CONTROLLER://kafka:29093,EXTERNAL://0.0.0.0:9092",
        KAFKA_ADVERTISED_LISTENERS: `PLAINTEXT://kafka:29092,EXTERNAL://localhost:${kafkaExternalPort}`,
        KAFKA_LISTENER_SECURITY_PROTOCOL_MAP:
          "CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT,EXTERNAL:PLAINTEXT",
        KAFKA_INTER_BROKER_LISTENER_NAME: "PLAINTEXT",
        KAFKA_CONTROLLER_LISTENER_NAMES: "CONTROLLER",
        KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: "1",
        KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: "1",
        KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: "1",
        KAFKA_AUTO_CREATE_TOPICS_ENABLE: "false",
      })
      .withNetwork(network)
      .withNetworkAliases("kafka")
      .withExposedPorts({ container: 9092, host: kafkaExternalPort })
      .withWaitStrategy(Wait.forLogMessage(/Kafka Server started/))
      .start();
    kafkaBroker = `${kafka.getHost()}:${kafka.getMappedPort(9092)}`;
    const kafkaAdmin = new KafkaJS.Kafka({
      kafkaJS: { brokers: [kafkaBroker] },
    }).admin();
    await kafkaAdmin.connect();
    await kafkaAdmin.createTopics({
      topics: [
        {
          topic: "event-store.events.v1",
          numPartitions: 24,
          replicationFactor: 1,
        },
        {
          topic: "_connect-event-store-configs",
          numPartitions: 1,
          replicationFactor: 1,
          configEntries: [{ name: "cleanup.policy", value: "compact" }],
        },
        {
          topic: "_connect-event-store-offsets",
          numPartitions: 25,
          replicationFactor: 1,
          configEntries: [{ name: "cleanup.policy", value: "compact" }],
        },
        {
          topic: "_connect-event-store-status",
          numPartitions: 5,
          replicationFactor: 1,
          configEntries: [{ name: "cleanup.policy", value: "compact" }],
        },
      ],
    });
    await kafkaAdmin.disconnect();
    connect = await new GenericContainer("quay.io/debezium/connect:3.6.0.Final")
      .withEnvironment({
        BOOTSTRAP_SERVERS: "kafka:29092",
        GROUP_ID: "event-store-connect-ha",
        CONFIG_STORAGE_TOPIC: "_connect-event-store-configs",
        OFFSET_STORAGE_TOPIC: "_connect-event-store-offsets",
        STATUS_STORAGE_TOPIC: "_connect-event-store-status",
        CONFIG_STORAGE_REPLICATION_FACTOR: "1",
        OFFSET_STORAGE_REPLICATION_FACTOR: "1",
        STATUS_STORAGE_REPLICATION_FACTOR: "1",
        KEY_CONVERTER: "org.apache.kafka.connect.storage.StringConverter",
        VALUE_CONVERTER: "org.apache.kafka.connect.storage.StringConverter",
        EXACTLY_ONCE_SOURCE_SUPPORT: "enabled",
        CONNECT_EXACTLY_ONCE_SOURCE_SUPPORT: "enabled",
      })
      .withNetwork(network)
      .withNetworkAliases("connect")
      .withExposedPorts(8083)
      .withWaitStrategy(Wait.forHttp("/connector-plugins", 8083))
      .start();
    connectUrl = `http://${connect.getHost()}:${connect.getMappedPort(8083)}`;
    await configureConnector("ha-primary");
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
    await connect?.stop().catch(() => undefined);
    await kafka?.stop().catch(() => undefined);
    await standby?.stop().catch(() => undefined);
    await primary?.stop().catch(() => undefined);
    await network?.stop().catch(() => undefined);
  }, 60_000);

  it("resumes Debezium delivery after promotion without losing acknowledged events", async () => {
    const beforePromotionRequestId = uuidv7();
    const afterPromotionRequestId = uuidv7();
    const expectedRequestIds = new Set([
      beforePromotionRequestId,
      afterPromotionRequestId,
    ]);
    const receivedRequestIds = new Set<string>();
    const consumer = new KafkaJS.Kafka({
      kafkaJS: { brokers: [kafkaBroker] },
    }).consumer({
      kafkaJS: {
        groupId: `ha-promotion-${uuidv7()}`,
        autoCommit: false,
        fromBeginning: true,
        readUncommitted: false,
      },
    });
    await consumer.connect();
    await consumer.subscribe({
      topics: ["event-store.events.v1"],
      replace: true,
    });
    let delivered!: () => void;
    let rejected!: (error: Error) => void;
    const allDelivered = new Promise<void>((resolve, reject) => {
      delivered = resolve;
      rejected = reject;
    });
    const deliveryTimeout = setTimeout(
      () =>
        rejected(new Error("CDC did not deliver every failover-window event")),
      60_000,
    );
    await consumer.run({
      eachMessage: async ({ message }) => {
        const value = message.value?.toString() ?? "";
        const envelope = JSON.parse(value || "{}") as {
          context?: { requestId?: string };
          recordedAt?: string;
        };
        const requestId = envelope.context?.requestId;
        if (requestId !== undefined && expectedRequestIds.has(requestId)) {
          expect(value).toBe(canonicalJson(envelope));
          expect(message.timestamp).toBe(
            String(Date.parse(envelope.recordedAt!)),
          );
          receivedRequestIds.add(requestId);
          if (receivedRequestIds.size === expectedRequestIds.size) delivered();
        }
      },
    });
    const appendProbe = async (
      databaseUrl: string,
      requestId: string,
      phase: string,
    ): Promise<void> => {
      const pool = new Pool({ connectionString: databaseUrl });
      try {
        await new PostgresEventStore(pool).append({
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
              payload: { phase },
            },
          ],
        });
      } finally {
        await pool.end();
      }
    };
    try {
      await appendProbe(
        primaryUrl,
        beforePromotionRequestId,
        "before-promotion",
      );
      await eventually(async () =>
        receivedRequestIds.has(beforePromotionRequestId),
      );
      const promotionStartedAt = Date.now();
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
      expect(Date.now() - promotionStartedAt).toBeLessThanOrEqual(60_000);
      await configureConnector("ha-standby");
      await expect(
        appendProbe(standbyUrl, afterPromotionRequestId, "after-promotion"),
      ).rejects.toMatchObject({ code: "P0001" });
      const promotedAdmission = new Pool({ connectionString: standbyUrl });
      try {
        const projectionName = "ha-promotion-reconciliation";
        const generationId = uuidv7();
        await promotedAdmission.query(
          `INSERT INTO projection_runtime.generations(projection_name,generation_id,status,created_at)
           VALUES ($1,$2,'active',clock_timestamp())`,
          [projectionName, generationId],
        );
        await promotedAdmission.query(
          `INSERT INTO projection_runtime.inbox(
             projection_name,generation_id,event_id,envelope_sha256,topic_name,partition_no,kafka_offset,processed_at
           )
           SELECT $1,$2,event_id,envelope_sha256,'event-store.events.v1',0,event_number,clock_timestamp()
             FROM event_store.events`,
          [projectionName, generationId],
        );
        await promotedAdmission.query(
          "SELECT event_store.record_cdc_timeline_reconciliation($1,$2,event_store.current_timeline_id())",
          [projectionName, generationId],
        );
        await promotedAdmission.query(
          "SELECT event_store.set_cdc_delivery_health_on_timeline(event_store.current_timeline_id())",
        );
      } finally {
        await promotedAdmission.end();
      }
      await appendProbe(standbyUrl, afterPromotionRequestId, "after-promotion");
      await allDelivered;
      await expect(reconcile(standbyUrl)).resolves.toMatchObject({
        count: "2",
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
        await expect(
          promoted.query(
            "SELECT event_store.assert_failover_candidate('event_store_live')",
          ),
        ).resolves.toBeDefined();
      } finally {
        await promoted.end();
      }
    } finally {
      clearTimeout(deliveryTimeout);
      await consumer.disconnect().catch(() => undefined);
    }
  }, 90_000);
});
