import {
  GenericContainer,
  Network,
  RandomPortGenerator,
  Wait,
} from "testcontainers";
import {
  ToxiProxyContainer,
  type CreatedProxy,
} from "@testcontainers/toxiproxy";
import { Pool } from "pg";
import { randomUUID } from "node:crypto";
import { migrate } from "@event-store/migrate";
import { KafkaJS } from "@confluentinc/kafka-javascript";

export class EventStoreStack {
  #network?: Network;
  #postgres?: Awaited<ReturnType<GenericContainer["start"]>>;
  #kafka?: Awaited<ReturnType<GenericContainer["start"]>>;
  #connect?: Awaited<ReturnType<GenericContainer["start"]>>;
  #toxiproxy?: Awaited<ReturnType<ToxiProxyContainer["start"]>>;
  #postgresConnectProxy?: CreatedProxy;

  async createPitrBaseBackup(): Promise<{
    basePath: string;
    archivePath: string;
  }> {
    if (this.#postgres === undefined) throw new Error("stack is not started");
    const basePath = `/tmp/pitr-base-${randomUUID()}`;
    const backup = await this.#postgres.exec([
      "bash",
      "-ceu",
      `PGPASSWORD=postgres pg_basebackup -h 127.0.0.1 -U postgres -D ${basePath} -X stream -Fp`,
    ]);
    if (backup.exitCode !== 0)
      throw new Error(`pg_basebackup failed: ${backup.stderr}`);
    return { basePath, archivePath: "/var/lib/postgresql/archive" };
  }

  async restorePitr(
    backup: { basePath: string; archivePath: string },
    targetTime: string,
  ): Promise<{ databaseUrl: string; stop: () => Promise<void> }> {
    if (this.#postgres === undefined || this.#network === undefined)
      throw new Error("stack is not started");
    const flush = await this.#postgres.exec([
      "bash",
      "-ceu",
      "psql -U postgres -d event_store -c 'CHECKPOINT; SELECT pg_switch_wal(); CHECKPOINT; SELECT pg_switch_wal()'",
    ]);
    if (flush.exitCode !== 0)
      throw new Error(`could not archive WAL for PITR: ${flush.stderr}`);
    const restored = await new GenericContainer("postgres:18.4-bookworm")
      .withEntrypoint(["bash"])
      .withCommand(["-ceu", "while true; do sleep 1; done"])
      .withNetwork(this.#network)
      .withNetworkAliases("pitr-restored")
      .withExposedPorts(5432)
      .withWaitStrategy(Wait.forSuccessfulCommand("true"))
      .start();
    try {
      const createRestoreDirectory = await restored.exec([
        "mkdir",
        "-p",
        "/restore",
      ]);
      if (createRestoreDirectory.exitCode !== 0)
        throw new Error(
          `could not prepare PITR directory: ${createRestoreDirectory.stderr}`,
        );
      await restored.copyArchiveToContainer(
        await this.#postgres.copyArchiveFromContainer(backup.basePath),
        "/restore",
      );
      await restored.copyArchiveToContainer(
        await this.#postgres.copyArchiveFromContainer(backup.archivePath),
        "/restore",
      );
      const configure = await restored.exec([
        "bash",
        "-ceu",
        `mv /restore/${backup.basePath.split("/").at(-1)} /restore/data
         cat >> /restore/data/postgresql.auto.conf <<'EOF'
restore_command = 'cp /restore/archive/%f %p'
recovery_target_time = '${targetTime.replaceAll("'", "''")}'
recovery_target_action = 'promote'
EOF
         touch /restore/data/recovery.signal
         chown -R postgres:postgres /restore
         gosu postgres postgres -D /restore/data \
           -c listen_addresses='*' \
           -c wal_level=logical \
           -c max_replication_slots=10 \
           -c max_wal_senders=10 \
           > /tmp/restored-postgres.log 2>&1 &`,
      ]);
      if (configure.exitCode !== 0)
        throw new Error(
          `could not configure restored PostgreSQL: ${configure.stderr}`,
        );
      const databaseUrl = `postgresql://postgres:postgres@${restored.getHost()}:${restored.getMappedPort(5432)}/event_store`;
      const deadline = Date.now() + 60_000;
      while (Date.now() < deadline) {
        const pool = new Pool({ connectionString: databaseUrl });
        try {
          await pool.query("SELECT pg_is_in_recovery()");
          await pool.end();
          return {
            databaseUrl,
            stop: () => restored.stop().then(() => undefined),
          };
        } catch {
          await pool.end().catch(() => undefined);
          await new Promise((resolve) => setTimeout(resolve, 250));
        }
      }
      throw new Error("restored PostgreSQL did not become ready within 60s");
    } catch (error) {
      await restored.stop().catch(() => undefined);
      throw error;
    }
  }

  get databaseUrl(): string {
    if (this.#postgres === undefined) throw new Error("stack is not started");
    return `postgresql://postgres:postgres@${this.#postgres.getHost()}:${this.#postgres.getMappedPort(5432)}/event_store`;
  }

  get connectUrl(): string {
    if (this.#connect === undefined) throw new Error("stack is not started");
    return `http://${this.#connect.getHost()}:${this.#connect.getMappedPort(8083)}`;
  }

  async start(
    options: { cdc?: boolean; toxiproxy?: boolean } = {},
  ): Promise<void> {
    this.#network = await new Network().start();
    this.#postgres = await new GenericContainer("postgres:18.4-bookworm")
      .withEnvironment({
        POSTGRES_USER: "postgres",
        POSTGRES_PASSWORD: "postgres",
        POSTGRES_DB: "event_store",
      })
      .withCommand([
        "postgres",
        "-c",
        "wal_level=logical",
        "-c",
        "max_replication_slots=10",
        "-c",
        "max_wal_senders=10",
        "-c",
        "max_slot_wal_keep_size=8GB",
        "-c",
        "synchronous_commit=on",
        "-c",
        "track_commit_timestamp=off",
        "-c",
        "archive_mode=on",
        "-c",
        "archive_timeout=1s",
        "-c",
        "archive_command=mkdir -p /var/lib/postgresql/archive && test ! -f /var/lib/postgresql/archive/%f && cp %p /var/lib/postgresql/archive/%f",
      ])
      .withNetwork(this.#network)
      .withNetworkAliases("postgres")
      .withExposedPorts(5432)
      // The official image logs readiness once for bootstrap and again for its TCP server.
      .withWaitStrategy(
        Wait.forLogMessage(/database system is ready to accept connections/, 2),
      )
      .start();
    await migrate(this.databaseUrl, true);
    if (options.cdc !== true) {
      // Storage-only tests intentionally do not start CDC. Production remains
      // fail-closed until bootstrap has proved the slot and connector ready.
      const database = new Pool({ connectionString: this.databaseUrl });
      await database.query(
        "UPDATE event_store.runtime_config SET append_admission_enabled=false WHERE singleton",
      );
      await database.end();
      return;
    }
    if (options.toxiproxy === true) {
      this.#toxiproxy = await new ToxiProxyContainer(
        "ghcr.io/shopify/toxiproxy:2.12.0",
      )
        .withNetwork(this.#network)
        .withNetworkAliases("toxiproxy")
        .start();
      this.#postgresConnectProxy = await this.#toxiproxy.createProxy({
        name: "postgres-connect",
        upstream: "postgres:5432",
      });
    }
    const database = new Pool({ connectionString: this.databaseUrl });
    await database.query("ALTER ROLE event_store_cdc PASSWORD 'cdc'");
    await database.query(
      "SELECT pg_create_logical_replication_slot('event_store_live', 'pgoutput', false, false, true)",
    );
    await database.end();
    const kafkaExternalPort = await new RandomPortGenerator().generatePort();
    this.#kafka = await new GenericContainer("apache/kafka:4.3.1")
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
      .withNetwork(this.#network)
      .withNetworkAliases("kafka")
      .withExposedPorts({ container: 9092, host: kafkaExternalPort })
      .withWaitStrategy(Wait.forLogMessage(/Kafka Server started/))
      .start();
    const kafka = new KafkaJS.Kafka({
      kafkaJS: {
        brokers: [`${this.#kafka.getHost()}:${kafkaExternalPort}`],
      },
    });
    const admin = kafka.admin();
    await admin.connect();
    await admin.createTopics({
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
    await admin.disconnect();
    this.#connect = await new GenericContainer(
      "quay.io/debezium/connect:3.6.0.Final",
    )
      .withEnvironment({
        BOOTSTRAP_SERVERS: "kafka:29092",
        GROUP_ID: "event-store-connect",
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
      .withNetwork(this.#network)
      .withNetworkAliases("connect")
      .withExposedPorts(8083)
      .withWaitStrategy(Wait.forHttp("/connector-plugins", 8083))
      .start();
    await this.createConnector(options.toxiproxy === true);
  }

  async createConnector(viaToxiproxy = false): Promise<void> {
    const response = await fetch(
      `${this.connectUrl}/connectors/event-store-live/config`,
      {
        method: "PUT",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({
          "connector.class":
            "io.debezium.connector.postgresql.PostgresConnector",
          "tasks.max": "1",
          "database.hostname": viaToxiproxy ? "toxiproxy" : "postgres",
          "database.port": viaToxiproxy ? "8666" : "5432",
          "database.user": "event_store_cdc",
          "database.password": "cdc",
          "database.dbname": "event_store",
          "topic.prefix": "event-store-cdc",
          "plugin.name": "pgoutput",
          "publication.name": "event_store_events",
          "publication.autocreate.mode": "disabled",
          "slot.name": "event_store_live",
          "slot.drop.on.stop": "false",
          "slot.failover": "true",
          "schema.include.list": "event_store",
          "table.include.list": "event_store.events",
          "snapshot.mode": "no_data",
          "poll.interval.ms": "5",
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
            "event-store-cdc\\.event_store\\.events",
          transforms: "outbox",
          "transforms.outbox.type": "io.debezium.transforms.outbox.EventRouter",
          "transforms.outbox.predicate": "isCanonicalEvents",
          "transforms.outbox.table.field.event.id": "event_id",
          "transforms.outbox.table.field.event.key": "partition_key",
          "transforms.outbox.table.field.event.type": "event_name",
          "transforms.outbox.table.field.event.payload": "event_envelope",
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
    const deadline = Date.now() + 60_000;
    while (Date.now() < deadline) {
      const status = await fetch(
        `${this.connectUrl}/connectors/event-store-live/status`,
      ).then(
        (result) =>
          result.json() as Promise<{
            connector?: { state?: string };
            tasks?: { state?: string }[];
          }>,
      );
      if (
        status.connector?.state === "RUNNING" &&
        status.tasks?.[0]?.state === "RUNNING"
      )
        return;
      await new Promise((resolve) => setTimeout(resolve, 200));
    }
    throw new Error("connector did not become RUNNING");
  }

  async createSnapshotRecoveryConnector(
    recoveryId: string,
    slotName: string,
    databaseHostname = "postgres",
  ): Promise<string> {
    if (!/^[a-z0-9-]{1,63}$/.test(recoveryId))
      throw new Error("recoveryId must be lowercase alphanumeric/hyphen");
    if (!/^event_store_[a-z0-9_]{1,50}$/.test(slotName))
      throw new Error("slotName must be an event_store logical slot");
    const name = `event-store-recovery-${recoveryId}`;
    const response = await fetch(
      `${this.connectUrl}/connectors/${name}/config`,
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
          "topic.prefix": `event-store-recovery-${recoveryId}`,
          "plugin.name": "pgoutput",
          "publication.name": "event_store_events",
          "publication.autocreate.mode": "disabled",
          "slot.name": slotName,
          "slot.drop.on.stop": "false",
          "slot.failover": "true",
          "schema.include.list": "event_store",
          "table.include.list": "event_store.events",
          "snapshot.mode": "initial",
          "poll.interval.ms": "5",
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
          "predicates.isCanonicalEvents.pattern": `event-store-recovery-${recoveryId}\\.event_store\\.events`,
          transforms: "outbox",
          "transforms.outbox.type": "io.debezium.transforms.outbox.EventRouter",
          "transforms.outbox.predicate": "isCanonicalEvents",
          "transforms.outbox.table.field.event.id": "event_id",
          "transforms.outbox.table.field.event.key": "partition_key",
          "transforms.outbox.table.field.event.type": "event_name",
          "transforms.outbox.table.field.event.payload": "event_envelope",
          "transforms.outbox.route.by.field": "topic_route",
          "transforms.outbox.route.topic.regex": "(.*)",
          "transforms.outbox.route.topic.replacement": "$1.events.v1",
          "transforms.outbox.table.expand.json.payload": "false",
          "transforms.outbox.table.op.invalid.behavior": "fatal",
          "transforms.outbox.table.fields.additional.placement":
            "event_id:header:id,event_name:header:type,envelope_sha256:header:envelopeHash,namespace:header:namespace,aggregate_type:header:aggregateType,stream_revision:header:streamRevision",
        }),
      },
    );
    if (!response.ok)
      throw new Error(
        `recovery connector creation failed: ${await response.text()}`,
      );
    await this.waitForConnector(name);
    return name;
  }

  async deleteConnector(name: string): Promise<void> {
    const response = await fetch(`${this.connectUrl}/connectors/${name}`, {
      method: "DELETE",
    });
    if (!response.ok && response.status !== 404)
      throw new Error(`connector deletion failed: ${await response.text()}`);
  }

  async waitForConnector(name: string): Promise<void> {
    const deadline = Date.now() + 60_000;
    while (Date.now() < deadline) {
      const response = await fetch(
        `${this.connectUrl}/connectors/${name}/status`,
      );
      if (response.ok) {
        const status = (await response.json()) as {
          connector?: { state?: string };
          tasks?: { state?: string }[];
        };
        if (
          status.connector?.state === "RUNNING" &&
          status.tasks?.length === 1 &&
          status.tasks[0]?.state === "RUNNING"
        )
          return;
      }
      await new Promise((resolve) => setTimeout(resolve, 200));
    }
    throw new Error(`connector ${name} did not become RUNNING`);
  }

  kafkaBroker(): string {
    if (this.#kafka === undefined) throw new Error("CDC stack is not started");
    return `${this.#kafka.getHost()}:${this.#kafka.getMappedPort(9092)}`;
  }

  async cdcDiagnostic(): Promise<unknown> {
    const status = await fetch(
      `${this.connectUrl}/connectors/event-store-live/status`,
    ).then((result) => result.json());
    const pool = new Pool({ connectionString: this.databaseUrl });
    try {
      const slot = await pool.query(
        "SELECT active, confirmed_flush_lsn, invalidation_reason FROM pg_replication_slots WHERE slot_name='event_store_live'",
      );
      return { status, slot: slot.rows[0] ?? null };
    } finally {
      await pool.end();
    }
  }

  async setPostgresConnectEnabled(enabled: boolean): Promise<void> {
    if (this.#postgresConnectProxy === undefined)
      throw new Error("Postgres-to-Connect Toxiproxy is not configured");
    await this.#postgresConnectProxy.setEnabled(enabled);
  }

  async stopConnect(): Promise<void> {
    await this.#connect?.stop();
    this.#connect = undefined;
  }

  async pool(): Promise<Pool> {
    return new Pool({ connectionString: this.databaseUrl });
  }

  async stop(): Promise<void> {
    await this.#connect?.stop();
    await this.#kafka?.stop();
    await this.#toxiproxy?.stop();
    await this.#postgres?.stop();
    await this.#network?.stop();
  }
}
