import { readFile } from "node:fs/promises";
import { describe, expect, it } from "vitest";

describe("production HA topology", () => {
  it("requires synchronous PostgreSQL, synchronized logical slots, and three runtime members", async () => {
    const [
      postgres,
      kafka,
      runtime,
      preflight,
      bootstrap,
      connector,
      localPostgres,
      compose,
      monitoring,
      postgresMetrics,
    ] = await Promise.all(
      ["postgres.yaml", "kafka.yaml", "runtime.yaml"]
        .map((file) => readFile(`deploy/production/${file}`, "utf8"))
        .concat([
          readFile("deploy/event-store/failover-preflight-job.yaml", "utf8"),
          readFile("deploy/event-store/bootstrap-job.yaml", "utf8"),
          readFile("deploy/connector/event-store-live.json", "utf8"),
          readFile("deploy/postgres/postgresql.conf", "utf8"),
          readFile("deploy/docker-compose.yml", "utf8"),
          readFile("deploy/production/monitoring.yaml", "utf8"),
          readFile("deploy/production/postgres-metrics.yaml", "utf8"),
        ]),
    );
    expect(postgres).toContain("instances: 3");
    expect(postgres).toContain('track_commit_timestamp: "off"');
    expect(localPostgres).toContain("track_commit_timestamp = off");
    expect(compose).toContain('"-c",\n        "track_commit_timestamp=off"');
    expect(postgres).toContain("bootstrap:");
    expect(postgres).toContain("database: event_store");
    expect(postgres).toContain("dataChecksums: true");
    expect(postgres).toContain("minSyncReplicas: 1");
    expect(postgres).toContain("synchronizeLogicalDecoding: true");
    expect(postgres).toContain("synchronizeReplicas:");
    expect(postgres).toContain('retentionPolicy: "35d"');
    expect(postgres).toContain("walStorage:");
    expect(postgres).toContain("size: 256Gi");
    expect(postgres).toContain('requests: { cpu: "8", memory: 32Gi }');
    expect(postgres).toContain("kind: ScheduledBackup");
    expect(postgres).toContain('schedule: "0 0 0 * * *"');
    expect(kafka).toContain("replicas: 3");
    expect(kafka).toContain("roles: [controller, broker]");
    expect(kafka).toContain("min.insync.replicas: 2");
    expect(kafka).toContain("size: 2Ti");
    expect(kafka).toContain('requests: { cpu: "8", memory: 32Gi }');
    expect(kafka).toContain("metricsConfig:");
    expect(kafka).toContain("event-store-kafka-metrics");
    expect(runtime.match(/replicas: 3/g)).toHaveLength(2);
    expect(runtime).toContain("EXACTLY_ONCE_SOURCE_SUPPORT");
    expect(runtime).toContain("CONNECT_EXACTLY_ONCE_SOURCE_SUPPORT");
    expect(runtime).toContain("CONSUMER_ISOLATION_LEVEL");
    expect(runtime).toContain("HEADER_CONVERTER");
    expect(runtime).toContain("TOPIC_CREATION_ENABLE");
    expect(runtime).toContain("CONNECT_METRICS_PORT");
    expect(runtime).toContain("jmx-exporter");
    expect(runtime).toContain("PRODUCER_SERVICE");
    expect(runtime).toContain("HTTP_LISTEN_ADDRESS");
    expect(runtime).toContain("NODE_OPTIONS");
    expect(runtime).toContain("name: NODE_ENV");
    expect(runtime).toContain("value: production");
    expect(runtime).toContain("--max-old-space-size=6144");
    expect(runtime).toContain("memory: 4Gi");
    expect(runtime).toContain("GRPC_TLS_CERT_PEM");
    expect(runtime).toContain("event-store-grpc-tls");
    expect(runtime).toContain("CRITICAL_DATABASE_URL");
    expect(runtime).toContain("CRITICAL_CLIENT_SUBJECTS");
    expect(runtime).toContain("event-store-critical-app");
    expect(runtime).toContain("CONNECT_CONFIG_STORAGE_REPLICATION_FACTOR");
    expect(runtime).toContain("CONNECT_CONSUMER_ISOLATION_LEVEL");
    expect(runtime).toContain("CONNECT_PRODUCER_COMPRESSION_TYPE");
    expect(runtime).toContain("CDC_LATENCY_PROBE_INTERVAL_MS");
    expect(runtime).toContain("name: connect");
    expect(runtime).toContain('limits: { cpu: "2", memory: 4Gi }');
    expect(preflight).toContain("assert_configured_failover_candidate()");
    expect(bootstrap).toContain("event-store-kafka-kafka-bootstrap:9092");
    expect(bootstrap).toContain("http://event-store-connect:8083");
    expect(connector).toContain(
      '"database.hostname": "event-store-postgres-rw"',
    );
    expect(connector).toContain('"topic.prefix": "event-store-live"');
    expect(connector).toContain(
      '"predicates.isCanonicalEvents.pattern": "event-store-live\\\\.event_store\\\\.events"',
    );
    expect(monitoring).toContain("kind: ServiceMonitor");
    expect(runtime).toContain(
      "kind: Service\nmetadata:\n  name: event-store\n  labels: { app: event-store }",
    );
    expect(runtime).toContain(
      "kind: Service\nmetadata:\n  name: event-store-connect\n  labels: { app: event-store-connect }",
    );
    expect(monitoring).toContain("kind: PrometheusRule");
    expect(monitoring).toContain("EventStoreAppendUnknownOutcome");
    expect(monitoring).toContain("EventStoreConnectSourceDisconnected");
    expect(monitoring).toContain("EventStoreConnectSourceLagP99High");
    expect(monitoring).toContain("EventStoreCommitToConsumerP95High");
    expect(monitoring).toContain("EventStoreCommitToConsumerProbeUnavailable");
    expect(monitoring).toContain("EventStoreKafkaUnderReplicatedPartitions");
    expect(monitoring).toContain("EventStoreKafkaUnderMinIsr");
    expect(monitoring).toContain("EventStoreProjectionPoisonEvent");
    for (const alert of [
      "EventStoreLogicalSlotWalWarning",
      "EventStoreLogicalSlotWalHigh",
      "EventStoreLogicalSlotWalCritical",
      "EventStoreLogicalSlotInvalidated",
      "EventStoreLogicalSlotUnsyncedStandby",
    ])
      expect(monitoring).toContain(alert);
    for (const metric of [
      "retained_wal_bytes",
      "confirmed_flush_lsn_bytes",
      "invalidated",
      "unsynced_standby",
    ])
      expect(postgresMetrics).toContain(metric);
  });
});
