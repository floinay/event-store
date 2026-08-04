import { readFile } from "node:fs/promises";
import { describe, expect, it } from "vitest";

describe("production HA topology", () => {
  it("requires synchronous PostgreSQL, synchronized logical slots, and three runtime members", async () => {
    const [postgres, kafka, runtime, preflight, bootstrap, connector] =
      await Promise.all(
        ["postgres.yaml", "kafka.yaml", "runtime.yaml"]
          .map((file) => readFile(`deploy/production/${file}`, "utf8"))
          .concat([
            readFile("deploy/event-store/failover-preflight-job.yaml", "utf8"),
            readFile("deploy/event-store/bootstrap-job.yaml", "utf8"),
            readFile("deploy/connector/event-store-live.json", "utf8"),
          ]),
      );
    expect(postgres).toContain("instances: 3");
    expect(postgres).toContain("bootstrap:");
    expect(postgres).toContain("database: event_store");
    expect(postgres).toContain("minSyncReplicas: 1");
    expect(postgres).toContain("synchronizeLogicalDecoding: true");
    expect(postgres).toContain("synchronizeReplicas:");
    expect(postgres).toContain('retentionPolicy: "35d"');
    expect(postgres).toContain("kind: ScheduledBackup");
    expect(postgres).toContain('schedule: "0 0 0 * * *"');
    expect(kafka).toContain("replicas: 3");
    expect(kafka).toContain("roles: [controller, broker]");
    expect(kafka).toContain("min.insync.replicas: 2");
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
    expect(runtime).toContain("GRPC_TLS_CERT_PEM");
    expect(runtime).toContain("event-store-grpc-tls");
    expect(runtime).toContain("CRITICAL_DATABASE_URL");
    expect(runtime).toContain("CRITICAL_CLIENT_SUBJECTS");
    expect(runtime).toContain("event-store-critical-app");
    expect(runtime).toContain("CONNECT_CONFIG_STORAGE_REPLICATION_FACTOR");
    expect(runtime).toContain("CONNECT_CONSUMER_ISOLATION_LEVEL");
    expect(runtime).toContain("CONNECT_PRODUCER_COMPRESSION_TYPE");
    expect(preflight).toContain(
      "assert_failover_candidate('event_store_live')",
    );
    expect(bootstrap).toContain("event-store-kafka-kafka-bootstrap:9092");
    expect(bootstrap).toContain("http://event-store-connect:8083");
    expect(connector).toContain(
      '"database.hostname": "event-store-postgres-rw"',
    );
  });
});
