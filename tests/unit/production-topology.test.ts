import { readFile } from "node:fs/promises";
import { describe, expect, it } from "vitest";

describe("production HA topology", () => {
  it("requires synchronous PostgreSQL, synchronized logical slots, and three runtime members", async () => {
    const [postgres, kafka, runtime, preflight] = await Promise.all(
      ["postgres.yaml", "kafka.yaml", "runtime.yaml"].map((file) =>
        readFile(`deploy/production/${file}`, "utf8"),
      ).concat(readFile("deploy/event-store/failover-preflight-job.yaml", "utf8")),
    );
    expect(postgres).toContain("instances: 3");
    expect(postgres).toContain("minSyncReplicas: 1");
    expect(postgres).toContain("synchronizeLogicalDecoding: true");
    expect(postgres).toContain("synchronizeReplicas:");
    expect(kafka).toContain("replicas: 3");
    expect(kafka).toContain("roles: [controller, broker]");
    expect(kafka).toContain("min.insync.replicas: 2");
    expect(runtime.match(/replicas: 3/g)).toHaveLength(2);
    expect(runtime).toContain("EXACTLY_ONCE_SOURCE_SUPPORT");
    expect(preflight).toContain("assert_failover_candidate('event_store_live')");
  });
});
