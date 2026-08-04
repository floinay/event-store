import { describe, expect, it } from "vitest";
import { z } from "zod";
import { uuidv7 } from "@event-store/contracts";
import {
  createProjectionEventTransformer,
  isProjectionInfrastructureEvent,
  ProjectionMetrics,
  ProjectionPayloadSchemas,
  ProjectionUnknownSchemaError,
} from "@event-store/projection-runtime";
import { UpcasterRegistry } from "@event-store/upcasting";

const event = () => {
  const requestId = uuidv7();
  return {
    eventId: uuidv7(),
    namespace: "orders",
    aggregateType: "Order",
    aggregateId: uuidv7(),
    streamRevision: "1",
    eventNumber: "1",
    eventName: "order.created",
    schemaVersion: 1,
    occurredAt: "2026-08-04T10:12:18.120Z",
    recordedAt: "2026-08-04T10:12:18.120Z",
    producerService: "orders-command",
    context: {
      requestId,
      correlationId: uuidv7(),
      causationId: null,
      actor: { kind: "user" as const, subjectRef: "usr_1" },
    },
    payload: { orderRef: "o1" },
  };
};

describe("projection event transformer", () => {
  it("exports projection reliability metrics", () => {
    const metrics = new ProjectionMetrics();
    metrics.observeHandler(5);
    metrics.observeTransaction(7);
    metrics.inboxDuplicate();
    metrics.gapIncident();
    metrics.poisonEvent();
    metrics.pausePartition();
    metrics.observeCheckpointAge(new Date(Date.now() - 1_000).toISOString());
    const output = metrics.prometheus();
    expect(output).toContain(
      "event_store_projection_handler_duration_seconds_count 1",
    );
    expect(output).toContain("event_store_projection_inbox_duplicates_total 1");
    expect(output).toContain("event_store_projection_gap_incidents_total 1");
    expect(output).toContain("event_store_projection_poison_events_total 1");
    expect(output).toContain("event_store_projection_paused_partitions 1");
  });

  it("identifies the reserved CDC latency probe as a handler-free record", () => {
    const probe = event();
    expect(
      isProjectionInfrastructureEvent({
        ...probe,
        namespace: "system",
        aggregateType: "CdcLatencyProbe",
        eventName: "system.cdc.latency.probe",
        producerService: "event-store-latency-probe",
      }),
    ).toBe(true);
    expect(isProjectionInfrastructureEvent(probe)).toBe(false);
  });

  it("upcasts and validates payloads before the projection handler", () => {
    const upcasters = new UpcasterRegistry();
    upcasters.register("order.created", 1, (payload) => ({
      ...(payload as object),
      source: "web",
    }));
    upcasters.setCurrentVersion("order.created", 2);
    const schemas = new ProjectionPayloadSchemas();
    schemas.register(
      "order.created",
      2,
      z.object({ orderRef: z.string(), source: z.literal("web") }).strict(),
    );
    expect(
      createProjectionEventTransformer(upcasters, schemas)(event()),
    ).toMatchObject({
      schemaVersion: 2,
      payload: { orderRef: "o1", source: "web" },
    });
  });

  it("rejects an event that has no registered current payload schema", () => {
    const upcasters = new UpcasterRegistry();
    upcasters.setCurrentVersion("order.created", 1);
    expect(() =>
      createProjectionEventTransformer(
        upcasters,
        new ProjectionPayloadSchemas(),
      )(event()),
    ).toThrow(ProjectionUnknownSchemaError);
  });
});
