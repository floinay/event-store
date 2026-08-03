import { createHash } from "node:crypto";
import {
  partitionKey,
  StoredEventSchema,
  type StoredEvent,
} from "@event-store/contracts";

export interface KafkaEnvelopeRecord {
  key: string;
  value: string;
  headers: Record<string, string>;
}

export function toKafkaEnvelopeRecord(input: unknown): KafkaEnvelopeRecord {
  const event = StoredEventSchema.parse(input);
  const value = JSON.stringify(event);
  const envelopeHash = createHash("sha256").update(value).digest("hex");
  return {
    key: partitionKey(event),
    value,
    headers: {
      id: event.eventId,
      type: event.eventName,
      envelopeHash,
      namespace: event.namespace,
      aggregateType: event.aggregateType,
      streamRevision: event.streamRevision,
    },
  };
}

export function parseKafkaEnvelope(value: Buffer | string): StoredEvent {
  return StoredEventSchema.parse(JSON.parse(value.toString()));
}
