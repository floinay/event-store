import { randomBytes } from "node:crypto";
import { z } from "zod";

export const DecimalBigInt = z.string().regex(/^(0|[1-9][0-9]*)$/);
export const UuidV7 = z.uuidv7();
export const UtcMillis = z.iso.datetime({ offset: false, precision: 3 });

export const ActorSchema = z.object({
  kind: z.enum(["user", "service", "system"]),
  subjectRef: z.string().min(1).max(256),
});

export const EventContextSchema = z
  .object({
    requestId: UuidV7,
    correlationId: UuidV7,
    causationId: UuidV7.nullable(),
    actor: ActorSchema,
    traceparent: z.string().max(128).optional(),
  })
  .passthrough();

export const StoredEventSchema = z
  .object({
    eventId: UuidV7,
    namespace: z.string().regex(/^[a-z][a-z0-9-]{1,62}$/),
    aggregateType: z.string().regex(/^[A-Z][A-Za-z0-9]{0,127}$/),
    aggregateId: UuidV7,
    streamRevision: DecimalBigInt,
    eventNumber: DecimalBigInt,
    eventName: z.string().regex(/^[a-z][a-z0-9]*(\.[a-z0-9]+)+$/),
    schemaVersion: z.int().positive(),
    occurredAt: UtcMillis,
    recordedAt: UtcMillis,
    producerService: z.string().regex(/^[a-z][a-z0-9-]{1,62}$/),
    context: EventContextSchema,
    payload: z.record(z.string(), z.unknown()),
  })
  .passthrough();

export const EventDraftSchema = z
  .object({
    eventName: z.string().regex(/^[a-z][a-z0-9]*(\.[a-z0-9]+)+$/),
    schemaVersion: z.int().positive(),
    occurredAt: UtcMillis,
    payload: z.record(z.string(), z.unknown()),
  })
  .strict();

export type StoredEvent = z.infer<typeof StoredEventSchema>;
export type EventDraft = z.infer<typeof EventDraftSchema>;
export type EventContext = z.infer<typeof EventContextSchema>;

export function partitionKey(
  event: Pick<StoredEvent, "namespace" | "aggregateType" | "aggregateId">,
): string {
  return `${event.namespace}|${event.aggregateType}|${event.aggregateId}`;
}

const forbiddenPiiKey =
  /^(?:name|firstName|lastName|email|phone|address|token|password|credential|cardNumber|pan|cvv)$/i;

/** Rejects direct PII-like fields before they can become immutable event data. */
export function assertNoDirectPii(
  value: unknown,
  path = "$",
  seen = new WeakSet<object>(),
): void {
  if (value === null || typeof value !== "object") return;
  if (seen.has(value)) return;
  seen.add(value);
  if (Array.isArray(value)) {
    value.forEach((entry, index) =>
      assertNoDirectPii(entry, `${path}[${index}]`, seen),
    );
    return;
  }
  for (const [key, nested] of Object.entries(value)) {
    if (forbiddenPiiKey.test(key))
      throw new Error(`direct PII field is prohibited: ${path}.${key}`);
    assertNoDirectPii(nested, `${path}.${key}`, seen);
  }
}

/** Numeric JSON lexemes are not stable across PostgreSQL numeric and JS Number. */
export function assertNoJsonNumbers(
  value: unknown,
  path = "$",
  seen = new WeakSet<object>(),
): void {
  if (typeof value === "number")
    throw new Error(
      `JSON numbers are prohibited; use a decimal string: ${path}`,
    );
  if (value === null || typeof value !== "object") return;
  if (seen.has(value)) return;
  seen.add(value);
  if (Array.isArray(value)) {
    value.forEach((entry, index) =>
      assertNoJsonNumbers(entry, `${path}[${index}]`, seen),
    );
    return;
  }
  for (const [key, nested] of Object.entries(value))
    assertNoJsonNumbers(nested, `${path}.${key}`, seen);
}

/** Generates an RFC 9562 UUIDv7 for request and aggregate identifiers. */
export function uuidv7(now = Date.now()): string {
  if (!Number.isSafeInteger(now) || now < 0 || now >= 2 ** 48)
    throw new RangeError("UUIDv7 timestamp is outside the 48-bit range");
  const bytes = randomBytes(16);
  for (let index = 5; index >= 0; index -= 1)
    bytes[index] = Math.floor(now / 2 ** (8 * (5 - index))) & 0xff;
  bytes[6] = (bytes[6]! & 0x0f) | 0x70;
  bytes[8] = (bytes[8]! & 0x3f) | 0x80;
  const hex = bytes.toString("hex");
  return `${hex.slice(0, 8)}-${hex.slice(8, 12)}-${hex.slice(12, 16)}-${hex.slice(16, 20)}-${hex.slice(20)}`;
}

/** Deterministic JSON encoding for hashes; object keys are lexicographically ordered. */
export function canonicalJson(value: unknown): string {
  if (value === null || typeof value === "boolean" || typeof value === "string")
    return JSON.stringify(value);
  if (typeof value === "number") {
    if (!Number.isFinite(value))
      throw new TypeError("non-finite numbers are not valid JSON");
    return JSON.stringify(value);
  }
  if (Array.isArray(value)) return `[${value.map(canonicalJson).join(",")}]`;
  if (typeof value === "object") {
    const record = value as Record<string, unknown>;
    return `{${Object.keys(record)
      // PostgreSQL COLLATE "C" compares UTF-8 bytes; use the same ordering
      // rather than JavaScript's UTF-16 code-unit sort.
      .sort((left, right) =>
        Buffer.compare(Buffer.from(left), Buffer.from(right)),
      )
      .map((key) => `${JSON.stringify(key)}:${canonicalJson(record[key])}`)
      .join(",")}}`;
  }
  throw new TypeError("value is not JSON-serializable");
}
