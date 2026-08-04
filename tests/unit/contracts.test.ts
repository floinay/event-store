import { describe, expect, it } from "vitest";
import {
  assertNoDirectPii,
  assertNoJsonNumbers,
  canonicalJson,
  partitionKey,
} from "@event-store/contracts";

describe("event contracts", () => {
  it("builds the canonical aggregate key", () => {
    expect(
      partitionKey({
        namespace: "orders",
        aggregateType: "Order",
        aggregateId: "0198f999-cacf-7000-8000-000000000001",
      }),
    ).toBe("orders|Order|0198f999-cacf-7000-8000-000000000001");
  });

  it("fails closed on direct PII-like keys", () => {
    expect(() =>
      assertNoDirectPii({
        actor: { subjectRef: "usr_1" },
        email: "a@example.test",
      }),
    ).toThrow("direct PII");
    expect(() =>
      assertNoDirectPii({
        encrypted: { keyId: "subject-k1", ciphertext: "..." },
      }),
    ).not.toThrow();
  });

  it("canonicalizes object order for snapshot hashes", () => {
    expect(canonicalJson({ b: [2, { z: true, a: null }], a: 1 })).toBe(
      '{"a":1,"b":[2,{"a":null,"z":true}]}',
    );
  });

  it("uses PostgreSQL C-collation ordering for Unicode keys", () => {
    expect(canonicalJson({ "😀": 2, "�": 1 })).toBe('{"�":1,"😀":2}');
  });

  it("rejects numeric JSON values before canonical hashing", () => {
    expect(() => assertNoJsonNumbers({ amount: 1.0 })).toThrow(
      "decimal string",
    );
    expect(() => assertNoJsonNumbers({ amount: "1.0" })).not.toThrow();
  });
});
