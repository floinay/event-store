import { describe, expect, it } from "vitest";
import {
  UnknownEventVersionError,
  UpcasterRegistry,
} from "@event-store/upcasting";

describe("upcaster registry", () => {
  it("requires a contiguous pure upcaster chain", () => {
    const registry = new UpcasterRegistry();
    registry.register("order.created", 1, (value) => ({
      ...(value as object),
      currency: "UAH",
    }));
    registry.register("order.created", 2, (value) => ({
      ...(value as object),
      source: "web",
    }));
    registry.setCurrentVersion("order.created", 3);
    expect(registry.upcast("order.created", 1, { id: "o1" })).toEqual({
      version: 3,
      payload: { id: "o1", currency: "UAH", source: "web" },
    });
    expect(() => registry.upcast("unknown.event", 1, {})).toThrow(
      UnknownEventVersionError,
    );
  });

  it("is deterministic, preserves frozen input, and is idempotent at current version", () => {
    const registry = new UpcasterRegistry();
    registry.register("order.created", 1, (value) => ({
      ...(value as Record<string, unknown>),
      currency: "UAH",
    }));
    registry.setCurrentVersion("order.created", 2);
    const input = Object.freeze({ id: "o1" });
    const first = registry.upcast("order.created", 1, input);
    const second = registry.upcast("order.created", 1, input);
    expect(input).toEqual({ id: "o1" });
    expect(first).toEqual(second);
    const current = registry.upcast("order.created", 2, first.payload);
    expect(current).toEqual(first);
    expect(current.payload).toBe(first.payload);
  });
});
