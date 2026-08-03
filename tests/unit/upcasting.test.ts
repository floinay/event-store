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
});
