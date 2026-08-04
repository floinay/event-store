import { afterEach, describe, expect, it } from "vitest";
import { mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { execFile } from "node:child_process";
import { promisify } from "node:util";

const run = promisify(execFile);
const temporaryRoots: string[] = [];

afterEach(async () => {
  await Promise.all(
    temporaryRoots
      .splice(0)
      .map((directory) => rm(directory, { recursive: true, force: true })),
  );
});

async function compatibilityResult(
  v1: unknown,
  v2: unknown,
): Promise<Error | undefined> {
  const root = await mkdtemp(join(tmpdir(), "event-store-schema-"));
  temporaryRoots.push(root);
  const family = join(root, "event-envelope");
  await writeFile(join(root, ".keep"), "");
  await (await import("node:fs/promises")).mkdir(family);
  await Promise.all([
    writeFile(join(family, "v1.json"), JSON.stringify(v1)),
    writeFile(join(family, "v2.json"), JSON.stringify(v2)),
  ]);
  try {
    await run(process.execPath, ["tools/check-json-schema-compatibility.mjs"], {
      cwd: process.cwd(),
      env: { ...process.env, JSON_SCHEMA_ROOT: root },
    });
  } catch (error) {
    return error as Error;
  }
  return undefined;
}

const historicalEnvelope = {
  type: "object",
  additionalProperties: true,
  properties: { eventId: { type: "string" } },
};

describe("JSON Schema compatibility checker", () => {
  it.each([
    ["unevaluatedProperties", false],
    ["not", { required: ["eventId"] }],
    ["propertyNames", { pattern: "^[a-z]+$" }],
    ["contains", { type: "string" }],
    ["$ref", "#/$defs/envelope"],
    ["additionalProperties", { type: "string" }],
  ])("rejects a newly restrictive %s keyword", async (keyword, value) => {
    const next = {
      ...historicalEnvelope,
      [keyword]: value,
      $defs: { envelope: historicalEnvelope },
    };
    const error = await compatibilityResult(historicalEnvelope, next);
    expect(error?.message).toContain(
      keyword === "additionalProperties"
        ? "additional properties were narrowed"
        : `${keyword} changed`,
    );
  });

  it("rejects narrowing a boolean true schema", async () => {
    const error = await compatibilityResult(true, false);
    expect(error?.message).toContain("boolean true schema was narrowed");
  });
});
