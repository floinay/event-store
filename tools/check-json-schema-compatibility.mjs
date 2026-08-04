import { readdir, readFile } from "node:fs/promises";
import { join } from "node:path";
import { fileURLToPath } from "node:url";

const schemaRoot = fileURLToPath(
  new globalThis.URL("../schemas/", import.meta.url),
);

async function schemaFiles(directory) {
  const entries = await readdir(directory, { withFileTypes: true });
  const files = await Promise.all(
    entries.map(async (entry) => {
      const path = join(directory, entry.name);
      if (entry.isDirectory()) return schemaFiles(path);
      return entry.isFile() && /^v[1-9][0-9]*\.json$/.test(entry.name)
        ? [path]
        : [];
    }),
  );
  return files.flat();
}

function version(path) {
  const match = /\/v([1-9][0-9]*)\.json$/.exec(path);
  if (match === null)
    throw new Error(`invalid versioned schema filename: ${path}`);
  return Number(match[1]);
}

function types(schema) {
  const value = schema.type;
  return new Set(Array.isArray(value) ? value : [value]);
}

function hasOwn(value, key) {
  return Object.prototype.hasOwnProperty.call(value, key);
}

function assertDoesNotNarrow(previous, next, label) {
  for (const [minimum, maximum] of [
    ["minimum", "maximum"],
    ["exclusiveMinimum", "exclusiveMaximum"],
    ["minLength", "maxLength"],
    ["minItems", "maxItems"],
    ["minProperties", "maxProperties"],
  ]) {
    if (
      hasOwn(previous, minimum) &&
      (!hasOwn(next, minimum) || next[minimum] <= previous[minimum])
    )
      continue;
    if (hasOwn(previous, minimum))
      throw new Error(`${label}: ${minimum} was tightened`);
    if (hasOwn(next, minimum) && next[minimum] > 0)
      throw new Error(`${label}: ${minimum} was added`);
    if (
      hasOwn(previous, maximum) &&
      (!hasOwn(next, maximum) || next[maximum] >= previous[maximum])
    )
      continue;
    if (hasOwn(previous, maximum))
      throw new Error(`${label}: ${maximum} was tightened`);
    if (hasOwn(next, maximum))
      throw new Error(`${label}: ${maximum} was added`);
  }
  for (const keyword of ["pattern", "format", "multipleOf", "const"]) {
    if (hasOwn(previous, keyword) && next[keyword] === previous[keyword])
      continue;
    if (hasOwn(previous, keyword))
      throw new Error(`${label}: ${keyword} changed`);
    if (hasOwn(next, keyword))
      throw new Error(`${label}: ${keyword} was added`);
  }
  if (hasOwn(previous, "enum")) {
    const nextValues = new Set((next.enum ?? []).map(JSON.stringify));
    for (const value of previous.enum)
      if (!nextValues.has(JSON.stringify(value)))
        throw new Error(`${label}: enum value was removed`);
  } else if (hasOwn(next, "enum")) {
    throw new Error(`${label}: enum was added`);
  }
  for (const keyword of ["oneOf", "anyOf", "allOf"]) {
    if (!hasOwn(previous, keyword)) {
      if (hasOwn(next, keyword))
        throw new Error(`${label}: ${keyword} was added`);
      continue;
    }
    const nextBranches = new Set((next[keyword] ?? []).map(JSON.stringify));
    for (const branch of previous[keyword])
      if (!nextBranches.has(JSON.stringify(branch)))
        throw new Error(`${label}: ${keyword} branch was removed or changed`);
  }
}

function assertBackwardCompatible(previous, next, label) {
  for (const required of next.required ?? [])
    if (!(previous.required ?? []).includes(required))
      throw new Error(
        `${label}: ${required} became required; additions must remain optional`,
      );
  for (const [name, previousProperty] of Object.entries(
    previous.properties ?? {},
  )) {
    const nextProperty = next.properties?.[name];
    if (nextProperty === undefined)
      throw new Error(`${label}: property ${name} was removed`);
    const previousTypes = types(previousProperty);
    const nextTypes = types(nextProperty);
    for (const type of previousTypes)
      if (!nextTypes.has(type))
        throw new Error(`${label}: property ${name} changed type`);
    assertDoesNotNarrow(previousProperty, nextProperty, `${label}.${name}`);
    assertBackwardCompatible(
      previousProperty,
      nextProperty,
      `${label}.${name}`,
    );
  }
  assertDoesNotNarrow(previous, next, label);
  if (previous.items !== undefined) {
    if (next.items === undefined)
      throw new Error(`${label}: items was removed`);
    assertBackwardCompatible(previous.items, next.items, `${label}.items`);
  }
  if (
    previous.additionalProperties !== undefined &&
    typeof previous.additionalProperties === "object"
  ) {
    if (typeof next.additionalProperties !== "object")
      throw new Error(`${label}: additionalProperties changed`);
    assertBackwardCompatible(
      previous.additionalProperties,
      next.additionalProperties,
      `${label}.additionalProperties`,
    );
  }
  if (
    previous.additionalProperties === true &&
    next.additionalProperties === false
  )
    throw new Error(`${label}: additional properties became forbidden`);
}

const files = await schemaFiles(schemaRoot);
if (files.length === 0) throw new Error("no versioned JSON Schemas found");
const groups = new Map();
for (const path of files) {
  const family = path.replace(/\/v[1-9][0-9]*\.json$/, "");
  const schema = JSON.parse(await readFile(path, "utf8"));
  const versions = groups.get(family) ?? [];
  versions.push({ path, schema, version: version(path) });
  groups.set(family, versions);
}
for (const [family, versions] of groups) {
  versions.sort((left, right) => left.version - right.version);
  for (let newer = 1; newer < versions.length; newer += 1)
    for (let older = 0; older < newer; older += 1)
      assertBackwardCompatible(
        versions[older].schema,
        versions[newer].schema,
        `${family} v${versions[older].version}→v${versions[newer].version}`,
      );
}
