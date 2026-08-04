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
