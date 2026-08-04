import { createHash } from "node:crypto";
import { readdir, readFile } from "node:fs/promises";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import { Client } from "pg";

interface AppliedMigration {
  version: string;
  checksum: string;
}

const loginRoles = new Set([
  "event_store_migrator",
  "event_store_app",
  "event_store_critical_app",
  "event_store_cdc",
  "projection_worker",
]);

function rolePasswordsFromEnvironment(): ReadonlyMap<string, string> {
  const encoded = process.env.EVENT_STORE_ROLE_PASSWORDS_JSON;
  if (encoded === undefined) return new Map();
  const parsed: unknown = JSON.parse(encoded);
  if (parsed === null || typeof parsed !== "object" || Array.isArray(parsed))
    throw new Error("EVENT_STORE_ROLE_PASSWORDS_JSON must be an object");
  const passwords = new Map<string, string>();
  for (const [role, password] of Object.entries(parsed)) {
    if (
      !loginRoles.has(role) ||
      typeof password !== "string" ||
      password === ""
    )
      throw new Error("invalid Event Store role password secret");
    passwords.set(role, password);
  }
  return passwords;
}

export async function migrate(
  databaseUrl: string,
  includeClusterMigration = false,
): Promise<void> {
  const migrationsDir = join(
    fileURLToPath(new URL("../../../migrations/", import.meta.url)),
  );
  const files = (await readdir(migrationsDir))
    .filter((file) => /^\d{3}_.+\.sql$/.test(file))
    .sort();
  const client = new Client({ connectionString: databaseUrl });
  await client.connect();
  try {
    if (includeClusterMigration)
      await client.query(
        await readFile(join(migrationsDir, "001_roles.sql"), "utf8"),
      );
    if (includeClusterMigration)
      await client.query(
        "DO $$ BEGIN CREATE ROLE event_store_critical_app LOGIN NOINHERIT; EXCEPTION WHEN duplicate_object THEN NULL; END $$",
      );
    if (includeClusterMigration) {
      for (const [role, password] of rolePasswordsFromEnvironment()) {
        // PostgreSQL utility statements do not accept bind parameters. Roles
        // are constrained by the allow-list above; quote the password literal
        // for the one remaining interpolated SQL value.
        if (password.includes("\0"))
          throw new Error("role password secret must not contain NUL");
        await client.query(
          `ALTER ROLE ${role} PASSWORD '${password.replaceAll("'", "''")}'`,
        );
      }
    }
    if (includeClusterMigration)
      await client.query(
        "GRANT CONNECT ON DATABASE event_store TO event_store_owner",
      );
    if (includeClusterMigration)
      await client.query(
        "ALTER DATABASE event_store OWNER TO event_store_owner",
      );
    await client.query("SET ROLE event_store_owner");
    const first = files.find((file) => file !== "001_roles.sql");
    if (first === undefined) throw new Error("no database migrations found");
    const migrationTable = await client.query<{ table_name: string | null }>(
      "SELECT to_regclass('event_store.schema_migrations') AS table_name",
    );
    if (migrationTable.rows[0]?.table_name === null) {
      const sql = await readFile(join(migrationsDir, first), "utf8");
      const checksum = createHash("sha256").update(sql).digest("hex");
      await client.query(sql);
      await client.query(
        "CREATE TABLE event_store.schema_migrations (version text PRIMARY KEY, checksum char(64) NOT NULL, applied_at timestamptz NOT NULL DEFAULT clock_timestamp())",
      );
      await client.query(
        "INSERT INTO event_store.schema_migrations(version, checksum) VALUES ($1, $2)",
        [first, checksum],
      );
    }
    const applied = await client.query<AppliedMigration>(
      "SELECT version, checksum FROM event_store.schema_migrations",
    );
    const checksums = new Map(
      applied.rows.map((row) => [row.version, row.checksum]),
    );
    for (const file of files.filter((entry) => entry !== "001_roles.sql")) {
      const sql = await readFile(join(migrationsDir, file), "utf8");
      const checksum = createHash("sha256").update(sql).digest("hex");
      const existing = checksums.get(file);
      if (existing !== undefined) {
        if (existing !== checksum)
          throw new Error(`migration checksum changed: ${file}`);
        continue;
      }
      await client.query("BEGIN");
      try {
        await client.query(sql);
        await client.query(
          "INSERT INTO event_store.schema_migrations(version, checksum) VALUES ($1, $2)",
          [file, checksum],
        );
        await client.query("COMMIT");
      } catch (error) {
        await client.query("ROLLBACK");
        throw error;
      }
    }
  } finally {
    await client.end();
  }
}

if (process.argv[1] === fileURLToPath(import.meta.url)) {
  const databaseUrl = process.env.DATABASE_URL;
  if (databaseUrl === undefined) throw new Error("DATABASE_URL is required");
  await migrate(databaseUrl, process.env.MIGRATE_CLUSTER === "true");
}
