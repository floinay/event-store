import { createHash } from "node:crypto";
import { readdir, readFile } from "node:fs/promises";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import { Client } from "pg";

interface AppliedMigration {
  version: string;
  checksum: string;
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
        "GRANT CONNECT ON DATABASE event_store TO event_store_owner",
      );
    if (includeClusterMigration)
      await client.query(
        "ALTER DATABASE event_store OWNER TO event_store_owner",
      );
    await client.query("SET ROLE event_store_owner");
    const first = files.find((file) => file !== "001_roles.sql");
    if (first === undefined) throw new Error("no database migrations found");
    await client.query(await readFile(join(migrationsDir, first), "utf8"));
    await client.query(
      "CREATE TABLE IF NOT EXISTS event_store.schema_migrations (version text PRIMARY KEY, checksum char(64) NOT NULL, applied_at timestamptz NOT NULL DEFAULT clock_timestamp())",
    );
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
