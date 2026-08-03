import { fileURLToPath } from "node:url";
import { Client } from "pg";

export interface ReconciliationReport {
  count: string;
  minEventNumber: string | null;
  maxEventNumber: string | null;
  revisionGaps: string;
  envelopeHashMismatches: string;
}

export async function reconcile(
  databaseUrl: string,
): Promise<ReconciliationReport> {
  const client = new Client({ connectionString: databaseUrl });
  await client.connect();
  try {
    const events = await client.query<{
      count: string;
      min: string | null;
      max: string | null;
    }>(
      "SELECT count(*)::text, min(event_number)::text AS min, max(event_number)::text AS max FROM event_store.events",
    );
    const gaps = await client.query<{ count: string }>(
      `SELECT count(*)::text FROM (SELECT stream_revision, lag(stream_revision) OVER (PARTITION BY namespace, aggregate_type, aggregate_id ORDER BY stream_revision) AS previous FROM event_store.events) q WHERE previous IS NOT NULL AND stream_revision <> previous + 1`,
    );
    const hashes = await client.query<{ count: string }>(
      `SELECT count(*)::text
       FROM event_store.events
       WHERE envelope_sha256 <> encode(event_store.digest(event_store.canonical_jsonb(event_envelope), 'sha256'), 'hex')`,
    );
    return {
      count: events.rows[0]?.count ?? "0",
      minEventNumber: events.rows[0]?.min ?? null,
      maxEventNumber: events.rows[0]?.max ?? null,
      revisionGaps: gaps.rows[0]?.count ?? "0",
      envelopeHashMismatches: hashes.rows[0]?.count ?? "0",
    };
  } finally {
    await client.end();
  }
}

if (process.argv[1] === fileURLToPath(import.meta.url)) {
  const url = process.env.DATABASE_URL;
  if (url === undefined) throw new Error("DATABASE_URL is required");
  const report = await reconcile(url);
  console.log(JSON.stringify(report));
  if (report.revisionGaps !== "0" || report.envelopeHashMismatches !== "0")
    process.exitCode = 2;
}
