import { fileURLToPath } from "node:url";
import { Client } from "pg";

export interface ReconciliationReport {
  count: string;
  minEventNumber: string | null;
  maxEventNumber: string | null;
  revisionGaps: string;
  envelopeHashMismatches: string;
  missingProjectionEvents?: string;
  unknownProjectionEvents?: string;
}

export interface ProjectionReconciliationTarget {
  projectionName: string;
  generationId: string;
}

export async function reconcile(
  databaseUrl: string,
  projection?: ProjectionReconciliationTarget,
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
    const report: ReconciliationReport = {
      count: events.rows[0]?.count ?? "0",
      minEventNumber: events.rows[0]?.min ?? null,
      maxEventNumber: events.rows[0]?.max ?? null,
      revisionGaps: gaps.rows[0]?.count ?? "0",
      envelopeHashMismatches: hashes.rows[0]?.count ?? "0",
    };
    if (projection !== undefined) {
      const [missing, unknown] = await Promise.all([
        client.query<{ count: string }>(
          `SELECT count(*)::text FROM event_store.events e
           WHERE NOT EXISTS (
             SELECT 1 FROM projection_runtime.inbox i
             WHERE i.projection_name=$1 AND i.generation_id=$2 AND i.event_id=e.event_id
           )`,
          [projection.projectionName, projection.generationId],
        ),
        client.query<{ count: string }>(
          `SELECT count(*)::text FROM projection_runtime.inbox i
           WHERE i.projection_name=$1 AND i.generation_id=$2
             AND NOT EXISTS (SELECT 1 FROM event_store.events e WHERE e.event_id=i.event_id)`,
          [projection.projectionName, projection.generationId],
        ),
      ]);
      report.missingProjectionEvents = missing.rows[0]?.count ?? "0";
      report.unknownProjectionEvents = unknown.rows[0]?.count ?? "0";
    }
    return report;
  } finally {
    await client.end();
  }
}

export async function recordTimelineReconciliation(
  databaseUrl: string,
  projection: ProjectionReconciliationTarget,
): Promise<void> {
  const client = new Client({ connectionString: databaseUrl });
  await client.connect();
  try {
    const timeline = await client.query<{ timeline_id: number }>(
      "SELECT event_store.current_timeline_id() AS timeline_id",
    );
    const timelineId = timeline.rows[0]?.timeline_id;
    if (timelineId === undefined)
      throw new Error("PostgreSQL timeline is unavailable");
    await client.query(
      "SELECT event_store.record_cdc_timeline_reconciliation($1,$2,$3)",
      [projection.projectionName, projection.generationId, timelineId],
    );
  } finally {
    await client.end();
  }
}

if (process.argv[1] === fileURLToPath(import.meta.url)) {
  const url = process.env.DATABASE_URL;
  if (url === undefined) throw new Error("DATABASE_URL is required");
  const projectionName = process.env.PROJECTION_NAME;
  const generationId = process.env.PROJECTION_GENERATION_ID;
  if ((projectionName === undefined) !== (generationId === undefined))
    throw new Error(
      "PROJECTION_NAME and PROJECTION_GENERATION_ID must be set together",
    );
  const report = await reconcile(
    url,
    projectionName === undefined || generationId === undefined
      ? undefined
      : { projectionName, generationId },
  );
  console.log(JSON.stringify(report));
  if (
    report.revisionGaps !== "0" ||
    report.envelopeHashMismatches !== "0" ||
    (report.missingProjectionEvents !== undefined &&
      report.missingProjectionEvents !== "0") ||
    (report.unknownProjectionEvents !== undefined &&
      report.unknownProjectionEvents !== "0")
  )
    process.exitCode = 2;
  else if (projectionName !== undefined && generationId !== undefined)
    await recordTimelineReconciliation(url, {
      projectionName,
      generationId,
    });
}
