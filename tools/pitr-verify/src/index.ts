import { fileURLToPath } from "node:url";
import { reconcile } from "@event-store/reconcile";

if (process.argv[1] === fileURLToPath(import.meta.url)) {
  const url = process.env.DATABASE_URL;
  if (url === undefined) throw new Error("DATABASE_URL is required");
  const report = await reconcile(url);
  if (report.revisionGaps !== "0" || report.envelopeHashMismatches !== "0")
    throw new Error(
      `PITR verification failed: ${report.revisionGaps} revision gaps, ${report.envelopeHashMismatches} hash mismatches`,
    );
  console.log(JSON.stringify({ ...report, verified: true }));
}
