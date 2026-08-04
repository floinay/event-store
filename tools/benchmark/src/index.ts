import { fileURLToPath } from "node:url";
import { Pool } from "pg";
import { uuidv7 } from "@event-store/contracts";
import { PostgresEventStore } from "@event-store/postgres-store";

const positive = (value: string | undefined, fallback: number): number =>
  value === undefined ? fallback : Number.parseInt(value, 10);

export async function benchmark(
  store: PostgresEventStore,
  count: number,
  concurrency: number,
): Promise<number[]> {
  const latencies: number[] = [];
  let next = 0;
  const worker = async (): Promise<void> => {
    while (next < count) {
      const index = next++;
      const requestId = uuidv7();
      const aggregateId = uuidv7();
      const started = performance.now();
      await store.append({
        producerService: "benchmark",
        namespace: "benchmark",
        aggregateType: "Benchmark",
        aggregateId,
        requestId,
        expectedRevision: { kind: "no_stream" },
        context: {
          requestId,
          correlationId: uuidv7(),
          causationId: null,
          actor: { kind: "service", subjectRef: "benchmark" },
        },
        events: [
          {
            eventName: "benchmark.appended",
            schemaVersion: 1,
            occurredAt: new Date().toISOString(),
            payload: { index: String(index) },
          },
        ],
      });
      latencies.push(performance.now() - started);
    }
  };
  await Promise.all(Array.from({ length: concurrency }, worker));
  return latencies;
}

if (process.argv[1] === fileURLToPath(import.meta.url)) {
  const url = process.env.DATABASE_URL;
  if (url === undefined) throw new Error("DATABASE_URL is required");
  const count = positive(process.env.BENCHMARK_APPEND_COUNT, 1_000);
  const concurrency = positive(process.env.BENCHMARK_CONCURRENCY, 20);
  const pool = new Pool({ connectionString: url, max: concurrency });
  try {
    const values = (
      await benchmark(new PostgresEventStore(pool), count, concurrency)
    ).sort((a, b) => a - b);
    const percentile = (p: number) =>
      values[Math.ceil(values.length * p) - 1] ?? 0;
    console.log(
      JSON.stringify({
        count,
        p50Ms: percentile(0.5),
        p95Ms: percentile(0.95),
        p99Ms: percentile(0.99),
      }),
    );
  } finally {
    await pool.end();
  }
}
