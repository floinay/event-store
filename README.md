# TypeScript Event Store v1

This repository is a small reference implementation of an event store built
with TypeScript, PostgreSQL, Debezium and Kafka.

It is a demo for testing a hypothesis, not a production-ready framework or a
drop-in database product. The hypothesis is that a PostgreSQL event store can
provide durable appends, recover safely after CDC/failover failures, and deliver
events to a Kafka consumer within a practical latency budget.

The demo focuses on:

- append idempotency and optimistic concurrency;
- PostgreSQL transactional outbox/CDC delivery through Debezium;
- at-least-once Kafka transport with exactly-once projection effects via inbox;
- delivery fencing, reconciliation, crash recovery and PostgreSQL promotion;
- a commit-to-consumer latency probe and production alert rules.

## Run

```bash
npm ci
npm test
```

Docker is required for integration and latency tests:

```bash
RUN_INTEGRATION=true npm run test:integration
RUN_LATENCY=true npx vitest run --no-file-parallelism tests/performance/cdc-latency.test.ts
```

The default latency run creates 100 measured events plus one warm-up event. A
local Docker run measured 20 ms p50 and 30 ms p95 from PostgreSQL commit to the
Kafka consumer. Treat those numbers as an experiment result on that environment,
not a production guarantee.

## Scope

The repository exists to make the architecture and failure-mode assumptions
testable. Before using this approach in production, validate capacity, security,
operations, disaster recovery and latency on the target infrastructure.
