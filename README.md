# TypeScript Event Store v1

This repository is a reference implementation of an event store built with
TypeScript, PostgreSQL, Debezium and Kafka.

It is a demo for testing a hypothesis, not a production-ready framework or a
drop-in database product. The hypothesis is that a PostgreSQL event store can
provide durable appends, recover safely after CDC/failover failures, and deliver
events to a Kafka consumer within a practical latency budget.

## What is implemented

- append idempotency and optimistic concurrency;
- canonical event envelopes and PostgreSQL durable writes;
- CDC delivery from PostgreSQL to Kafka through Debezium;
- at-least-once Kafka transport with exactly-once projection effects via inbox;
- delivery fencing, reconciliation, crash recovery and PostgreSQL promotion;
- a commit-to-consumer latency probe and production alert rules.

## Design

```text
gRPC client
    │ append (idempotency + expected revision)
    ▼
PostgreSQL event_store.events ──logical replication──► Debezium ──► Kafka
    │                                                          │
    │                                                          ▼
    └──── recovery/reconciliation ◄── inbox + checkpoint ◄── projection consumer
```

An append is acknowledged only after PostgreSQL commits it. Debezium then
publishes the canonical event to Kafka. Kafka transport is at-least-once, so a
projection stores an inbox record and its read-model update in the same
PostgreSQL transaction. Repeated Kafka records therefore do not repeat the
projection effect.

`eventNumber` is a unique allocation order, not a global commit order. A
rolled-back transaction can leave a gap, and concurrent transactions can commit
in a different order from their allocated numbers. It is therefore never a
live cursor, pagination cursor, or recovery watermark. CDC consumers resume
from their Debezium/Kafka offsets; per-aggregate order is defined by
`streamRevision` and preserved by the aggregate Kafka key.

When delivery health, a slot, or failover is suspect, appends are durably
fenced. Reopening requires an event-ID reconciliation on the current PostgreSQL
timeline; recovery barriers and all append entry points are serialized with
that proof.

## Why this approach is useful

- PostgreSQL is the single durable source of truth; there is no application
  dual-write to a database and Kafka.
- Consumers can recover from process crashes and Kafka duplicates without
  double-applying business effects.
- A delivery outage cannot silently reopen writes: recovery has a durable
  reconciliation and promotion-timeline proof.
- The repository includes executable failure scenarios rather than relying on
  diagrams alone.
- The latency probe measures the meaningful span: SQL commit completion to a
  `read_committed` Kafka consumer receipt.

## Tests

| Command                                                                                       | Covers                                                                                                                                             |
| --------------------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------- |
| `npm test`                                                                                    | Unit and contract-level behaviour: canonical JSON, append semantics, error mapping, metrics and topology.                                          |
| `RUN_INTEGRATION=true npm run test:integration`                                               | PostgreSQL, Debezium, Kafka, gRPC, CDC recovery, promotion, slot loss, reconciliation, crash boundaries and projection inbox/checkpoint behaviour. |
| `RUN_LATENCY=true npx vitest run --no-file-parallelism tests/performance/cdc-latency.test.ts` | PostgreSQL commit-to-Kafka-consumer latency, append latency, controlled Connect/Kafka restart profiles and no-loss delivery.                       |

The crash-recovery tests intentionally include duplicate Kafka records and
assert exactly-once projection effects at the inbox/read-model boundary.

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

## Measured latency

The default latency run creates 100 measured events and one warm-up event.
This local Docker run measured the following:

| Span                                |     p50 |     p95 |     p99 |    Mean |
| ----------------------------------- | ------: | ------: | ------: | ------: |
| PostgreSQL commit to Kafka consumer |   20 ms |   30 ms |   31 ms | 20.4 ms |
| Durable append ACK                  | 3.12 ms | 4.46 ms | 5.43 ms |       — |

The commit-to-consumer test also measured p99.9 at 32 ms. These are experiment
results on one local Docker environment, not a production guarantee. Shared
GitHub runners use a regression guard of p50 ≤ 85 ms, mean ≤ 100 ms, p95 ≤ 175
ms and p99.9 ≤ 300 ms. The release profile runs on the dedicated performance
runner and enforces the production SLOs: p50 ≤ 50 ms, mean ≤ 80 ms, p95 ≤ 100
ms and p99.9 ≤ 200 ms.

## Scope

The repository exists to make the architecture and failure-mode assumptions
testable. Before using this approach in production, validate capacity, security,
operations, disaster recovery and latency on the target infrastructure.
