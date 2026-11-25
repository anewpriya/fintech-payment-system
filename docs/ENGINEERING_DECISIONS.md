# Engineering decisions

## Scope
Demonstrate payment reliability trade-offs for a technical product portfolio using synthetic transactions.

## Priorities
1. Prevent repeated settlement and partial balance updates.
2. Deduplicate API requests using client-provided UUID keys.
3. Distinguish simulated fraud scoring from trained ML.
4. Reproduce service-level tests in CI without deployment credentials.

## Remaining risks
Database commit and Kafka publication are separate; failed publication can leave a pending record. Design a transactional outbox, durable retry and dead-letter handling before claiming delivery guarantees. Consumer offset commits are not coordinated with settlement. Multi-currency accounting, complete ledger, authentication, authorisation, secure tokenisation, reconciliation and migration management are out of scope.

## Measurement plan
Validate replay, conflicting payload, concurrent settlement, insufficient funds, rollback and cent rounding. Then benchmark a reproducible synthetic workload with stated hardware, throughput, p50/p95/p99 latency, error rate and resource usage. Measure fraud precision/recall only against labelled independent data. No benchmark or accuracy result is asserted.

Legacy architecture and operations documents describe design ideas, not verified implementation.
