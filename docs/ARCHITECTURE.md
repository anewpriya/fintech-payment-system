# Fintech Payment Processing System - Architecture Design Document

**Date:** July 9, 2026
**Author:** Anupriya Singh
**Version:** 1.0

## Executive Summary

This document describes the architecture of a production-grade payment processing system designed to handle 100K transactions per second with sub-50ms fraud detection latency.

The system demonstrates enterprise-level thinking on:

- Real-time fraud detection (multi-tier)
- Distributed event streaming
- Eventual consistency at scale
- Idempotency and exactly-once semantics
- Audit trails for compliance

## Design Goals

1. **Scalability:** Handle 100K transactions/second
2. **Reliability:** Never lose a transaction (Kafka durability)
3. **Speed:** Fraud detection in <50ms
4. **Compliance:** PCI-DSS audit trails
5. **Simplicity:** Clear separation of concerns

## Architecture Overview

### System Diagram

┌─────────────────────────────────────────────────────────┐
│ Client (User/Merchant App) │
└──────────────────┬──────────────────────────────────────┘
│ HTTPS POST /transactions
↓
┌─────────────────────────────────────────────────────────┐
│ API Gateway (FastAPI) │
│ - Validates request (Pydantic) │
│ - Creates transaction in PostgreSQL │
│ - Publishes to Kafka │
│ - Returns 202 ACCEPTED immediately │
└──────────────────┬──────────────────────────────────────┘
│ transaction event
↓
┌──────────────────────┐
│ Kafka Topic: │
│ transactions │
│ (7-day retention) │
└──────┬───────────────┘
│
┌──────┴────────────────────────┐
│ │
↓ ↓
┌─────────────────┐ ┌──────────────────────┐
│ Fraud Detection │ │ Feature Store │
│ Service │ │ (Flink + Redis) │
│ │ │ │
│ Tier 1: Rules │ │ Computes velocity │
│ Tier 2: ML │ │ features in real-time│
│ Tier 3: Async │ │ Serves from Redis │
└────────┬────────┘ └──────────────────────┘
│
↓
┌──────────────────────┐
│ Kafka Topic: │
│ fraud-scores │
└────────┬─────────────┘
│
↓
┌──────────────────────┐
│ Settlement Service │
│ │
│ Process approved txn │
│ Update balances │
└────────┬─────────────┘
│
├→ PostgreSQL (transactions, settlements)
├→ Kafka (settlements topic)
└→ ClickHouse (analytics)

### Component Responsibilities

**API Gateway (FastAPI)**

- Receive transaction requests
- Validate input (Pydantic)
- Create transaction record (PostgreSQL)
- Publish to Kafka
- Return 202 ACCEPTED

**Fraud Detection Service**

- Consume from Kafka
- Tier 1: Rule-based checks
- Tier 2: ML model scoring
- Tier 3: Async investigation
- Publish fraud_scores

**Settlement Service**

- Consume fraud decisions
- Update user/merchant balances
- Record in settlements table
- Create audit logs

**Feature Store (Flink + Redis)**

- Real-time feature computation
- Velocity aggregation
- Device trust scores
- User baselines

## Data Flow

### Transaction Lifecycle

User initiates payment: $100
API Gateway:

Validates Pydantic request
Creates Transaction(status=PENDING)
Publishes to Kafka
Returns 202 ACCEPTED

Fraud Detection (Async):

Fetches features from Redis (2-5ms)
Tier 1: Rule checks (1ms)
Tier 2: ML scoring (15ms)
Total latency: ~50ms
Publishes fraud_scores

Settlement (if approved):

Locks user account (optimistic locking)
Deducts $100 from user
Credits $98 to merchant (after fee)
Records in settlements table
Updates audit_logs

Feature Store (continuous):

Flink updates velocity features
Redis receives updates
Used by next fraud check

## Technology Choices & Trade-Offs

### 1. Kafka vs RabbitMQ

**Kafka chosen because:**

- Throughput: 2M+ messages/second (vs RabbitMQ 50K)
- Partitioning: Preserves order by user_id
- Durability: 7-day retention for audit
- Replay: Can replay events for debugging

**Trade-off:**

- More complex to operate
- Requires Zookeeper
- Higher memory footprint

### 2. PostgreSQL + Redis + ClickHouse

**Instead of single database:**

| Purpose    | Why Separate                 |
| ---------- | ---------------------------- |
| PostgreSQL | Transactions (ACID required) |
| Redis      | Features (sub-ms latency)    |
| ClickHouse | Analytics (time-series)      |

**Trade-off:**

- Eventual consistency (data sync delay)
- More operational burden
- Better at scale

### 3. Optimistic Locking

**Version field prevents race conditions without locks:**

Transaction 1: SELECT balance=1000, version=5
UPDATE balance=900 WHERE version=5 ✓
Transaction 2: SELECT balance=1000, version=5
UPDATE balance=850 WHERE version=5 ✗
(version is now 6, retry)

**Trade-off:**

- No blocking (faster)
- Retry logic needed
- Scales to 100K TPS

### 4. Eventual Consistency

**Redis features lag PostgreSQL by 1-2 seconds:**

**Trade-off:**

- ✓ Sub-millisecond reads
- ✗ Might use stale data

**Acceptable because:**

- Fraud detection is probabilistic (small lag OK)
- Alternative (strong consistency) would require locks

## Latency Breakdown

### Transaction Processing Path (50ms budget)

API Receive: 1ms
Pydantic Validate: 2ms
PostgreSQL INSERT: 10ms
Kafka Publish: 5ms
────────────────────────
API Response: 18ms ✓ (within budget)
Fraud Detection:
Redis Fetch: 5ms
Rule Checks: 1ms
ML Inference: 15ms
Kafka Publish: 5ms
────────────────────────
Total Fraud: 26ms ✓ (within 50ms budget)
Settlement (async):
PostgreSQL UPDATE: 10ms
Balance calc: 2ms
Audit log: 5ms
────────────────────────
Total Settlement: 17ms

## Scalability Analysis

### Throughput Capacity

**Single API Instance:** ~5,000 TPS

- FastAPI can handle ~10K req/sec
- PostgreSQL bottleneck: ~5K writes/sec

**With horizontal scaling:**

- 20 API instances: 100K TPS ✓
- Load balancer routes traffic
- Each instance connects to same PostgreSQL (via connection pool)

### Database Scaling

**PostgreSQL:**

- Connection pooling: 20 connections/instance
- 20 instances × 20 = 400 connections
- PostgreSQL max: 1000 connections ✓

**Redis:**

- All reads (no writes from Flink)
- Can handle millions of ops/sec
- No scaling needed for 100K TPS

**Kafka:**

- 1000 partitions (one per TPS unit)
- Each partition: ~100 TPS
- 1000 × 100 = 100K TPS ✓

## Failure Scenarios & Recovery

### Scenario 1: PostgreSQL Down

**What happens:**

- API can't create transactions
- Kafka still accepts publishes (buffered)
- Returns 500 error to user

**Recovery:**

- User retries (idempotency_key prevents duplicates)
- Database comes back online
- Kafka replays messages

### Scenario 2: Kafka Broker Down

**What happens:**

- API can't publish (times out after 10s)
- Returns 500 error

**Recovery:**

- Auto-failover to replica broker
- Kafka cluster survives broker loss

### Scenario 3: Redis Down

**What happens:**

- Fraud detection can't fetch features
- Falls back to simpler rules
- Slightly higher false positives

**Recovery:**

- Flink rebuilds Redis from Kafka replay
- Service restores in minutes

## Compliance Considerations

### PCI-DSS

1. **Card Data Protection:**
   - Never store full card number
   - Only SHA256 hash stored
   - Encryption in transit (TLS)

2. **Audit Trails:**
   - Every transaction logged in audit_logs
   - Immutable (append-only)
   - 7-year retention

3. **Access Control:**
   - API authentication required
   - Role-based access (not implemented here)
   - Logging of all access

### GDPR

1. **Data Retention:**
   - Keep only necessary data
   - Delete after 7 years (PCI requirement)
   - User has right to deletion

2. **Data Security:**
   - Encryption at rest and transit
   - Regular backups
   - Disaster recovery plan

## Monitoring Strategy

### Key Metrics

| Metric                  | Target | Action if exceeded     |
| ----------------------- | ------ | ---------------------- |
| API Latency (p99)       | <100ms | Scale API instances    |
| Fraud Detection Latency | <50ms  | Optimize ML model      |
| Kafka Lag               | <1s    | Add consumers          |
| PostgreSQL Connections  | <80%   | Increase pool size     |
| Redis Memory            | <80%   | Increase instance size |

### Alerts

1. **Critical:** API down, database down, Kafka lag >10s
2. **Warning:** Latency spike, error rate >1%, Redis memory >80%
3. **Info:** New deployment, traffic spike

## Future Enhancements

1. **Multi-region deployment** (active-active)
2. **Advanced fraud ML** (deep learning)
3. **Real-time dashboards** (ClickHouse + Grafana)
4. **API rate limiting** (prevent abuse)
5. **Circuit breakers** (graceful degradation)

## References

- [Kafka Documentation](https://kafka.apache.org/)
- [PostgreSQL Performance Tuning](https://www.postgresql.org/docs/current/performance-tips.html)
- [Redis Persistence](https://redis.io/docs/management/persistence/)
- [PCI DSS Compliance](https://www.pcisecuritystandards.org/)
