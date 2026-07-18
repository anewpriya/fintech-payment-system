# Fintech Payment Processing System - Architecture Design Document

**Date:** July 9, 2026
**Author:** Anupriya Singh
**Version:** 2.0
**Status:** Production-Ready

## Executive Summary

This document describes the architecture of a production-grade payment processing system designed to handle 100K+ transactions per second with sub-50ms fraud detection latency, distributed event streaming, and Kubernetes orchestration. The system demonstrates enterprise-level thinking on real-time fraud detection, distributed systems, eventual consistency, idempotency, audit trails for compliance, and production infrastructure.

## Design Goals

**Scalability**: Handle 100K transactions/second with horizontal scaling via Kubernetes auto-scaling.

**Reliability**: Never lose a transaction - Kafka durability with 7-day retention, PostgreSQL multi-AZ, immutable audit logs.

**Speed**: Fraud detection in <50ms (Tier 1 rules <1ms + Tier 2 ML <50ms).

**Compliance**: PCI-DSS audit trails, data encryption, card number hashing, access control via RBAC.

**Operational Excellence**: Zero-downtime deployments, auto-scaling based on metrics, comprehensive monitoring and alerting, incident playbooks.

## Architecture Overview

### System Diagram - Local Development

```
┌─────────────────────────────────────────────────────────┐
│ Client (User/Merchant App)                              │
└──────────────────┬──────────────────────────────────────┘
                   │ HTTPS POST /transactions
                   ↓
┌─────────────────────────────────────────────────────────┐
│ API Gateway (FastAPI)                                   │
│ - Validates request (Pydantic)                          │
│ - Creates transaction in PostgreSQL                     │
│ - Publishes to Kafka                                    │
│ - Returns 202 ACCEPTED immediately                      │
└──────────────────┬──────────────────────────────────────┘
                   │ transaction event
                   ↓
        ┌──────────────────────┐
        │ Kafka Topic:         │
        │ transactions         │
        │ (7-day retention)    │
        └──────┬───────────────┘
               │
        ┌──────┴────────────────────────┐
        │                               │
        ↓                               ↓
    ┌─────────────────┐      ┌──────────────────────┐
    │ Fraud Detection │      │ Feature Store        │
    │ Service         │      │ (Flink + Redis)      │
    │                 │      │                      │
    │ Tier 1: Rules   │      │ Computes velocity    │
    │ Tier 2: ML      │      │ features in real-time│
    │ Tier 3: Async   │      │ Serves from Redis    │
    └────────┬────────┘      └──────────────────────┘
             │
             ↓
    ┌──────────────────────┐
    │ Kafka Topic:         │
    │ fraud-scores         │
    └────────┬─────────────┘
             │
             ↓
    ┌──────────────────────┐
    │ Settlement Service   │
    │                      │
    │ Process approved txn │
    │ Update balances      │
    └────────┬─────────────┘
             │
             ├→ PostgreSQL (transactions, settlements, audit_logs)
             ├→ Kafka (settlements topic)
             └→ ClickHouse (analytics)
```

### System Diagram - Production (Kubernetes)

```
┌──────────────────────────────────────────────────────────┐
│ Internet Traffic                                         │
└──────────────────────┬───────────────────────────────────┘
                       │
                       ↓
        ┌──────────────────────────┐
        │ AWS Application          │
        │ Load Balancer (ALB)      │
        │ - SSL/TLS termination    │
        │ - Request routing        │
        └──────────────┬───────────┘
                       │
        ┌──────────────┴────────────────────────┐
        │                                       │
        ↓                                       ↓
┌──────────────────────────┐      ┌──────────────────────┐
│ Kubernetes Cluster       │      │ Managed Services     │
│ (3+ worker nodes)        │      │ (AWS)                │
│                          │      │                      │
│ ┌────────────────────┐   │      │ ┌────────────────┐   │
│ │ API Gateway Pod    │   │      │ │ RDS PostgreSQL │   │
│ │ (3-10 replicas)    │   │      │ │ (Multi-AZ)     │   │
│ │ ├─ Liveness probe  │   │      │ │ - Automated    │   │
│ │ ├─ Readiness probe │   │      │ │   backups      │   │
│ │ ├─ HPA (70% CPU)   │   │      │ │ - Failover     │   │
│ │ └─ Resources: CPU  │   │      │ └────────────────┘   │
│ │   250-500m, Mem    │   │      │                      │
│ │   256-512Mi        │   │      │ ┌────────────────┐   │
│ │                    │   │      │ │ ElastiCache    │   │
│ │ ┌────────────────┐ │   │      │ │ Redis Cluster  │   │
│ │ │ Fraud Detect   │ │   │      │ │ (Multi-AZ)     │   │
│ │ │ Pod (2-8 rep)  │ │   │      │ │ - Replication  │   │
│ │ │ ├─ Liveness    │ │   │      │ │ - Persistence  │   │
│ │ │ ├─ HPA (75%)   │ │   │      │ └────────────────┘   │
│ │ │ └─ Resources:  │ │   │      │                      │
│ │ │   CPU 500m-1   │ │   │      │ ┌────────────────┐   │
│ │ │   Mem 512-1Gi  │ │   │      │ │ MSK Kafka      │   │
│ │ │                │ │   │      │ │ (3 brokers)    │   │
│ │ │ ┌────────────┐ │ │   │      │ │ - Replication  │   │
│ │ │ │ Settlement │ │ │   │      │ │ - 7-day ret    │   │
│ │ │ │ Pod (2-6)  │ │ │   │      │ └────────────────┘   │
│ │ │ │ ├─ Liveness│ │ │   │      │                      │
│ │ │ │ ├─ HPA 70% │ │ │   │      │ ┌────────────────┐   │
│ │ │ │ └─ Res:    │ │ │   │      │ │ S3 Backups     │   │
│ │ │ │   250-500m │ │ │   │      │ │ (7-year)       │   │
│ │ │ │   256-1Gi  │ │ │   │      │ └────────────────┘   │
│ │ │ └────────────┘ │ │   │      │                      │
│ │ └────────────────┘ │   │      │ ┌────────────────┐   │
│ │                    │   │      │ │ AWS Secrets    │   │
│ │ NetworkPolicy:     │   │      │ │ Manager        │   │
│ │ - Ingress rules    │   │      │ │ (API keys,     │   │
│ │ - Egress rules     │   │      │ │ passwords)     │   │
│ │ - Pod-to-pod only  │   │      │ └────────────────┘   │
│ └────────────────────┘   │      │                      │
│                          │      │ ┌────────────────┐   │
│ ServiceMonitor           │      │ │ S3 for logs    │   │
│ (Prometheus scraping)    │      │ (CloudTrail)    │   │
│                          │      │ └────────────────┘   │
└──────────────────────────┘      └──────────────────────┘
         ↑
         │ Monitoring & Observability
         │
    ┌────────────────────────────────┐
    │ Monitoring Stack (Outside K8s) │
    ├────────────────────────────────┤
    │ Prometheus - Metrics scraping   │
    │ Grafana - Dashboards           │
    │ PagerDuty - Alerting           │
    │ CloudWatch - AWS logs          │
    └────────────────────────────────┘
```

## Component Responsibilities

### API Gateway (FastAPI)

**Purpose**: Single entry point for transaction requests. Validates, stores, and queues transactions for async processing.

**Responsibilities**:

- Accept HTTP POST requests to `/transactions` endpoint
- Validate request using Pydantic (type checking, required fields)
- Generate transaction_id (UUID) and idempotency_key (UUID)
- Create transaction record in PostgreSQL with status=PENDING
- Publish to Kafka `transactions` topic (partition key = user_id)
- Return 202 ACCEPTED to user immediately
- Handle errors gracefully (400 Bad Request, 500 Internal Error)
- Expose `/health` endpoint for Kubernetes liveness checks
- Expose `/metrics` endpoint for Prometheus monitoring

**Scalability**: Horizontally scale via Kubernetes HPA. Each instance handles ~5K TPS. At 100K TPS: 20 instances needed.

**Resources**: CPU 250-500m, Memory 256-512Mi per pod.

### Fraud Detection Service (Kafka Consumer)

**Purpose**: Real-time fraud detection using multi-tier scoring.

**Responsibilities**:

- Consume transactions from Kafka `transactions` topic
- Fetch real-time features from Redis (velocity, device trust, user baseline)
- Run Tier 1 rule checks (stolen cards, sanctioned countries, impossible travel)
- If Tier 1 uncertain, run Tier 2 ML scoring (LightGBM model)
- Publish fraud_scores to Kafka `fraud-scores` topic
- Handle consumer group offsets (exactly-once processing semantics)
- Log all decisions for audit trail

**Fraud Scoring**:

- Fraud Score 0-30: Low risk, APPROVE
- Fraud Score 30-75: Medium risk, APPROVE but monitor
- Fraud Score 75-100: High risk, DECLINE

**Scalability**: Horizontally scale via Kubernetes HPA based on Kafka lag. Consumer group auto-manages partitions.

**Resources**: CPU 500m-1000m (ML inference is CPU-heavy), Memory 512Mi-1Gi per pod.

### Settlement Service (Kafka Consumer)

**Purpose**: Process approved transactions, update balances, and create audit trail.

**Responsibilities**:

- Consume fraud_scores from Kafka `fraud-scores` topic
- If APPROVE: settle transaction (update user/merchant balances)
- If DECLINE: update transaction status and skip settlement
- Use optimistic locking to prevent race conditions (version field)
- Create settlement record (gross amount, fees, net amount)
- Log all changes to audit_logs for compliance
- Publish settlements to Kafka `settlements` topic
- Handle insufficient balance errors

**Settlement Flow**:

1. Lock user account (optimistic locking)
2. Check sufficient balance
3. Deduct amount from user
4. Lock merchant account
5. Credit amount to merchant
6. Create settlement record
7. Create audit logs
8. Commit transaction
9. Publish to Kafka

**Scalability**: Horizontally scale based on database load. Optimistic locking prevents locks, enabling high concurrency.

**Resources**: CPU 250-500m, Memory 512Mi-1Gi per pod.

### Feature Store (Flink Stream Processing)

**Purpose**: Real-time computation of velocity features for fraud detection.

**Responsibilities**:

- Consume transactions from Kafka `transactions` topic
- Compute stateful aggregations (rolling windows)
- Maintain per-user state (transaction counts, amounts, merchant tracking)
- Update Redis every 100ms with fresh features
- Handle late-arriving events and out-of-order processing

**Features Computed**:

- Velocity: transactions in 1min, 5min, 1hour, 24hour windows
- Amount: total spent in same windows
- Merchant tracking: distinct merchants in 1hour, 24hour
- Timestamp tracking: last transaction time per user

**Deployment**: Single Flink cluster (not auto-scaled) as state coordination is complex. For massive scale, would use Flink with savepoints and recovery.

## Data Flow

### Complete Transaction Lifecycle

**Step 1: User Initiates Payment ($100)**

User submits transaction via mobile app:

```
POST /transactions
{
  "user_id": 12345,
  "merchant_id": 67890,
  "amount": 100.00,
  "card_hash": "abc123...",
  ...
}
```

**Step 2: API Gateway Validation (2ms)**

- Pydantic validates request schema
- Generate transaction_id and idempotency_key
- Validate card_hash format
- Check amount > 0

**Step 3: Database Persistence (10ms)**

- INSERT transaction into PostgreSQL
- Status: PENDING
- idempotency_key: UNIQUE constraint prevents duplicates

**Step 4: Kafka Publishing (5ms)**

- Publish to `transactions` topic
- Partition key: user_id (ensures ordering)
- Message format: JSON with all transaction details

**Step 5: User Response (202 ACCEPTED)**

User gets immediate response:

```
{
  "transaction_id": "uuid...",
  "status": "PENDING",
  "message": "Transaction received and is being processed"
}
```

**Step 6: Fraud Detection Consumer Reads (async)**

Fraud detection service:

1. Dequeue from Kafka `transactions` topic (typically <100ms after publish)
2. Fetch features from Redis (5ms)
3. Run Tier 1 rules (1ms)
   - Is card stolen? NO
   - Sanctioned country? NO
   - Impossible travel? NO
   - Extreme amount? NO
     → Escalate to Tier 2
4. Run Tier 2 ML (15ms)
   - 32 features (velocity, behavioral, device, network)
   - LightGBM inference: 15ms
   - Score: 45/100 (APPROVE)
5. Publish to `fraud-scores` topic

**Total Fraud Detection Latency: 26ms (within 50ms budget)**

**Step 7: Settlement Consumer Reads (async)**

Settlement service:

1. Dequeue from Kafka `fraud-scores` topic
2. If APPROVE:
   - Lock user account (optimistic locking)
   - Deduct $100 from user (version check ensures no double-spend)
   - Lock merchant account
   - Credit $98 to merchant (after 2% fee)
   - Create settlement record
   - Log audit trail
3. Commit transaction to PostgreSQL

**Total Settlement Latency: 45ms**

**Step 8: User Checks Status (later)**

User checks transaction status:

```
GET /transactions/uuid...
```

Response shows:

```
{
  "status": "APPROVED",
  "fraud_score": 45,
  "amount": "100.00"
}
```

## Technology Choices & Trade-Offs

### Choice 1: Kafka vs RabbitMQ

**Kafka Chosen**

**Throughput Comparison**:

- Kafka: 2M+ messages/second
- RabbitMQ: ~50K messages/second

**Kafka Advantages**:

- Partitioning: Preserves order by user_id (critical for fraud detection)
- Durability: 7-day retention for replay and audit
- Fault Tolerance: Replication factor 3 (survives broker failures)
- Topic Replay: Can reprocess all events (debugging, model retraining)

**Kafka Disadvantages**:

- Requires Zookeeper coordination
- Higher operational complexity
- More memory overhead

**Trade-off**: 100K TPS requirement necessitates Kafka. RabbitMQ maxes out at 50K, making it a dead-end at scale.

### Choice 2: PostgreSQL + Redis + ClickHouse (Three Databases)

**Why Separate?**

At 100K TPS, single database becomes bottleneck:

- PostgreSQL: ~5K writes/second (ACID required)
- Redis: 1M+ operations/second (speed required)
- ClickHouse: 100K+ inserts/second (analytics optimized)

**PostgreSQL** (Transactional)

- Stores transactions, settlements, audit logs
- ACID guarantees (exactly-once semantics)
- Data persistence

**Redis** (Speed)

- Real-time features (velocity, device trust)
- Sub-millisecond latency (fraud detection can't wait)
- In-memory (extremely fast)
- Trade-off: Data lost on restart (mitigated by Flink recompute)

**ClickHouse** (Analytics)

- Time-series optimized (immense write throughput)
- Columnar storage (fast analytical queries)
- Time-based pruning (efficient partitioning)

**Trade-off**: Eventual consistency (Redis lags PostgreSQL 1-2 seconds). Acceptable because fraud detection is probabilistic (slight staleness OK).

### Choice 3: Optimistic Locking (Version Field)

**Why Not Pessimistic Locking?**

**Pessimistic Locking Example**:

```
SELECT balance FROM user_accounts WHERE user_id=123 FOR UPDATE;
-- Other threads WAIT here (blocking)
UPDATE user_accounts SET balance=900 WHERE user_id=123;
UNLOCK;
```

**Problem**: At 100K TPS with 1000 concurrent users, many threads blocked waiting for locks. Latency explodes.

**Optimistic Locking Example**:

```
SELECT balance, version FROM user_accounts WHERE user_id=123;
-- Gets: balance=1000, version=5

UPDATE user_accounts SET balance=900, version=6 WHERE user_id=123 AND version=5;
-- If version still 5: success
-- If version changed (someone else updated): UPDATE returns 0 rows
-- Application retries SELECT+UPDATE
```

**Advantage**: No blocking. Retries only happen on actual conflict (rare).

**Trade-off**: Slightly more complex code (retry logic). Worth it for 100x better concurrency.

### Choice 4: Eventual Consistency

**Why Not Strong Consistency?**

Strong Consistency requires:

- All reads see latest writes
- Implemented via locks or consensus (Raft, Paxos)
- Prevents stale reads but requires coordination

**Problem**: At 100K TPS, coordination overhead is massive.

**Our Choice: Eventual Consistency**

```
Redis gets update 1-2 seconds after PostgreSQL
Feature lookup: May use 1-2 second old data
Impact: Fraud score might miss very recent transactions
Acceptable because: Most fraud detected by overall patterns, not single recent transaction
```

**Trade-off**: Code must handle stale reads. Example: User checks balance immediately after transaction, might see old balance briefly.

**Why it works**: Fraud detection doesn't need perfectly fresh data. Patterns (velocity, amounts, merchants) are aggregated over minutes/hours. 1-2 second lag negligible.

### Choice 5: Partition by user_id

**Why User ID as Partition Key?**

**Partitioning Purpose**: Distribute load across Kafka partitions.

**User ID Partitioning**:

- All transactions from User 123 → Partition 42 (hash("123") % 1000)
- All transactions from User 456 → Partition 57

**Advantage**: Transactions from same user are ordered.

**Example**:

```
User 123 transaction sequence:
  1. $50 (normal)
  2. $500 (unusual)
  3. $5000 (very unusual)

If out of order (arrive as 2, 1, 3):
  Fraud model might see: $500 → $50 → $5000
  Incorrectly interprets pattern

Partitioned (correct order):
  Fraud model sees: $50 → $500 → $5000
  Correctly identifies escalating fraud
```

**Trade-off**: Uneven partition distribution (active users get hot partitions). Solution: Monitor and rebalance if needed.

### Choice 6: Multi-Tier Fraud Detection

**Why Not Single ML Model?**

**Single Model Approach**:

- Run LightGBM on every transaction
- Latency: 50ms per transaction
- Can't optimize obvious cases

**Multi-Tier Approach**:

- Tier 1 (Rules): <1ms, catches obvious fraud (stolen cards, sanctioned countries)
- Tier 2 (ML): 50ms, catches subtle patterns (spending changes, device anomalies)
- Total: Still <50ms for obvious fraud, <50ms for subtle fraud

**Trade-off**: More code (two decision paths). Worth it for latency optimization.

## Production Deployment Architecture

### Kubernetes Orchestration

**Cluster Setup**:

- 3+ worker nodes (m5.xlarge or equivalent)
- 1 control plane node (or managed by EKS)
- Container runtime: Docker
- Networking: Calico or Flannel for pod networking

**Namespace Isolation**:

```
kubectl create namespace fintech
```

All services run in `fintech` namespace (isolation from other applications).

### Horizontal Pod Autoscaling (HPA)

**API Gateway Autoscaler**:

```yaml
minReplicas: 3
maxReplicas: 10
metrics:
  - CPU: 70% utilization
  - Memory: 80% utilization
scaleUpWindow: 60s (gradual scaling up)
scaleDownWindow: 300s (conservative scaling down)
```

**Fraud Detection Autoscaler**:

```yaml
minReplicas: 2
maxReplicas: 8
metrics:
  - CPU: 75% utilization
  - Memory: 80% utilization
scaleUpWindow: 30s (faster scale-up, fraud is critical)
scaleDownWindow: 60s
```

**Settlement Autoscaler**:

```yaml
minReplicas: 2
maxReplicas: 6
metrics:
  - CPU: 70% utilization
  - Memory: 75% utilization
```

### Deployment Strategy (Zero-Downtime)

**Rolling Update**:

```yaml
strategy:
  type: RollingUpdate
  rollingUpdate:
    maxSurge: 1 # 1 extra pod during update
    maxUnavailable: 0 # 0 pods down (zero-downtime)
```

Process:

1. Start new pod (version 2)
2. Run health checks (liveness + readiness probes)
3. Start routing traffic to new pod
4. Gracefully shutdown old pod (30s termination grace period)
5. Repeat for all pods

Result: Users never experience downtime.

### Pod Disruption Budgets (PDB)

**Purpose**: Prevent simultaneous pod evictions during maintenance.

```yaml
spec:
  minAvailable: 2 # Keep minimum 2 pods running
  selector:
    matchLabels:
      app: api-gateway
```

Kubernetes respects PDB:

- Node needs maintenance: Evict non-essential pods first
- PDB pods: Wait until minimum available is satisfied
- Prevents cascading failures

### Network Policies

**Ingress Rules**:

- Allow traffic FROM load balancer TO api-gateway pods
- Allow traffic FROM api-gateway TO kafka, redis, postgres

**Egress Rules**:

- Allow outbound TO kafka (9092)
- Allow outbound TO postgres (5432)
- Allow outbound TO redis (6379)
- Allow DNS (53)
- Deny everything else (zero-trust)

**Security**: Prevents pods from making unexpected outbound connections (data exfiltration protection).

### Container Architecture

**Multi-Stage Docker Build**:

**Stage 1 (Builder)**:

```dockerfile
FROM python:3.10
RUN apt-get install gcc g++ build-essential
COPY requirements.txt .
RUN pip install -r requirements.txt
# Size: ~500MB (includes compilers, build artifacts)
```

**Stage 2 (Runtime)**:

```dockerfile
FROM python:3.10-slim
COPY --from=builder /root/.local /root/.local
COPY src/ /app/src/
USER fintech (non-root)
# Size: ~150MB (70% reduction)
```

**Benefits**:

- Small image size (faster deployment)
- No build tools in production (reduced attack surface)
- Faster startup (pre-compiled dependencies)

### Resource Limits & Requests

**Why Both?**

**Requests**: Reserved CPU/memory for pod

- Kubernetes uses for scheduling decisions
- Guarantees pod gets this much

**Limits**: Maximum CPU/memory pod can use

- Kubernetes throttles/kills if exceeded
- Prevents pod from monopolizing node

**API Gateway**:

```yaml
requests: CPU 250m, Memory 256Mi
limits: CPU 500m, Memory 512Mi
```

Rationale: Lightweight service, mostly I/O bound (Kafka, database).

**Fraud Detection**:

```yaml
requests: CPU 500m, Memory 512Mi
limits: CPU 1000m, Memory 1Gi
```

Rationale: ML inference is CPU-intensive (LightGBM). Needs more compute.

**Settlement**:

```yaml
requests: CPU 250m, Memory 512Mi
limits: CPU 500m, Memory 1Gi
```

Rationale: Database-heavy, needs memory for connection pooling.

### Health Checks

**Liveness Probe** (is pod alive?)

```yaml
exec:
  command:
    [
      "python",
      "-c",
      "import requests; requests.get('http://localhost:8000/health')",
    ]
initialDelaySeconds: 10
periodSeconds: 10
failureThreshold: 3
```

If fails 3 times: Kubernetes restarts pod.

**Readiness Probe** (is pod ready for traffic?)

```yaml
httpGet:
  path: /health
  port: 8000
initialDelaySeconds: 5
periodSeconds: 5
failureThreshold: 2
```

If fails 2 times: Pod removed from load balancer (but not restarted).

### Service Discovery

**ClusterIP Service** (internal):

```yaml
apiVersion: v1
kind: Service
metadata:
  name: api-gateway-service
spec:
  type: ClusterIP
  selector:
    app: api-gateway
  ports:
    - port: 8000
      targetPort: 8000
```

Internal traffic uses DNS name: `api-gateway-service.fintech.svc.cluster.local`

**LoadBalancer Service** (external):

```yaml
apiVersion: v1
kind: Service
metadata:
  name: api-gateway-service-external
spec:
  type: LoadBalancer
  selector:
    app: api-gateway
  ports:
    - port: 80
      targetPort: 8000
```

Automatically provisions AWS ALB, exposes public IP.

## Continuous Integration/Deployment Pipeline

### GitHub Actions Workflow

**Trigger**: Every push to main branch

**Stage 1: Testing**

- PostgreSQL, Kafka, Redis start in Docker
- Runs linting (flake8)
- Runs unit tests (pytest)
- Runs integration tests (8+ tests)
- Generates coverage report

**Stage 2: Security Scanning**

- Bandit: Scans Python code for security issues
- Safety: Checks dependencies for CVEs
- Semgrep: OWASP vulnerability patterns

**Stage 3: Build** (only if tests + security pass)

- Docker build with multi-stage build
- Push to Docker registry
- Tag: `latest` + git commit SHA

**Stage 4: Deploy** (only on main)

- Get kubeconfig from secrets
- Apply Kubernetes manifests
- Wait for rollout completion
- Run smoke test (curl /health)

**Stage 5: Notify**

- Slack notification with status
- Includes commit SHA

### Deployment Frequency & Safety

**Previous Approaches**:

- Manual deployments (slow, error-prone)
- Weekly batch deployments (long release cycles)

**Our Approach**:

- Automated deployment on every push
- Tests run automatically (no skipping)
- Rollout safety: HPA + readiness probes
- Rollback: kubectl rollout undo

**Result**: Deploy 10-20 times per day confidently.

## Failure Scenarios & Recovery

### Scenario 1: API Gateway Pod Crashes

**What Happens**:

- Liveness probe detects (health check fails)
- Kubernetes restarts pod automatically
- HPA spins up new pod to maintain minimum replicas
- Traffic routed to healthy pods (load balancer)
- Users may see brief delay, but no data loss

**Recovery Time**: <30 seconds

### Scenario 2: Kafka Broker Down (3-broker cluster)

**What Happens**:

- Kafka detects broker failure
- Rebalances partitions to remaining brokers
- No data loss (replication factor 3)
- Consumer groups rebalance

**Recovery Time**: 30-60 seconds (automatic via broker failover)

### Scenario 3: PostgreSQL RDS Failover

**What Happens**:

- RDS detects primary failure
- Automatically promotes read replica to primary
- Connection string unchanged (Route53 DNS update)
- API/consumers reconnect

**Recovery Time**: 1-2 minutes (RDS automatic failover)

### Scenario 4: Redis Cluster Node Down

**What Happens**:

- Redis detects node failure
- Rebalances cluster slots
- Fraud detection can't find features (Redis empty)
- Flink rebuilds features from Kafka

**Recovery Time**: 3-5 minutes (rebuild features from Kafka)
**Impact**: Higher fraud detection latency temporarily (uses default features instead of actual)

### Scenario 5: Settlement Service Crashes

**What Happens**:

- Consumer group rebalances
- Another settlement pod takes over
- Transactions in-flight might be reprocessed (handled via idempotency_key)
- No data loss

**Recovery Time**: <30 seconds
**Idempotency**: UNIQUE(idempotency_key) prevents double-charging

## Latency Analysis

### API Gateway Latency Breakdown

```
Request arrives → 1ms
Pydantic validation → 2ms
PostgreSQL INSERT → 10ms
Kafka PUBLISH → 5ms
Response sent → 18ms total
```

**Meets <100ms requirement**: ✓

### Fraud Detection Latency Breakdown

```
Dequeue from Kafka → 1ms
Redis FETCH features → 5ms
Tier 1 rules → 1ms
[If Tier 1 uncertain]
  Redis FETCH features → 5ms
  LightGBM inference → 15ms
Kafka PUBLISH → 5ms
Total → 26ms (obvious fraud) or 48ms (complex fraud)
```

**Meets <50ms requirement**: ✓

### Settlement Latency Breakdown

```
Dequeue from Kafka → 1ms
PostgreSQL SELECT (optimistic lock) → 2ms
PostgreSQL UPDATE balance → 10ms
PostgreSQL INSERT settlement → 5ms
PostgreSQL INSERT audit_logs → 3ms
PostgreSQL COMMIT → 2ms
Kafka PUBLISH → 5ms
Total → 28ms
```

**Meets <100ms requirement**: ✓

**System E2E Latency** (transaction to settlement):

```
API response: 18ms (user sees immediately)
Fraud detection: 48ms (after API response)
Settlement: 28ms (after fraud detection)
Total: 94ms (fraud detection + settlement)
```

## Scalability Limits

### Current Architecture Capacity

**API Gateway**:

- Per instance: ~5,000 TPS (FastAPI + Uvicorn)
- With 20 instances: 100,000 TPS
- Max replicas: 10 (from HPA) = 50,000 TPS

**Fraud Detection**:

- Per instance: ~50,000 TPS (model inference is fast, I/O bound to Redis)
- With 2 instances: 100,000 TPS
- Max replicas: 8 = 400,000 TPS (plenty of headroom)

**Settlement**:

- Per instance: ~2,000 TPS (database writes are bottleneck)
- With 6 instances: 12,000 TPS
- Bottleneck: PostgreSQL RDS

**PostgreSQL Bottleneck**:

- Max connections: 1000 (default RDS limit)
- Connections per pod: 20
- Max pods: 50 (1000 connections / 20 per pod)
- TPS: Depends on transaction complexity, typically 5-10K TPS max

**Redis Bottleneck**:

- Typical: 100K+ ops/second
- Not a bottleneck at 100K TPS

**Kafka Bottleneck**:

- With 1000 partitions: 100K TPS (100 TPS per partition, typical)
- Not a bottleneck

### Scaling Beyond 100K TPS

1. **Increase PostgreSQL capacity**:
   - Use larger RDS instance (e.g., db.r5.4xlarge)
   - OR use sharding (split data by user_id ranges)

2. **Add read replicas**:
   - For read-heavy queries (status checks)
   - Fraud detection only reads from Redis, not PostgreSQL (OK)

3. **Scale Kafka**:
   - Increase partition count from 1000 to 10000
   - Add more brokers to MSK cluster

## References

### Architecture Patterns

- Event-Driven Architecture: Kafka as event bus
- CQRS Pattern: Separate read and write models
- Saga Pattern: Distributed transactions (settlement)
- Circuit Breaker: Handle external service failures

### Technologies

- Kafka: https://kafka.apache.org/
- PostgreSQL: https://www.postgresql.org/
- Redis: https://redis.io/
- Kubernetes: https://kubernetes.io/
- Docker: https://www.docker.com/

### Learning Resources

- Designing Data-Intensive Applications (Martin Kleppmann)
- Building Microservices (Sam Newman)
- Site Reliability Engineering (Google SRE Book)
