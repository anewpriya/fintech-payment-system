# Fintech Payment System - Operations Guide

**Date:** July 9, 2026
**Version:** 1.0

## System Overview

This is a production-grade payment processing system with three main services:

1. **API Gateway** (FastAPI) - Receives transactions
2. **Fraud Detection Service** - Scores transactions in real-time
3. **Settlement Service** - Processes approved transactions

All services communicate via Kafka for event streaming.

---

## Local Development Setup

### Prerequisites

- Docker & Docker Compose
- Python 3.10+
- Git

### Start Local Environment

```bash
# Navigate to project
cd 02-fintech-payment-system

# Start all services (PostgreSQL, Kafka, Redis, Zookeeper)
docker-compose up -d

# Verify all services are healthy
docker-compose ps
# All should show "healthy" or "running"

# Create virtual environment
python -m venv venv
source venv/bin/activate  # Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt

# Initialize database
python -c "from src.database import init_db; init_db()"
```

### Run Services

**Terminal 1 - API Gateway:**

```bash
python -m uvicorn src.api.main:app --reload --host 0.0.0.0 --port 8000
```

**Terminal 2 - Fraud Detection Consumer:**

```bash
python -m src.run_fraud_consumer
```

**Terminal 3 - Settlement Consumer:**

```bash
python -m src.run_settlement_consumer
```

### Test the System

```bash
# Health check
curl http://localhost:8000/health

# Create transaction
curl -X POST http://localhost:8000/transactions \
  -H "Content-Type: application/json" \
  -d '{
    "user_id": 12345,
    "merchant_id": 67890,
    "user_email": "alice@example.com",
    "user_country": "US",
    "amount": 100.00,
    "currency": "USD",
    "card_last_four": "4242",
    "card_brand": "VISA",
    "card_hash": "abc123def456",
    "device_fingerprint": "dev-xyz"
  }'

# Response (202 ACCEPTED):
# {
#   "transaction_id": "uuid-...",
#   "idempotency_key": "uuid-...",
#   "status": "PENDING",
#   "message": "Transaction received and is being processed"
# }

# Check transaction status
curl http://localhost:8000/transactions/{transaction_id}
```

---

## Production Deployment

### Architecture

Internet
↓
Load Balancer (AWS ALB)
↓
[API Gateway] × N (auto-scaled)
↓
Kafka Cluster (3 brokers)
├→ [Fraud Detection] × N (auto-scaled)
├→ [Settlement] × N (auto-scaled)
└→ [Feature Store] × 1 (Flink)
↓
PostgreSQL (RDS, Multi-AZ)
Redis Cluster (ElastiCache)

### Kubernetes Deployment

See `infrastructure/k8s/` for manifests:

- `api-gateway-deployment.yaml`
- `fraud-detection-deployment.yaml`
- `settlement-deployment.yaml`

Deploy with:

```bash
kubectl apply -f infrastructure/k8s/
```

### Monitoring & Alerting

#### Key Metrics

| Metric                  | Target | Alert Threshold |
| ----------------------- | ------ | --------------- |
| API Latency (p99)       | <100ms | >200ms          |
| Fraud Detection Latency | <50ms  | >75ms           |
| Settlement Latency      | <100ms | >150ms          |
| Kafka Lag               | <1s    | >10s            |
| PostgreSQL Connections  | <80%   | >90%            |
| Redis Memory            | <80%   | >90%            |

#### Prometheus Endpoints

- API: `http://localhost:8000/metrics`
- Fraud Detection: Configure in consumer
- Settlement: Configure in consumer

#### PagerDuty Integration

Critical alerts:

- API down
- PostgreSQL down
- Kafka lag > 10s
- Error rate > 1%

---

## Incident Response

### Scenario 1: High Fraud Detection Latency

**Symptoms:** Fraud detection taking >50ms

**Investigation:**

```bash
# Check Redis latency
redis-cli --latency

# Check Kafka lag
kafka-consumer-groups --bootstrap-server localhost:9092 \
  --group fintech-service \
  --describe

# Check PostgreSQL queries
SELECT query, mean_exec_time FROM pg_stat_statements
ORDER BY mean_exec_time DESC LIMIT 10;
```

**Recovery:**

1. Scale fraud detection service (add more instances)
2. Optimize ML model inference (use quantization)
3. Increase Redis cluster size

### Scenario 2: PostgreSQL Connection Pool Exhausted

**Symptoms:** "Too many connections" errors

**Investigation:**

```bash
# Check current connections
psql -c "SELECT count(*) FROM pg_stat_activity;"

# Check idle connections
psql -c "SELECT state, count(*) FROM pg_stat_activity GROUP BY state;"
```

**Recovery:**

1. Increase `DATABASE_POOL_SIZE` in config
2. Kill idle connections: `SELECT pg_terminate_backend(pid) ...`
3. Restart API instances gracefully

### Scenario 3: Kafka Broker Down

**Symptoms:** "Failed to connect to broker"

**Investigation:**

```bash
# Check broker status
kafka-broker-api-versions --bootstrap-server localhost:9092

# Check cluster status
kafka-topics --bootstrap-server localhost:9092 --describe
```

**Recovery:**

1. Kafka auto-failover (if cluster > 1 broker)
2. Restart broker: `docker-compose restart kafka`
3. Verify replication: `kafka-topics --describe`

### Scenario 4: Settlement Failures

**Symptoms:** Transactions not settling (stuck in PENDING)

**Investigation:**

```sql
-- Check pending transactions
SELECT * FROM transactions WHERE status = 'PENDING'
AND created_at < NOW() - INTERVAL '5 minutes';

-- Check settlement errors
SELECT * FROM audit_logs
WHERE entity_type = 'settlement' AND action IN ('error', 'failed')
ORDER BY created_at DESC LIMIT 20;
```

**Recovery:**

1. Check settlement consumer logs
2. Verify PostgreSQL and Redis connectivity
3. Restart settlement consumer
4. Manual settlement for stuck transactions:

```sql
UPDATE transactions SET status = 'APPROVED'
WHERE transaction_id = 'uuid-...' AND status = 'PENDING';
```

---

## Maintenance

### Daily Tasks

- [ ] Check error rate (< 0.1%)
- [ ] Verify Kafka lag (< 1s)
- [ ] Check database backup completion
- [ ] Review fraud detection metrics

### Weekly Tasks

- [ ] Analyze fraud patterns
- [ ] Review PostgreSQL slow queries
- [ ] Update ML model (retrain if drift detected)
- [ ] Test disaster recovery

### Monthly Tasks

- [ ] Database maintenance (VACUUM, ANALYZE)
- [ ] PostgreSQL log rotation
- [ ] Kafka topic retention review
- [ ] PCI-DSS compliance audit

---

## Scaling Guide

### When to Scale

**API Gateway:**

- Scale up when: API latency > 100ms or error rate > 0.5%
- Scale down when: CPU < 20% for 10 minutes

**Fraud Detection:**

- Scale up when: Fraud detection latency > 50ms or Kafka lag > 5s
- Scale down when: CPU < 20% for 10 minutes

**Settlement:**

- Scale up when: Settlement latency > 100ms or PostgreSQL connections > 80%
- Scale down when: CPU < 20% for 10 minutes

### Scaling Commands

```bash
# Scale API gateway
kubectl scale deployment api-gateway --replicas=5

# Scale fraud detection
kubectl scale deployment fraud-detection --replicas=3

# Scale settlement
kubectl scale deployment settlement --replicas=2
```

---

## Disaster Recovery

### Backup Strategy

- **PostgreSQL:** Automated daily backups to S3 (7-day retention)
- **Redis:** RDB snapshots every 6 hours
- **Kafka:** 7-day retention (replay capability)

### Recovery Procedures

**Restore PostgreSQL:**

```bash
# List backups
aws s3 ls s3://fintech-backups/postgres/

# Restore from backup
pg_restore -d fintech_db backup-2026-07-09.sql
```

**Restore Redis:**

```bash
# If using RDB snapshot
redis-server --appendonly yes /path/to/dump.rdb
```

**Replay from Kafka:**

```bash
# Reset consumer group offset
kafka-consumer-groups --bootstrap-server localhost:9092 \
  --group fintech-service \
  --reset-offsets --to-earliest \
  --execute
```

---

## Security Checklist

- [ ] All services use TLS (HTTPS)
- [ ] Database password rotated quarterly
- [ ] Kafka SASL/SSL enabled
- [ ] Redis password set and enforced
- [ ] API authentication enabled (OAuth2)
- [ ] Rate limiting active
- [ ] DDoS protection configured (AWS Shield)
- [ ] Security audit log enabled
- [ ] Secrets stored in HashiCorp Vault

---

## Support & Escalation

### On-Call Runbook

1. **P1 (Critical):** API down, data loss risk
   - Page on-call immediately
   - Check incident response playbook
   - Engage database team

2. **P2 (High):** Service degraded, fraud not detecting
   - Create incident ticket
   - Investigate root cause
   - Implement fix or workaround

3. **P3 (Medium):** Performance degradation
   - Log ticket
   - Analyze metrics
   - Schedule fix

### Escalation Path

Developer → Tech Lead → Manager → VP Engineering
