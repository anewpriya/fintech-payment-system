# Fintech Payment System - Operations & Deployment Guide

**Date:** July 9, 2026
**Version:** 2.0
**Audience:** Operations engineers, DevOps engineers, SREs

## System Overview

The Fintech Payment System is a production-grade payment processor with three core services (API Gateway, Fraud Detection, Settlement) running on Kubernetes with automated CI/CD deployment. This guide covers how to deploy, operate, monitor, and troubleshoot the system in production.

## Local Development Setup

### Prerequisites

- Docker & Docker Compose (for local development)
- Python 3.10+ (for running services directly)
- Git (for version control)
- kubectl (for Kubernetes operations)
- AWS CLI (for production deployment)
- curl (for testing API)

### Step 1: Clone Repository and Navigate

```bash
cd 02-fintech-payment-system
```

### Step 2: Start All Services with Docker Compose

Docker Compose provides PostgreSQL, Kafka, Zookeeper, and Redis for local development with one command.

```bash
docker-compose up -d
```

Verify all services are healthy:

```bash
docker-compose ps
```

Expected output shows all services in "healthy" or "running" state:

```
NAME               STATUS
fintech-postgres   healthy
fintech-kafka      healthy
fintech-zookeeper  running
fintech-redis      healthy
```

### Step 3: Create Python Virtual Environment

```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

### Step 4: Install Dependencies

```bash
pip install -r requirements.txt
```

### Step 5: Initialize Database

Create all PostgreSQL tables (transactions, settlements, user_accounts, audit_logs):

```bash
python -c "from src.database import init_db; init_db()"
```

### Step 6: Start API Gateway (Terminal 1)

```bash
python -m uvicorn src.api.main:app --reload --host 0.0.0.0 --port 8000
```

The API starts at http://localhost:8000. Logs show startup messages like:

```
INFO:     Started server process
INFO:     Waiting for application startup.
INFO:     Application startup complete
```

### Step 7: Start Fraud Detection Consumer (Terminal 2)

```bash
python -m src.run_fraud_consumer
```

Expected output:

```
Starting Fraud Detection Service - STARTING
✓ Database initialized
✓ Kafka producer connected
✓ Consumer ready, waiting for transactions...
```

### Step 8: Start Settlement Consumer (Terminal 3)

```bash
python -m src.run_settlement_consumer
```

Expected output:

```
Starting Settlement Service - STARTING
✓ Kafka connection established
✓ Consumer ready, waiting for fraud decisions...
```

### Step 9: Verify Everything Works

```bash
curl http://localhost:8000/health
```

Expected response:

```json
{
  "status": "healthy",
  "timestamp": "2026-07-09T23:59:00",
  "service": "fintech-api",
  "version": "0.1.0"
}
```

## Testing the System Locally

### Create a Transaction

Submit a transaction via the API:

```bash
curl -X POST http://localhost:8000/transactions \
  -H "Content-Type: application/json" \
  -d '{
    "user_id": 12345,
    "merchant_id": 67890,
    "user_email": "alice@example.com",
    "user_country": "US",
    "amount": 100.00,
    "currency": "USD",
    "description": "Coffee purchase",
    "card_last_four": "4242",
    "card_brand": "VISA",
    "card_hash": "abc123def456xyz789",
    "device_fingerprint": "device-xyz-123",
    "ip_address": "192.168.1.1",
    "user_agent": "Mozilla/5.0"
  }'
```

Expected response (202 ACCEPTED):

```json
{
  "transaction_id": "550e8400-e29b-41d4-a716-446655440000",
  "idempotency_key": "660f9511-f30c-52e5-b827-557766551111",
  "status": "PENDING",
  "message": "Transaction received and is being processed",
  "timestamp": "2026-07-09T23:59:00"
}
```

Watch the fraud and settlement consumers process the transaction. In fraud consumer terminal, you'll see:

```
Processing transaction: 550e8400... (user: 12345, amount: $100)
[TIER 1] Checking rules for transaction...
[TIER 2] Running ML model...
Fraud detection complete: APPROVE (score: 35, latency: 48ms)
✓ Published fraud_score
```

In settlement consumer terminal:

```
Processing fraud decision: 550e8400... → APPROVE
Settlement complete: SETTLED
✓ Published settlement
```

### Check Transaction Status

```bash
curl http://localhost:8000/transactions/550e8400-e29b-41d4-a716-446655440000
```

Response:

```json
{
  "transaction_id": "550e8400-e29b-41d4-a716-446655440000",
  "status": "APPROVED",
  "fraud_score": 35.5,
  "fraud_tier": 2,
  "amount": "100.00",
  "created_at": "2026-07-09T23:59:00"
}
```

## Automated Testing

### Running Tests Locally

Before committing, run the full test suite:

```bash
# Unit tests
pytest tests/unit/ -v

# Integration tests (end-to-end transaction flow)
pytest tests/integration/ -v

# With coverage report
pytest tests/ --cov=src --cov-report=html

# Linting (code style)
flake8 src/

# Security scanning
bandit -r src/
```

### Integration Tests Included

The test suite (`tests/integration/test_transaction_flow.py`) covers:

- **Legitimate transaction flow**: Transaction → Fraud detection (APPROVE) → Settlement
- **Fraudulent transaction decline**: Suspicious features → Fraud detection (DECLINE)
- **Idempotency prevention**: Duplicate settlement attempts prevented by unique constraint
- **Insufficient balance handling**: Settlement fails gracefully when user lacks funds
- **Audit logging**: All changes recorded in audit_logs table
- **Tier 1 fraud rules**: Stolen card detection, sanctioned country detection
- **Tier 2 ML scoring**: Spending pattern deviation, device trust, merchant familiarity

### CI/CD Test Execution

When you push to GitHub, GitHub Actions automatically:

1. Spins up test database (PostgreSQL)
2. Spins up test Kafka cluster
3. Spins up test Redis
4. Runs linting (flake8)
5. Runs all unit + integration tests
6. Uploads coverage to Codecov
7. Runs security scans (Bandit, Safety, Semgrep)
8. Builds Docker images
9. Deploys to Kubernetes (if all tests pass)

## Production Deployment to Kubernetes

### Prerequisites for Production

- Kubernetes cluster (AWS EKS, Google GKE, or on-premises)
- kubectl configured and authenticated
- Docker images pushed to registry (automated via CI/CD)
- Kubernetes secrets configured (database credentials, API keys)
- Persistent volumes for PostgreSQL backups (optional, if using StatefulSets)
- Ingress controller installed (for routing external traffic)

### Step 1: Create Kubernetes Namespace

```bash
kubectl create namespace fintech
```

Verify:

```bash
kubectl get namespace fintech
```

### Step 2: Configure Secrets

Create a file `secrets.yaml` with your production credentials:

```yaml
apiVersion: v1
kind: Secret
metadata:
  name: fintech-secrets
  namespace: fintech
type: Opaque
stringData:
  database-url: "postgresql://fintech_prod:STRONG_PASSWORD_HERE@postgres-rds.us-east-1.rds.amazonaws.com:5432/fintech_prod"
  ip-reputation-api-key: "your-maxmind-api-key-here"
  oauth2-client-secret: "your-oauth2-secret-here"
```

Apply the secret:

```bash
kubectl apply -f secrets.yaml
```

Verify:

```bash
kubectl get secrets -n fintech
```

### Step 3: Apply Kubernetes Manifests

Apply all configuration:

```bash
# Apply namespace and configuration (if not already done)
kubectl apply -f infrastructure/k8s/namespace-and-secrets.yaml

# Deploy API Gateway
kubectl apply -f infrastructure/k8s/api-gateway-deployment.yaml

# Deploy Fraud Detection
kubectl apply -f infrastructure/k8s/fraud-detection-deployment.yaml

# Deploy Settlement
kubectl apply -f infrastructure/k8s/settlement-deployment.yaml
```

### Step 4: Verify Deployments

Check deployment status:

```bash
kubectl get deployments -n fintech
```

Expected output:

```
NAME                READY   UP-TO-DATE   AVAILABLE
api-gateway         3/3     3            3
fraud-detection     2/2     2            2
settlement          2/2     2            2
```

Check pods are running:

```bash
kubectl get pods -n fintech
```

Expected output:

```
NAME                           READY   STATUS    RESTARTS
api-gateway-abc123-xyz         1/1     Running   0
api-gateway-def456-uvw         1/1     Running   0
api-gateway-ghi789-rst         1/1     Running   0
fraud-detection-jkl012-pqr     1/1     Running   0
fraud-detection-mno345-opq     1/1     Running   0
settlement-stu678-nop          1/1     Running   0
settlement-vwx901-mnk          1/1     Running   0
```

Wait for rollout completion (up to 5 minutes):

```bash
kubectl rollout status deployment/api-gateway -n fintech --timeout=5m
kubectl rollout status deployment/fraud-detection -n fintech --timeout=5m
kubectl rollout status deployment/settlement -n fintech --timeout=5m
```

### Step 5: Expose API to External Traffic

Get the LoadBalancer external IP:

```bash
kubectl get service api-gateway-service -n fintech
```

Expected output:

```
NAME                       TYPE           CLUSTER-IP      EXTERNAL-IP
api-gateway-service        LoadBalancer   10.0.0.100      a1b2c3d4.elb.us-east-1.amazonaws.com
```

### Step 6: Test Production Deployment

From your local machine (or from a pod in the cluster):

```bash
curl http://a1b2c3d4.elb.us-east-1.amazonaws.com/health
```

Expected response:

```json
{
  "status": "healthy",
  "timestamp": "2026-07-09T23:59:00",
  "service": "fintech-api",
  "version": "0.1.0"
}
```

### Step 7: Configure DNS

Add DNS CNAME record pointing to LoadBalancer DNS:

```
CNAME: api.fintech.example.com → a1b2c3d4.elb.us-east-1.amazonaws.com
```

Now traffic flows: users → api.fintech.example.com → AWS ALB → LoadBalancer → API Gateway pods

## Docker Build & Push

### Building Docker Images Locally

Build the application image:

```bash
docker build -f infrastructure/docker/Dockerfile -t fintech/api-gateway:latest .
```

Run container locally to test:

```bash
docker run \
  -e DATABASE_URL="postgresql://user:pass@postgres:5432/fintech" \
  -e KAFKA_BROKERS="kafka:9092" \
  -e REDIS_URL="redis://redis:6379/0" \
  -p 8000:8000 \
  fintech/api-gateway:latest
```

Test from another terminal:

```bash
curl http://localhost:8000/health
```

### Pushing to Docker Registry

Tag image for Docker Hub:

```bash
docker tag fintech/api-gateway:latest anewpriya/fintech-api-gateway:latest
docker tag fintech/api-gateway:latest anewpriya/fintech-api-gateway:$(git rev-parse --short HEAD)
```

Push to registry:

```bash
docker login  # Enter Docker Hub credentials
docker push anewpriya/fintech-api-gateway:latest
docker push anewpriya/fintech-api-gateway:$(git rev-parse --short HEAD)
```

**Note**: This is done automatically by GitHub Actions CI/CD on every push to main.

## Continuous Integration & Deployment (CI/CD)

### GitHub Actions Workflow Overview

File: `.github/workflows/ci-cd.yml`

The workflow runs automatically on every push to the main branch:

**Stage 1: Testing**

- Spins up test PostgreSQL (docker service)
- Spins up test Kafka and Zookeeper
- Spins up test Redis
- Installs Python dependencies
- Runs flake8 linting
- Runs pytest unit tests
- Runs pytest integration tests
- Uploads coverage to Codecov

If any test fails, pipeline stops (doesn't proceed to build/deploy).

**Stage 2: Security Scanning**

- Bandit: Scans Python code for security vulnerabilities
- Safety: Checks requirements.txt for known CVEs
- Semgrep: Static analysis against OWASP Top 10

**Stage 3: Build** (only if tests + security pass, and on main branch)

- Builds Docker image with multi-stage build
- Tags image: `fintech/api-gateway:latest` and `fintech/api-gateway:<SHA>`
- Pushes to Docker registry (Docker Hub)
- Uses Docker BuildKit for faster builds and layer caching

**Stage 4: Deploy** (only if build succeeds, and on main branch)

- Authenticates with Kubernetes cluster (kubeconfig from GitHub Secrets)
- Creates fintech namespace
- Applies Kubernetes manifests (deployments, services, HPA)
- Waits for rollout completion (max 5 minutes)
- Runs smoke test: `curl http://api-gateway:8000/health`
- Fails if smoke test doesn't pass (rollback triggered)

**Stage 5: Notify**

- Sends Slack notification with pipeline status
- Includes commit SHA, branch, and success/failure

### Configuring CI/CD Secrets

Add these secrets to GitHub repository:

**Settings → Secrets and variables → Actions → New repository secret**

```
DOCKER_USERNAME = your-docker-hub-username
DOCKER_PASSWORD = your-docker-hub-personal-access-token
KUBE_CONFIG = base64-encoded kubeconfig file
SLACK_WEBHOOK_URL = https://hooks.slack.com/services/YOUR/WEBHOOK/URL
```

**Encoding kubeconfig**:

```bash
cat ~/.kube/config | base64 -w 0 | pbcopy  # macOS
# OR
cat ~/.kube/config | base64 -w 0 > kube_config_b64.txt  # Linux/Windows
```

Copy the base64 output and paste into GitHub Secrets as KUBE_CONFIG.

### Monitoring Pipeline Execution

View pipeline status in GitHub:

```
Repository → Actions → CI/CD Pipeline
```

Click on a workflow run to see detailed logs.

### Troubleshooting Failed Builds

**If tests fail**:

- Click on failed job → see test output
- Run tests locally: `pytest tests/ -v`
- Fix code locally, push again

**If security scan fails**:

- Review Bandit output for flagged code
- Fix security issues or add exceptions if false positive
- Re-push

**If deployment fails**:

- Check kubeconfig validity: `kubectl cluster-info`
- Verify Kubernetes cluster is accessible
- Check pod logs: `kubectl logs -n fintech <pod-name>`

## Monitoring & Observability

### Health Check Endpoints

All services expose health check endpoints used by Kubernetes:

**API Gateway**:

```bash
curl http://localhost:8000/health
```

Fraud Detection & Settlement: Kubernetes checks via liveness probes (automatic).

### Kubernetes Liveness Probes

Each pod has a liveness probe that checks pod health:

```yaml
livenessProbe:
  httpGet:
    path: /health
    port: 8000
  initialDelaySeconds: 10
  periodSeconds: 10
  failureThreshold: 3
```

If health check fails 3 times consecutively, Kubernetes restarts the pod automatically.

### Prometheus Metrics

Metrics endpoint for Prometheus scraping:

```bash
curl http://localhost:8000/metrics
```

**Production Monitoring Setup**:

1. Deploy Prometheus (scrapes metrics every 30 seconds)
2. Deploy Grafana (visualizes metrics)
3. Configure alerting rules in Prometheus

**Key metrics to monitor**:

- API latency (p50, p95, p99)
- Request rate (per second)
- Error rate (4xx, 5xx)
- Pod CPU/memory usage
- Database connection pool usage
- Kafka consumer lag

### Logging Strategy

All services log to stdout (captured by Kubernetes):

```bash
# View API Gateway logs
kubectl logs deployment/api-gateway -n fintech

# View Fraud Detection logs
kubectl logs deployment/fraud-detection -n fintech

# View Settlement logs
kubectl logs deployment/settlement -n fintech

# Stream logs live
kubectl logs -f deployment/api-gateway -n fintech

# View logs from specific pod
kubectl logs pod/api-gateway-abc123-xyz -n fintech
```

**Log Levels**:

- INFO: Normal operations
- WARNING: Anomalies (fraud flagged, settlement retried)
- ERROR: Failures (database connection lost, Kafka unavailable)

**Production Log Aggregation**:
Use CloudWatch (AWS) or ELK Stack (self-hosted):

1. Fluentd/Fluent-bit → collects logs from all pods
2. Sends to CloudWatch Logs (or Elasticsearch)
3. View/search logs in CloudWatch console

## Incident Response

### Scenario 1: High Fraud Detection Latency (>75ms)

**Symptoms**:

- Fraud detection latency increased from <50ms to >75ms
- Alert fires: "Fraud detection p99 > 75ms"

**Investigation**:

```bash
# Check pod resource usage
kubectl top pods -n fintech

# Check Redis latency
redis-cli -h redis-cluster --latency

# Check Kafka lag
kafka-consumer-groups --bootstrap-server kafka:9092 \
  --group fintech-fraud-detection \
  --describe
```

**Possible Causes**:

- Redis node down or slow → Check Redis cluster health
- High CPU usage → Model inference bottleneck
- Kafka lag high → Consumer falling behind
- Network latency spike → Check AWS VPC metrics

**Recovery Steps**:

1. Scale fraud detection: `kubectl scale deployment fraud-detection -n fintech --replicas=6`
2. Monitor latency: `kubectl logs -f deployment/fraud-detection -n fintech`
3. Once latency returns to normal, scale down if needed
4. Post-incident: Profile ML model, optimize features

### Scenario 2: PostgreSQL Connection Pool Exhausted

**Symptoms**:

- API returns 500 errors
- Error logs: "could not connect to server... all connection slots reserved"

**Investigation**:

```bash
# Check connection count
psql -h postgres-rds.us-east-1.rds.amazonaws.com -U postgres -c \
  "SELECT count(*) FROM pg_stat_activity;"

# Check connections per pod
psql -h postgres-rds -U postgres -c \
  "SELECT datname, state, count(*) FROM pg_stat_activity GROUP BY datname, state;"

# Check Kubernetes pods connecting to database
kubectl get pods -n fintech -o wide
```

**Possible Causes**:

- Connection pool not releasing connections (connection leak)
- Increased traffic requiring more connections
- Database query hanging (deadlock)

**Recovery Steps**:

1. Increase connection pool: Update DATABASE_POOL_SIZE in config
2. Restart API pods to reconnect: `kubectl rollout restart deployment/api-gateway -n fintech`
3. Kill idle connections if needed:
   ```bash
   psql -h postgres-rds -U postgres -c \
     "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE state='idle' AND query_start < NOW() - INTERVAL '10 minutes';"
   ```
4. Monitor connections: `watch "psql -h postgres-rds -U postgres -c 'SELECT count(*) FROM pg_stat_activity;'"`

### Scenario 3: Kafka Broker Down (in 3-broker cluster)

**Symptoms**:

- Fraud detection consumer lag increasing
- Error logs: "LEADER_NOT_AVAILABLE"

**Investigation**:

```bash
# Check broker status
kafka-broker-api-versions --bootstrap-server kafka-1:9092

# Check cluster metadata
kafka-metadata --snapshot /var/kafka/metadata/cluster/00000000000000000000.log

# Check partition leadership
kafka-topics --bootstrap-server kafka:9092 --describe --topic transactions
```

**Expected Behavior**:

- Kafka cluster detects broker failure
- Rebalances partitions to healthy brokers
- No data loss (replication factor 3)
- Consumer groups rebalance automatically

**Recovery Time**: 30-60 seconds (automatic)

**Action**: Monitor but typically no manual intervention needed.

### Scenario 4: Settlement Service Crashes

**Symptoms**:

- Settlement pod shows CrashLoopBackOff
- Transactions stuck in PENDING status

**Investigation**:

```bash
# Check pod status
kubectl get pods -n fintech -l app=settlement

# View pod events
kubectl describe pod <settlement-pod-name> -n fintech

# Check logs
kubectl logs <settlement-pod-name> -n fintech
```

**Possible Causes**:

- Database connection error
- Kafka consumer group error
- Out of memory (OOM)

**Recovery Steps**:

1. Check logs for error
2. Verify database connectivity: `kubectl run -it --rm debug --image=postgres:16 --restart=Never -- psql -h <db-host> -U <user>`
3. Restart settlement pod: `kubectl delete pod <settlement-pod-name> -n fintech`
4. HPA will spawn new pod automatically
5. Consumer group rebalances, new pod takes over processing

### Scenario 5: Out of Disk Space on Node

**Symptoms**:

- Pods evicted from node
- Error logs: "disk-pressure" condition

**Investigation**:

```bash
# Check node disk usage
kubectl describe node <node-name>

# Check disk usage on node
kubectl exec -it <pod-name> -n fintech -- df -h

# Find large files
kubectl exec -it <pod-name> -n fintech -- du -sh /tmp/*
```

**Recovery Steps**:

1. SSH to node: `aws ec2-instance-connect send-ssh-public-key ...`
2. Clean up: `sudo rm -rf /var/log/pods/*`
3. Restart kubelet: `sudo systemctl restart kubelet`
4. Monitor: `kubectl top nodes`

## Scaling Procedures

### Manual Scaling

Scale a deployment immediately:

```bash
# Scale to 5 replicas
kubectl scale deployment api-gateway -n fintech --replicas=5

# Check status
kubectl get deployment api-gateway -n fintech
```

### Automatic Scaling (HPA)

Horizontal Pod Autoscaler automatically scales based on metrics:

**API Gateway** (3-10 replicas):

```bash
kubectl get hpa api-gateway-hpa -n fintech
```

Check current metrics:

```bash
kubectl top pods -n fintech -l app=api-gateway
```

**Manual tuning of HPA**:

```bash
kubectl patch hpa api-gateway-hpa -n fintech -p '{"spec":{"minReplicas":5,"maxReplicas":15}}'
```

### Scaling Down Safely

When scaling down, Kubernetes gracefully terminates pods:

```bash
# Terminates pods, gives them 30 seconds to finish requests
# (preStop hook defined in deployment)
# Then sends SIGTERM and waits up to terminationGracePeriodSeconds
kubectl scale deployment api-gateway -n fintech --replicas=2
```

Pods in-flight transactions complete before termination.

## Maintenance Procedures

### Database Backups

**Automated Backups**:
RDS takes automated snapshots daily (configured in RDS).

**Manual Backup**:

```bash
aws rds create-db-snapshot \
  --db-instance-identifier fintech-postgres \
  --db-snapshot-identifier fintech-postgres-manual-$(date +%Y%m%d)
```

**7-Year Retention** (PCI-DSS requirement):
All snapshots stored in S3 with Glacier transition after 30 days.

### Updating Code (Rolling Deployment)

When you push to main branch, GitHub Actions:

1. Tests code
2. Builds Docker image
3. Pushes to registry
4. Deploys to Kubernetes

**Manual deployment** (if needed):

```bash
# Update image tag in deployment
kubectl set image deployment/api-gateway \
  api-gateway=fintech/api-gateway:v2.0.0 \
  -n fintech

# Monitor rollout
kubectl rollout status deployment/api-gateway -n fintech

# Rollback if needed
kubectl rollout undo deployment/api-gateway -n fintech
```

### Upgrading Dependencies

Update requirements.txt with new versions:

```bash
# Update all packages
pip install --upgrade -r requirements.txt

# Or specific package
pip install --upgrade FastAPI==0.105.0

# Update requirements.txt
pip freeze > requirements.txt
```

Commit and push. GitHub Actions tests new dependencies before deploying.

### Kubernetes Version Upgrades

EKS handles control plane upgrades automatically. For node upgrades:

```bash
# Cordon node (no new pods scheduled)
kubectl cordon <node-name>

# Drain node (move existing pods to other nodes)
kubectl drain <node-name> --ignore-daemonsets --delete-emptydir-data

# Upgrade node (in AWS console or via eksctl)
# Node comes back online automatically
```

## Disaster Recovery

### Scenario: PostgreSQL Data Corruption

**Recovery from Snapshot**:

```bash
# List snapshots
aws rds describe-db-snapshots --db-instance-identifier fintech-postgres

# Restore to new instance
aws rds restore-db-instance-from-db-snapshot \
  --db-instance-identifier fintech-postgres-restore \
  --db-snapshot-identifier fintech-postgres-manual-20260708

# Wait for restoration (5-10 minutes)
aws rds wait db-instance-available \
  --db-instance-identifiers fintech-postgres-restore

# Verify data integrity
psql -h <new-instance-endpoint> -U postgres -c "SELECT COUNT(*) FROM transactions;"

# Update connection string to point to restored instance
kubectl set env deployment/api-gateway \
  DATABASE_URL=postgresql://user:pass@restored-instance:5432/fintech \
  -n fintech
```

### Scenario: Entire Kubernetes Cluster Down

**Recovery Procedure**:

1. Spin up new Kubernetes cluster (AWS EKS)
2. Apply Kubernetes manifests from git
3. Configure secrets (from AWS Secrets Manager)
4. Deploy services
5. Point LoadBalancer to new cluster

Estimated recovery time: 20-30 minutes (mostly waiting for infrastructure).

## Maintenance Schedule

**Daily**:

- Check error logs for patterns
- Verify Kafka lag < 5 seconds
- Confirm all pods running
- Verify database backups completed

**Weekly**:

- Review fraud metrics (detection rate, false positives)
- Audit access logs (RBAC)
- Test disaster recovery procedures
- Update security patches

**Monthly**:

- Database maintenance (VACUUM, ANALYZE)
- Review and optimize slow queries
- Rotate credentials (API keys, DB passwords)
- Conduct capacity planning
- Update dependencies

**Quarterly**:

- Load test with peak traffic simulation
- Chaos engineering: intentionally break things to test recovery
- Security audit
- Compliance review (PCI-DSS)

## Troubleshooting Guide

### Pod Stuck in Pending

```bash
# Check why pod not scheduled
kubectl describe pod <pod-name> -n fintech

# Common causes:
# 1. Insufficient resources: Scale nodes or reduce resource requests
# 2. Node affinity: Check pod affinity rules
# 3. PVC not bound: Check PersistentVolumeClaim status
```

### High Latency

```bash
# Check pod CPU/memory
kubectl top pods -n fintech

# Check network latency to database
kubectl exec -it <pod-name> -n fintech -- ping -c 3 postgres-rds.us-east-1.rds.amazonaws.com

# Check database slow queries
psql -h postgres-rds -U postgres -c "SELECT query, mean_exec_time FROM pg_stat_statements ORDER BY mean_exec_time DESC LIMIT 10;"
```

### Pod Restart Loop

```bash
# Check pod logs
kubectl logs <pod-name> -n fintech --previous  # See previous crash logs

# Check recent events
kubectl describe pod <pod-name> -n fintech

# Common causes:
# 1. Liveness probe failing
# 2. Out of memory (OOM)
# 3. Application crashing
```

## Performance Tuning

### PostgreSQL Connection Pool Tuning

```bash
# Current pool settings
echo $DATABASE_POOL_SIZE  # Requests
echo $DATABASE_MAX_OVERFLOW  # Extra connections

# Recommendations:
# - Small cluster (3 pods): pool_size=10, max_overflow=5
# - Large cluster (20 pods): pool_size=20, max_overflow=10
# - Per pod: allocate 1-2 connections per potential concurrent request
```

### Redis Memory Optimization

```bash
# Check memory usage
redis-cli -h redis-cluster INFO memory

# Current memory limit: 6GB
# If exceeding 80%, add more nodes or evict old data

# Monitor key expiration
redis-cli -h redis-cluster DBSIZE  # Total keys
redis-cli -h redis-cluster SCAN 0 COUNT 100  # Sample keys with TTL
```

### Kafka Topic Optimization

```bash
# Check topic lag
kafka-consumer-groups --bootstrap-server kafka:9092 \
  --group fintech-fraud-detection \
  --describe

# If lag > 5s, increase parallelism:
# 1. Increase consumer instances (HPA)
# 2. Increase topic partitions (if needed)
```

## References

### Official Documentation

- Kubernetes: https://kubernetes.io/docs/
- Docker: https://docs.docker.com/
- PostgreSQL: https://www.postgresql.org/docs/
- Kafka: https://kafka.apache.org/documentation/
- GitHub Actions: https://docs.github.com/en/actions

### Tools

- kubectl: Kubernetes command-line interface
- psql: PostgreSQL command-line client
- redis-cli: Redis command-line client
- kube-ps1: Kubernetes prompt helper (shows current context)
