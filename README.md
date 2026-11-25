# Event-driven payment system

A synthetic fintech engineering prototype exploring asynchronous transaction intake, rule-based fraud screening, heuristic risk scoring and PostgreSQL settlement.

## Architecture

FastAPI → Kafka → fraud consumer → settlement consumer → PostgreSQL. Redis provides experimental feature storage. Docker Compose and Kubernetes manifests are development examples.

## What the prototype demonstrates

- Client-supplied UUID idempotency keys, with payload conflict detection.
- Settlement transaction row locks, duplicate settlement protection and atomic balance/audit updates.
- Decimal amounts and explicit cent rounding for settlement fees.
- Rule-based screening and a simulated second-tier risk score (not a trained ML model).

## Run tests

Python 3.10 and a disposable PostgreSQL database are required.

```bash
pip install -r requirements.txt
export DATABASE_URL=postgresql://test_user:test_password@localhost:5432/test_db
python -c "from src.database import init_db; init_db()"
python -m pytest tests/integration -q
```

Use an empty test database. CI exercises service-level tests; it does not establish full Kafka end-to-end reliability.

## Limitations

Synthetic data only. No real payments or card numbers. No PCI compliance, calibrated fraud accuracy, production security or verified throughput/latency claim. The 100,000 TPS and sub-50 ms figures are design targets only. API/database and Kafka publication are not atomic; a transactional outbox and consumer acknowledgement/retry strategy remain necessary. Settlement is USD-only and does not implement a complete double-entry ledger. Audit records are not tamper-proof. Existing databases require migration review. Do not expose this prototype publicly as a payment service.

See [engineering decisions](docs/ENGINEERING_DECISIONS.md).
