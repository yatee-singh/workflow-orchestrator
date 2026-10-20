# OrderFlow

Learn **RabbitMQ and message queues** by building an order-fulfillment system with **FastAPI**.

Monorepo layout: one API process + separate worker processes, all talking through RabbitMQ.

## Concepts you will learn (by phase)

| Phase | What you learn |
|------|----------------|
| **0 — Scaffold** | Process layout, Docker Compose, health checks for broker/DB/cache |
| **1 — Core** | Producers/consumers, queues, connections vs channels, manual ack, prefetch |
| **2 — Routing** | Fanout, direct, topic, headers exchanges + bindings |
| **3 — Reliability** | Durable queues, persistent messages, publisher confirms, retries, DLQ, idempotency |
| **4 — Advanced** | RPC, priority queues, delayed messages (TTL+DLX), saga cancel/restock |
| **5 — Ops** | Admin APIs, DLQ replay, correlation IDs, demo scripts |

## Architecture (end state)

```
Client → FastAPI API → RabbitMQ → inventory / payment / shipping / notify / analytics workers
                ↘ Postgres
                ↘ Redis (idempotency)
```

## Quick start

```bash
cp .env.example .env
docker compose up --build
docker compose exec api python -m scripts.seed
```

| Service | URL |
|---------|-----|
| API docs | http://localhost:8000/docs |
| Health | http://localhost:8000/health |
| RabbitMQ Management | http://localhost:15672 (guest / guest) |

### Phase 3 smoke test (reliability)

Queue args changed — **stop app containers first**, then recreate the broker (avoids old workers re-declaring queues without DLX):

```bash
docker compose stop api worker-inventory worker-payment worker-shipping \
  worker-notify worker-analytics worker-audit
docker compose up -d --force-recreate rabbitmq
docker compose up -d --scale worker-payment=2 api worker-inventory worker-payment \
  worker-shipping worker-notify worker-analytics worker-audit
```

```bash
# Place several orders; with PAYMENT_FAIL_RATE=0.35 some will retry / DLQ
for i in 1 2 3 4 5; do
  curl -s -X POST http://localhost:8000/orders \
    -H 'Content-Type: application/json' \
    -d "{\"customer_email\":\"u$i@example.com\",\"items\":[{\"sku\":\"SKU-TEE-BLK\",\"quantity\":1}]}" \
    | jq -c '{id:.order.id,status:.order.status}'
done

docker compose logs -f worker-payment worker-notify
```

Watch for: `Scheduled retry`, competing `[hostname]` lines, `Dead-lettered`, `payment FAILED`.
In UI: Queues → `q.delay.1000` / `q.dlq`.

## Repo layout

```
api/           FastAPI application
workers/       Separate consumer processes (one module per domain)
shared/        Config, DB, Redis, messaging helpers
scripts/       Seed and demo scripts (later phases)
docker-compose.yml
```

## Local Python (optional, without Docker for the API only)

```bash
python -m venv .venv
source .venv/bin/activate
pip install -e .
# still need Postgres/Redis/RabbitMQ running (e.g. docker compose up postgres redis rabbitmq)
uvicorn api.main:app --reload
```

## Learning tip

After each phase: hit the API, watch worker logs (`docker compose logs -f worker-inventory`), and inspect queues in the RabbitMQ Management UI (Queues / Exchanges tabs).
