# OrderFlow

Learn **RabbitMQ and message queues** by building an order-fulfillment system with **FastAPI**.

Monorepo layout: one API process + separate worker processes, all talking through RabbitMQ.

## Concepts you will learn (by phase)

| Phase | What you learn |
|------|----------------|
| **0 — Scaffold** (you are here) | Process layout, Docker Compose, health checks for broker/DB/cache |
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

### Phase 2 smoke test (routing)

```bash
# Happy path → status ends as shipped
curl -s -X POST http://localhost:8000/orders \
  -H 'Content-Type: application/json' \
  -d '{"customer_email":"learner@example.com","items":[{"sku":"SKU-TEE-BLK","quantity":1}]}' | jq

# VIP + EU → also triggers headers-based notify alert
curl -s -X POST http://localhost:8000/orders \
  -H 'Content-Type: application/json' \
  -d '{"customer_email":"vip@example.com","items":[{"sku":"SKU-HAT-RED","quantity":1}],"is_vip":true,"region":"eu"}' | jq

docker compose logs -f \
  worker-inventory worker-payment worker-shipping \
  worker-notify worker-analytics worker-audit
```

In RabbitMQ UI → **Exchanges**: inspect `orders.fanout`, `orders.direct`, `orders.topic`, `orders.headers`.

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
