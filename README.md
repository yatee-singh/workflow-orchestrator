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

## Quick start (Phase 0)

```bash
cp .env.example .env
docker compose up --build
```

Then open:

| Service | URL |
|---------|-----|
| API docs | http://localhost:8000/docs |
| Health | http://localhost:8000/health |
| RabbitMQ Management | http://localhost:15672 (guest / guest) |

Expected health response:

```json
{
  "status": "ok",
  "phase": 0,
  "checks": { "postgres": true, "redis": true, "rabbitmq": true }
}
```

Workers log a stub message and wait — real consumers start in Phase 1.

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
