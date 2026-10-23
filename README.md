# OrderFlow

Learn **RabbitMQ and message queues** by building an order-fulfillment system with **FastAPI**.

Monorepo layout: one API process + separate worker processes, all talking through RabbitMQ.

---

## Contents

- [Concepts by phase](#concepts-you-will-learn-by-phase)
- [Architecture](#architecture)
  - [Services](#services)
  - [Topology: exchanges, queues, bindings](#topology-exchanges-queues-bindings)
  - [Order lifecycle](#order-lifecycle)
  - [Message envelope](#message-envelope)
  - [Repo layout](#repo-layout)
- [Phase-by-phase concepts](#phase-by-phase-concepts)
- [Quick start](#quick-start)
- [Smoke tests](#smoke-tests)
- [Glossary](#glossary)

---

## Concepts you will learn (by phase)

| Phase | What you learn |
|------|----------------|
| **0 — Scaffold** | Process layout, Docker Compose, health checks for broker/DB/cache |
| **1 — Core** | Producers/consumers, queues, connections vs channels, manual ack, prefetch |
| **2 — Routing** | Fanout, direct, topic, headers exchanges + bindings |
| **3 — Reliability** | Durable queues, persistent messages, publisher confirms, retries, DLQ, idempotency |
| **4 — Advanced** | RPC (request-reply), priority queues, delayed messages (TTL+DLX), saga cancel/restock |
| **5 — Ops** | Admin APIs, DLQ replay, correlation IDs, demo scripts |

---

## Architecture

```
                        ┌──────────────────────────────────────────────┐
                        │                 RabbitMQ                     │
   POST /orders         │  exchanges: fanout / direct / topic /        │
 ┌──────────┐  publish  │  headers / delay / dlx / rpc                 │
 │ FastAPI  │──────────►│  queues: inventory, payment, shipping,       │
 │  (API)   │           │  notify, analytics, audit, dlq, reminder,    │
 └────┬─────┘           │  delay.1000/5000/25000, inventory.rpc        │
      │                 └───────┬──────┬──────┬──────┬──────┬─────────┘
      │ write/read              │      │      │      │      │
      ▼                         ▼      ▼      ▼      ▼      ▼
 ┌─────────┐            ┌──────────┐ ┌──────────┐ ┌──────────┐ ┌──────────┐ ┌─────────┐
 │ Postgres│            │ inventory│ │ payment  │ │ shipping │ │ notify   │ │analytics│
 │ (orders,│            │  worker  │ │ (×2 pts) │ │  worker  │ │  worker  │ │ + audit │
 │  stock) │            └──────────┘ └──────────┘ └──────────┘ └──────────┘ └─────────┘
 └─────────┘                 ▲           │
      ▲                      │    ┌──────▼─────┐
      │      reads stock     │    │   Redis    │  idempotency keys
      └──────────────────────┘    └────────────┘  (payment:done:{order_id})
```

Everything is **event-driven**: the API never calls a worker directly. It persists the order and publishes an event; whoever is bound to that routing key picks it up. Workers never talk to each other either — they publish follow-up events and let the broker fan them out.

### Services

| Service | Process | Role |
|---------|---------|------|
| `postgres` | DB | Orders, order items, inventory stock |
| `redis` | Cache | Idempotency keys so retries never double-charge |
| `rabbitmq` | Broker | All messaging; management UI on `:15672` |
| `api` | `uvicorn api.main:app` | Producers + RPC client; confirms-enabled channel |
| `worker-inventory` | `workers.inventory` | Reserves stock, restocks on cancel, **serves the RPC queue** |
| `worker-payment` | `workers.payment` | Charges card (flaky, retries, DLQ); run ×2 for competing consumers |
| `worker-shipping` | `workers.shipping` | On `payment.succeeded` → generate tracking, mark shipped |
| `worker-notify` | `workers.notify` | Customer-facing "emails" (logs): payment/shipment/VIP/reminders |
| `worker-analytics` | `workers.analytics` | Counts every event (log-only metrics) |
| `worker-audit` | `workers.audit` | Compliance mirror of `order.placed` |

### Topology: exchanges, queues, bindings

Declared idempotently by **every** process at startup from one source of truth: `shared/messaging/topology.py`.

**Exchanges** (the routers):

| Exchange | Type | What it routes |
|----------|------|----------------|
| `orders.fanout` | fanout | `order.placed` → **every** bound queue (inventory, analytics, audit) |
| `orders.direct` | direct | Step handoffs: `payment.charge`, `shipping.create`, `inventory.reserve`, `notify.reminder` |
| `orders.topic` | topic | Wildcard events: `payment.*`, `shipment.*`, `inventory.*`, `order.cancelled` |
| `orders.headers` | headers | Match on message headers, ignoring routing keys (`priority=high` AND `region=eu`) |
| `orders.delay` | direct | Holds messages for a TTL, then dead-letters them onward |
| `orders.dlx` | topic | Dead-letter parking lot → `q.dlq` |
| `orders.rpc` | direct | Request channel for the availability RPC |

**Queues** (the mailboxes) and their bindings:

| Queue | Bound to | Routing keys |
|-------|----------|--------------|
| `q.inventory` | fanout, direct, topic | (fanout), `inventory.reserve`, `order.cancelled` |
| `q.payment` | direct | `payment.charge` — declared with `x-max-priority: 10` |
| `q.shipping` | direct, topic | `shipping.create`, `payment.succeeded` |
| `q.notify` | topic, direct, headers | `payment.*`, `shipment.*`, `inventory.reservation_failed`, `notify.reminder`, headers `priority=high`+`region=eu` |
| `q.analytics` | fanout, topic | (fanout), `payment.*`, `shipment.*`, `inventory.*` |
| `q.audit` | fanout | (fanout) |
| `q.dlq` | `orders.dlx` `#` | everything dead-lettered |
| `q.delay.1000/5000/25000` | `orders.delay` | TTL → dead-letter back to `payment.charge` (retry ladder) |
| `q.reminder` | `orders.delay` | TTL 60s → dead-letter to `notify.reminder` |
| `q.inventory.rpc` | `orders.rpc` | `inventory.availability` |

### Order lifecycle

```
pending ──► reserved ──► paid ──► shipped            (happy path)
   │            │          │
   │            │          └──► payment_failed       (retries exhausted)
   └──► reservation_failed (not enough stock)
```

Happy path, message by message:

1. `POST /orders` → row written to Postgres, **`order.placed`** published to fanout
2. **inventory** reserves stock (row locks), status → `reserved`, publishes **`payment.charge`** (direct)
3. **payment** charges (with simulated flakiness), status → `paid`, publishes **`payment.succeeded`** (topic)
4. **shipping** generates tracking, status → `shipped`, publishes **`shipment.created`** (topic)
5. **notify / analytics / audit** observe the events in parallel and log

Failure paths:

- Not enough stock → `inventory.reservation_failed` (topic) → notify emails the customer
- Payment fails transiently → message parked in `q.delay.1000` → 1s → `q.delay.5000` → 5s → `q.delay.25000` → 25s, then DLQ + **`payment.failed`** + **`order.cancelled`**
- `order.cancelled` → **inventory restocks** (saga compensation) and notify informs the customer

### Message envelope

Every message body is the same JSON shape (`shared/messaging/publisher.py`):

```json
{
  "event_id": "uuid",          // unique per publish (also AMQP message_id)
  "correlation_id": "uuid",    // one id threads the whole order saga
  "order_id": "uuid",          // ties the event back to a DB row
  "type": "payment.succeeded",
  "occurred_at": "ISO-8601",
  "payload": { "order_id": "...", "items": [...], "total_cents": 4999 }
}
```

`correlation_id` is generated once at `POST /orders` and copied into every downstream event, so you can grep one value across all worker logs to reconstruct a single order's journey.

### Repo layout

```
api/           FastAPI application (producers, RPC client, admin endpoints)
workers/       Separate consumer processes (one module per domain)
  base.py        shared bootstrap: connect → declare topology → consume → graceful stop
shared/
  config.py      pydantic-settings (.env)
  db.py          async SQLAlchemy engine/session
  models.py      Order / OrderItem / InventoryItem
  idempotency.py Redis idempotency keys
  messaging/
    topology.py  single source of truth: exchanges, queues, bindings
    connection.py singleton robust connection + publisher-confirms channel
    publisher.py envelope builder + publish_event helper
    retry.py     TTL retry ladder + DLQ dispatch
    rpc.py       RPC client/server (reply_to + correlation_id)
scripts/       seed catalog
docker-compose.yml
```

---

## Phase-by-phase concepts

### Phase 0 — Scaffold

- **Process layout**: the app is not one process. API (producers) and workers (consumers) are separate containers so you learn to reason about *independent lifecycles* — a worker can crash, scale, or redeploy without touching the API.
- **Docker Compose**: orchestrates postgres/redis/rabbitmq with healthchecks; depends_on uses `condition: service_healthy` so the app never races an unready broker.
- **Health checks**: `GET /health` pings all three dependencies — the pattern you need before a load balancer will trust an instance.
- **Config via `.env`**: `pydantic-settings` reads `PAYMENT_FAIL_RATE`, `MAX_RETRIES`, URLs. All chaos knobs for later phases live here.

### Phase 1 — Core messaging

- **Producer / consumer**: API *publishes*, workers *consume*. Neither knows about the other's existence.
- **Connection vs channel**: one TCP **connection** per process (expensive, authenticated); many lightweight **channels** multiplexed over it (one per consumer/purpose). Never share channels across threads — channels are not thread-safe.
- **Queues as buffers**: if all workers are down, messages wait in the queue instead of being lost. Postgres and RabbitMQ together give you *state* + *in-flight work*.
- **Manual ack**: `async with message.process(requeue=True)` acks only after the handler succeeds. Kill a worker mid-task and the message is **unacked → redelivered** to another consumer. This is at-least-once delivery — hence idempotency later.
- **Prefetch (QoS)**: `set_qos(prefetch_count=1)` caps in-flight messages per consumer so one slow task doesn't hoard the whole queue.

### Phase 2 — Routing (exchange types)

An **exchange** receives a published message and decides which queues get it, based on routing key *or* headers. Binding = the rule connecting an exchange to a queue.

- **Fanout** (`orders.fanout`): ignores the routing key, copies to every bound queue. Used for `order.placed` — inventory must reserve, analytics must count, audit must log; three independent reactions to one event.
- **Direct** (`orders.direct`): exact routing-key match. Used for step handoffs (`payment.charge` → only `q.payment`) where exactly one consumer should get the message.
- **Topic** (`orders.topic`): wildcard matching with `*` (one word) and `#` (zero or more words). `payment.*` catches `payment.succeeded` *and* `payment.failed` with one binding — new events under a family are picked up for free.
- **Headers** (`orders.headers`): routes on message headers with `x-match: all/any`. Used for VIP+EU alerts — routing logic is data, not key naming conventions.
- **Competing consumers**: two `worker-payment` replicas both consume `q.payment`; the broker round-robins. Horizontal scale by adding replicas, zero code changes.

### Phase 3 — Reliability

The realistic failure story: brokers drop connections, networks blip, gateways time out, and consumers crash.

- **Durable queues + persistent messages**: queue survives a broker restart; `DeliveryMode.PERSISTENT` marks messages to be reloaded from disk. Durable queue without persistent messages (or vice versa) is a footgun — you need both.
- **Publisher confirms**: `set_confirm_delivery()` makes each `exchange.publish()` wait for the broker's `basic.ack`. The API now *knows* the message was persisted before returning `201` — closing the gap between "DB commit" and "message accepted".
- **Retries with backoff (TTL + DLX)**: on transient failure, the message is acked off the work queue and republished to `q.delay.1000`. The queue's `x-message-ttl` + `x-dead-letter-exchange` forward it back to `payment.charge` after 1s, then 5s, then 25s. Attempts tracked in the `x-retry-count` header. *Message TTL is per-queue, so one TTL queue per step in the ladder.*
- **Dead-letter queue**: after `MAX_RETRIES`, `publish_to_dlq` parks the message on `q.dlq` with `x-last-error` for inspection — poison messages never block the work queue.
- **Idempotency (Redis)**: because delivery is at-least-once, a redelivered `payment.charge` must not charge twice. `SET payment:done:{order_id} EX 86400` before processing; skip if the key exists. The database status check (`paid`/`payment_failed`) is a second, belt-and-braces guard.
- **Chaos testing**: `PAYMENT_FAIL_RATE=0.35` makes 35% of charges fail so you can watch retries, competing consumers, and DLQ behavior on demand.

### Phase 4 — Advanced patterns

- **RPC (request-reply)**: request/response over an async broker. The API creates an **exclusive callback queue**, publishes with `reply_to` (where to answer) + `correlation_id` (which call this is), the inventory worker queries Postgres and answers on the **default exchange** (`routing_key = reply_to` — every queue is bound to `""` by its own name). The caller rebuilds *synchronous* semantics with an `asyncio.Future` + `wait_for(10s)`; timeout ⇒ `503`. See `shared/messaging/rpc.py`, endpoint `GET /orders/inventory/{sku}/availability`.
- **Priority queues**: `q.payment` is declared with `x-max-priority: 10`. Messages carrying a `priority` header are delivered in descending priority first (FIFO within a level). `POST /orders {"priority": 8}` puts express orders ahead of normal ones. Caveat: RabbitMQ uses a bucketed heuristic at high queue depths — it's not a strict total order under load.
- **Delayed messages (TTL + DLX)**: `POST /orders/{id}/remind` publishes to `q.reminder`, which holds the message 60s (`x-message-ttl`) then dead-letters it to `notify.reminder`. Same trick as retries, applied to *scheduling* — a "send later" primitive with no plugin required. (The `rabbitmq_delayed_message_exchange` plugin is the alternative that supports per-message TTL.)
- **Saga choreography compensation**: there is no orchestrator. Each service reacts to events: when payment exhausts its retries it publishes `payment.failed` **and** `order.cancelled` (topic); inventory — which already bound to `order.cancelled` — puts the reserved stock back. Each step's *compensation* is just another event handler, so rollback logic is decentralized and independently deployable.
- **Dual consumers in one process**: the inventory container runs both the main consumer and the RPC server via `asyncio.gather`, sharing one connection and one stop event (`close_rmq_on_stop=False` on the second — closing a shared connection twice is the classic bug here).

### Phase 5 — Ops (planned)

- **Admin APIs**: inspect queues, consumer counts, message rates from FastAPI.
- **DLQ replay**: re-publish parked messages back onto their work queue after fixing the root cause.
- **Correlation logging**: one middleware/log filter that stamps `correlation_id` everywhere, so `grep <corr-id>` reconstructs a request across all containers.
- **Demo/seed scripts**: reproducible chaos scenarios for demos.

---

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

## Smoke tests

### Phase 3 — reliability

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

### Phase 4 — advanced

Drain all queues for a clean topology (recreate broker):

```bash
docker compose stop api worker-inventory worker-payment worker-shipping \
  worker-notify worker-analytics worker-audit
docker compose up -d --force-recreate rabbitmq
docker compose up -d --scale worker-payment=2 --build api worker-inventory worker-payment \
  worker-shipping worker-notify worker-analytics worker-audit
docker compose exec api python -m scripts.seed
```

**RPC availability check** — call before you order:

```bash
curl -s http://localhost:8000/orders/inventory/SKU-TEE-BLK/availability | jq
```

**Priority ordering** — higher numbers jump the payment queue:

```bash
curl -s -X POST http://localhost:8000/orders \
  -H 'Content-Type: application/json' \
  -d '{"customer_email":"express@example.com","items":[{"sku":"SKU-MUG-WHT","quantity":1}],"priority":8}' | jq
```

**Delayed reminder** — schedule and wait ~60s for the notify:

```bash
OID=$(curl -s -X POST http://localhost:8000/orders \
  -H 'Content-Type: application/json' \
  -d '{"customer_email":"forgetful@example.com","items":[{"sku":"SKU-HAT-RED","quantity":1}]}' | python3 -c 'import sys,json; print(json.load(sys.stdin)["order"]["id"])')
echo "order=$OID"
curl -s -X POST http://localhost:8000/orders/$OID/remind
sleep 65
docker compose logs --tail=5 worker-notify
# look for "REMINDER EMAIL"
```

**Saga cancel/restock** — set `PAYMENT_FAIL_RATE=1.0` and `MAX_RETRIES=1` in `.env`, then:

```bash
docker compose up -d --scale worker-payment=2 --force-recreate worker-payment
OID2=$(curl -s -X POST http://localhost:8000/orders \
  -H 'Content-Type: application/json' \
  -d '{"customer_email":"saga@example.com","items":[{"sku":"SKU-TEE-BLK","quantity":2}]}' | jq -r '.order.id')
sleep 8
docker compose logs --tail=5 worker-inventory | grep -i 'restock\|cancelled'
# stock was restored
```

## Local Python (optional, without Docker for the API only)

```bash
python -m venv .venv
source .venv/bin/activate
pip install -e .
# still need Postgres/Redis/RabbitMQ running (e.g. docker compose up postgres redis rabbitmq)
uvicorn api.main:app --reload
```

---

## Glossary

| Term | Meaning |
|------|---------|
| **Broker** | RabbitMQ itself — the post office between processes |
| **Connection / Channel** | Authenticated TCP session / lightweight logical session multiplexed inside it |
| **Exchange** | Router: receives published messages and decides which queues get them |
| **Binding** | Rule linking an exchange to a queue (with a routing key or header matcher) |
| **Routing key** | The string a publisher stamps on a message; how direct/topic exchanges match |
| **Ack** | Consumer's receipt confirming "processed, delete it" — omit it and the message is redelivered |
| **Prefetch (QoS)** | Max unacked messages handed to one consumer at a time |
| **Delivery mode** | `PERSISTENT` = survive broker restart (needs a durable queue too) |
| **Publisher confirm** | Broker's ack to the *producer* that the message is stored |
| **TTL** | Time-to-live — message expires after N ms in a queue |
| **Dead letter** | Where a message goes when it expires, is rejected, or overflows — usually a DLQ |
| **DLX / DLQ** | Dead-letter exchange / dead-letter queue |
| **Idempotency** | "Do it once even if you receive it many times" — required under at-least-once delivery |
| **RPC / reply-to** | Request-reply pattern: `reply_to` queue + `correlation_id` matched by the caller |
| **Priority** | `x-max-priority` on a queue; higher-priority messages are delivered first |
| **Saga** | Long-running transaction split into steps, each with a compensating action |
| **Correlation ID** | One id threaded through every event of a single business transaction for tracing |

## Learning tip

After each phase: hit the API, watch worker logs (`docker compose logs -f worker-inventory`), and inspect queues in the RabbitMQ Management UI (Queues / Exchanges tabs). Grep one `correlation_id` across all logs to follow a single order end to end.