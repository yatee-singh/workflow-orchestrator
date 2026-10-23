"""Declare exchanges, queues, and bindings.

Phase 4 adds:
  - orders.rpc + q.inventory.rpc (RPC availability check)
  - x-max-priority on q.payment (express orders jump ahead)
  - q.reminder (TTL+DLX scheduled reminders)
  - order.cancelled topic + restock bindings
"""

from __future__ import annotations

import logging

from aio_pika import ExchangeType
from aio_pika.abc import AbstractChannel

logger = logging.getLogger(__name__)

# --- exchanges ---
EXCHANGE_FANOUT = "orders.fanout"
EXCHANGE_DIRECT = "orders.direct"
EXCHANGE_TOPIC = "orders.topic"
EXCHANGE_HEADERS = "orders.headers"
EXCHANGE_DLX = "orders.dlx"
EXCHANGE_DELAY = "orders.delay"
EXCHANGE_RPC = "orders.rpc"

# --- queues ---
QUEUE_INVENTORY = "q.inventory"
QUEUE_PAYMENT = "q.payment"
QUEUE_SHIPPING = "q.shipping"
QUEUE_NOTIFY = "q.notify"
QUEUE_ANALYTICS = "q.analytics"
QUEUE_AUDIT = "q.audit"
QUEUE_DLQ = "q.dlq"
QUEUE_INVENTORY_RPC = "q.inventory.rpc"
QUEUE_REMINDER = "q.reminder"

# Delay queues: message sits for TTL ms, then dead-letters back to direct exchange
DELAY_TTLS_MS = (1_000, 5_000, 25_000)
REMINDER_TTL_MS = 60_000  # 1 minute demo — set longer in production

# --- routing keys ---
RK_INVENTORY_RESERVE = "inventory.reserve"
RK_PAYMENT_CHARGE = "payment.charge"
RK_SHIPPING_CREATE = "shipping.create"
RK_INVENTORY_RESERVED = "inventory.reserved"
RK_INVENTORY_FAILED = "inventory.reservation_failed"
RK_PAYMENT_SUCCEEDED = "payment.succeeded"
RK_PAYMENT_FAILED = "payment.failed"
RK_SHIPMENT_CREATED = "shipment.created"
RK_INVENTORY_AVAILABILITY = "inventory.availability"
RK_NOTIFY_REMINDER = "notify.reminder"
RK_ORDER_CANCELLED = "order.cancelled"


def delay_queue_name(ttl_ms: int) -> str:
    return f"q.delay.{ttl_ms}"


def _dlx_args(routing_key: str) -> dict:
    """When a message is nacked/rejected/expired, send it to the DLX."""
    return {
        "x-dead-letter-exchange": EXCHANGE_DLX,
        "x-dead-letter-routing-key": routing_key,
    }


async def declare_topology(channel: AbstractChannel) -> None:
    """Idempotent: safe to call from API and every worker on startup."""
    fanout = await channel.declare_exchange(EXCHANGE_FANOUT, ExchangeType.FANOUT, durable=True)
    direct = await channel.declare_exchange(EXCHANGE_DIRECT, ExchangeType.DIRECT, durable=True)
    topic = await channel.declare_exchange(EXCHANGE_TOPIC, ExchangeType.TOPIC, durable=True)
    headers = await channel.declare_exchange(EXCHANGE_HEADERS, ExchangeType.HEADERS, durable=True)
    dlx = await channel.declare_exchange(EXCHANGE_DLX, ExchangeType.TOPIC, durable=True)
    delay = await channel.declare_exchange(EXCHANGE_DELAY, ExchangeType.DIRECT, durable=True)
    rpc = await channel.declare_exchange(EXCHANGE_RPC, ExchangeType.DIRECT, durable=True)

    # Dead-letter queue
    dlq = await channel.declare_queue(QUEUE_DLQ, durable=True)
    await dlq.bind(dlx, routing_key="#")

    # Work queues with DLX
    inventory_q = await channel.declare_queue(
        QUEUE_INVENTORY, durable=True, arguments=_dlx_args(QUEUE_INVENTORY)
    )
    payment_q = await channel.declare_queue(
        QUEUE_PAYMENT,
        durable=True,
        arguments={
            **_dlx_args(QUEUE_PAYMENT),
            "x-max-priority": 10,  # Phase 4: allow priority ordering
        },
    )
    shipping_q = await channel.declare_queue(
        QUEUE_SHIPPING, durable=True, arguments=_dlx_args(QUEUE_SHIPPING)
    )
    notify_q = await channel.declare_queue(
        QUEUE_NOTIFY, durable=True, arguments=_dlx_args(QUEUE_NOTIFY)
    )
    analytics_q = await channel.declare_queue(
        QUEUE_ANALYTICS, durable=True, arguments=_dlx_args(QUEUE_ANALYTICS)
    )
    audit_q = await channel.declare_queue(
        QUEUE_AUDIT, durable=True, arguments=_dlx_args(QUEUE_AUDIT)
    )

    # RPC queue: reply-to + correlation-id pattern
    rpc_q = await channel.declare_queue(QUEUE_INVENTORY_RPC, durable=True)
    await rpc_q.bind(rpc, routing_key=RK_INVENTORY_AVAILABILITY)

    # Reminder queue: message sits for TTL, then DLX → orders.topic with notify.reminder
    reminder_q = await channel.declare_queue(
        QUEUE_REMINDER,
        durable=True,
        arguments={
            "x-message-ttl": REMINDER_TTL_MS,
            "x-dead-letter-exchange": EXCHANGE_DIRECT,
            "x-dead-letter-routing-key": RK_NOTIFY_REMINDER,
        },
    )
    await reminder_q.bind(delay, routing_key=QUEUE_REMINDER)

    # Delay queues (retry backoff)
    for ttl in DELAY_TTLS_MS:
        q = await channel.declare_queue(
            delay_queue_name(ttl),
            durable=True,
            arguments={
                "x-message-ttl": ttl,
                "x-dead-letter-exchange": EXCHANGE_DIRECT,
                "x-dead-letter-routing-key": RK_PAYMENT_CHARGE,
            },
        )
        await q.bind(delay, routing_key=delay_queue_name(ttl))

    # Fanout
    await inventory_q.bind(fanout)
    await analytics_q.bind(fanout)
    await audit_q.bind(fanout)

    # Direct
    await inventory_q.bind(direct, routing_key=RK_INVENTORY_RESERVE)
    await payment_q.bind(direct, routing_key=RK_PAYMENT_CHARGE)
    await shipping_q.bind(direct, routing_key=RK_SHIPPING_CREATE)
    await notify_q.bind(direct, routing_key=RK_NOTIFY_REMINDER)

    # Topic
    await notify_q.bind(topic, routing_key="payment.*")
    await notify_q.bind(topic, routing_key="shipment.*")
    await notify_q.bind(topic, routing_key=RK_INVENTORY_FAILED)
    await analytics_q.bind(topic, routing_key="payment.*")
    await analytics_q.bind(topic, routing_key="shipment.*")
    await analytics_q.bind(topic, routing_key="inventory.*")
    await shipping_q.bind(topic, routing_key=RK_PAYMENT_SUCCEEDED)
    # Phase 4: inventory restocks on cancelled
    await inventory_q.bind(topic, routing_key=RK_ORDER_CANCELLED)

    # Headers
    await notify_q.bind(
        headers,
        arguments={"x-match": "all", "priority": "high", "region": "eu"},
    )

    logger.info("Topology ready (phase 4): RPC, priority, reminders, saga restock")