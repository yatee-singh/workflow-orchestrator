"""Declare exchanges, queues, and bindings.

Phase 2 routing map
-------------------
FANOUT  orders.fanout
  order.placed  →  q.inventory, q.analytics, q.audit
  (routing key ignored — every bound queue gets a copy)

DIRECT  orders.direct
  payment.charge   →  q.payment
  shipping.create  →  q.shipping

TOPIC   orders.topic
  inventory.reservation_failed  →  q.notify, q.analytics
  payment.*                     →  q.notify, q.analytics
  payment.succeeded             →  q.shipping  (also)
  shipment.*                    →  q.notify, q.analytics

HEADERS orders.headers  (x-match=all)
  priority=high + region=eu  →  q.notify   (VIP alert path)
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

QUEUE_INVENTORY = "q.inventory"
QUEUE_PAYMENT = "q.payment"
QUEUE_SHIPPING = "q.shipping"
QUEUE_NOTIFY = "q.notify"
QUEUE_ANALYTICS = "q.analytics"
QUEUE_AUDIT = "q.audit"

RK_INVENTORY_RESERVE = "inventory.reserve"  # legacy Phase 1; still bound for demos
RK_PAYMENT_CHARGE = "payment.charge"
RK_SHIPPING_CREATE = "shipping.create"
RK_INVENTORY_RESERVED = "inventory.reserved"
RK_INVENTORY_FAILED = "inventory.reservation_failed"
RK_PAYMENT_SUCCEEDED = "payment.succeeded"
RK_PAYMENT_FAILED = "payment.failed"
RK_SHIPMENT_CREATED = "shipment.created"


async def declare_topology(channel: AbstractChannel) -> None:
    """Idempotent: safe to call from API and every worker on startup."""
    fanout = await channel.declare_exchange(EXCHANGE_FANOUT, ExchangeType.FANOUT, durable=True)
    direct = await channel.declare_exchange(EXCHANGE_DIRECT, ExchangeType.DIRECT, durable=True)
    topic = await channel.declare_exchange(EXCHANGE_TOPIC, ExchangeType.TOPIC, durable=True)
    headers = await channel.declare_exchange(EXCHANGE_HEADERS, ExchangeType.HEADERS, durable=True)

    inventory_q = await channel.declare_queue(QUEUE_INVENTORY, durable=True)
    payment_q = await channel.declare_queue(QUEUE_PAYMENT, durable=True)
    shipping_q = await channel.declare_queue(QUEUE_SHIPPING, durable=True)
    notify_q = await channel.declare_queue(QUEUE_NOTIFY, durable=True)
    analytics_q = await channel.declare_queue(QUEUE_ANALYTICS, durable=True)
    audit_q = await channel.declare_queue(QUEUE_AUDIT, durable=True)

    await inventory_q.bind(fanout)
    await analytics_q.bind(fanout)
    await audit_q.bind(fanout)

    await inventory_q.bind(direct, routing_key=RK_INVENTORY_RESERVE)
    await payment_q.bind(direct, routing_key=RK_PAYMENT_CHARGE)
    await shipping_q.bind(direct, routing_key=RK_SHIPPING_CREATE)

    await notify_q.bind(topic, routing_key="payment.*")
    await notify_q.bind(topic, routing_key="shipment.*")
    await notify_q.bind(topic, routing_key=RK_INVENTORY_FAILED)
    await analytics_q.bind(topic, routing_key="payment.*")
    await analytics_q.bind(topic, routing_key="shipment.*")
    await analytics_q.bind(topic, routing_key="inventory.*")
    await shipping_q.bind(topic, routing_key=RK_PAYMENT_SUCCEEDED)


    await notify_q.bind(
        headers,
        arguments={"x-match": "all", "priority": "high", "region": "eu"},
    )

    logger.info(
        "Topology ready: fanout/direct/topic/headers + queues inventory/payment/"
        "shipping/notify/analytics/audit"
    )
