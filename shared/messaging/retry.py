"""Retry via TTL delay queues + dead-letter helpers.

Flow on transient failure:
  1. Ack the original message (so it leaves the work queue)
  2. Publish a copy to q.delay.{ttl} with x-retry-count incremented
  3. After TTL, RabbitMQ dead-letters it back to payment.charge
  4. After max retries → publish to DLX / emit payment.failed
"""

from __future__ import annotations

import json
import logging
from typing import Any

import aio_pika
from aio_pika.abc import AbstractChannel, AbstractIncomingMessage

from shared.config import get_settings
from shared.messaging.topology import (
    DELAY_TTLS_MS,
    EXCHANGE_DELAY,
    EXCHANGE_DLX,
    delay_queue_name,
)

logger = logging.getLogger(__name__)

HEADER_RETRY_COUNT = "x-retry-count"


def get_retry_count(message: AbstractIncomingMessage | dict[str, Any]) -> int:
    if isinstance(message, dict):
        headers = message
    else:
        headers = dict(message.headers or {})
    raw = headers.get(HEADER_RETRY_COUNT, 0)
    try:
        return int(raw)
    except (TypeError, ValueError):
        return 0


def next_delay_ttl_ms(retry_count: int) -> int | None:
    """retry_count is how many retries have already been scheduled (from headers)."""
    if retry_count >= max_retries():
        return None
    return DELAY_TTLS_MS[min(retry_count, len(DELAY_TTLS_MS) - 1)]


async def schedule_retry(
    channel: AbstractChannel,
    message: AbstractIncomingMessage,
    body: dict[str, Any],
    *,
    reason: str,
) -> bool:
    """Publish to the next delay queue. Returns False if max retries exhausted."""
    current = get_retry_count(message)
    ttl = next_delay_ttl_ms(current)
    if ttl is None:
        return False

    new_count = current + 1
    headers = dict(message.headers or {})
    headers[HEADER_RETRY_COUNT] = new_count
    headers["x-last-error"] = reason[:200]

    envelope_bytes = json.dumps(body).encode()
    delay_rk = delay_queue_name(ttl)
    exchange = await channel.get_exchange(EXCHANGE_DELAY)
    await exchange.publish(
        aio_pika.Message(
            body=envelope_bytes,
            delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
            content_type="application/json",
            correlation_id=body.get("correlation_id"),
            message_id=body.get("event_id"),
            type=body.get("type"),
            headers=headers,
        ),
        routing_key=delay_rk,
    )
    logger.warning(
        "Scheduled retry #%s in %sms for %s order=%s reason=%s",
        new_count,
        ttl,
        body.get("type"),
        body.get("order_id") or body.get("payload", {}).get("order_id"),
        reason,
    )
    return True


async def publish_to_dlq(
    channel: AbstractChannel,
    body: dict[str, Any],
    *,
    reason: str,
    source_queue: str,
) -> None:
    """Manually park a poison message on the DLX (also used after max retries)."""
    headers = {
        HEADER_RETRY_COUNT: get_retry_count(body.get("_headers", {})),
        "x-last-error": reason[:200],
        "x-source-queue": source_queue,
    }
    # Prefer headers carried on the live message if caller stuffed them onto body
    if "_headers" in body:
        headers = {**body["_headers"], **headers}

    exchange = await channel.get_exchange(EXCHANGE_DLX)
    await exchange.publish(
        aio_pika.Message(
            body=json.dumps({k: v for k, v in body.items() if k != "_headers"}).encode(),
            delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
            content_type="application/json",
            correlation_id=body.get("correlation_id"),
            message_id=body.get("event_id"),
            type=body.get("type"),
            headers=headers,
        ),
        routing_key=source_queue,
    )
    logger.error(
        "Dead-lettered %s order=%s reason=%s",
        body.get("type"),
        body.get("order_id") or body.get("payload", {}).get("order_id"),
        reason,
    )


def max_retries() -> int:
    return get_settings().max_retries
