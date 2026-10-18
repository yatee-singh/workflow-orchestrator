"""Publish helpers.

Phase 1: fire-and-forget publish (publisher confirms land in Phase 3).
"""

from __future__ import annotations

import json
import logging
from datetime import UTC, datetime
from typing import Any
from uuid import uuid4

import aio_pika
from aio_pika.abc import AbstractChannel

from shared.messaging.topology import EXCHANGE_DIRECT

logger = logging.getLogger(__name__)


def build_envelope(
    event_type: str,
    payload: dict[str, Any],
    *,
    order_id: str | None = None,
    correlation_id: str | None = None,
) -> dict[str, Any]:
    return {
        "event_id": str(uuid4()),
        "correlation_id": correlation_id or str(uuid4()),
        "order_id": order_id,
        "type": event_type,
        "occurred_at": datetime.now(UTC).isoformat(),
        "payload": payload,
    }


async def publish_event(
    channel: AbstractChannel,
    *,
    routing_key: str,
    event_type: str,
    payload: dict[str, Any],
    order_id: str | None = None,
    correlation_id: str | None = None,
    exchange_name: str = EXCHANGE_DIRECT,
) -> dict[str, Any]:
    envelope = build_envelope(
        event_type,
        payload,
        order_id=order_id,
        correlation_id=correlation_id,
    )
    exchange = await channel.get_exchange(exchange_name)
    body = json.dumps(envelope).encode()
    message = aio_pika.Message(
        body=body,
        delivery_mode=aio_pika.DeliveryMode.PERSISTENT,  # survive broker restart
        content_type="application/json",
        correlation_id=envelope["correlation_id"],
        message_id=envelope["event_id"],
        type=event_type,
    )
    await exchange.publish(message, routing_key=routing_key)
    logger.info(
        "Published %s rk=%s order_id=%s event_id=%s",
        event_type,
        routing_key,
        order_id,
        envelope["event_id"],
    )
    return envelope
