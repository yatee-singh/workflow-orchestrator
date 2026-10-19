"""Publish helpers for all exchange types."""

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
    headers: dict[str, Any] | None = None,
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
        delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
        content_type="application/json",
        correlation_id=envelope["correlation_id"],
        message_id=envelope["event_id"],
        type=event_type,
        headers=headers or {},
    )
    # Fanout ignores routing_key; headers exchange ignores it too — pass "" for those.
    await exchange.publish(message, routing_key=routing_key)
    logger.info(
        "Published %s exchange=%s rk=%s order_id=%s event_id=%s headers=%s",
        event_type,
        exchange_name,
        routing_key,
        order_id,
        envelope["event_id"],
        headers or {},
    )
    return envelope
