"""Shipping worker — listens on topic `payment.succeeded` (also direct shipping.create)."""

from __future__ import annotations

import asyncio
import logging
from typing import Any
from uuid import uuid4

from aio_pika.abc import AbstractIncomingMessage
from sqlalchemy import select

from shared.config import get_settings
from shared.db import SessionLocal
from shared.messaging.connection import get_channel
from shared.messaging.publisher import publish_event
from shared.messaging.topology import EXCHANGE_TOPIC, QUEUE_SHIPPING, RK_SHIPMENT_CREATED
from shared.models import Order, OrderStatus
from workers.base import run_queue_worker

logging.basicConfig(level=get_settings().log_level)
logger = logging.getLogger("worker.shipping")


async def handle(body: dict[str, Any], _message: AbstractIncomingMessage) -> None:
    event_type = body.get("type")
    if event_type not in {"payment.succeeded", "shipping.create"}:
        logger.warning("Ignoring %s", event_type)
        return

    payload = body["payload"]
    order_id = payload["order_id"]
    correlation_id = body["correlation_id"]

    async with SessionLocal() as session:
        result = await session.execute(select(Order).where(Order.id == order_id))
        order = result.scalar_one_or_none()
        if order is None:
            logger.error("Order %s not found", order_id)
            return
        if order.status == OrderStatus.shipped:
            logger.info("Order %s already shipped — skip", order_id)
            return
        if order.status != OrderStatus.paid:
            logger.warning("Order %s not paid yet (%s)", order_id, order.status.value)
            return

        order.status = OrderStatus.shipped
        await session.commit()

    tracking = f"TRK-{uuid4().hex[:10].upper()}"
    channel = await get_channel()
    await publish_event(
        channel,
        exchange_name=EXCHANGE_TOPIC,
        routing_key=RK_SHIPMENT_CREATED,
        event_type="shipment.created",
        order_id=order_id,
        correlation_id=correlation_id,
        payload={**payload, "tracking_number": tracking},
    )
    logger.info("Shipped order=%s tracking=%s", order_id, tracking)


def main() -> None:
    asyncio.run(run_queue_worker("worker.shipping", QUEUE_SHIPPING, handle, prefetch=5))


if __name__ == "__main__":
    main()
