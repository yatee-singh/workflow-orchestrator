"""Payment worker — consumes direct `payment.charge`, emits topic payment.* events."""

from __future__ import annotations

import asyncio
import logging
from typing import Any

from aio_pika.abc import AbstractIncomingMessage
from sqlalchemy import select

from shared.config import get_settings
from shared.db import SessionLocal
from shared.messaging.connection import get_channel
from shared.messaging.publisher import publish_event
from shared.messaging.topology import (
    EXCHANGE_TOPIC,
    QUEUE_PAYMENT,
    RK_PAYMENT_SUCCEEDED,
)
from shared.models import Order, OrderStatus
from workers.base import run_queue_worker

logging.basicConfig(level=get_settings().log_level)
logger = logging.getLogger("worker.payment")


async def handle(body: dict[str, Any], _message: AbstractIncomingMessage) -> None:
    if body.get("type") != "payment.charge":
        logger.warning("Ignoring %s", body.get("type"))
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
        if order.status == OrderStatus.paid:
            logger.info("Order %s already paid — skip", order_id)
            return
        if order.status != OrderStatus.reserved:
            logger.warning("Order %s in unexpected status %s", order_id, order.status.value)
            return

        # Phase 2: payments always succeed. Phase 3 adds flaky failures + retries.
        order.status = OrderStatus.paid
        await session.commit()

    channel = await get_channel()
    await publish_event(
        channel,
        exchange_name=EXCHANGE_TOPIC,
        routing_key=RK_PAYMENT_SUCCEEDED,
        event_type="payment.succeeded",
        order_id=order_id,
        correlation_id=correlation_id,
        payload=payload,
    )
    logger.info("Charged order=%s total_cents=%s", order_id, payload.get("total_cents"))


def main() -> None:
    asyncio.run(run_queue_worker("worker.payment", QUEUE_PAYMENT, handle, prefetch=1))


if __name__ == "__main__":
    main()
