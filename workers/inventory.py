"""Inventory worker — consumes fanout `order.placed`, then handoffs via direct/topic."""

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
    EXCHANGE_DIRECT,
    EXCHANGE_TOPIC,
    QUEUE_INVENTORY,
    RK_INVENTORY_FAILED,
    RK_PAYMENT_CHARGE,
)
from shared.models import InventoryItem, Order, OrderStatus
from workers.base import run_queue_worker

logging.basicConfig(level=get_settings().log_level)
logger = logging.getLogger("worker.inventory")


async def handle(body: dict[str, Any], _message: AbstractIncomingMessage) -> None:
    if body.get("type") != "order.placed":
        logger.warning("Ignoring %s", body.get("type"))
        return

    payload = body["payload"]
    order_id = payload["order_id"]
    items = payload["items"]
    correlation_id = body["correlation_id"]

    async with SessionLocal() as session:
        result = await session.execute(select(Order).where(Order.id == order_id))
        order = result.scalar_one_or_none()
        if order is None:
            logger.error("Order %s not found", order_id)
            return
        if order.status != OrderStatus.pending:
            logger.info("Order %s already %s — skip", order_id, order.status.value)
            return

        for line in items:
            inv_result = await session.execute(
                select(InventoryItem).where(InventoryItem.sku == line["sku"]).with_for_update()
            )
            inv = inv_result.scalar_one_or_none()
            if inv is None or inv.quantity_available < line["quantity"]:
                order.status = OrderStatus.reservation_failed
                await session.commit()
                channel = await get_channel()
                await publish_event(
                    channel,
                    exchange_name=EXCHANGE_TOPIC,
                    routing_key=RK_INVENTORY_FAILED,
                    event_type="inventory.reservation_failed",
                    order_id=order_id,
                    correlation_id=correlation_id,
                    payload=payload,
                )
                logger.warning("Reservation FAILED order=%s", order_id)
                return
            inv.quantity_available -= line["quantity"]

        order.status = OrderStatus.reserved
        await session.commit()

    # DIRECT handoff to payment (exact routing key)
    channel = await get_channel()
    await publish_event(
        channel,
        exchange_name=EXCHANGE_DIRECT,
        routing_key=RK_PAYMENT_CHARGE,
        event_type="payment.charge",
        order_id=order_id,
        correlation_id=correlation_id,
        payload=payload,
    )
    logger.info("Reserved + published payment.charge for order=%s", order_id)


def main() -> None:
    asyncio.run(run_queue_worker("worker.inventory", QUEUE_INVENTORY, handle, prefetch=1))


if __name__ == "__main__":
    main()
