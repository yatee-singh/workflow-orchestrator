"""Inventory worker — the *consumer* of the work queue.

Phase 1 concepts demonstrated here:
- Connect + open a channel
- Declare topology (idempotent)
- basic.qos / prefetch (how many unacked msgs a worker may hold)
- Manual ack after successful DB work
- nack(requeue=True) on unexpected errors so another try can happen
"""

from __future__ import annotations

import asyncio
import json
import logging
import signal
from typing import Any

from aio_pika.abc import AbstractIncomingMessage
from sqlalchemy import select

from shared.config import get_settings
from shared.db import SessionLocal, engine
from shared.messaging.connection import close_connection, get_channel, get_connection
from shared.messaging.topology import QUEUE_INVENTORY, declare_topology
from shared.models import Base, InventoryItem, Order, OrderStatus

logging.basicConfig(level=get_settings().log_level)
logger = logging.getLogger("worker.inventory")

PREFETCH = 1  # fair dispatch: don't hog messages while busy


async def handle_order_placed(payload: dict[str, Any], correlation_id: str) -> None:
    order_id = payload["order_id"]
    items = payload["items"]

    async with SessionLocal() as session:
        result = await session.execute(select(Order).where(Order.id == order_id))
        order = result.scalar_one_or_none()
        if order is None:
            logger.error("Order %s not found — acking to drop poison message", order_id)
            return
        if order.status != OrderStatus.pending:
            logger.info("Order %s already %s — idempotent skip", order_id, order.status.value)
            return

        # Reserve stock (simple decrement). Phase 4 will add compensate/restock.
        for line in items:
            inv_result = await session.execute(
                select(InventoryItem).where(InventoryItem.sku == line["sku"]).with_for_update()
            )
            inv = inv_result.scalar_one_or_none()
            if inv is None or inv.quantity_available < line["quantity"]:
                order.status = OrderStatus.reservation_failed
                await session.commit()
                logger.warning(
                    "Reservation FAILED order=%s sku=%s need=%s have=%s corr=%s",
                    order_id,
                    line["sku"],
                    line["quantity"],
                    None if inv is None else inv.quantity_available,
                    correlation_id,
                )
                return
            inv.quantity_available -= line["quantity"]

        order.status = OrderStatus.reserved
        await session.commit()
        logger.info(
            "Reserved stock for order=%s corr=%s items=%s",
            order_id,
            correlation_id,
            items,
        )


async def on_message(message: AbstractIncomingMessage) -> None:
    async with message.process(requeue=True):
        # message.process() acks on clean exit, nacks+requeues on exception
        body = json.loads(message.body.decode())
        event_type = body.get("type")
        correlation_id = body.get("correlation_id", message.correlation_id or "")
        logger.info(
            "Received %s event_id=%s corr=%s",
            event_type,
            body.get("event_id"),
            correlation_id,
        )
        if event_type != "order.placed":
            logger.warning("Ignoring unexpected event type %s", event_type)
            return
        await handle_order_placed(body["payload"], correlation_id)


async def run() -> None:
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, stop.set)
        except NotImplementedError:
            pass

    # Ensure tables exist (same as API) in case worker starts first.
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    await get_connection()
    channel = await get_channel()
    # Prefetch = QoS: max unacked messages this consumer holds at once.
    await channel.set_qos(prefetch_count=PREFETCH)
    await declare_topology(channel)

    queue = await channel.get_queue(QUEUE_INVENTORY)
    consumer_tag = await queue.consume(on_message)
    logger.info(
        "Inventory worker listening on %s (prefetch=%s, manual ack via process())",
        QUEUE_INVENTORY,
        PREFETCH,
    )

    await stop.wait()
    logger.info("Shutting down inventory worker...")
    await queue.cancel(consumer_tag)
    await close_connection()


def main() -> None:
    asyncio.run(run())


if __name__ == "__main__":
    main()
