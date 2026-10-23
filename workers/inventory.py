"""Inventory worker — consumes fanout `order.placed`, RPC availability, saga restock."""

from __future__ import annotations

import asyncio
import json
import logging
from typing import Any

from aio_pika.abc import AbstractIncomingMessage
from sqlalchemy import select

from shared.config import get_settings
from shared.db import SessionLocal
from shared.messaging.connection import get_channel
from shared.messaging.publisher import publish_event
from shared.messaging.rpc import rpc_respond
from shared.messaging.topology import (
    EXCHANGE_DIRECT,
    EXCHANGE_TOPIC,
    QUEUE_INVENTORY,
    QUEUE_INVENTORY_RPC,
    RK_INVENTORY_FAILED,
    RK_ORDER_CANCELLED,
    RK_PAYMENT_CHARGE,
)
from shared.models import InventoryItem, Order, OrderStatus
from workers.base import make_stop_event, run_queue_worker

logging.basicConfig(level=get_settings().log_level)
logger = logging.getLogger("worker.inventory")


async def handle(body: dict[str, Any], message: AbstractIncomingMessage) -> None:
    event_type = body.get("type")

    if event_type == "order.placed":
        await _handle_order_placed(body, message)
    elif event_type == RK_ORDER_CANCELLED:
        await _handle_order_cancelled(body)
    else:
        logger.warning("Ignoring %s", event_type)


async def _handle_order_placed(body: dict[str, Any], _message: AbstractIncomingMessage) -> None:
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


async def _handle_order_cancelled(body: dict[str, Any]) -> None:
    payload = body.get("payload", {})
    order_id = payload.get("order_id") or body.get("order_id")
    items = payload.get("items", [])
    correlation_id = body.get("correlation_id")

    if not items or not order_id:
        logger.warning("order.cancelled missing payload — skip restock")
        return

    async with SessionLocal() as session:
        result = await session.execute(select(Order).where(Order.id == order_id))
        order = result.scalar_one_or_none()
        if order is None:
            logger.error("Order %s not found — cannot restock", order_id)
            return
        if order.status not in (OrderStatus.payment_failed, OrderStatus.reserved):
            logger.info("Order %s status %s — no restock needed", order_id, order.status.value)
            return

        for line in items:
            inv_result = await session.execute(
                select(InventoryItem).where(InventoryItem.sku == line["sku"]).with_for_update()
            )
            inv = inv_result.scalar_one_or_none()
            if inv is not None:
                inv.quantity_available += line["quantity"]

        order.status = OrderStatus.payment_failed
        await session.commit()

    logger.info("Restocked order=%s corr=%s", order_id, correlation_id)


async def handle_rpc(body: dict[str, Any], message: AbstractIncomingMessage) -> None:
    """Handle RPC availability check running on a separate queue."""
    action = body.get("action")
    if action != "check_availability":
        logger.warning("Unknown RPC action %s", action)
        await rpc_respond(message, {"error": f"unknown action: {action}"})
        return

    sku = body.get("sku")
    if not sku:
        await rpc_respond(message, {"error": "sku required"})
        return

    async with SessionLocal() as session:
        result = await session.execute(
            select(InventoryItem).where(InventoryItem.sku == sku)
        )
        inv = result.scalar_one_or_none()
        if inv is None:
            response = {"sku": sku, "available": False, "reason": "unknown_sku"}
        else:
            response = {
                "sku": sku,
                "available": inv.quantity_available > 0,
                "quantity_available": inv.quantity_available,
                "unit_price_cents": inv.unit_price_cents,
            }

    await rpc_respond(message, response)
    logger.info("RPC availability sku=%s → %s", sku, response)


async def run_consumers(stop: asyncio.Event) -> None:
    """Run both main inventory consumer and RPC server concurrently."""
    await asyncio.gather(
        run_queue_worker(
            "worker.inventory", QUEUE_INVENTORY, handle, prefetch=1, stop=stop,
        ),
        run_queue_worker(
            "worker.inventory.rpc", QUEUE_INVENTORY_RPC, handle_rpc, prefetch=1,
            stop=stop, close_rmq_on_stop=False,
        ),
    )


def main() -> None:
    async def _run():
        stop = make_stop_event()
        await run_consumers(stop)
    asyncio.run(_run())


if __name__ == "__main__":
    main()