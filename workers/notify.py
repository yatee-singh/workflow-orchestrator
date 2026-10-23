"""Notify worker — topic (payment.*/shipment.*) + headers VIP alerts + delayed reminders."""

from __future__ import annotations

import asyncio
import logging
from typing import Any

from aio_pika.abc import AbstractIncomingMessage

from shared.config import get_settings
from shared.messaging.topology import QUEUE_NOTIFY
from workers.base import run_queue_worker

logging.basicConfig(level=get_settings().log_level)
logger = logging.getLogger("worker.notify")


async def handle(body: dict[str, Any], message: AbstractIncomingMessage) -> None:
    event_type = body.get("type")
    payload = body.get("payload", {})
    email = payload.get("customer_email", "?")
    order_id = payload.get("order_id") or body.get("order_id")
    headers = dict(message.headers or {})

    if event_type == "order.vip_alert":
        logger.info(
            "VIP ALERT (headers match %s) → email+SMS to %s order=%s",
            headers,
            email,
            order_id,
        )
        return

    if event_type == "payment.succeeded":
        logger.info("EMAIL to %s: payment received for order %s", email, order_id)
    elif event_type == "payment.failed":
        logger.info(
            "EMAIL to %s: payment FAILED for order %s reason=%s",
            email,
            order_id,
            payload.get("reason"),
        )
    elif event_type == "shipment.created":
        logger.info(
            "EMAIL+SMS to %s: shipped order %s tracking=%s",
            email,
            order_id,
            payload.get("tracking_number"),
        )
    elif event_type == "inventory.reservation_failed":
        logger.info("EMAIL to %s: out of stock for order %s", email, order_id)
    elif event_type == "notify.reminder":
        logger.info(
            "REMINDER EMAIL to %s: you have an order %s still pending!",
            email,
            order_id,
        )
    elif event_type == "order.cancelled":
        logger.info("EMAIL to %s: order %s was cancelled", email, order_id)
    else:
        logger.info("Notify ignoring %s", event_type)


def main() -> None:
    asyncio.run(
        run_queue_worker("worker.notify", QUEUE_NOTIFY, handle, prefetch=10, init_db=False)
    )


if __name__ == "__main__":
    main()