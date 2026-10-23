"""Payment worker — flaky gateway, retries via delay queues, Redis idempotency, DLQ.

Phase 4 addition: on permanent payment failure, also publish `order.cancelled`
so inventory restocks (saga compensate).
"""

from __future__ import annotations

import asyncio
import logging
import os
import random
from typing import Any

from aio_pika.abc import AbstractIncomingMessage
from sqlalchemy import select

from shared.config import get_settings
from shared.db import SessionLocal
from shared.idempotency import already_done, mark_done, payment_done_key
from shared.messaging.connection import get_channel
from shared.messaging.publisher import publish_event
from shared.messaging.retry import max_retries, publish_to_dlq, schedule_retry
from shared.messaging.topology import (
    EXCHANGE_TOPIC,
    QUEUE_PAYMENT,
    RK_ORDER_CANCELLED,
    RK_PAYMENT_FAILED,
    RK_PAYMENT_SUCCEEDED,
)
from shared.models import Order, OrderStatus
from workers.base import run_queue_worker

logging.basicConfig(level=get_settings().log_level)
logger = logging.getLogger("worker.payment")

WORKER_ID = os.environ.get("HOSTNAME", "payment-worker")


class TransientPaymentError(Exception):
    """Simulated gateway blip — safe to retry."""


async def _simulate_charge(order_id: str, total_cents: int) -> None:
    settings = get_settings()
    await asyncio.sleep(0.15)
    if random.random() < settings.payment_fail_rate:
        raise TransientPaymentError(
            f"simulated gateway timeout (fail_rate={settings.payment_fail_rate})"
        )
    logger.info("[%s] gateway OK order=%s amount=%s", WORKER_ID, order_id, total_cents)


async def handle(body: dict[str, Any], message: AbstractIncomingMessage) -> None:
    if body.get("type") != "payment.charge":
        logger.warning("Ignoring %s", body.get("type"))
        return

    payload = body["payload"]
    order_id = payload["order_id"]
    correlation_id = body["correlation_id"]
    event_id = body.get("event_id", "")
    retry_hdr = (message.headers or {}).get("x-retry-count", 0)

    logger.info(
        "[%s] charging order=%s retry=%s event=%s",
        WORKER_ID,
        order_id,
        retry_hdr,
        event_id,
    )

    if await already_done(payment_done_key(order_id)):
        logger.info("[%s] idempotent skip — already charged order=%s", WORKER_ID, order_id)
        return

    channel = await get_channel()

    async with SessionLocal() as session:
        result = await session.execute(select(Order).where(Order.id == order_id))
        order = result.scalar_one_or_none()
        if order is None:
            logger.error("Order %s not found — parking in DLQ", order_id)
            await publish_to_dlq(
                channel, body, reason="order_not_found", source_queue=QUEUE_PAYMENT
            )
            return
        if order.status == OrderStatus.paid:
            await mark_done(payment_done_key(order_id), event_id)
            logger.info("Order %s already paid in DB — skip", order_id)
            return
        if order.status == OrderStatus.payment_failed:
            logger.info("Order %s already payment_failed — skip", order_id)
            return
        if order.status != OrderStatus.reserved:
            logger.warning("Order %s unexpected status %s", order_id, order.status.value)
            return

        try:
            await _simulate_charge(order_id, payload.get("total_cents", 0))
        except TransientPaymentError as exc:
            scheduled = await schedule_retry(channel, message, body, reason=str(exc))
            if scheduled:
                return

            # Max retries exhausted → saga compensate: order.cancelled → restock
            order.status = OrderStatus.payment_failed
            await session.commit()

            await publish_event(
                channel,
                exchange_name=EXCHANGE_TOPIC,
                routing_key=RK_PAYMENT_FAILED,
                event_type="payment.failed",
                order_id=order_id,
                correlation_id=correlation_id,
                payload={**payload, "reason": str(exc), "retries": max_retries()},
            )
            # Phase 4: emit order.cancelled so inventory restocks
            await publish_event(
                channel,
                exchange_name=EXCHANGE_TOPIC,
                routing_key=RK_ORDER_CANCELLED,
                event_type=RK_ORDER_CANCELLED,
                order_id=order_id,
                correlation_id=correlation_id,
                payload=payload,
            )
            await publish_to_dlq(
                channel,
                {**body, "_headers": dict(message.headers or {})},
                reason=str(exc),
                source_queue=QUEUE_PAYMENT,
            )
            logger.error(
                "[%s] payment FAILED permanently order=%s — saga cancel emitted",
                WORKER_ID,
                order_id,
            )
            return

        order.status = OrderStatus.paid
        await session.commit()

    await mark_done(payment_done_key(order_id), event_id)
    await publish_event(
        channel,
        exchange_name=EXCHANGE_TOPIC,
        routing_key=RK_PAYMENT_SUCCEEDED,
        event_type="payment.succeeded",
        order_id=order_id,
        correlation_id=correlation_id,
        payload=payload,
    )
    logger.info("[%s] Charged order=%s total_cents=%s", WORKER_ID, order_id, payload.get("total_cents"))


def main() -> None:
    asyncio.run(run_queue_worker("worker.payment", QUEUE_PAYMENT, handle, prefetch=1))


if __name__ == "__main__":
    main()