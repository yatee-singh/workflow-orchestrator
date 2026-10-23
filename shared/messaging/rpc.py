"""RPC client and server helpers for RabbitMQ request-reply.

Pattern:
  1. Client publishes to `orders.rpc` with `reply_to` = callback queue
  2. Server consumes from `q.inventory.rpc`, processes, responds to `reply_to`
  3. Client waits for response on callback queue (matching correlation_id)
"""

from __future__ import annotations

import asyncio
import json
import logging
from typing import Any
from uuid import uuid4

import aio_pika
from aio_pika.abc import AbstractChannel

from shared.messaging.topology import EXCHANGE_RPC, RK_INVENTORY_AVAILABILITY

logger = logging.getLogger(__name__)

RPC_TIMEOUT = 10.0  # seconds


async def rpc_call(
    channel: AbstractChannel,
    *,
    body: dict[str, Any],
) -> dict[str, Any] | None:
    """Make an RPC call. Returns response dict or None on timeout/error.

    Creates an exclusive callback queue, publishes with reply-to + correlation-id,
    and waits for a matching response.
    """
    callback_queue = await channel.declare_queue(exclusive=True)
    correlation_id = str(uuid4())
    response_future: asyncio.Future[dict[str, Any] | None] = asyncio.Future()

    async def on_response(message: aio_pika.abc.AbstractIncomingMessage) -> None:
        async with message.process(requeue=True):
            if message.correlation_id == correlation_id:
                if not response_future.done():
                    try:
                        response_future.set_result(json.loads(message.body.decode()))
                    except Exception as exc:
                        response_future.set_exception(exc)

    consumer_tag = await callback_queue.consume(on_response)

    exchange = await channel.get_exchange(EXCHANGE_RPC)
    payload = json.dumps(body).encode()
    await exchange.publish(
        aio_pika.Message(
            body=payload,
            delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
            content_type="application/json",
            correlation_id=correlation_id,
            reply_to=callback_queue.name,
        ),
        routing_key=RK_INVENTORY_AVAILABILITY,
    )
    logger.info("RPC call %s corr=%s", body.get("action"), correlation_id)

    try:
        result = await asyncio.wait_for(response_future, timeout=RPC_TIMEOUT)
    except (asyncio.TimeoutError, asyncio.CancelledError):
        logger.warning("RPC timeout corr=%s", correlation_id)
        result = None
    finally:
        await callback_queue.cancel(consumer_tag)

    return result


async def rpc_respond(
    message: aio_pika.abc.AbstractIncomingMessage,
    response: dict[str, Any],
) -> None:
    """Publish response back to the client's callback queue."""
    from shared.messaging.connection import get_channel

    payload = json.dumps(response).encode()
    # aio-pika 10.x: default exchange must be referenced, never declared
    channel = await get_channel()
    exchange = aio_pika.Exchange(
        channel=channel,
        name="",
        type=aio_pika.ExchangeType.DIRECT,
        passive=True,
    )
    await exchange.publish(
        aio_pika.Message(
            body=payload,
            content_type="application/json",
            correlation_id=message.correlation_id,
        ),
        routing_key=message.reply_to,
    )