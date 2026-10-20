"""RabbitMQ connection helpers.

Concept: one *connection* per process, reuse a channel for publishes.
Publisher confirms (Phase 3): broker ACKs each publish so we know it was persisted.
"""

from __future__ import annotations

import logging

import aio_pika
from aio_pika.abc import AbstractChannel, AbstractRobustConnection

from shared.config import get_settings

logger = logging.getLogger(__name__)

_connection: AbstractRobustConnection | None = None
_channel: AbstractChannel | None = None


async def get_connection() -> AbstractRobustConnection:
    global _connection
    if _connection is None or _connection.is_closed:
        url = get_settings().rabbitmq_url
        logger.info("Connecting to RabbitMQ at %s", url.split("@")[-1])
        _connection = await aio_pika.connect_robust(url)
    return _connection


async def get_channel(*, publisher_confirms: bool = False) -> AbstractChannel:
    """Reuse one channel per process.

    Set publisher_confirms=True on the API (and any critical producer) so
    `exchange.publish(...)` waits until the broker confirms the message.
    """
    global _channel
    if _channel is None or _channel.is_closed:
        connection = await get_connection()
        _channel = await connection.channel()
        if publisher_confirms:
            # aio-pika / aiormq: enable confirm mode on this channel
            confirm = getattr(_channel, "set_confirm_delivery", None) or getattr(
                _channel, "confirm_delivery", None
            )
            if confirm is not None:
                result = confirm()
                if hasattr(result, "__await__"):
                    await result  # type: ignore[misc]
                logger.info("Publisher confirms ENABLED on channel")
            else:
                logger.warning("Publisher confirms API not found — publishes are unconfirmed")
    return _channel


async def close_connection() -> None:
    global _connection, _channel
    if _channel is not None and not _channel.is_closed:
        await _channel.close()
    _channel = None
    if _connection is not None and not _connection.is_closed:
        await _connection.close()
        logger.info("RabbitMQ connection closed")
    _connection = None
