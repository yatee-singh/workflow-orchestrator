"""RabbitMQ connection helpers.

Concept: one *connection* per process, many *channels* for concurrent work.
aio-pika's connect_robust reconnects automatically if the broker restarts.
"""

from __future__ import annotations

import logging

import aio_pika
from aio_pika.abc import AbstractChannel, AbstractRobustConnection

from shared.config import get_settings

logger = logging.getLogger(__name__)

_connection: AbstractRobustConnection | None = None


async def get_connection() -> AbstractRobustConnection:
    global _connection
    if _connection is None or _connection.is_closed:
        url = get_settings().rabbitmq_url
        logger.info("Connecting to RabbitMQ at %s", url.split("@")[-1])
        _connection = await aio_pika.connect_robust(url)
    return _connection


async def get_channel() -> AbstractChannel:
    connection = await get_connection()
    return await connection.channel()


async def close_connection() -> None:
    global _connection
    if _connection is not None and not _connection.is_closed:
        await _connection.close()
        logger.info("RabbitMQ connection closed")
    _connection = None
