"""Shared async worker bootstrap (connect → topology → consume → graceful stop)."""

from __future__ import annotations

import asyncio
import json
import logging
import signal
from collections.abc import Awaitable, Callable
from typing import Any

from aio_pika.abc import AbstractIncomingMessage

from shared.config import get_settings
from shared.db import engine
from shared.db_migrate import apply_schema_patches
from shared.messaging.connection import close_connection, get_channel, get_connection
from shared.messaging.topology import declare_topology
from shared.models import Base

logging.basicConfig(level=get_settings().log_level)

MessageHandler = Callable[[dict[str, Any], AbstractIncomingMessage], Awaitable[None]]


def make_stop_event() -> asyncio.Event:
    """Create a stop event and wire SIGINT/SIGTERM to set it."""
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, stop.set)
        except NotImplementedError:
            pass
    return stop


async def run_queue_worker(
    worker_name: str,
    queue_name: str,
    handler: MessageHandler,
    *,
    prefetch: int = 10,
    init_db: bool = True,
    stop: asyncio.Event | None = None,
    close_rmq_on_stop: bool = True,
) -> None:
    """
    Bootstrap a consumer: connect, declare topology, consume until stop, clean up.

    If *stop* is None a new Event is created with signal handlers.
    Pass a shared stop event to run multiple workers in one process.
    Set *close_rmq_on_stop* = False for all but the last worker sharing a connection.
    """
    logger = logging.getLogger(worker_name)
    if stop is None:
        stop = make_stop_event()

    if init_db:
        async with engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)
        await apply_schema_patches(engine)

    await get_connection()
    channel = await get_channel()
    await channel.set_qos(prefetch_count=prefetch)
    await declare_topology(channel)

    async def _on_message(message: AbstractIncomingMessage) -> None:
        async with message.process(requeue=True):
            body = json.loads(message.body.decode())
            logger.info(
                "Received %s event_id=%s corr=%s",
                body.get("type"),
                body.get("event_id"),
                body.get("correlation_id"),
            )
            await handler(body, message)

    queue = await channel.get_queue(queue_name)
    consumer_tag = await queue.consume(_on_message)
    logger.info("%s listening on %s (prefetch=%s)", worker_name, queue_name, prefetch)

    await stop.wait()
    logger.info("Shutting down %s...", worker_name)
    await queue.cancel(consumer_tag)
    if close_rmq_on_stop:
        await close_connection()