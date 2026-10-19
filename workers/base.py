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


async def run_queue_worker(
    worker_name: str,
    queue_name: str,
    handler: MessageHandler,
    *,
    prefetch: int = 10,
    init_db: bool = True,
) -> None:
    logger = logging.getLogger(worker_name)
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, stop.set)
        except NotImplementedError:
            pass

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
    await close_connection()
