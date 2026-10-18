"""Shared Phase-0 stub loop for workers until real consumers land."""

import asyncio
import logging
import signal

from shared.config import get_settings

logging.basicConfig(level=get_settings().log_level)
logger = logging.getLogger(__name__)


async def run_stub(worker_name: str) -> None:
    stop = asyncio.Event()

    def _handle_signal(*_args: object) -> None:
        logger.info("%s received shutdown signal", worker_name)
        stop.set()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, _handle_signal)
        except NotImplementedError:
            # Windows / some environments
            signal.signal(sig, lambda *_: _handle_signal())

    logger.info(
        "%s stub running — messaging arrives in Phase 1. Waiting...",
        worker_name,
    )
    await stop.wait()
    logger.info("%s stub stopped", worker_name)
