"""Audit worker — fanout-only mirror of order.placed for compliance logging."""

from __future__ import annotations

import asyncio
import logging
from typing import Any

from aio_pika.abc import AbstractIncomingMessage

from shared.config import get_settings
from shared.messaging.topology import QUEUE_AUDIT
from workers.base import run_queue_worker

logging.basicConfig(level=get_settings().log_level)
logger = logging.getLogger("worker.audit")


async def handle(body: dict[str, Any], _message: AbstractIncomingMessage) -> None:
    logger.info(
        "AUDIT trail event=%s order_id=%s corr=%s payload_keys=%s",
        body.get("type"),
        body.get("order_id"),
        body.get("correlation_id"),
        list((body.get("payload") or {}).keys()),
    )


def main() -> None:
    asyncio.run(
        run_queue_worker("worker.audit", QUEUE_AUDIT, handle, prefetch=20, init_db=False)
    )


if __name__ == "__main__":
    main()
