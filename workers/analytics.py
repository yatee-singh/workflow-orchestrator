"""Analytics worker — fanout + topic listeners; counts events (log-only for now)."""

from __future__ import annotations

import asyncio
import logging
from typing import Any

from aio_pika.abc import AbstractIncomingMessage

from shared.config import get_settings
from shared.messaging.topology import QUEUE_ANALYTICS
from workers.base import run_queue_worker

logging.basicConfig(level=get_settings().log_level)
logger = logging.getLogger("worker.analytics")

_counts: dict[str, int] = {}


async def handle(body: dict[str, Any], _message: AbstractIncomingMessage) -> None:
    event_type = body.get("type") or "unknown"
    _counts[event_type] = _counts.get(event_type, 0) + 1
    logger.info(
        "metric event=%s order_id=%s totals=%s",
        event_type,
        body.get("order_id") or body.get("payload", {}).get("order_id"),
        dict(_counts),
    )


def main() -> None:
    asyncio.run(
        run_queue_worker(
            "worker.analytics", QUEUE_ANALYTICS, handle, prefetch=20, init_db=False
        )
    )


if __name__ == "__main__":
    main()
