"""Declare exchanges, queues, and bindings.

Phase 1: classic work queue — durable `q.inventory`.
Later phases add fanout/topic/headers, DLX, delay queues, etc.
"""

from __future__ import annotations

import logging

from aio_pika import ExchangeType
from aio_pika.abc import AbstractChannel

logger = logging.getLogger(__name__)

# --- names (single source of truth) ---
EXCHANGE_DIRECT = "orders.direct"
QUEUE_INVENTORY = "q.inventory"
RK_INVENTORY_RESERVE = "inventory.reserve"


async def declare_topology(channel: AbstractChannel) -> None:
    """Idempotent: safe to call from API and every worker on startup."""
    # Direct exchange: route by exact routing key (Phase 2 will use this more).
    # In Phase 1 we publish with RK_INVENTORY_RESERVE → q.inventory.
    direct = await channel.declare_exchange(
        EXCHANGE_DIRECT,
        ExchangeType.DIRECT,
        durable=True,
    )

    inventory_q = await channel.declare_queue(
        QUEUE_INVENTORY,
        durable=True,  # survives broker restart
    )
    await inventory_q.bind(direct, routing_key=RK_INVENTORY_RESERVE)

    logger.info(
        "Topology ready: exchange=%s queue=%s rk=%s",
        EXCHANGE_DIRECT,
        QUEUE_INVENTORY,
        RK_INVENTORY_RESERVE,
    )
