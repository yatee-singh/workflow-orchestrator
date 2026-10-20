"""Redis-backed idempotency keys so retries don't double-charge."""

from __future__ import annotations

import logging

from shared.redis_client import get_redis

logger = logging.getLogger(__name__)

TTL_SECONDS = 60 * 60 * 24  # 24h


async def already_done(key: str) -> bool:
    return bool(await get_redis().exists(key))


async def mark_done(key: str, value: str = "1") -> None:
    await get_redis().set(key, value, ex=TTL_SECONDS)
    logger.info("Idempotency key set: %s", key)


def payment_done_key(order_id: str) -> str:
    return f"payment:done:{order_id}"
