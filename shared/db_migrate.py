"""Lightweight schema patches for the learning project (no Alembic yet)."""

from __future__ import annotations

import logging

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine

logger = logging.getLogger(__name__)

_ENUM_VALUES = (
    "pending",
    "reserved",
    "reservation_failed",
    "paid",
    "payment_failed",
    "shipped",
)


async def apply_schema_patches(engine: AsyncEngine) -> None:
    async with engine.begin() as conn:
        await _ensure_order_status_values(conn)
        await _ensure_order_columns(conn)


async def _ensure_order_status_values(conn: AsyncConnection) -> None:
    for value in _ENUM_VALUES:
        await conn.execute(
            text(f"ALTER TYPE order_status ADD VALUE IF NOT EXISTS '{value}'")
        )


async def _ensure_order_columns(conn: AsyncConnection) -> None:
    await conn.execute(
        text(
            "ALTER TABLE orders "
            "ADD COLUMN IF NOT EXISTS is_vip BOOLEAN NOT NULL DEFAULT FALSE"
        )
    )
    await conn.execute(
        text(
            "ALTER TABLE orders "
            "ADD COLUMN IF NOT EXISTS region VARCHAR(8) NOT NULL DEFAULT 'us'"
        )
    )
    logger.info("Schema patches applied (order_status enum + vip/region columns)")
