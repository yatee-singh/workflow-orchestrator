"""Seed catalog inventory so POST /orders has SKUs to reserve against."""

from __future__ import annotations

import asyncio
import logging

from sqlalchemy import select

from shared.config import get_settings
from shared.db import SessionLocal, engine
from shared.models import Base, InventoryItem

logging.basicConfig(level=get_settings().log_level)
logger = logging.getLogger("seed")

SEED_ITEMS = [
    InventoryItem(sku="SKU-TEE-BLK", name="Black T-Shirt", quantity_available=100, unit_price_cents=1999),
    InventoryItem(sku="SKU-MUG-WHT", name="White Mug", quantity_available=50, unit_price_cents=1299),
    InventoryItem(sku="SKU-HAT-RED", name="Red Cap", quantity_available=25, unit_price_cents=2499),
]


async def main() -> None:
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    async with SessionLocal() as session:
        for item in SEED_ITEMS:
            existing = await session.execute(select(InventoryItem).where(InventoryItem.sku == item.sku))
            if existing.scalar_one_or_none() is None:
                session.add(
                    InventoryItem(
                        sku=item.sku,
                        name=item.name,
                        quantity_available=item.quantity_available,
                        unit_price_cents=item.unit_price_cents,
                    )
                )
                logger.info("Seeded %s (%s)", item.sku, item.name)
            else:
                logger.info("Already present: %s", item.sku)
        await session.commit()
    logger.info("Seed complete")


if __name__ == "__main__":
    asyncio.run(main())
