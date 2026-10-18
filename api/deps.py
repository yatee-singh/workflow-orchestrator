from collections.abc import AsyncGenerator

from sqlalchemy.ext.asyncio import AsyncSession

from shared.db import SessionLocal


async def get_db() -> AsyncGenerator[AsyncSession, None]:
    async with SessionLocal() as session:
        yield session
