import logging
from contextlib import asynccontextmanager
from typing import Any

from fastapi import FastAPI

from api.routes import orders
from shared.config import get_settings
from shared.db import Base, check_db, engine
from shared.messaging.connection import close_connection, get_channel, get_connection
from shared.messaging.topology import declare_topology
from shared.redis_client import check_redis, close_redis

logging.basicConfig(level=get_settings().log_level)
logger = logging.getLogger(__name__)


async def check_rabbitmq() -> bool:
    try:
        connection = await get_connection()
        return not connection.is_closed
    except Exception:
        return False


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("OrderFlow API starting (Phase 1 — core messaging)")
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    connection = await get_connection()
    channel = await get_channel()
    await declare_topology(channel)
    app.state.rmq_connection = connection
    app.state.rmq_channel = channel

    yield

    await close_connection()
    await close_redis()
    logger.info("OrderFlow API shut down")


app = FastAPI(
    title="OrderFlow",
    description="Learn RabbitMQ via order fulfillment — Phase 1: work queue + manual ack",
    version="0.1.0",
    lifespan=lifespan,
)
app.include_router(orders.router)


@app.get("/health")
async def health() -> dict[str, Any]:
    """Report connectivity to Postgres, Redis, and RabbitMQ."""
    db_ok = await check_db()
    redis_ok = await check_redis()
    rmq_ok = await check_rabbitmq()
    healthy = db_ok and redis_ok and rmq_ok
    return {
        "status": "ok" if healthy else "degraded",
        "phase": 1,
        "checks": {
            "postgres": db_ok,
            "redis": redis_ok,
            "rabbitmq": rmq_ok,
        },
    }


@app.get("/")
async def root() -> dict[str, str]:
    return {
        "app": get_settings().app_name,
        "docs": "/docs",
        "health": "/health",
        "phase": "1 — core messaging (work queue)",
    }
