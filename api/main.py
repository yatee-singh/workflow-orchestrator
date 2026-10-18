import logging
from contextlib import asynccontextmanager
from typing import Any

import aio_pika
from fastapi import FastAPI

from shared.config import get_settings
from shared.db import check_db
from shared.redis_client import check_redis, close_redis

logging.basicConfig(level=get_settings().log_level)
logger = logging.getLogger(__name__)


async def check_rabbitmq() -> bool:
    try:
        connection = await aio_pika.connect_robust(get_settings().rabbitmq_url)
        await connection.close()
        return True
    except Exception:
        return False


@asynccontextmanager
async def lifespan(_app: FastAPI):
    logger.info("OrderFlow API starting (Phase 0 scaffold)")
    yield
    await close_redis()
    logger.info("OrderFlow API shut down")


app = FastAPI(
    title="OrderFlow",
    description="Learn RabbitMQ via order fulfillment — Phase 0 scaffold",
    version="0.1.0",
    lifespan=lifespan,
)


@app.get("/health")
async def health() -> dict[str, Any]:
    """Report connectivity to Postgres, Redis, and RabbitMQ."""
    db_ok = await check_db()
    redis_ok = await check_redis()
    rmq_ok = await check_rabbitmq()
    healthy = db_ok and redis_ok and rmq_ok
    return {
        "status": "ok" if healthy else "degraded",
        "phase": 0,
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
        "phase": "0 — scaffold",
    }
