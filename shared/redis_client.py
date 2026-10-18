from redis.asyncio import Redis

from shared.config import get_settings

_redis: Redis | None = None


def get_redis() -> Redis:
    global _redis
    if _redis is None:
        _redis = Redis.from_url(get_settings().redis_url, decode_responses=True)
    return _redis


async def check_redis() -> bool:
    try:
        return bool(await get_redis().ping())
    except Exception:
        return False


async def close_redis() -> None:
    global _redis
    if _redis is not None:
        await _redis.aclose()
        _redis = None
