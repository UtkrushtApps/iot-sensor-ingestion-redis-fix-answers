"""Shared Redis connection pool and client.

NOTE: The pool is created once and reused across the application.
Do not create new Redis clients per request or per reading.
"""

import redis
from src.config import REDIS_HOST, REDIS_PORT, REDIS_DB

_pool = redis.ConnectionPool(
    host=REDIS_HOST,
    port=REDIS_PORT,
    db=REDIS_DB,
    max_connections=20,
    decode_responses=True,
)


def get_redis_client() -> redis.Redis:
    """Return a Redis client backed by the shared connection pool."""
    return redis.Redis(connection_pool=_pool)
