"""IoT ingestion worker.

This module receives batches of SensorReading objects and writes them
into Redis. It also exposes get_active_devices() which returns the set
of device IDs that have sent readings recently.

Fixes applied:
- Device activity is written to time-bucketed Set keys with TTL.
- Latest readings are stored in per-device Hash keys with TTL.
- Redis writes are grouped into pipeline executions for each chunk.
- Pipeline failures are logged and do not crash the worker.
"""

import logging
import time
from datetime import datetime
from typing import Iterable, List, Optional

from src.config import (
    ACTIVITY_BUCKET_TTL_SECONDS,
    BATCH_SIZE,
    DEVICE_LATEST_TTL_SECONDS,
)
from src.models import SensorReading
from src.redis_client import get_redis_client

logger = logging.getLogger(__name__)


def _device_latest_key(device_id: str) -> str:
    """Return the Redis key storing the latest reading for a device."""
    return f"device:{device_id}:latest"


def _activity_bucket_key(at: Optional[datetime] = None) -> str:
    """Return the Redis key for the minute-bucket of device activity."""
    moment = at or datetime.utcnow()
    return f"active_devices:{moment.strftime('%Y%m%d%H%M')}"


def _chunked(readings: List[SensorReading], chunk_size: int) -> Iterable[List[SensorReading]]:
    """Yield successive chunks from the provided readings list."""
    safe_chunk_size = max(1, chunk_size)
    for index in range(0, len(readings), safe_chunk_size):
        yield readings[index:index + safe_chunk_size]


def ingest_batch(readings: List[SensorReading]) -> None:
    """Ingest a batch of sensor readings into Redis.

    For each reading:
    - Store the latest values for the device.
    - Record the device as active.

    Writes are buffered into a Redis pipeline per chunk to reduce
    network round-trips and improve throughput.

    Args:
        readings: List of SensorReading objects to persist.
    """
    if not readings:
        return

    client = get_redis_client()

    for chunk in _chunked(readings, BATCH_SIZE):
        activity_key = _activity_bucket_key()
        latest_by_device = {}
        active_device_ids = set()

        for reading in chunk:
            latest_by_device[reading.device_id] = reading.to_hash_fields()
            active_device_ids.add(reading.device_id)

        pipe = client.pipeline()

        for device_id, fields in latest_by_device.items():
            latest_key = _device_latest_key(device_id)
            pipe.hset(latest_key, mapping=fields)
            pipe.expire(latest_key, DEVICE_LATEST_TTL_SECONDS)

        if active_device_ids:
            pipe.sadd(activity_key, *active_device_ids)
            pipe.expire(activity_key, ACTIVITY_BUCKET_TTL_SECONDS)

        started = time.monotonic()
        try:
            pipe.execute()
            latency_ms = (time.monotonic() - started) * 1000.0
            logger.info(
                "pipeline_flush_success batch_size=%d unique_devices=%d latency_ms=%.2f activity_bucket=%s",
                len(chunk),
                len(active_device_ids),
                latency_ms,
                activity_key,
            )
        except Exception as exc:
            latency_ms = (time.monotonic() - started) * 1000.0
            logger.error(
                "pipeline_flush_failed batch_size=%d unique_devices=%d latency_ms=%.2f activity_bucket=%s error=%s",
                len(chunk),
                len(active_device_ids),
                latency_ms,
                activity_key,
                exc,
                exc_info=True,
            )


def get_active_devices() -> List[str]:
    """Return the list of device IDs that have been active recently.

    Returns:
        Sorted list of active device ID strings.
    """
    from src.service import get_active_devices as service_get_active_devices

    return service_get_active_devices()
