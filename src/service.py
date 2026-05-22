"""Higher-level service functions used by main.py and tests.

This module wraps worker functions to provide richer behaviour,
such as fetching the latest reading for a specific device.
"""

from datetime import datetime, timedelta
from typing import Dict, List, Optional

from src.config import ACTIVITY_LOOKBACK_MINUTES
from src.redis_client import get_redis_client


def _device_latest_key(device_id: str) -> str:
    """Return the Redis key storing the latest reading for a device."""
    return f"device:{device_id}:latest"


def _activity_bucket_key(at: datetime) -> str:
    """Return the Redis key for a given minute-bucket."""
    return f"active_devices:{at.strftime('%Y%m%d%H%M')}"


def _recent_activity_bucket_keys(now: Optional[datetime] = None) -> List[str]:
    """Build the list of recent minute-bucket keys to query."""
    if ACTIVITY_LOOKBACK_MINUTES <= 0:
        return []

    current = (now or datetime.utcnow()).replace(second=0, microsecond=0)
    return [
        _activity_bucket_key(current - timedelta(minutes=offset))
        for offset in range(ACTIVITY_LOOKBACK_MINUTES)
    ]


def get_device_latest(device_id: str) -> Optional[Dict[str, str]]:
    """Fetch the latest sensor reading fields for a device from Redis.

    Returns None if no reading has been stored for the device.

    Args:
        device_id: The device identifier string.

    Returns:
        Dict of field -> value strings, or None.
    """
    client = get_redis_client()
    fields = client.hgetall(_device_latest_key(device_id))
    return fields or None


def get_active_devices() -> List[str]:
    """Return recently active devices by unioning recent activity buckets."""
    client = get_redis_client()
    bucket_keys = _recent_activity_bucket_keys()
    if not bucket_keys:
        return []

    members = client.sunion(bucket_keys)
    return sorted(members)
