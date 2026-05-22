"""
Test suite for the IoT ingestion worker.

These tests expose the bugs in the current implementation.
All tests should PASS after the candidate applies their fixes.

Tests cover:
1. Per-device key design for latest readings.
2. TTL presence on per-device keys.
3. Time-bucketed activity keys with TTL.
4. Absence of the old global hot keys.
5. get_active_devices() returns correct members from bucketed keys.
6. Pipelining: round-trips are reduced (verified indirectly via throughput).
7. Failure resilience: a bad pipeline does not crash the worker.
"""

import time
import threading
from datetime import datetime
from unittest.mock import patch, MagicMock

import pytest
import redis

from src.models import SensorReading
from src.worker import ingest_batch, get_active_devices
from src.redis_client import get_redis_client


@pytest.fixture(autouse=True)
def flush_redis():
    """Flush the Redis DB before each test for isolation."""
    client = get_redis_client()
    client.flushdb()
    yield
    client.flushdb()


def _make_reading(device_id: str = "dev-001") -> SensorReading:
    return SensorReading(
        device_id=device_id,
        temperature=22.5,
        humidity=55.0,
        pressure=1013.0,
        timestamp=datetime.utcnow(),
    )


# ---------------------------------------------------------------------------
# 1. Per-device key for latest readings
# ---------------------------------------------------------------------------

class TestPerDeviceLatestKey:
    def test_per_device_key_exists_after_ingest(self):
        """After ingestion, a per-device Hash key must exist, not 'sensor:latest'."""
        client = get_redis_client()
        ingest_batch([_make_reading("dev-001")])

        # The old shared key must NOT exist.
        assert client.exists("sensor:latest") == 0, (
            "'sensor:latest' should not be used; use a per-device key instead."
        )

        # A per-device key must exist.
        per_device_key = "device:dev-001:latest"
        assert client.exists(per_device_key) == 1, (
            f"Expected per-device key '{per_device_key}' to exist after ingestion."
        )

    def test_per_device_key_contains_reading_fields(self):
        """The per-device Hash must contain temperature, humidity, pressure, timestamp."""
        client = get_redis_client()
        ingest_batch([_make_reading("dev-002")])

        fields = client.hgetall("device:dev-002:latest")
        assert "temperature" in fields, "Missing 'temperature' field in per-device Hash."
        assert "humidity" in fields, "Missing 'humidity' field in per-device Hash."
        assert "pressure" in fields, "Missing 'pressure' field in per-device Hash."
        assert "timestamp" in fields, "Missing 'timestamp' field in per-device Hash."

    def test_multiple_devices_have_separate_keys(self):
        """Each device must have its own key, not share a single Hash."""
        client = get_redis_client()
        readings = [_make_reading(f"dev-{i:03d}") for i in range(5)]
        ingest_batch(readings)

        for i in range(5):
            key = f"device:dev-{i:03d}:latest"
            assert client.exists(key) == 1, f"Expected per-device key '{key}' to exist."


# ---------------------------------------------------------------------------
# 2. TTL on per-device keys
# ---------------------------------------------------------------------------

class TestPerDeviceTTL:
    def test_per_device_key_has_positive_ttl(self):
        """Per-device latest-reading key must have a positive TTL set."""
        client = get_redis_client()
        ingest_batch([_make_reading("dev-ttl")])

        ttl = client.ttl("device:dev-ttl:latest")
        assert ttl > 0, (
            f"Expected a positive TTL on 'device:dev-ttl:latest', got {ttl}. "
            "Keys without TTL cause unbounded Redis memory growth."
        )

    def test_per_device_ttl_is_reasonable(self):
        """TTL on per-device key should be between 60 and 3600 seconds."""
        client = get_redis_client()
        ingest_batch([_make_reading("dev-ttl2")])

        ttl = client.ttl("device:dev-ttl2:latest")
        assert 60 <= ttl <= 3600, (
            f"TTL {ttl}s is outside the expected range [60, 3600]. "
            "Choose a TTL aligned to how long stale readings should remain accessible."
        )


# ---------------------------------------------------------------------------
# 3. Time-bucketed activity keys
# ---------------------------------------------------------------------------

class TestActivityBuckets:
    def test_global_active_devices_key_does_not_exist(self):
        """The old global 'active_devices' key must not be written."""
        client = get_redis_client()
        ingest_batch([_make_reading("dev-001")])

        assert client.exists("active_devices") == 0, (
            "'active_devices' is a hot key with no TTL. "
            "Replace it with time-bucketed keys."
        )

    def test_bucketed_activity_key_exists(self):
        """A time-bucketed activity Set key must exist after ingestion."""
        client = get_redis_client()
        ingest_batch([_make_reading("dev-001")])

        bucket_prefix = "active_devices:"
        matching_keys = [
            k for k in client.keys("*")
            if k.startswith(bucket_prefix)
        ]
        assert len(matching_keys) >= 1, (
            f"Expected at least one key starting with '{bucket_prefix}', found none. "
            "Activity should be stored in time-bucketed Sets."
        )

    def test_bucketed_activity_key_has_ttl(self):
        """Every active_devices bucket key must have a positive TTL."""
        client = get_redis_client()
        ingest_batch([_make_reading("dev-001"), _make_reading("dev-002")])

        bucket_keys = [k for k in client.keys("*") if k.startswith("active_devices:")]
        assert len(bucket_keys) >= 1, "No activity bucket keys found."

        for key in bucket_keys:
            ttl = client.ttl(key)
            assert ttl > 0, (
                f"Bucket key '{key}' has no TTL ({ttl}). "
                "All activity buckets must expire automatically."
            )

    def test_bucketed_activity_key_contains_device_id(self):
        """The current-minute bucket must contain the ingested device ID."""
        client = get_redis_client()
        ingest_batch([_make_reading("dev-bucket-test")])

        bucket_keys = [k for k in client.keys("*") if k.startswith("active_devices:")]
        all_members = set()
        for key in bucket_keys:
            all_members.update(client.smembers(key))

        assert "dev-bucket-test" in all_members, (
            "'dev-bucket-test' was not found in any activity bucket after ingestion."
        )


# ---------------------------------------------------------------------------
# 4. get_active_devices correctness
# ---------------------------------------------------------------------------

class TestGetActiveDevices:
    def test_returns_ingested_devices(self):
        """get_active_devices() must return all recently ingested device IDs."""
        device_ids = [f"dev-{i:03d}" for i in range(10)]
        readings = [_make_reading(did) for did in device_ids]
        ingest_batch(readings)

        active = get_active_devices()
        for did in device_ids:
            assert did in active, f"Expected '{did}' in active devices, got: {active}"

    def test_returns_sorted_list(self):
        """get_active_devices() must return a sorted list."""
        readings = [_make_reading(f"dev-{i:03d}") for i in range(5)]
        ingest_batch(readings)

        active = get_active_devices()
        assert active == sorted(active), (
            "get_active_devices() must return a sorted list."
        )

    def test_empty_when_no_ingestion(self):
        """get_active_devices() must return an empty list when nothing was ingested."""
        active = get_active_devices()
        assert active == [], f"Expected empty list, got: {active}"

    def test_does_not_read_from_global_key(self):
        """get_active_devices() must not depend on the old 'active_devices' global key."""
        client = get_redis_client()
        # Manually seed the old global key — the fixed implementation must ignore it.
        client.sadd("active_devices", "ghost-device")

        # Ingest a real device via the worker.
        ingest_batch([_make_reading("real-device")])

        active = get_active_devices()
        assert "ghost-device" not in active, (
            "get_active_devices() must not read from the old 'active_devices' key."
        )
        assert "real-device" in active, (
            "'real-device' was ingested but not returned by get_active_devices()."
        )


# ---------------------------------------------------------------------------
# 5. Pipelining — throughput regression check
# ---------------------------------------------------------------------------

class TestPipelining:
    def test_large_batch_completes_quickly(self):
        """A batch of 200 readings should complete in under 2 seconds on localhost Redis.

        This acts as a regression guard: the non-pipelined implementation
        sends 400+ individual commands and is measurably slower.
        """
        readings = [_make_reading(f"dev-{i:04d}") for i in range(200)]
        start = time.monotonic()
        ingest_batch(readings)
        elapsed = time.monotonic() - start

        assert elapsed < 2.0, (
            f"ingest_batch(200 readings) took {elapsed:.3f}s, expected < 2.0s. "
            "Ensure Redis writes are batched using a pipeline."
        )


# ---------------------------------------------------------------------------
# 6. Failure resilience
# ---------------------------------------------------------------------------

class TestFailureResilience:
    def test_pipeline_failure_does_not_raise(self):
        """If the Redis pipeline fails, ingest_batch must not propagate the exception.

        The worker should log the error and return without crashing.
        """
        # Patch the pipeline execute to raise a connection error.
        client = get_redis_client()

        original_pipeline = client.pipeline

        def failing_pipeline(*args, **kwargs):
            pipe = original_pipeline(*args, **kwargs)
            pipe.execute = MagicMock(side_effect=redis.ConnectionError("Simulated failure"))
            return pipe

        with patch.object(client.__class__, "pipeline", failing_pipeline):
            with patch("src.worker.get_redis_client", return_value=client):
                # Must not raise.
                try:
                    ingest_batch([_make_reading("dev-resilience")])
                except Exception as exc:
                    pytest.fail(
                        f"ingest_batch raised {type(exc).__name__} on pipeline failure. "
                        "Pipeline errors must be caught and logged, not propagated."
                    )

    def test_concurrent_ingestion_does_not_crash(self):
        """Multiple threads calling ingest_batch concurrently must all succeed."""
        errors = []

        def worker_thread(thread_id: int):
            try:
                readings = [_make_reading(f"dev-t{thread_id}-{i:02d}") for i in range(10)]
                ingest_batch(readings)
            except Exception as exc:
                errors.append(exc)

        threads = [threading.Thread(target=worker_thread, args=(t,)) for t in range(10)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=10)

        assert not errors, (
            f"Concurrent ingest_batch calls raised exceptions: {errors}"
        )
