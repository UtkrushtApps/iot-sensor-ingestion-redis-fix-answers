# Solution Steps

1. Update the Redis key design for latest readings by replacing the shared `sensor:latest` Hash with a per-device Hash key pattern such as `device:{device_id}:latest`.

2. When writing a reading, store its fields with `HSET ... mapping=reading.to_hash_fields()` so each field is queryable directly instead of serializing the whole dict into a string.

3. Set a TTL on every per-device latest-reading key using `EXPIRE` and the existing `DEVICE_LATEST_TTL_SECONDS` config value so inactive devices age out automatically.

4. Replace the single global `active_devices` Set with minute-bucketed Set keys named like `active_devices:{yyyyMMddHHmm}`.

5. Set a TTL on each activity bucket key using `ACTIVITY_BUCKET_TTL_SECONDS` so old activity data expires automatically and memory stays bounded.

6. Keep the worker structure recognizable, but add small helper functions in `src/worker.py` for building per-device keys, activity bucket keys, and chunking batches.

7. Refactor `ingest_batch()` to process readings in chunks based on `BATCH_SIZE` so large batches are flushed through Redis in manageable groups.

8. Within each chunk, aggregate the latest reading per device in a dictionary and track active device IDs in a set; this avoids redundant writes when the same device appears multiple times in one chunk.

9. Create one Redis pipeline per chunk and enqueue all `HSET`, `EXPIRE`, `SADD`, and bucket `EXPIRE` commands before executing the pipeline.

10. Measure pipeline execution time with `time.monotonic()` and log an INFO message for each flush that includes the chunk batch size and Redis round-trip latency.

11. Wrap each pipeline `execute()` call in `try/except`; on failure, log a clear ERROR with `exc_info=True` and continue to the next chunk instead of re-raising the exception.

12. Do not write to the old keys `sensor:latest` or `active_devices` anywhere in the refactored code.

13. Refactor `src/service.py` so `get_device_latest(device_id)` reads from the new per-device key with `HGETALL`, returning `None` when the hash is missing.

14. Add a corrected `get_active_devices()` implementation in `src/service.py` that computes the recent minute bucket keys for the configured lookback window and unions them with Redis `SUNION`.

15. Make `src/worker.py:get_active_devices()` keep its public signature but delegate to the service-layer query function so existing imports and tests continue to work.

16. Preserve all model definitions and infrastructure files unchanged, and keep the script-style layout of the project.

17. Run the provided test suite after the refactor; verify that per-device keys exist with TTLs, bucketed activity keys exist with TTLs, old global keys are absent, large batches complete quickly, and pipeline failures are logged without crashing.

