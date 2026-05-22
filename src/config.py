# Configuration for the IoT ingestion worker.

REDIS_HOST = "127.0.0.1"
REDIS_PORT = 6379
REDIS_DB = 0

# How many readings to process per ingestion batch.
BATCH_SIZE = 50

# TTL in seconds for per-device latest-reading keys.
# Devices that stop sending data should expire from Redis after this period.
DEVICE_LATEST_TTL_SECONDS = 300  # 5 minutes

# TTL in seconds for activity bucket keys.
# Each minute-bucket should expire after this window.
ACTIVITY_BUCKET_TTL_SECONDS = 600  # 10 minutes

# How many recent minute-buckets to union when querying active devices.
ACTIVITY_LOOKBACK_MINUTES = 10
