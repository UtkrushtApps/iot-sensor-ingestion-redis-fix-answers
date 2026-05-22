"""Main simulation script.

Runs a short ingestion simulation and prints active devices.
Use this to manually verify your changes work end-to-end.

    python src/main.py
"""

import logging
import random
import time
from datetime import datetime

from src.models import SensorReading
from src.worker import ingest_batch, get_active_devices

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s — %(message)s",
)
logger = logging.getLogger("main")


def _make_reading(device_id: str) -> SensorReading:
    return SensorReading(
        device_id=device_id,
        temperature=round(random.uniform(18.0, 35.0), 2),
        humidity=round(random.uniform(30.0, 90.0), 2),
        pressure=round(random.uniform(990.0, 1030.0), 2),
        timestamp=datetime.utcnow(),
    )


def main() -> None:
    device_ids = [f"device-{i:04d}" for i in range(1, 21)]

    logger.info("Starting ingestion simulation with %d devices.", len(device_ids))

    for cycle in range(3):
        readings = [_make_reading(did) for did in device_ids]
        logger.info("Cycle %d: ingesting %d readings.", cycle + 1, len(readings))
        start = time.monotonic()
        ingest_batch(readings)
        elapsed = time.monotonic() - start
        logger.info("Cycle %d complete in %.4fs.", cycle + 1, elapsed)
        time.sleep(0.1)

    active = get_active_devices()
    logger.info("Active devices (%d): %s", len(active), active[:5])


if __name__ == "__main__":
    main()
