"""Domain models for the IoT ingestion worker."""

from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional


@dataclass
class SensorReading:
    """A single sensor reading from a device."""
    device_id: str
    temperature: float
    humidity: float
    pressure: float
    timestamp: datetime = field(default_factory=datetime.utcnow)

    def to_hash_fields(self) -> dict:
        """Serialize reading fields suitable for Redis HSET."""
        return {
            "temperature": str(self.temperature),
            "humidity": str(self.humidity),
            "pressure": str(self.pressure),
            "timestamp": self.timestamp.isoformat(),
        }


@dataclass
class Device:
    """Metadata for a registered device."""
    device_id: str
    location: str
    model: str
    active: bool = True

    def __repr__(self) -> str:
        return f"Device(id={self.device_id!r}, location={self.location!r})"
