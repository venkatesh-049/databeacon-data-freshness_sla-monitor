from dataclasses import dataclass
import os
from pathlib import Path


@dataclass(frozen=True)
class Settings:
    db_path: Path = Path(os.getenv("DATABEACON_DB", "data/databeacon.db"))
    output_dir: Path = Path(os.getenv("DATABEACON_OUTPUT_DIR", "data/output"))
    incoming_dir: Path = Path(os.getenv("DATABEACON_INCOMING_DIR", "data/incoming"))
    monitor_interval_seconds: int = int(os.getenv("DATABEACON_MONITOR_INTERVAL_SECONDS", "30"))
    arrival_grace_minutes: int = int(os.getenv("DATABEACON_ARRIVAL_GRACE_MINUTES", "15"))

    def __post_init__(self) -> None:
        if self.monitor_interval_seconds <= 0:
            raise ValueError("monitor_interval_seconds must be positive")
        if self.arrival_grace_minutes < 0:
            raise ValueError("arrival_grace_minutes must be non-negative")

    def ensure_directories(self) -> None:
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.incoming_dir.mkdir(parents=True, exist_ok=True)
