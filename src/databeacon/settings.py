from dataclasses import dataclass
import os
from pathlib import Path


@dataclass(frozen=True)
class Settings:
    db_path: Path = Path(os.getenv("DATABEACON_DB", "data/databeacon.db"))
    output_dir: Path = Path(os.getenv("DATABEACON_OUTPUT_DIR", "data/output"))

    def ensure_directories(self) -> None:
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self.output_dir.mkdir(parents=True, exist_ok=True)
