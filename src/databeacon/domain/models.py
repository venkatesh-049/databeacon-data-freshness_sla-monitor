from dataclasses import dataclass
from datetime import datetime

from pydantic import BaseModel, ConfigDict


class ArrivalInput(BaseModel):
    model_config = ConfigDict(extra="forbid")

    dataset_id: str
    expected_at: datetime
    arrived_at: datetime | None = None
    completed_at: datetime | None = None
    row_count: int


@dataclass(frozen=True)
class DatasetConfig:
    dataset_id: str
    dataset_name: str
    expected_hour_utc: int
    expected_minute_utc: int
    sla_minutes: int
