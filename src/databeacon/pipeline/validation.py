import csv
import json
from datetime import datetime
from pathlib import Path
from typing import Any

from pydantic import ValidationError

from databeacon.domain.models import ArrivalInput

REQUIRED = {"dataset_id", "expected_at", "arrived_at", "completed_at", "row_count"}


def read_records(path: Path) -> list[tuple[int, dict[str, Any]]]:
    with path.open(newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        if reader.fieldnames is None or set(reader.fieldnames) != REQUIRED:
            raise ValueError(f"unexpected schema: expected columns {sorted(REQUIRED)}")
        return [(index, row) for index, row in enumerate(reader, start=2)]


def validate_record(row: dict[str, Any], datasets: dict[str, Any],
                    seen: set[str]) -> tuple[ArrivalInput | None, tuple[str, str] | None]:
    try:
        parsed = ArrivalInput.model_validate({
            **row,
            "row_count": int(row["row_count"]) if row.get("row_count") is not None else None,
            "arrived_at": datetime.fromisoformat(row["arrived_at"]) if row.get("arrived_at") else None,
            "completed_at": datetime.fromisoformat(row["completed_at"]) if row.get("completed_at") else None,
            "expected_at": datetime.fromisoformat(row["expected_at"]) if row.get("expected_at") else None,
        })
    except (ValueError, TypeError, ValidationError) as exc:
        return None, ("invalid_record", str(exc))
    if parsed.dataset_id not in datasets:
        return None, ("invalid_reference", f"unknown dataset_id {parsed.dataset_id}")
    if parsed.row_count < 0:
        return None, ("invalid_value", "row_count must be non-negative")
    if parsed.completed_at and not parsed.arrived_at:
        return None, ("invalid_sequence", "completed_at requires arrived_at")
    if parsed.arrived_at and parsed.completed_at and parsed.completed_at < parsed.arrived_at:
        return None, ("out_of_order", "completed_at precedes arrived_at")
    from databeacon.domain.rules import event_key
    key = event_key(parsed.dataset_id, parsed.expected_at)
    if key in seen:
        return None, ("duplicate_key", f"duplicate event key {key}")
    seen.add(key)
    return parsed, None


def raw_json(row: dict[str, Any]) -> str:
    return json.dumps(row, sort_keys=True)
