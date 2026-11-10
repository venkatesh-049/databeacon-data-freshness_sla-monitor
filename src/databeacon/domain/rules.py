from datetime import datetime, timezone
from hashlib import sha256


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def event_key(dataset_id: str, expected_at: datetime) -> str:
    value = f"{dataset_id}|{expected_at.isoformat()}"
    return sha256(value.encode("utf-8")).hexdigest()


def minutes_between(start: datetime, end: datetime) -> float:
    return round((end - start).total_seconds() / 60, 2)


def sla_status(arrived_at: datetime | None, completed_at: datetime | None,
               expected_at: datetime, sla_minutes: int, now: datetime) -> str:
    if arrived_at is None:
        return "missing" if now >= expected_at else "incomplete"
    if completed_at is None:
        return "incomplete"
    return "fresh" if minutes_between(expected_at, completed_at) <= sla_minutes else "late"


def alert_for(status: str, dataset_name: str, event_key_value: str,
              completion_minutes: float | None, sla_minutes: int) -> tuple[str, str, str] | None:
    if status == "missing":
        return ("critical", "missing_load", f"Load missing for {dataset_name}",
                "Investigate the source producer and trigger a backfill.")
    if status == "late":
        return ("warning", "sla_breach",
                f"Load completed in {completion_minutes} minutes for {dataset_name}",
                f"Review the pipeline; completion exceeded the {sla_minutes}-minute SLA.")
    if status == "incomplete":
        return ("warning", "incomplete_load", f"Load incomplete for {dataset_name}",
                "Check whether the source has finished publishing.")
    return None
