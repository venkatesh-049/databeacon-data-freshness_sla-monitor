import hashlib
import sqlite3
from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path

from databeacon.db.connection import connect
from databeacon.db.repository import dataset_map, seed_datasets
from databeacon.domain.rules import utc_now
from databeacon.observability import logger


def _window_key(dataset_id: str, expected_at: datetime) -> str:
    identity = f"{dataset_id}|{expected_at.isoformat()}"
    return hashlib.sha256(identity.encode("utf-8")).hexdigest()


def _event_for_window(connection: sqlite3.Connection, dataset_id: str,
                      window_date: str) -> sqlite3.Row | None:
    return connection.execute(
        """SELECT event_key, expected_at, arrived_at, completed_at, sla_status
           FROM arrival_events
           WHERE dataset_id=? AND substr(expected_at, 1, 10)=?
           ORDER BY (arrived_at IS NOT NULL) DESC,
                    (completed_at IS NOT NULL) DESC,
                    completed_at DESC
           LIMIT 1""",
        (dataset_id, window_date),
    ).fetchone()


def _persist_alert(connection: sqlite3.Connection, *, window_key: str,
                   dataset_id: str, severity: str, alert_type: str,
                   message: str, action: str, created_at: str,
                   event_key: str | None = None) -> bool:
    cursor = connection.execute(
        """INSERT OR IGNORE INTO alerts
           (run_id, event_key, window_key, dataset_id, severity, alert_type,
            message, action, created_at)
           VALUES (NULL, ?, ?, ?, ?, ?, ?, ?, ?)""",
        (event_key, window_key, dataset_id, severity, alert_type, message, action, created_at),
    )
    return cursor.rowcount == 1


def monitor_expected_windows(db_path: Path, now: datetime | None = None,
                             arrival_grace_minutes: int = 15) -> dict[str, int]:
    checked_at = now or utc_now()
    if checked_at.tzinfo is None or checked_at.utcoffset() is None:
        raise ValueError("monitor time must include a timezone")
    if arrival_grace_minutes < 0:
        raise ValueError("arrival grace minutes must be non-negative")
    checked_at = checked_at.astimezone(timezone.utc)
    connection = connect(db_path)
    created_missing = created_incomplete = resolved_alerts = 0
    status_counts = {"scheduled": 0, "arrived": 0, "fresh": 0,
                     "late": 0, "incomplete": 0, "missing": 0}
    try:
        seed_datasets(connection)
        datasets = dataset_map(connection)
        for day_offset in (1, 0):
            window_date = (checked_at.date() - timedelta(days=day_offset)).isoformat()
            window_day = date.fromisoformat(window_date)
            for dataset_id, dataset in datasets.items():
                if not dataset["active"]:
                    continue
                expected_at = datetime.combine(
                    window_day,
                    time(dataset["expected_hour_utc"], dataset["expected_minute_utc"]),
                    tzinfo=timezone.utc,
                )
                key = _window_key(dataset_id, expected_at)
                arrival_deadline = expected_at + timedelta(minutes=arrival_grace_minutes)
                completion_deadline = expected_at + timedelta(minutes=dataset["sla_minutes"])
                connection.execute(
                    """INSERT INTO expected_windows
                       (window_key, dataset_id, expected_at, arrival_deadline_at,
                        completion_deadline_at, status, last_checked_at)
                       VALUES (?, ?, ?, ?, ?, 'scheduled', ?)
                       ON CONFLICT(window_key) DO UPDATE SET
                           arrival_deadline_at=excluded.arrival_deadline_at,
                           completion_deadline_at=excluded.completion_deadline_at""",
                    (key, dataset_id, expected_at.isoformat(), arrival_deadline.isoformat(),
                     completion_deadline.isoformat(), checked_at.isoformat()),
                )
                event = _event_for_window(connection, dataset_id, window_date)
                status = "scheduled"
                if event is not None and event["arrived_at"]:
                    arrived_at = datetime.fromisoformat(event["arrived_at"]).astimezone(timezone.utc)
                    if arrived_at <= checked_at:
                        resolved = connection.execute(
                            """UPDATE alerts SET resolved_at=?
                               WHERE window_key=? AND alert_type='missing_load'
                                 AND resolved_at IS NULL""",
                            (checked_at.isoformat(), key),
                        ).rowcount
                        resolved_alerts += resolved
                        if event["completed_at"]:
                            completed_at = datetime.fromisoformat(event["completed_at"]).astimezone(timezone.utc)
                            if completed_at <= checked_at:
                                status = "fresh" if completed_at <= completion_deadline else "late"
                                resolved = connection.execute(
                                    """UPDATE alerts SET resolved_at=?
                                       WHERE window_key=? AND alert_type='incomplete_load'
                                         AND resolved_at IS NULL""",
                                    (checked_at.isoformat(), key),
                                ).rowcount
                                resolved_alerts += resolved
                            elif checked_at > completion_deadline:
                                status = "incomplete"
                        elif checked_at > completion_deadline:
                            status = "incomplete"
                        else:
                            status = "arrived"
                if status == "scheduled" and checked_at >= arrival_deadline:
                    status = "missing"
                    if _persist_alert(
                        connection, window_key=key, dataset_id=dataset_id,
                        severity="critical", alert_type="missing_load",
                        message=f"No arrival observed for {dataset['dataset_name']} by "
                                f"{arrival_deadline.isoformat()}",
                        action="Investigate the source producer and trigger a backfill.",
                        created_at=checked_at.isoformat(),
                        event_key=event["event_key"] if event else None,
                    ):
                        created_missing += 1
                if status == "incomplete":
                    if _persist_alert(
                        connection, window_key=key, dataset_id=dataset_id,
                        severity="warning", alert_type="incomplete_load",
                        message=f"{dataset['dataset_name']} has not completed by its SLA deadline "
                                f"{completion_deadline.isoformat()}",
                        action="Check the producer and processing job, then complete or backfill the load.",
                        created_at=checked_at.isoformat(),
                        event_key=event["event_key"] if event else None,
                    ):
                        created_incomplete += 1
                connection.execute(
                    """UPDATE expected_windows
                       SET status=?, last_checked_at=? WHERE window_key=?""",
                    (status, checked_at.isoformat(), key),
                )
                status_counts[status] += 1
        connection.commit()
        result = {
            "windows_checked": sum(status_counts.values()),
            "missing_alerts_created": created_missing,
            "incomplete_alerts_created": created_incomplete,
            "alerts_resolved": resolved_alerts,
            **{f"{status}_windows": count for status, count in status_counts.items()},
        }
        logger.info("sla_monitor_completed checked_at=%s %s", checked_at.isoformat(), result)
        return result
    except Exception:
        connection.rollback()
        logger.exception("sla_monitor_failed checked_at=%s", checked_at.isoformat())
        raise
    finally:
        connection.close()
