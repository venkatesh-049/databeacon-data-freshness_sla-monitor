import json
import sqlite3
from datetime import datetime, timezone
from typing import Any

from databeacon.domain.rules import minutes_between, sla_status


def seed_datasets(connection: sqlite3.Connection) -> None:
    connection.executemany(
        """INSERT OR IGNORE INTO datasets
           (dataset_id, dataset_name, expected_hour_utc, expected_minute_utc, sla_minutes)
           VALUES (?, ?, ?, ?, ?)""",
        [
            ("sales_daily", "Daily Sales", 6, 0, 120),
            ("inventory_daily", "Daily Inventory", 7, 0, 180),
            ("customers_daily", "Daily Customers", 8, 0, 90),
            ("orders_daily", "Daily Orders", 6, 0, 120),
            ("payments_daily", "Daily Payments", 7, 0, 120),
            ("products_daily", "Daily Products", 8, 0, 180),
            ("shipments_daily", "Daily Shipments", 9, 0, 180),
            ("returns_daily", "Daily Returns", 10, 0, 240),
            ("promotions_daily", "Daily Promotions", 10, 0, 240),
            ("suppliers_daily", "Daily Suppliers", 11, 0, 240),
            ("finance_daily", "Daily Finance", 12, 0, 180),
            ("support_daily", "Daily Support", 13, 0, 180),
        ],
    )
    connection.commit()


def dataset_map(connection: sqlite3.Connection) -> dict[str, sqlite3.Row]:
    return {row["dataset_id"]: row for row in connection.execute("SELECT * FROM datasets")}


def create_run(connection: sqlite3.Connection, run_id: str, fingerprint: str,
               source_file: str, started_at: str) -> None:
    connection.execute(
        "INSERT INTO runs(run_id,input_fingerprint,source_file,started_at,status) VALUES(?,?,?,?,?)",
        (run_id, fingerprint, source_file, started_at, "running"),
    )
    connection.commit()


def existing_fingerprint(connection: sqlite3.Connection, fingerprint: str) -> bool:
    return connection.execute(
        "SELECT 1 FROM runs WHERE input_fingerprint = ? AND status IN ('succeeded','partial')",
        (fingerprint,),
    ).fetchone() is not None


def finish_run(connection: sqlite3.Connection, run_id: str, ended_at: str, status: str,
               input_count: int, accepted_count: int, rejected_count: int,
               error_message: str | None = None, duplicate_count: int = 0,
               updated_count: int = 0) -> None:
    connection.execute(
        """UPDATE runs SET ended_at=?, status=?, input_count=?, accepted_count=?,
           rejected_count=?, duplicate_count=?, updated_count=?, error_message=? WHERE run_id=?""",
        (ended_at, status, input_count, accepted_count, rejected_count, duplicate_count, updated_count,
         error_message, run_id),
    )
    connection.commit()


def insert_event(connection: sqlite3.Connection, event: dict[str, Any]) -> dict[str, Any]:
    existing = connection.execute(
        "SELECT * FROM arrival_events WHERE event_key=?", (event["event_key"],)
    ).fetchone()
    observed = dict(event)
    action = "inserted"
    if existing is None:
        connection.execute(
            """INSERT INTO arrival_events
               (event_key,run_id,dataset_id,source_file,expected_at,arrived_at,completed_at,
                row_count,freshness_minutes,completion_minutes,sla_status,raw_payload)
               VALUES (:event_key,:run_id,:dataset_id,:source_file,:expected_at,:arrived_at,
                       :completed_at,:row_count,:freshness_minutes,:completion_minutes,
                       :sla_status,:raw_payload)""",
            event,
        )
    else:
        arrival = event["arrived_at"] or existing["arrived_at"]
        completion = event["completed_at"] or existing["completed_at"]
        if arrival and completion and datetime.fromisoformat(completion) < datetime.fromisoformat(arrival):
            return {"action": "conflict", "sla_status": existing["sla_status"],
                    "completion_minutes": existing["completion_minutes"]}
        changed = (
            arrival != existing["arrived_at"]
            or completion != existing["completed_at"]
            or event["row_count"] != existing["row_count"]
        )
        if not changed:
            return {"action": "duplicate", "sla_status": existing["sla_status"],
                    "completion_minutes": existing["completion_minutes"]}

        action = "updated"
        observed.update(arrived_at=arrival, completed_at=completion)
        if arrival != existing["arrived_at"] or completion != existing["completed_at"]:
            expected_at = datetime.fromisoformat(event["expected_at"])
            arrived_at = datetime.fromisoformat(arrival) if arrival else None
            completed_at = datetime.fromisoformat(completion) if completion else None
            observed["sla_status"] = sla_status(
                arrived_at, completed_at, expected_at, event["sla_minutes"],
                datetime.fromisoformat(event["observed_at"]),
            )
            observed["freshness_minutes"] = (
                minutes_between(arrived_at, datetime.fromisoformat(event["observed_at"]))
                if arrived_at else None
            )
            observed["completion_minutes"] = (
                minutes_between(expected_at, completed_at) if completed_at else None
            )
        else:
            observed["sla_status"] = existing["sla_status"]
            observed["freshness_minutes"] = existing["freshness_minutes"]
            observed["completion_minutes"] = existing["completion_minutes"]
        connection.execute(
            """UPDATE arrival_events SET arrived_at=?, completed_at=?, row_count=?,
                       freshness_minutes=?, completion_minutes=?, sla_status=?, raw_payload=?
               WHERE event_key=?""",
            (observed["arrived_at"], observed["completed_at"], event["row_count"],
             observed["freshness_minutes"], observed["completion_minutes"],
             observed["sla_status"], event["raw_payload"], event["event_key"]),
        )

    connection.execute(
        """INSERT INTO event_observations
           (event_key,run_id,dataset_id,source_file,expected_at,arrived_at,completed_at,
            row_count,freshness_minutes,completion_minutes,sla_status,raw_payload,observed_at)
           VALUES (:event_key,:run_id,:dataset_id,:source_file,:expected_at,:arrived_at,
                   :completed_at,:row_count,:freshness_minutes,:completion_minutes,
                   :sla_status,:raw_payload,:observed_at)""",
        observed,
    )
    if observed["arrived_at"]:
        connection.execute(
            """UPDATE alerts SET resolved_at=?
               WHERE event_key=? AND alert_type='missing_load' AND resolved_at IS NULL""",
            (event["observed_at"], event["event_key"]),
        )
    if observed["completed_at"]:
        connection.execute(
            """UPDATE alerts SET resolved_at=?
               WHERE event_key=? AND alert_type='incomplete_load' AND resolved_at IS NULL""",
            (event["observed_at"], event["event_key"]),
        )
    return {
        "action": action,
        "sla_status": observed["sla_status"],
        "completion_minutes": observed["completion_minutes"],
    }


def insert_reject(connection: sqlite3.Connection, reject: dict[str, Any]) -> None:
    connection.execute(
        """INSERT INTO rejects(run_id,source_file,source_row,reason_code,reason_detail,raw_payload)
           VALUES (:run_id,:source_file,:source_row,:reason_code,:reason_detail,:raw_payload)""",
        reject,
    )


def insert_duplicate(connection: sqlite3.Connection, duplicate: dict[str, Any]) -> None:
    connection.execute(
        """INSERT INTO duplicate_records
           (run_id, dataset_id, event_key, source_file, source_row, reason)
           VALUES (?, ?, ?, ?, ?, ?)""",
        (duplicate["run_id"], duplicate["dataset_id"], duplicate["event_key"],
         duplicate["source_file"], duplicate["source_row"], duplicate["reason"]),
    )


def insert_alert(connection: sqlite3.Connection, alert: dict[str, Any]) -> None:
    if alert.get("event_key") and connection.execute(
        """SELECT 1 FROM alerts WHERE event_key=? AND alert_type=? AND resolved_at IS NULL""",
        (alert["event_key"], alert["alert_type"]),
    ).fetchone():
        return
    connection.execute(
        """INSERT INTO alerts(run_id,event_key,dataset_id,severity,alert_type,message,action,created_at)
           VALUES (:run_id,:event_key,:dataset_id,:severity,:alert_type,:message,:action,:created_at)""",
        alert,
    )


def run_dataset_health(connection: sqlite3.Connection, run_id: str) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    priority = {"missing": 0, "fresh": 1, "late": 2, "incomplete": 3}
    for dataset in connection.execute(
        "SELECT dataset_id, dataset_name FROM datasets WHERE active=1 ORDER BY dataset_id"
    ):
        observations = [dict(row) for row in connection.execute(
            """SELECT expected_at, arrived_at, completed_at, sla_status,
                      completion_minutes
               FROM event_observations
               WHERE run_id=? AND dataset_id=?
               ORDER BY expected_at""",
            (run_id, dataset["dataset_id"]),
        )]
        if not observations:
            rows.append({
                "dataset_id": dataset["dataset_id"],
                "dataset_name": dataset["dataset_name"],
                "expected_at": None,
                "arrived_at": None,
                "completed_at": None,
                "sla_status": "missing",
                "completion_minutes": None,
                "event_count": 0,
                "status_detail": "No arrival event was observed in this run.",
            })
            continue
        latest = observations[-1]
        status = max(
            (item["sla_status"] for item in observations),
            key=lambda value: priority[value],
        )
        rows.append({
            "dataset_id": dataset["dataset_id"],
            "dataset_name": dataset["dataset_name"],
            "expected_at": observations[0]["expected_at"],
            "arrived_at": latest["arrived_at"],
            "completed_at": latest["completed_at"],
            "sla_status": status,
            "completion_minutes": latest["completion_minutes"],
            "event_count": len(observations),
            "status_detail": f"{len(observations)} arrival record(s) in this run.",
        })
    return rows


def dashboard_data(connection: sqlite3.Connection) -> dict[str, Any]:
    latest_run = connection.execute(
        "SELECT * FROM runs ORDER BY started_at DESC LIMIT 1"
    ).fetchone()
    latest_run_id = latest_run["run_id"] if latest_run else None
    now = datetime.now(timezone.utc)
    current_date = now.date().isoformat()
    datasets: list[dict[str, Any]] = []
    for config in connection.execute("SELECT * FROM datasets WHERE active=1 ORDER BY dataset_id"):
        expected = now.replace(
            hour=config["expected_hour_utc"],
            minute=config["expected_minute_utc"],
            second=0,
            microsecond=0,
        )
        window = connection.execute(
            """SELECT status, expected_at, arrival_deadline_at, completion_deadline_at
               FROM expected_windows
               WHERE dataset_id=? AND expected_at=?""",
            (config["dataset_id"], expected.isoformat()),
        ).fetchone()
        event = connection.execute(
            """SELECT expected_at, arrived_at, completed_at, completion_minutes, sla_status
               FROM arrival_events
               WHERE dataset_id=? AND substr(expected_at, 1, 10)=?
               ORDER BY (arrived_at IS NOT NULL) DESC,
                        (completed_at IS NOT NULL) DESC,
                        completed_at DESC
               LIMIT 1""",
            (config["dataset_id"], current_date),
        ).fetchone()
        status = window["status"] if window else (
            "scheduled" if now < expected else "missing"
        )
        datasets.append({
            "dataset_id": config["dataset_id"],
            "dataset_name": config["dataset_name"],
            "expected_at": window["expected_at"] if window else expected.isoformat(),
            "arrival_deadline_at": window["arrival_deadline_at"] if window else None,
            "completion_deadline_at": window["completion_deadline_at"] if window else None,
            "arrived_at": event["arrived_at"] if event else None,
            "completed_at": event["completed_at"] if event else None,
            "completion_minutes": event["completion_minutes"] if event else None,
            "event_status": event["sla_status"] if event else None,
            "sla_status": status,
            "status_detail": f"Current expected window status: {status}.",
        })
    alerts = [dict(row) for row in connection.execute(
        """SELECT * FROM alerts
           WHERE acknowledged=0 AND resolved_at IS NULL
             AND (window_key IS NOT NULL OR run_id=?)
           ORDER BY created_at DESC LIMIT 100""",
        (latest_run_id,))]
    runs = [dict(row) for row in connection.execute(
        "SELECT * FROM runs ORDER BY started_at DESC")]
    for run in runs:
        run["datasets"] = run_dataset_health(connection, run["run_id"])
    attention_statuses = {"late", "missing", "incomplete"}
    attention_count = sum(dataset["sla_status"] in attention_statuses for dataset in datasets)
    healthy_count = sum(dataset["sla_status"] == "fresh" for dataset in datasets)
    dataset_count = len(datasets)
    return {
        "datasets": datasets,
        "alerts": alerts,
        "runs": runs,
        "latest_run": dict(latest_run) if latest_run else None,
        "summary": {
            "datasets_monitored": dataset_count,
            "datasets_healthy": healthy_count,
            "datasets_needing_attention": attention_count,
            "health_percent": round(healthy_count / dataset_count * 100) if dataset_count else 0,
            "open_alerts": len(alerts),
        },
    }


def list_rejects(connection: sqlite3.Connection) -> list[dict[str, Any]]:
    return [dict(row) for row in connection.execute(
        "SELECT * FROM rejects ORDER BY reject_id DESC LIMIT 100"
    )]


def list_alerts(connection: sqlite3.Connection) -> list[dict[str, Any]]:
    return [dict(row) for row in connection.execute(
        "SELECT * FROM alerts ORDER BY created_at DESC LIMIT 100"
    )]


def acknowledge_alert(connection: sqlite3.Connection, alert_id: int) -> bool:
    cursor = connection.execute(
        """UPDATE alerts SET acknowledged=1
           WHERE alert_id=? AND acknowledged=0 AND resolved_at IS NULL""",
        (alert_id,),
    )
    connection.commit()
    return cursor.rowcount == 1


def report_data(connection: sqlite3.Connection) -> dict[str, Any]:
    run = connection.execute("SELECT * FROM runs ORDER BY started_at DESC LIMIT 1").fetchone()
    status_counts = {row["sla_status"]: row["count"] for row in connection.execute(
        "SELECT sla_status, COUNT(*) AS count FROM arrival_events GROUP BY sla_status")}
    return {
        "latest_run": dict(run) if run else None,
        "status_counts": status_counts,
        "published_events": connection.execute("SELECT COUNT(*) FROM arrival_events").fetchone()[0],
        "rejects": connection.execute("SELECT COUNT(*) FROM rejects").fetchone()[0],
        "alerts": connection.execute("SELECT COUNT(*) FROM alerts").fetchone()[0],
    }
