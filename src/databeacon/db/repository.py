import json
import sqlite3
from datetime import datetime, timezone
from typing import Any


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
               error_message: str | None = None, duplicate_count: int = 0) -> None:
    connection.execute(
        """UPDATE runs SET ended_at=?, status=?, input_count=?, accepted_count=?,
           rejected_count=?, duplicate_count=?, error_message=? WHERE run_id=?""",
        (ended_at, status, input_count, accepted_count, rejected_count, duplicate_count,
         error_message, run_id),
    )
    connection.commit()


def insert_event(connection: sqlite3.Connection, event: dict[str, Any]) -> bool:
    cursor = connection.execute(
        """INSERT OR IGNORE INTO arrival_events
           (event_key,run_id,dataset_id,source_file,expected_at,arrived_at,completed_at,
            row_count,freshness_minutes,completion_minutes,sla_status,raw_payload)
           VALUES (:event_key,:run_id,:dataset_id,:source_file,:expected_at,:arrived_at,
                   :completed_at,:row_count,:freshness_minutes,:completion_minutes,
                   :sla_status,:raw_payload)""",
        event,
    )
    return cursor.rowcount == 1


def insert_reject(connection: sqlite3.Connection, reject: dict[str, Any]) -> None:
    connection.execute(
        """INSERT INTO rejects(run_id,source_file,source_row,reason_code,reason_detail,raw_payload)
           VALUES (:run_id,:source_file,:source_row,:reason_code,:reason_detail,:raw_payload)""",
        reject,
    )


def insert_alert(connection: sqlite3.Connection, alert: dict[str, Any]) -> None:
    connection.execute(
        """INSERT INTO alerts(run_id,event_key,dataset_id,severity,alert_type,message,action,created_at)
           VALUES (:run_id,:event_key,:dataset_id,:severity,:alert_type,:message,:action,:created_at)""",
        alert,
    )


def run_dataset_health(connection: sqlite3.Connection, run_id: str) -> list[dict[str, Any]]:
    rows = [dict(row) for row in connection.execute(
        """SELECT d.dataset_id, d.dataset_name, e.expected_at, e.arrived_at,
                  e.completed_at, e.sla_status, e.completion_minutes
           FROM datasets d LEFT JOIN arrival_events e
             ON e.dataset_id=d.dataset_id AND e.run_id=?
           WHERE d.active=1
           ORDER BY d.dataset_id""", (run_id,))]
    for row in rows:
        if row["sla_status"] is None:
            row["sla_status"] = "missing"
            row["status_detail"] = "No arrival event was observed in this run."
        else:
            row["status_detail"] = "Arrival event published in this run."
    return rows


def dashboard_data(connection: sqlite3.Connection) -> dict[str, Any]:
    latest_run = connection.execute(
        "SELECT * FROM runs ORDER BY started_at DESC LIMIT 1"
    ).fetchone()
    latest_run_id = latest_run["run_id"] if latest_run else None
    datasets = [dict(row) for row in connection.execute(
        """SELECT d.dataset_id, d.dataset_name, e.expected_at, e.arrived_at, e.completed_at,
                  e.sla_status, e.completion_minutes
           FROM datasets d LEFT JOIN arrival_events e
             ON e.dataset_id=d.dataset_id AND e.run_id=?
           WHERE d.active=1
           ORDER BY d.dataset_id""", (latest_run_id,))]
    now = datetime.now(timezone.utc)
    has_run = latest_run is not None
    for dataset in datasets:
        if dataset["sla_status"] is None:
            expected = now.replace(
                hour=connection.execute(
                    "SELECT expected_hour_utc FROM datasets WHERE dataset_id=?",
                    (dataset["dataset_id"],),
                ).fetchone()[0],
                minute=connection.execute(
                    "SELECT expected_minute_utc FROM datasets WHERE dataset_id=?",
                    (dataset["dataset_id"],),
                ).fetchone()[0],
                second=0,
                microsecond=0,
            )
            dataset["expected_at"] = expected.isoformat()
            dataset["sla_status"] = "missing" if has_run or now >= expected else "scheduled"
            dataset["status_detail"] = (
                "No arrival event was observed in the latest run."
                if has_run else "No arrival event has been observed for the current window."
            )
        else:
            dataset["status_detail"] = "Latest published arrival event."
    alerts = [dict(row) for row in connection.execute(
        "SELECT * FROM alerts WHERE acknowledged=0 AND run_id=? ORDER BY created_at DESC LIMIT 100",
        (latest_run_id,))] if latest_run_id else []
    for dataset in datasets:
        if dataset["sla_status"] == "missing":
            alerts.append({
                "alert_id": None,
                "dataset_id": dataset["dataset_id"],
                "severity": "critical",
                "alert_type": "missing_load",
                "message": f"No arrival observed for {dataset['dataset_name']}",
                "action": "Investigate the source producer and trigger a backfill.",
                "acknowledged": 0,
            })
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
        "UPDATE alerts SET acknowledged=1 WHERE alert_id=? AND acknowledged=0",
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
