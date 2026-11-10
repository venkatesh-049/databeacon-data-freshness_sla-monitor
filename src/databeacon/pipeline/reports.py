import json
from pathlib import Path

from databeacon.db.connection import connect
from databeacon.db.repository import dashboard_data, report_data


def write_report(db_path: Path, output_path: Path) -> dict:
    connection = connect(db_path)
    data = report_data(connection)
    connection.close()
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(data, indent=2, default=str), encoding="utf-8")
    return data


def reconciliation(db_path: Path) -> dict:
    connection = connect(db_path)
    run_rows = connection.execute(
        """SELECT run_id, input_count, accepted_count, rejected_count,
                  duplicate_count, updated_count, status FROM runs ORDER BY started_at"""
    ).fetchall()
    event_keys = connection.execute(
        "SELECT COUNT(*) AS total, COUNT(DISTINCT event_key) AS unique_keys FROM arrival_events"
    ).fetchone()
    result = {
        "runs": [dict(row) for row in run_rows],
        "input_count": sum(row["input_count"] for row in run_rows),
        "accepted_count": sum(row["accepted_count"] for row in run_rows),
        "rejected_count": sum(row["rejected_count"] for row in run_rows),
        "duplicate_count": sum(row["duplicate_count"] for row in run_rows),
        "updated_count": sum(row["updated_count"] for row in run_rows),
        "published_event_count": event_keys["total"],
        "unique_event_key_count": event_keys["unique_keys"],
    }
    result["counts_reconcile"] = (
        result["input_count"] == result["accepted_count"] + result["rejected_count"] +
        result["duplicate_count"]
        or any(row["status"] == "skipped" for row in result["runs"])
    )
    result["unique_keys_reconcile"] = (
        result["published_event_count"] == result["unique_event_key_count"]
    )
    connection.close()
    return result
