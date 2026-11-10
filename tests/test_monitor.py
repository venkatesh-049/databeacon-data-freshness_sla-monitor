import csv
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

from databeacon.db.connection import connect
from databeacon.db.repository import seed_datasets
from databeacon.pipeline.ingestion import run_pipeline
from databeacon.pipeline.monitor import monitor_expected_windows


def test_missing_alert_is_persisted_idempotently_and_resolved_on_arrival(tmp_path: Path):
    database = tmp_path / "monitor.sqlite"
    before_deadline = datetime(2026, 4, 15, 6, 10, tzinfo=timezone.utc)
    after_deadline = datetime(2026, 4, 15, 6, 16, tzinfo=timezone.utc)
    with connect(database) as connection:
        seed_datasets(connection)
        connection.execute("UPDATE datasets SET active=0 WHERE dataset_id!='sales_daily'")
        connection.commit()

    before = monitor_expected_windows(database, before_deadline, arrival_grace_minutes=15)
    with connect(database) as connection:
        sales_window = connection.execute(
            """SELECT status, arrival_deadline_at FROM expected_windows
               WHERE dataset_id='sales_daily' AND expected_at='2026-04-15T06:00:00+00:00'"""
        ).fetchone()
        assert sales_window["status"] == "scheduled"
    monitor_expected_windows(database, before_deadline, arrival_grace_minutes=30)
    with connect(database) as connection:
        deadline = connection.execute(
            """SELECT arrival_deadline_at FROM expected_windows
               WHERE dataset_id='sales_daily' AND expected_at='2026-04-15T06:00:00+00:00'"""
        ).fetchone()["arrival_deadline_at"]
        assert deadline == "2026-04-15T06:30:00+00:00"

    first_check = monitor_expected_windows(database, after_deadline, arrival_grace_minutes=15)
    repeated_check = monitor_expected_windows(database, after_deadline, arrival_grace_minutes=15)
    assert first_check["missing_alerts_created"] == 1
    assert repeated_check["missing_alerts_created"] == 0

    source = tmp_path / "arrival.csv"
    with source.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(
            handle, fieldnames=["dataset_id", "expected_at", "arrived_at", "completed_at", "row_count"]
        )
        writer.writeheader()
        writer.writerow({
            "dataset_id": "sales_daily",
            "expected_at": "2026-04-15T06:00:00+00:00",
            "arrived_at": "2026-04-15T06:20:00+00:00",
            "completed_at": "2026-04-15T07:10:00+00:00",
            "row_count": "3",
        })
    assert run_pipeline(database, source)["accepted_count"] == 1

    resolved = monitor_expected_windows(
        database, datetime(2026, 4, 15, 7, 15, tzinfo=timezone.utc), arrival_grace_minutes=15
    )
    assert resolved["alerts_resolved"] == 1
    with connect(database) as connection:
        sales = connection.execute(
            """SELECT w.status, a.resolved_at
               FROM expected_windows w JOIN alerts a ON a.window_key=w.window_key
               WHERE w.dataset_id='sales_daily' AND w.expected_at='2026-04-15T06:00:00+00:00'
                 AND a.alert_type='missing_load'"""
        ).fetchone()
        assert sales["status"] == "fresh"
        assert sales["resolved_at"] == "2026-04-15T07:15:00+00:00"

    completed = monitor_expected_windows(
        database, datetime(2026, 4, 15, 8, 1, tzinfo=timezone.utc), arrival_grace_minutes=15
    )
    assert completed["fresh_windows"] >= 1


def test_completion_deadline_persists_incomplete_alert_and_acknowledgement(tmp_path: Path):
    database = tmp_path / "incomplete.sqlite"
    source = tmp_path / "incomplete.csv"
    source.write_text(
        "dataset_id,expected_at,arrived_at,completed_at,row_count\n"
        "inventory_daily,2026-04-15T07:00:00+00:00,2026-04-15T07:10:00+00:00,,4\n",
        encoding="utf-8",
    )
    run_pipeline(database, source)
    with connect(database) as connection:
        connection.execute("UPDATE datasets SET active=0 WHERE dataset_id!='inventory_daily'")
        connection.commit()
    result = monitor_expected_windows(
        database, datetime(2026, 4, 15, 10, 1, tzinfo=timezone.utc), arrival_grace_minutes=15
    )
    assert result["incomplete_alerts_created"] == 1
    assert monitor_expected_windows(
        database, datetime(2026, 4, 15, 10, 2, tzinfo=timezone.utc), arrival_grace_minutes=15
    )["incomplete_alerts_created"] == 0
    with connect(database) as connection:
        alert = connection.execute(
            """SELECT alert_id FROM alerts
               WHERE dataset_id='inventory_daily' AND alert_type='incomplete_load'
                 AND window_key IS NOT NULL"""
        ).fetchone()
        assert alert is not None
        alert_id = alert["alert_id"]
        assert connection.execute(
            "UPDATE alerts SET acknowledged=1 WHERE alert_id=? AND resolved_at IS NULL",
            (alert_id,),
        ).rowcount == 1
        connection.commit()
        assert connection.execute(
            "SELECT acknowledged FROM alerts WHERE alert_id=?", (alert_id,)
        ).fetchone()[0] == 1

    completion_source = tmp_path / "completed.csv"
    completion_source.write_text(
        "dataset_id,expected_at,arrived_at,completed_at,row_count\n"
        "inventory_daily,2026-04-15T07:00:00+00:00,2026-04-15T07:10:00+00:00,"
        "2026-04-15T09:00:00+00:00,4\n",
        encoding="utf-8",
    )
    updated = run_pipeline(database, completion_source)
    assert updated["accepted_count"] == 1
    assert updated["updated_count"] == 1
    monitor_expected_windows(
        database, datetime(2026, 4, 15, 10, 5, tzinfo=timezone.utc), arrival_grace_minutes=15
    )
    with connect(database) as connection:
        assert connection.execute(
            "SELECT resolved_at FROM alerts WHERE alert_id=?", (alert_id,)
        ).fetchone()["resolved_at"] is not None
        assert connection.execute(
            "SELECT COUNT(*) FROM event_observations WHERE dataset_id='inventory_daily'"
        ).fetchone()[0] == 2
        snapshots = connection.execute(
            """SELECT sla_status FROM event_observations
               WHERE dataset_id='inventory_daily' ORDER BY observed_at"""
        ).fetchall()
        assert [row["sla_status"] for row in snapshots] == ["incomplete", "fresh"]


def test_legacy_alert_table_migrates_without_losing_existing_alerts(tmp_path: Path):
    database = tmp_path / "legacy.sqlite"
    legacy = sqlite3.connect(database)
    legacy.executescript(
        """        CREATE TABLE datasets(
            dataset_id TEXT PRIMARY KEY, dataset_name TEXT NOT NULL UNIQUE,
            expected_hour_utc INTEGER NOT NULL, expected_minute_utc INTEGER NOT NULL,
            sla_minutes INTEGER NOT NULL, active INTEGER NOT NULL DEFAULT 1
        );
        INSERT INTO datasets VALUES ('sales_daily', 'Daily Sales', 6, 0, 120, 1);
        CREATE TABLE runs(
            run_id TEXT PRIMARY KEY, input_fingerprint TEXT NOT NULL, source_file TEXT NOT NULL,
            started_at TEXT NOT NULL, ended_at TEXT, status TEXT NOT NULL,
            input_count INTEGER NOT NULL DEFAULT 0, accepted_count INTEGER NOT NULL DEFAULT 0,
            rejected_count INTEGER NOT NULL DEFAULT 0, error_message TEXT
        );
        INSERT INTO runs(run_id, input_fingerprint, source_file, started_at, status)
        VALUES ('legacy-run', 'fingerprint', 'legacy.csv', '2026-01-01T00:00:00+00:00', 'succeeded');
           CREATE TABLE alerts(
               alert_id INTEGER PRIMARY KEY AUTOINCREMENT,
               run_id TEXT NOT NULL REFERENCES runs(run_id),
               event_key TEXT,
               dataset_id TEXT NOT NULL REFERENCES datasets(dataset_id),
               severity TEXT NOT NULL,
               alert_type TEXT NOT NULL,
               message TEXT NOT NULL,
               action TEXT NOT NULL,
               created_at TEXT NOT NULL,
               acknowledged INTEGER NOT NULL DEFAULT 0
           );
           INSERT INTO alerts(run_id, dataset_id, severity, alert_type, message, action, created_at)
           VALUES ('legacy-run', 'sales_daily', 'warning', 'sla_breach', 'old', 'inspect', '2026-01-01T00:00:00+00:00');"""
    )
    legacy.close()

    with connect(database) as connection:
        alert = connection.execute("SELECT * FROM alerts").fetchone()
        run_id_column = next(row for row in connection.execute("PRAGMA table_info(alerts)")
                             if row["name"] == "run_id")
        assert alert["run_id"] == "legacy-run"
        assert "window_key" in alert.keys()
        assert "resolved_at" in alert.keys()
        assert run_id_column["notnull"] == 0


def test_existing_events_are_backfilled_into_observation_history(tmp_path: Path):
    database = tmp_path / "observations.sqlite"
    source = tmp_path / "events.csv"
    source.write_text(
        "dataset_id,expected_at,arrived_at,completed_at,row_count\n"
        "sales_daily,2026-04-15T06:00:00+00:00,2026-04-15T06:05:00+00:00,"
        "2026-04-15T07:00:00+00:00,2\n",
        encoding="utf-8",
    )
    run_pipeline(database, source)
    with connect(database) as connection:
        connection.execute("DROP TABLE event_observations")
        connection.execute("PRAGMA user_version=0")
        connection.commit()

    with connect(database) as connection:
        observation = connection.execute(
            """SELECT event_key, run_id, sla_status FROM event_observations
               WHERE dataset_id='sales_daily'"""
        ).fetchone()
        assert observation is not None
        assert observation["sla_status"] == "fresh"
