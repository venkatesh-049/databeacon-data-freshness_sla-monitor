import sqlite3
from pathlib import Path

SCHEMA_PATH = Path(__file__).with_name("schema.sql")
SCHEMA_VERSION = 1


def connect(db_path: Path) -> sqlite3.Connection:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(db_path)
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA busy_timeout = 5000")
    connection.execute("PRAGMA foreign_keys = ON")
    connection.executescript(SCHEMA_PATH.read_text(encoding="utf-8"))
    _migrate_alerts(connection)
    columns = {row[1] for row in connection.execute("PRAGMA table_info(runs)")}
    if "duplicate_count" not in columns:
        connection.execute("ALTER TABLE runs ADD COLUMN duplicate_count INTEGER NOT NULL DEFAULT 0")
    if "updated_count" not in columns:
        connection.execute("ALTER TABLE runs ADD COLUMN updated_count INTEGER NOT NULL DEFAULT 0")
    schema_version = connection.execute("PRAGMA user_version").fetchone()[0]
    if schema_version < SCHEMA_VERSION:
        _migrate_event_observations(connection)
        connection.execute(f"PRAGMA user_version = {SCHEMA_VERSION}")
    connection.commit()
    return connection


def _migrate_alerts(connection: sqlite3.Connection) -> None:
    columns = {row[1]: row for row in connection.execute("PRAGMA table_info(alerts)")}
    if not columns:
        return
    needs_rebuild = (
        columns["run_id"][3] != 0
        or "window_key" not in columns
        or "resolved_at" not in columns
    )
    if needs_rebuild:
        connection.execute("DROP INDEX IF EXISTS ix_alerts_open")
        connection.execute("ALTER TABLE alerts RENAME TO alerts_legacy")
        connection.execute(
            """CREATE TABLE alerts (
                alert_id INTEGER PRIMARY KEY AUTOINCREMENT,
                run_id TEXT REFERENCES runs(run_id),
                event_key TEXT,
                window_key TEXT REFERENCES expected_windows(window_key),
                dataset_id TEXT NOT NULL REFERENCES datasets(dataset_id),
                severity TEXT NOT NULL CHECK(severity IN ('info', 'warning', 'critical')),
                alert_type TEXT NOT NULL,
                message TEXT NOT NULL,
                action TEXT NOT NULL,
                created_at TEXT NOT NULL,
                acknowledged INTEGER NOT NULL DEFAULT 0 CHECK(acknowledged IN (0, 1)),
                resolved_at TEXT
            )"""
        )
        connection.execute(
            """INSERT INTO alerts
               (alert_id, run_id, event_key, dataset_id, severity, alert_type,
                message, action, created_at, acknowledged)
               SELECT alert_id, run_id, event_key, dataset_id, severity, alert_type,
                      message, action, created_at, acknowledged
               FROM alerts_legacy"""
        )
        connection.execute("DROP TABLE alerts_legacy")
    open_index = connection.execute(
        "SELECT sql FROM sqlite_master WHERE type='index' AND name='ix_alerts_open'"
    ).fetchone()
    if open_index is None or "resolved_at" not in open_index["sql"].lower():
        connection.execute("DROP INDEX IF EXISTS ix_alerts_open")
        connection.execute(
            "CREATE INDEX ix_alerts_open ON alerts(acknowledged, resolved_at, severity, created_at)"
        )
    connection.execute(
        """CREATE UNIQUE INDEX IF NOT EXISTS uq_alert_window_type
           ON alerts(window_key, alert_type) WHERE window_key IS NOT NULL"""
    )
    connection.commit()


def _migrate_event_observations(connection: sqlite3.Connection) -> None:
    connection.execute(
        """INSERT OR IGNORE INTO event_observations
           (event_key, run_id, dataset_id, source_file, expected_at, arrived_at,
            completed_at, row_count, freshness_minutes, completion_minutes,
            sla_status, raw_payload, observed_at)
           SELECT e.event_key, e.run_id, e.dataset_id, e.source_file, e.expected_at,
                  e.arrived_at, e.completed_at, e.row_count, e.freshness_minutes,
                  e.completion_minutes, e.sla_status, e.raw_payload, r.started_at
           FROM arrival_events e JOIN runs r ON r.run_id=e.run_id"""
    )
