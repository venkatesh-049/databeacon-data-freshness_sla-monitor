PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS datasets (
    dataset_id TEXT PRIMARY KEY,
    dataset_name TEXT NOT NULL UNIQUE,
    expected_hour_utc INTEGER NOT NULL CHECK(expected_hour_utc BETWEEN 0 AND 23),
    expected_minute_utc INTEGER NOT NULL CHECK(expected_minute_utc BETWEEN 0 AND 59),
    sla_minutes INTEGER NOT NULL CHECK(sla_minutes > 0),
    active INTEGER NOT NULL DEFAULT 1 CHECK(active IN (0, 1))
);

CREATE TABLE IF NOT EXISTS runs (
    run_id TEXT PRIMARY KEY,
    input_fingerprint TEXT NOT NULL,
    source_file TEXT NOT NULL,
    started_at TEXT NOT NULL,
    ended_at TEXT,
    status TEXT NOT NULL CHECK(status IN ('running', 'succeeded', 'partial', 'failed')),
    input_count INTEGER NOT NULL DEFAULT 0,
    accepted_count INTEGER NOT NULL DEFAULT 0,
    rejected_count INTEGER NOT NULL DEFAULT 0,
    duplicate_count INTEGER NOT NULL DEFAULT 0,
    error_message TEXT
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_successful_input
ON runs(input_fingerprint) WHERE status IN ('succeeded', 'partial');

CREATE TABLE IF NOT EXISTS arrival_events (
    event_key TEXT PRIMARY KEY,
    run_id TEXT NOT NULL REFERENCES runs(run_id),
    dataset_id TEXT NOT NULL REFERENCES datasets(dataset_id),
    source_file TEXT NOT NULL,
    expected_at TEXT NOT NULL,
    arrived_at TEXT,
    completed_at TEXT,
    row_count INTEGER NOT NULL CHECK(row_count >= 0),
    freshness_minutes REAL,
    completion_minutes REAL,
    sla_status TEXT NOT NULL CHECK(sla_status IN ('fresh', 'late', 'missing', 'incomplete')),
    raw_payload TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS ix_events_dataset_expected
ON arrival_events(dataset_id, expected_at);

CREATE TABLE IF NOT EXISTS rejects (
    reject_id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id TEXT NOT NULL REFERENCES runs(run_id),
    source_file TEXT NOT NULL,
    source_row INTEGER NOT NULL,
    reason_code TEXT NOT NULL,
    reason_detail TEXT NOT NULL,
    raw_payload TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS alerts (
    alert_id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id TEXT NOT NULL REFERENCES runs(run_id),
    event_key TEXT,
    dataset_id TEXT NOT NULL REFERENCES datasets(dataset_id),
    severity TEXT NOT NULL CHECK(severity IN ('info', 'warning', 'critical')),
    alert_type TEXT NOT NULL,
    message TEXT NOT NULL,
    action TEXT NOT NULL,
    created_at TEXT NOT NULL,
    acknowledged INTEGER NOT NULL DEFAULT 0 CHECK(acknowledged IN (0, 1))
);

CREATE INDEX IF NOT EXISTS ix_alerts_open ON alerts(acknowledged, severity, created_at);
