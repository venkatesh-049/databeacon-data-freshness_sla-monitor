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
    updated_count INTEGER NOT NULL DEFAULT 0,
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

CREATE TABLE IF NOT EXISTS event_observations (
    observation_id INTEGER PRIMARY KEY AUTOINCREMENT,
    event_key TEXT NOT NULL REFERENCES arrival_events(event_key),
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
    raw_payload TEXT NOT NULL,
    observed_at TEXT NOT NULL,
    UNIQUE(event_key, run_id)
);

CREATE INDEX IF NOT EXISTS ix_observations_run_dataset
ON event_observations(run_id, dataset_id);

CREATE TABLE IF NOT EXISTS expected_windows (
    window_key TEXT PRIMARY KEY,
    dataset_id TEXT NOT NULL REFERENCES datasets(dataset_id),
    expected_at TEXT NOT NULL,
    arrival_deadline_at TEXT NOT NULL,
    completion_deadline_at TEXT NOT NULL,
    status TEXT NOT NULL CHECK(status IN ('scheduled', 'arrived', 'fresh', 'late', 'incomplete', 'missing')),
    last_checked_at TEXT NOT NULL,
    UNIQUE(dataset_id, expected_at)
);

CREATE INDEX IF NOT EXISTS ix_expected_windows_status_deadline
ON expected_windows(status, arrival_deadline_at);

CREATE TABLE IF NOT EXISTS rejects (
    reject_id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id TEXT NOT NULL REFERENCES runs(run_id),
    source_file TEXT NOT NULL,
    source_row INTEGER NOT NULL,
    reason_code TEXT NOT NULL,
    reason_detail TEXT NOT NULL,
    raw_payload TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS duplicate_records (
    duplicate_id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id TEXT NOT NULL REFERENCES runs(run_id),
    dataset_id TEXT NOT NULL REFERENCES datasets(dataset_id),
    event_key TEXT NOT NULL,
    source_file TEXT NOT NULL,
    source_row INTEGER NOT NULL,
    reason TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS ix_duplicates_run_dataset
ON duplicate_records(run_id, dataset_id);

CREATE TABLE IF NOT EXISTS alerts (
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
);
