# DataBeacon architecture and design note

## 1. Purpose

DataBeacon is a local monitoring platform for dataset arrival windows and
completion SLAs. It detects four operational states:

* **fresh** — completed within the configured SLA;
* **late** — completed after the configured SLA;
* **incomplete** — arrival exists but completion is missing;
* **missing** — the expected window has passed without an arrival.

The design favors a complete, testable vertical slice over infrastructure
complexity. SQLite is the local analytical store, FastAPI is the API/UI host,
and synthetic or producer-supplied CSV files represent source telemetry.

## 2. Logical flow

```text
                  +----------------------+
                  | CSV file / producer  |
                  +----------+-----------+
                             | file path or streamed upload
                             v
                  +----------------------+
                  | Fingerprint + run    |
                  | lifecycle creation  |
                  +----------+-----------+
                             |
                             v
                  +----------------------+
                  | Schema and business  |
                  | validation           |
                  +-----+-----------+----+
                        |           |
                 accepted rows   rejected rows
                        |           |
                        v           v
              +----------------+  +----------------+
              | SLA transform  |  | rejects table  |
              | + event key    |  | raw trace      |
              +-------+--------+  +----------------+
                      |
                      v
              +--------------------+
              | SQLite target      |
              | events + run alerts|
              +---------+----------+
                        |
             +----------+---------------------+
             |                                |
             v                                v
       REST API/dashboard             report/reconcile
             ^
             |
   +---------+-----------+
   | Background expected |
   | window monitor      |
   +---------+-----------+
             |
             v
   expected windows + persisted
   missing/incomplete alerts
```

## 3. Components

| Component | Responsibility | Main location |
|---|---|---|
| Settings | Configurable database/output paths | `src/databeacon/settings.py` |
| Connection | SQLite bootstrap, migrations, foreign keys | `src/databeacon/db/connection.py` |
| Schema | Tables, expected windows, checks, foreign keys, indexes | `src/databeacon/db/schema.sql` |
| Repository | Transactional reads/writes | `src/databeacon/db/repository.py` |
| Domain rules | Deterministic keys, durations, SLA states | `src/databeacon/domain/rules.py` |
| Validation | CSV headers, types, nulls, references, duplicates | `src/databeacon/pipeline/validation.py` |
| Ingestion | Run lifecycle and publishing orchestration | `src/databeacon/pipeline/ingestion.py` |
| Expected-window monitor | UTC window materialization, deadline checks, persisted/idempotent alerts | `src/databeacon/pipeline/monitor.py` |
| Reports | JSON validation report and reconciliation | `src/databeacon/pipeline/reports.py` |
| API/UI | Health, monitoring, alerts, dashboard | `src/databeacon/api/app.py` |
| Generator | Fixed-seed sample and scalable data | `src/databeacon/pipeline/generator.py` |

## 4. Run lifecycle

1. Read source bytes and calculate a SHA-256 `input_fingerprint`.
2. If the fingerprint already succeeded or partially succeeded, return
   `skipped` without writing duplicate facts.
3. Create a `running` record with a UUID `run_id`, source path, and start time.
4. Validate each source row.
5. Persist accepted events and alerts; persist rejected rows with source context.
6. Finish the run as `succeeded` or `partial`, including counts and end time.
7. On fatal errors, retain a `failed` run with `error_message`.
8. When a later run enriches an existing event (for example, reports its
   completion), update the canonical row and append an `event_observations`
   snapshot. Historical run health reads its own observation, not the latest
   mutable state.
9. Independently of ingestion, the API monitor materializes yesterday's and
   today's UTC windows for active datasets. It raises a persisted critical
   `missing_load` after the arrival grace period and a warning
   `incomplete_load` after the completion SLA. Alerts resolve as arrival or
   completion evidence is observed; repeated checks reuse the window/type key.
10. The dashboard polls the read APIs every five seconds, so it reflects the
   latest run state without a manual refresh. While an inline pipeline-history
   dataset detail is open, browser polling is paused to prevent flicker and
   repeated detail refetches; the explicit Refresh data action still forces a
   refresh.

## 5. Idempotency and recovery

The event key is `SHA-256(dataset_id + "|" + expected_at)`. It is the primary
key of `arrival_events`, so the same logical event cannot be published twice.
The input fingerprint protects full-file reruns. A failed schema run remains
visible and does not block a corrected file with a different fingerprint.

For a retry:

```powershell
python -m databeacon run --file data/sample/corrected.csv
```

For a backfill, use a source file containing the intended historical
`expected_at` values. Do not edit an existing published event in place.

## 6. Design trade-offs

* **SQLite instead of PostgreSQL**: zero infrastructure and easy review, but
  lower concurrency and no distributed scheduling.
* **CSV instead of object-store ingestion**: deterministic local reproduction
  with a bounded CSV upload endpoint for producer-supplied arrival telemetry,
  but no cloud connector, schema registry, or business-payload warehouse load.
* **In-process monitor instead of a distributed scheduler**: independently
  evaluates daily UTC windows while the API is running, but has no HA,
  external notification delivery, or calendar/holiday awareness.
* **Deterministic rules instead of an ML model**: explainable and testable SLA
  outcomes; AI is not used to make acceptance decisions.
* **Inline dashboard instead of a frontend build**: quick startup and fewer
  dependencies; a production team could split it into a typed SPA.
* **Compact inline tables instead of empty fixed panels**: populated detail
  tables retain bounded scrolling, while empty accepted/rejected/duplicate
  sections collapse to their no-records row. Serial numbers and column
  separators make the tables easier to reconcile with downloaded CSV files.

## 7. Production extension path

The repository boundary allows future object-store and business-payload
adapters. PostgreSQL could replace SQLite, a distributed scheduler could
replace the in-process worker, and an alert adapter could deliver email, Slack,
or PagerDuty. The deterministic event and run contracts should remain
unchanged.
