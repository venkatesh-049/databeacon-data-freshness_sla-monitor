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
and synthetic CSV files represent source producers.

## 2. Logical flow

```text
                  +----------------------+
                  | CSV / file producer  |
                  +----------+-----------+
                             |
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
              | events + alerts    |
              +---------+----------+
                        |
             +----------+-----------+
             |                      |
             v                      v
       REST API/dashboard     report/reconcile
```

## 3. Components

| Component | Responsibility | Main location |
|---|---|---|
| Settings | Configurable database/output paths | `src/databeacon/settings.py` |
| Connection | SQLite bootstrap and foreign keys | `src/databeacon/db/connection.py` |
| Schema | Tables, checks, foreign keys, indexes | `src/databeacon/db/schema.sql` |
| Repository | Transactional reads/writes | `src/databeacon/db/repository.py` |
| Domain rules | Deterministic keys, durations, SLA states | `src/databeacon/domain/rules.py` |
| Validation | CSV headers, types, nulls, references, duplicates | `src/databeacon/pipeline/validation.py` |
| Ingestion | Run lifecycle and publishing orchestration | `src/databeacon/pipeline/ingestion.py` |
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
8. The dashboard polls the read APIs every five seconds, so it reflects the
   latest run state without a manual refresh.

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
* **CSV instead of object-store ingestion**: deterministic local reproduction,
  but no cloud connector or schema registry.
* **Deterministic rules instead of an ML model**: explainable and testable SLA
  outcomes; AI is not used to make acceptance decisions.
* **Inline dashboard instead of a frontend build**: quick startup and fewer
  dependencies; a production team could split it into a typed SPA.

## 7. Production extension path

The repository boundary allows a future object-store/API adapter. A scheduler
could materialize expected windows, PostgreSQL could replace SQLite, and an
alert adapter could deliver email, Slack, or PagerDuty notifications. The
deterministic event and run contracts should remain unchanged.
