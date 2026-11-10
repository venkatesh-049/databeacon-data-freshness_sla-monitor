# Requirements verification matrix

| ID | Requirement | Implementation evidence | Verification |
|---|---|---|---|
| R1 | Define source/target schemas, keys, assumptions | `schema.sql`, `data-dictionary.md` | Schema constraints and docs |
| R2 | Small reproducible and scalable data | `generator.py`, `data/sample/` | Optional fixed seed, append-only randomized batches; 100k benchmark |
| R3 | Ingestion, validation, transformation, publishing | `validation.py`, `ingestion.py` | Pipeline tests |
| R4 | Safe reruns and run metrics | `runs` table, fingerprint, insert/update/duplicate counters, CLI | Idempotency and event-update tests |
| R5 | Schema/business/duplicate/referential checks | validation module, foreign keys | Negative-case tests |
| R6 | Freshness, missing/late loads, alerts | `rules.py`, `pipeline/monitor.py`, `expected_windows`, persistent `alerts` | Boundary, idempotency, resolution, acknowledgement tests |
| R7 | Automated tests and expected output | `tests/`, report, fixture labels | `pytest` |
| R8 | Traceability and history | run/source/raw payload columns, append-only `event_observations` | Reject, lifecycle-update, and run-history tests |
| R9 | Operational logging | `observability.py`, ingestion logs | CLI run evidence |
| R10 | Reconciliation | `reports.py`, `databeacon reconcile` | PASS assertions |
| R11 | Performance and optimization | `scripts/benchmark.py`, indexed SQLite | 100k benchmark |
| R12 | Web/API delivery | FastAPI routes and dashboard | HTTP/API tests |
| R13 | Auto-updated dashboard and independent deadline evaluation | 5-second browser polling plus API-lifespan monitor (30-second default); polling pauses while an inline detail is open | Dashboard polling, detail-stability, and monitor lifecycle tests |
| R14 | Explain runtime decision-making and synthetic-data rights | `architecture.md`, `data/manifest.json` | Deterministic SLA rules; synthetic-only data provenance |
| R15 | Demonstrable attention scenario | `data/sample/attention_events.csv` | Late, incomplete, and missing alert demo |
| R16 | Provide normalized producer CSV without synthetic generation | `POST /api/runs/upload`, `data/incoming/` | Upload, safe filename, validation, size/content-type, and rerun tests |

## Required negative scenarios

| Scenario | Covered by |
|---|---|
| Malformed record | `test_rejects_malformed_duplicate_and_unknown_reference` |
| Missing required/null field | `test_empty_batch_is...` |
| Duplicate event | duplicate-key assertion in pipeline test |
| Unexpected schema change | `test_schema_change_fails_explicitly` |
| Empty batch | `test_empty_batch_is...` |
| Partial failure | invalid fixture and partial run |
| Retry/restart | `test_failed_schema_run_can_be_retried...` |
| Late/out-of-order | generator, validation, SLA rules, deadline monitor |
| Invalid reference | invalid-reference assertion |
| Same-input rerun | `test_happy_path_and_idempotency` |
| Before/after arrival deadline | `test_missing_alert_is_persisted_idempotently_and_resolved_on_arrival` |
| Completion deadline and acknowledgement | `test_completion_deadline_persists_incomplete_alert_and_acknowledgement` |
| Later completion updates incomplete event without losing prior run state | `test_completion_deadline_persists_incomplete_alert_and_acknowledgement` |
| Legacy alert-schema migration | `test_legacy_alert_table_migrates_without_losing_existing_alerts` |
| Legacy event observation backfill | `test_existing_events_are_backfilled_into_observation_history` |
| Direct producer file upload | `test_producer_csv_upload_and_validation` |

## Current dashboard behavior

The dashboard uses compact CSV-style tables with vertical column separators.
Pipeline run history is full-width and appears above the Data quality pulse.
Inline accepted, rejected, and duplicate detail tables include `S.No`; their
populated sections scroll internally, while empty sections collapse to the
single no-records row. Opening any history dataset pauses background polling
until the detail is closed or an explicit refresh/action is requested.

## Final status

All listed local vertical-slice requirements have implementation evidence and
automated or executable verification. The uploader accepts arrival telemetry,
not the underlying business payload. External source connectors, business-data
warehouse loading, authentication, distributed scheduling, and notification
delivery remain documented production extensions.
