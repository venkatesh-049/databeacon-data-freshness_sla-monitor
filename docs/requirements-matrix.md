# Requirements verification matrix

| ID | Requirement | Implementation evidence | Verification |
|---|---|---|---|
| R1 | Define source/target schemas, keys, assumptions | `schema.sql`, `data-dictionary.md` | Schema constraints and docs |
| R2 | Small reproducible and scalable data | `generator.py`, `data/sample/` | Optional fixed seed, append-only randomized batches; 100k benchmark |
| R3 | Ingestion, validation, transformation, publishing | `validation.py`, `ingestion.py` | Pipeline tests |
| R4 | Safe reruns and run metrics | `runs` table, fingerprint, CLI | Idempotency test |
| R5 | Schema/business/duplicate/referential checks | validation module, foreign keys | Negative-case tests |
| R6 | Freshness, missing/late loads, alerts | `rules.py`, `alerts` table, dashboard | API/dashboard tests |
| R7 | Automated tests and expected output | `tests/`, report, fixture labels | `pytest` |
| R8 | Traceability and history | run/source/raw payload columns | Reject and event schema |
| R9 | Operational logging | `observability.py`, ingestion logs | CLI run evidence |
| R10 | Reconciliation | `reports.py`, `databeacon reconcile` | PASS assertions |
| R11 | Performance and optimization | `scripts/benchmark.py`, indexed SQLite | 100k benchmark |
| R12 | Web/API delivery | FastAPI routes and dashboard | HTTP/API tests |
| R13 | Auto-updated dashboard | polling every 5 seconds | `test_dashboard_auto_refreshes...` |
| R14 | AI transparency and synthetic-data rights | `ai_transparency/`, manifest | Review artifacts |
| R15 | Demonstrable attention scenario | `data/sample/attention_events.csv` | Late, incomplete, and missing alert demo |

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
| Late/out-of-order | generator, validation, SLA rules |
| Invalid reference | invalid-reference assertion |
| Same-input rerun | `test_happy_path_and_idempotency` |

## Final status

All listed requirements have implementation evidence and automated or
executable verification. External integrations remain documented limitations,
not acceptance blockers for this local vertical slice.
