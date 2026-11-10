# DataBeacon project map

This document explains what each project folder and file does, why it exists,
and how it contributes to the dataset-arrival and SLA-monitoring problem.

## 1. Repository layout

```text
DataBeacon/
├── README.md
├── pyproject.toml
├── src/databeacon/
│   ├── __main__.py
│   ├── __init__.py
│   ├── settings.py
│   ├── observability.py
│   ├── cli.py
│   ├── api/
│   │   ├── __init__.py
│   │   └── app.py
│   ├── db/
│   │   ├── __init__.py
│   │   ├── connection.py
│   │   ├── repository.py
│   │   └── schema.sql
│   ├── domain/
│   │   ├── __init__.py
│   │   ├── models.py
│   │   └── rules.py
│   └── pipeline/
│       ├── __init__.py
│       ├── generator.py
│       ├── ingestion.py
│       ├── reports.py
│       └── validation.py
├── scripts/
│   └── benchmark.py
├── tests/
│   ├── test_api.py
│   └── test_pipeline.py
├── data/
│   ├── sample/
│   │   ├── events.csv
│   │   ├── invalid_events.csv
│   │   └── invalid_events.expected.json
│   ├── output/
│   │   └── validation_report.json
│   ├── manifest.json
│   └── databeacon.db
├── docs/
│   ├── architecture.md
│   ├── data-dictionary.md
│   ├── evaluation_metrics.md
│   ├── project-map.md
│   └── requirements-matrix.md
└── ai_transparency/
    └── PROMPT_HISTORY.md
```

The local `.venv/` directory, Python bytecode, and pytest cache are generated
environment artifacts. They are not application source or required deliverables.

## 2. Root files

| File | What it does | Problem alignment |
|---|---|---|
| `README.md` | Primary setup, execution, API, operations, limitations, and evidence guide | Makes the solution runnable and reviewable by a data engineer or operator |
| `pyproject.toml` | Defines package metadata, runtime dependencies, test dependencies, and CLI entry point | Reproducible clean-environment installation |

## 3. Application package: `src/databeacon/`

| File | What it does | Problem alignment |
|---|---|---|
| `__init__.py` | Marks `databeacon` as a Python package | Enables maintainable module boundaries |
| `__main__.py` | Allows `python -m databeacon` execution | Provides a simple operator entry point |
| `settings.py` | Holds configurable database and output paths | Supports local runs, tests, and alternate environments without code edits |
| `observability.py` | Configures the `databeacon` logger and structured operational messages | Exposes run progress, counts, skips, and failures instead of hiding them |
| `cli.py` | Implements `generate`, `run`, `report`, and `reconcile` commands | Gives the pipeline operator repeatable ingestion, reporting, and validation controls |

## 4. API and web UI: `src/databeacon/api/`

| File | What it does | Problem alignment |
|---|---|---|
| `__init__.py` | Marks the API package | Keeps delivery concerns isolated |
| `app.py` | Creates the FastAPI application, REST endpoints, and responsive dashboard | Exposes dataset health, alerts, rejects, reports, run triggering, and operator actions |

Important endpoints:

| Endpoint | Purpose |
|---|---|
| `GET /api/health` | Confirms the service and database can be opened |
| `GET /api/dashboard` | Returns current dataset states, open alerts, and recent runs |
| `GET /api/report` | Returns status counts and data-quality totals |
| `GET /api/rejects` | Shows quarantined source rows |
| `GET /api/alerts` | Lists actionable alerts |
| `POST /api/alerts/{id}/acknowledge` | Records an operator acknowledgement |
| `POST /api/runs?source_file=...` | Starts a local source-file run |
| `GET /` | Serves the browser dashboard |

The dashboard polls the dashboard and report endpoints every five seconds. This
keeps it aligned with newly completed, partial, failed, or acknowledged runs
without requiring a manual browser refresh.

## 5. Storage: `src/databeacon/db/`

| File | What it does | Problem alignment |
|---|---|---|
| `__init__.py` | Marks the database package | Isolates persistence from business rules |
| `connection.py` | Opens SQLite, enables foreign keys, and applies the schema | Makes the target store reproducible and integrity-aware |
| `schema.sql` | Defines datasets, runs, events, rejects, alerts, checks, and indexes | Implements source/target entities, lineage, referential integrity, uniqueness, and query performance |
| `repository.py` | Provides seeded configuration, transactional writes, dashboard queries, alert actions, and report queries | Centralizes persistence so reruns and monitoring views behave consistently |

The important lineage path is:

```text
run_id + source_file
       ├── arrival_events
       ├── rejects
       └── alerts
```

## 6. Domain layer: `src/databeacon/domain/`

| File | What it does | Problem alignment |
|---|---|---|
| `__init__.py` | Marks the domain package | Keeps rules reusable by CLI, API, and tests |
| `models.py` | Defines typed input and dataset configuration models | Makes required fields, optional timestamps, and types explicit |
| `rules.py` | Calculates deterministic event keys, time durations, SLA status, and alert content | Implements freshness, missing/late detection, completion SLA, and actionable remediation text |

The domain layer contains no database or HTTP code, which makes the core
business decisions easy to test and explain.

## 7. Pipeline layer: `src/databeacon/pipeline/`

| File | What it does | Problem alignment |
|---|---|---|
| `__init__.py` | Marks the pipeline package | Groups ingestion and reporting workflow code |
| `generator.py` | Creates fixed-seed normal synthetic data at configurable volume | Supplies reproducible sample and scalable 100,000+ row inputs |
| `validation.py` | Reads exact CSV headers, parses values, validates references, nulls, duplicates, and event ordering | Prevents malformed or invalid source data from contaminating the target |
| `ingestion.py` | Orchestrates fingerprinting, run lifecycle, validation, publishing, metrics, alerts, retries, and progress output | Provides the complete ingestion-to-target vertical slice |
| `reports.py` | Writes JSON validation reports and computes reconciliation results | Demonstrates measurable output correctness and explains accepted/rejected differences |

## 8. Scripts and tests

### `scripts/benchmark.py`

Generates a configurable input, runs it in a temporary SQLite database, and
prints elapsed seconds and rows per second. It addresses scalability and
performance reasoning without requiring cloud infrastructure.

### `scripts/run_tests.py`

Runs pytest while avoiding a known Windows machine-level `py.py` shadowing
module. It exists only as a developer-environment compatibility wrapper; it
does not change application behavior.

### `tests/test_pipeline.py`

Covers:

* happy path;
* same-input idempotent rerun;
* malformed records;
* duplicate event keys;
* invalid reference keys;
* empty input;
* missing values;
* completion-before-arrival;
* unexpected schema change;
* failed-run retry with corrected input;
* reconciliation and unique-key assertions.

### `tests/test_api.py`

Covers:

* service health;
* run triggering;
* dashboard/report/reject/alert endpoints;
* empty-database dataset visibility;
* dashboard auto-refresh JavaScript.

Together, the tests provide evidence for both data-engineering correctness and
operator-facing behavior.

## 9. Data and deliverables

| Path | What it contains | Why it matters |
|---|---|---|
| `data/sample/events.csv` | Append-only generated input | New runs increase cumulative events and randomly demonstrate health states |
| `data/sample/invalid_events.csv` | Explicitly labelled negative scenarios | Demonstrates quarantine and partial-run behavior |
| `data/sample/invalid_events.expected.json` | Expected counts and reason labels kept outside input | Prevents expected answers from contaminating source data |
| `data/sample/attention_events.csv` | Demonstration input with late and incomplete loads | Gives reviewers a realistic operational-alert scenario |
| `data/sample/attention_events.expected.json` | Expected late/incomplete/missing statuses and actions | Makes the attention demo deterministic and reviewable |
| `data/sample/critical_missing_events.csv` | Healthy events plus one deliberately omitted dataset | Demonstrates a critical missing-load condition |
| `data/sample/critical_missing_events.expected.json` | Expected critical status, severity, and action | Keeps the critical demo measurable |
| `data/sample/dashboard_demo_events.csv` | One-run demo input for late, fresh, and omitted dataset states | Lets reviewers see all key dashboard states without UI changes |
| `data/sample/dashboard_demo_events.expected.json` | Expected combined dashboard statuses and alerts | Makes the visual demo deterministic |
| `data/output/validation_report.json` | Sample status, alert, reject, and run evidence | Reviewable output artifact |
| `data/test_evidence.json` | Consolidated test inputs, expected results, observed outputs, reconciliation, and coverage | Single evidence file for project review |
| `data/manifest.json` | Generator, seed, file list, schema, and synthetic-data rights | Reproducibility and data provenance |
| `data/databeacon.db` | Local SQLite target after a sample run | Inspectable local target state |

## 10. Documentation and governance

| Path | What it does |
|---|---|
| `docs/architecture.md` | Explains system flow, lifecycle, trade-offs, and extension path |
| `docs/data-dictionary.md` | Defines source contract, target columns, nullability, keys, and relationships |
| `docs/evaluation_metrics.md` | Defines measurable metrics and acceptance evidence |
| `docs/requirements-matrix.md` | Maps every business requirement and negative scenario to implementation evidence |
| `docs/project-map.md` | This file: explains folders/files and their business alignment |
| `ai_transparency/PROMPT_HISTORY.md` | Discloses AI assistance, human decisions, deterministic runtime behavior, and verification |

## 11. End-to-end alignment

```text
Expected arrivals + SLAs
        -> datasets configuration + source contract
Source producer
        -> generator / CSV input
Ingestion
        -> validation + quarantine + run metrics
Transformation
        -> freshness, completion duration, SLA status, deterministic event key
Target analytical consumer
        -> arrival_events + JSON report + reconciliation
Pipeline operator
        -> CLI logs + API + auto-refresh dashboard + actionable alerts
```

This structure directly addresses the problem statement while keeping each
responsibility small enough for a 0–3 years experience implementation to
understand, test, and extend.
