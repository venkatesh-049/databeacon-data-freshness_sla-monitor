# Data dictionary and data contract

## Conventions

* All table and column names are `snake_case`.
* Source timestamps must be ISO-8601 strings with an explicit UTC offset and
  are normalized to UTC before deterministic identity and persistence.
* IDs are stable where business identity matters and UUIDs where execution
  identity matters.
* `run_id` and `source_file` provide lineage for accepted and rejected rows.
* The source grain is one dataset arrival window: `(dataset_id, expected_at)`.

## Source contract

Input is a CSV with the exact header:

```text
dataset_id,expected_at,arrived_at,completed_at,row_count
```

| Column | Type | Nullability | Rules |
|---|---|---:|---|
| `dataset_id` | string | not null | Must exist in `datasets` |
| `expected_at` | ISO-8601 datetime | not null | Defines the monitored window |
| `arrived_at` | ISO-8601 datetime | nullable | May be absent for missing loads |
| `completed_at` | ISO-8601 datetime | nullable | Requires arrival; cannot precede arrival |
| `row_count` | integer | not null | Must be >= 0 |

An unexpected header is a fatal schema error. Row-level parse and business
errors are quarantined so valid rows can still be published.

The same contract can be submitted as a CSV file through the bounded
`POST /api/runs/upload` endpoint. The server stores uploaded bytes in the
configured incoming directory under a generated filename before invoking the
normal validation and ingestion path.

## Target entities

### `datasets`

One row per configured source dataset.

| Column | Type | Key/constraint | Meaning |
|---|---|---|---|
| `dataset_id` | text | PK | Stable source identifier |
| `dataset_name` | text | unique, not null | Human-readable name |
| `expected_hour_utc` | integer | 0-23 | Expected arrival hour |
| `expected_minute_utc` | integer | 0-59 | Expected arrival minute |
| `sla_minutes` | integer | > 0 | Maximum completion duration |
| `active` | integer | 0/1 | Configuration activation flag |

The seed catalog contains 12 datasets: `sales_daily`, `inventory_daily`,
`customers_daily`, `orders_daily`, `payments_daily`, `products_daily`,
`shipments_daily`, `returns_daily`, `promotions_daily`, `suppliers_daily`,
`finance_daily`, and `support_daily`.

| Dataset ID | Expected UTC time | SLA minutes |
|---|---:|---:|
| `sales_daily` | 06:00 | 120 |
| `inventory_daily` | 07:00 | 180 |
| `customers_daily` | 08:00 | 90 |
| `orders_daily` | 06:00 | 120 |
| `payments_daily` | 07:00 | 120 |
| `products_daily` | 08:00 | 180 |
| `shipments_daily` | 09:00 | 180 |
| `returns_daily` | 10:00 | 240 |
| `promotions_daily` | 10:00 | 240 |
| `suppliers_daily` | 11:00 | 240 |
| `finance_daily` | 12:00 | 180 |
| `support_daily` | 13:00 | 180 |

### `runs`

One row per attempted source-file execution.

| Column | Type | Meaning |
|---|---|---|
| `run_id` | text PK | Execution identity |
| `input_fingerprint` | text | SHA-256 of source bytes |
| `source_file` | text | Source lineage |
| `started_at`, `ended_at` | ISO-8601 text | Execution timing |
| `status` | enum | `running`, `succeeded`, `partial`, `failed` |
| `input_count` | integer | Rows read |
| `accepted_count` | integer | Rows published to target |
| `rejected_count` | integer | Rows quarantined |
| `duplicate_count` | integer | Valid rows whose event key was already published |
| `updated_count` | integer | Accepted rows that enriched an existing arrival event |
| `error_message` | text nullable | Fatal failure detail |

### `arrival_events`

One row per published `(dataset_id, expected_at)` event.

| Column | Type | Meaning |
|---|---|---|
| `event_key` | text PK | Deterministic SHA-256 event identity |
| `run_id`, `source_file` | lineage | Publishing context |
| `expected_at`, `arrived_at`, `completed_at` | ISO-8601 | Event lifecycle |
| `row_count` | integer | Source-reported target row count |
| `freshness_minutes` | real nullable | Arrival-to-observation duration |
| `completion_minutes` | real nullable | Expected-to-completion duration |
| `sla_status` | enum | `fresh`, `late`, `missing`, `incomplete` |
| `raw_payload` | JSON text | Original accepted row |

### `event_observations`

Append-only snapshots of an arrival event as producers report lifecycle
changes. The initial event and each accepted enrichment (for example,
completion arriving after an earlier incomplete observation) are tied to the
run and source file that reported them. The canonical `arrival_events` row
contains the latest known values; observations preserve every accepted state
without silently erasing prior run history.

| Column | Type | Key/constraint | Meaning |
|---|---|---|---|
| `observation_id` | integer | PK | Observation sequence |
| `event_key` | text | FK to `arrival_events` | Logical event identity |
| `run_id` | text | FK to `runs` | Run that supplied this state |
| `dataset_id` | text | FK to `datasets` | Dataset |
| `source_file` | text | not null | Source lineage for this observation |
| `expected_at`, `arrived_at`, `completed_at` | ISO-8601 UTC | nullable except expected | Lifecycle snapshot |
| `row_count` | integer | >= 0 | Reported row count at this observation |
| `freshness_minutes`, `completion_minutes` | real | nullable | Derived metrics at observation time |
| `sla_status` | enum | not null | State observed in this run |
| `raw_payload` | JSON text | not null | Source record for the observation |
| `observed_at` | ISO-8601 UTC | not null | Ingestion observation time |

### `expected_windows`

One row per active dataset and configured daily UTC window. The monitor
materializes the previous and current UTC day so a restart can detect missed
deadlines. Window identity is a deterministic SHA-256 hash of
`dataset_id|expected_at`.

| Column | Type | Key/constraint | Meaning |
|---|---|---|---|
| `window_key` | text | PK | Stable identity for this dataset/window |
| `dataset_id` | text | FK to `datasets` | Monitored dataset |
| `expected_at` | ISO-8601 UTC | unique with dataset | Configured daily arrival time |
| `arrival_deadline_at` | ISO-8601 UTC | not null | `expected_at` plus configured grace |
| `completion_deadline_at` | ISO-8601 UTC | not null | `expected_at` plus dataset SLA minutes |
| `status` | enum | not null | `scheduled`, `arrived`, `fresh`, `late`, `incomplete`, or `missing` |
| `last_checked_at` | ISO-8601 UTC | not null | Most recent monitor evaluation |

### `rejects`

One row per invalid source record. `source_row`, `source_file`, `run_id`, and
`raw_payload` make every anomaly traceable.

`reason_code` values include `invalid_record`, `invalid_reference`,
`invalid_value`, `invalid_sequence`, `out_of_order`, and `duplicate_key`.

### `alerts`

One row per detected issue. Alerts include `severity`, `alert_type`, human
message, recommended action, dataset, optional event/run/window references,
creation time, acknowledgement state, and optional resolution time.

Event-based late/incomplete alerts reference their ingestion run. Scheduler
alerts have a nullable `run_id` and reference `window_key`. The unique partial
index on `(window_key, alert_type)` makes scheduled alerts idempotent. Missing
alerts resolve when an arrival is observed; incomplete alerts resolve when
completion is observed. Acknowledged and resolved rows remain as history.

### Monitor state transitions

* `scheduled`: the arrival grace deadline has not passed and no arrival is
  observed.
* `missing`: no arrival was observed by `arrival_deadline_at`; a persistent
  critical `missing_load` alert is created.
* `arrived`: arrival is observed but completion is not yet due.
* `incomplete`: completion is not observed by `completion_deadline_at`; a
  persistent warning `incomplete_load` alert is created.
* `fresh` / `late`: completion is observed by or after the configured SLA
  deadline. The corresponding monitor status is based on completion time, not
  on the timestamp when the monitor happens to run.

The API worker checks at a configurable interval (30 seconds by default),
immediately on service startup, and via `POST /api/monitor/run`. It evaluates
the previous and current UTC dates; business calendars, weekends, and
holidays are not modeled.

## Dashboard presentation contract

The web dashboard presents these entities as compact CSV-style tables. Inline
accepted, rejected, and duplicate detail tables prepend a display-only `S.No`
column; it is not persisted and does not replace `observation_id`, `reject_id`,
or `duplicate_id`. Populated detail sections use bounded internal scrolling.
Empty sections collapse to their no-records row. Pipeline run history is
full-width above the Data quality pulse, and opening a dataset detail pauses
browser polling until the user closes it or explicitly refreshes.

## Relationships and integrity

* `arrival_events.dataset_id -> datasets.dataset_id`.
* `arrival_events.run_id -> runs.run_id`.
* `event_observations.event_key -> arrival_events.event_key`.
* `event_observations.run_id -> runs.run_id`.
* `event_observations.dataset_id -> datasets.dataset_id`.
* `expected_windows.dataset_id -> datasets.dataset_id`.
* `rejects.run_id -> runs.run_id`.
* `alerts.dataset_id -> datasets.dataset_id`.
* `alerts.run_id -> runs.run_id` (nullable for monitor-generated alerts).
* `alerts.window_key -> expected_windows.window_key` (nullable for event alerts).
* `arrival_events.event_key` is unique.
* `expected_windows(dataset_id, expected_at)` is unique.
* Successful/partial input fingerprints are unique.
