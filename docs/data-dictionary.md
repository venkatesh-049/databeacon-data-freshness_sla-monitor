# Data dictionary and data contract

## Conventions

* All table and column names are `snake_case`.
* Timestamps are ISO-8601 strings with an explicit UTC offset.
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

### `rejects`

One row per invalid source record. `source_row`, `source_file`, `run_id`, and
`raw_payload` make every anomaly traceable.

`reason_code` values include `invalid_record`, `invalid_reference`,
`invalid_value`, `invalid_sequence`, `out_of_order`, and `duplicate_key`.

### `alerts`

One row per detected issue. Alerts include `severity`, `alert_type`, human
message, recommended action, dataset, event, run, creation time, and
acknowledgement state.

Event-based late and incomplete alerts are persisted in this table. Missing
load alerts for datasets with no event in a run are derived in the dashboard
response from the dataset catalog; they do not have a persisted alert row or
acknowledgement ID in the current implementation.

## Relationships and integrity

* `arrival_events.dataset_id -> datasets.dataset_id`.
* `arrival_events.run_id -> runs.run_id`.
* `rejects.run_id -> runs.run_id`.
* `alerts.dataset_id -> datasets.dataset_id`.
* `alerts.run_id -> runs.run_id`.
* `arrival_events.event_key` is unique.
* Successful/partial input fingerprints are unique.
