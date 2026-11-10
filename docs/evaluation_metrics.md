# Project evaluation metrics and evidence

## Metric definitions

| Metric | Formula | Why it matters | Evidence |
|---|---|---|---|
| Completeness | `input = accepted + rejected` | No silent loss | `databeacon reconcile` |
| Publish uniqueness | `published = distinct event_key` | Safe reruns | `databeacon reconcile` |
| Freshness | `now - arrived_at` | Consumer recency | `arrival_events.freshness_minutes` |
| SLA compliance | `fresh / published * 100` | Service reliability | `/api/report` |
| Late-load rate | `late / published * 100` | SLA risk | `/api/report` |
| Reject rate | `rejected / input * 100` | Data quality | run metrics/report |
| Alert coverage | Issue states produce alerts | Operator actionability | `/api/alerts` |
| Recovery | Failed run retained, retry succeeds | Operability | retry test |
| Throughput | `rows / elapsed_seconds` | Scalability | `scripts/benchmark.py` |

## Current sample evidence

The checked-in sample contains 12 valid rows. The generated report records:

* 12 input rows;
* 12 accepted rows;
* 0 rejected rows;
* 10 fresh events;
* 2 late events;
* 2 alerts.

The labelled negative fixture contains 5 rows, with 1 accepted and 4 rejected.
Expected reason labels are isolated from the input file.

## Attention-state demonstration

Run:

```powershell
python -m databeacon run --file data/sample/attention_events.csv --db data/attention_demo.db
python -m databeacon report --db data/attention_demo.db
$env:DATABEACON_DB = "data/attention_demo.db"
python -m uvicorn databeacon.api.app:app --reload
```

The attention fixture intentionally produces:

* `sales_daily`: **late**, because completion is 210 minutes after expected and
  the configured SLA is 120 minutes;
* `inventory_daily`: **incomplete**, because it arrived but has no completion;
* `customers_daily`: **missing**, because no event is supplied for its expected
  window and the dashboard creates a missing-load alert.

The dashboard action center shows the alert severity, explanation, recommended
operator action, and acknowledgement control.

## Critical alert demonstration

`data/sample/critical_missing_events.csv` supplies healthy sales and inventory
events while deliberately omitting `customers_daily`. The resulting dashboard
state is `customers_daily: missing`, with severity `critical`, alert type
`missing_load`, and the action to investigate the producer and trigger a
backfill.

This differs from a late load: a late load eventually arrived and is a warning
SLA breach, while a missing load has not arrived after its expected window and
can block downstream consumers.

## Combined dashboard demonstration

`data/sample/dashboard_demo_events.csv` intentionally produces all three key
dashboard states in one latest run:

* `sales_daily`: late and warning;
* `inventory_daily`: fresh;
* `customers_daily`: missing and critical.

The expected statuses are isolated in
`data/sample/dashboard_demo_events.expected.json`. This proves that the
dashboard can display mixed operational states from one run without any UI
change.

## Acceptance evidence

Run:

```powershell
python -m pytest
python -m databeacon reconcile
python scripts/benchmark.py --rows 100000
```

The reconciliation command must print `counts balanced: PASS` and
`keys unique: PASS`. The benchmark measures local synthetic processing only;
it is not a production capacity guarantee.

## Review limitations

There is no external scheduler, cloud object store, notification provider,
multi-process load test, or production authentication in this local slice.
Those are deliberate scope boundaries and are listed as extension points rather
than being simulated as completed capabilities.
