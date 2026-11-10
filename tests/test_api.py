from pathlib import Path
import time
from datetime import datetime, timezone

from fastapi.testclient import TestClient

from databeacon.api.app import create_app
from databeacon.db.connection import connect
from databeacon.db.repository import seed_datasets
from databeacon.pipeline.generator import generate_csv
from databeacon.pipeline.monitor import monitor_expected_windows
from databeacon.settings import Settings


def test_api_dashboard_and_trigger(tmp_path: Path):
    settings = Settings(tmp_path / "db.sqlite", tmp_path / "output", tmp_path / "incoming")
    client = TestClient(create_app(settings))
    assert client.get("/api/health").json() == {"status": "ok"}
    source = generate_csv(tmp_path / "events.csv", rows=3)
    response = client.post("/api/runs", params={"source_file": str(source)})
    assert response.status_code == 200
    assert response.json()["accepted_count"] == 3
    assert client.get("/api/dashboard").status_code == 200
    assert client.get("/api/report").json()["published_events"] == 3
    assert client.get("/api/rejects").json() == []
    assert client.get("/api/alerts").status_code == 200


def test_empty_database_surfaces_missing_dataset_alerts(tmp_path: Path):
    settings = Settings(tmp_path / "db.sqlite", tmp_path / "output", tmp_path / "incoming")
    client = TestClient(create_app(settings))
    data = client.get("/api/dashboard").json()
    assert len(data["datasets"]) == 12
    assert all(item["sla_status"] in {"missing", "scheduled"} for item in data["datasets"])
    if any(item["sla_status"] == "missing" for item in data["datasets"]):
        assert any(item["alert_type"] == "missing_load" for item in data["alerts"])


def test_dashboard_auto_refreshes_against_latest_run_state():
    response = TestClient(create_app(Settings())).get("/")
    assert response.status_code == 200
    assert "setInterval(()=>load(false),5000)" in response.text
    assert "if(detailRunId&&!force)return" in response.text
    assert "One row per dataset per run" in response.text
    assert "current UTC window" in response.text
    assert "Historical snapshot for each run" in response.text
    assert "run.datasets" in response.text
    assert "openRuns" in response.text
    assert "data-run-id" in response.text
    assert "max-width:1600px" in response.text
    assert "overflow-x:hidden" in response.text
    assert "click for details" in response.text
    assert "Rejected source rows" in response.text
    assert "Duplicate event records" in response.text
    assert "inline-detail" in response.text
    assert "inline-detail-row" in response.text
    assert "detailScrollTop" in response.text
    assert "scrollIntoView" not in response.text
    assert "detail-open" in response.text
    assert "column-filter" not in response.text
    assert "Observation ID" in response.text
    assert "Reason code" in response.text
    assert "Raw payload" in response.text
    assert "S.No" in response.text
    assert "padding:5px 8px;line-height:1.15" in response.text
    assert ".detail-table.empty{height:auto;overflow:visible}" in response.text
    assert 'detail-table${(data.rejects||[]).length?"":" empty"}' in response.text
    assert "history-layout" in response.text
    assert "quality-panel" in response.text
    assert "border-right:1px solid #263957" in response.text


def test_run_dataset_details_exposes_accepted_rejected_and_duplicates(tmp_path: Path):
    settings = Settings(tmp_path / "db.sqlite", tmp_path / "output")
    client = TestClient(create_app(settings))
    source = tmp_path / "details.csv"
    source.write_text(
        "dataset_id,expected_at,arrived_at,completed_at,row_count\n"
        "sales_daily,2026-03-01T06:00:00+00:00,2026-03-01T06:05:00+00:00,2026-03-01T07:00:00+00:00,1\n"
        "sales_daily,2026-03-01T06:00:00+00:00,2026-03-01T06:05:00+00:00,2026-03-01T07:00:00+00:00,1\n"
        "unknown,2026-03-01T07:00:00+00:00,2026-03-01T07:05:00+00:00,2026-03-01T08:00:00+00:00,1\n",
        encoding="utf-8",
    )
    result = client.post("/api/runs", params={"source_file": str(source)}).json()
    details = client.get(
        f"/api/runs/{result['run_id']}/datasets/sales_daily"
    )
    assert details.status_code == 200
    assert details.json()["counts"]["accepted"] == 1
    assert details.json()["counts"]["rejected"] == 1
    assert details.json()["counts"]["duplicates"] == 0
    for kind, header in (
        ("accepted", "observation_id"),
        ("rejected", "reject_id"),
        ("duplicates", "duplicate_id"),
    ):
        response = client.get(
            f"/api/download/run-dataset/{result['run_id']}/sales_daily/{kind}"
        )
        assert response.status_code == 200
        assert response.text.splitlines()[0].startswith(header)


def test_dashboard_includes_dataset_health_for_each_run(tmp_path: Path):
    settings = Settings(tmp_path / "db.sqlite", tmp_path / "output")
    client = TestClient(create_app(settings))
    first = generate_csv(tmp_path / "first.csv", rows=2, seed=10)
    second = generate_csv(tmp_path / "second.csv", rows=3, seed=11)
    client.post("/api/runs", params={"source_file": str(first)})
    client.post("/api/runs", params={"source_file": str(second)})
    runs = client.get("/api/dashboard").json()["runs"]
    assert len(runs) == 2
    assert all(len(run["datasets"]) == 12 for run in runs)
    assert all(any(dataset["sla_status"] == "missing" for dataset in run["datasets"]) for run in runs)
    assert all(len(run["datasets"]) == len({dataset["dataset_id"] for dataset in run["datasets"]}) for run in runs)
    assert all("event_count" in dataset for run in runs for dataset in run["datasets"])


def test_dashboard_history_includes_more_than_twenty_runs(tmp_path: Path):
    settings = Settings(tmp_path / "db.sqlite", tmp_path / "output")
    client = TestClient(create_app(settings))
    for seed in range(21):
        source = generate_csv(tmp_path / f"run-{seed}.csv", rows=1, seed=seed)
        response = client.post("/api/runs", params={"source_file": str(source)})
        assert response.status_code == 200
    runs = client.get("/api/dashboard").json()["runs"]
    assert len(runs) == 21


def test_attention_demo_surfaces_late_incomplete_and_missing(tmp_path: Path):
    source = tmp_path / "attention.csv"
    source.write_text(
        "dataset_id,expected_at,arrived_at,completed_at,row_count\n"
        "sales_daily,2026-01-10T06:00:00+00:00,2026-01-10T06:15:00+00:00,2026-01-10T09:30:00+00:00,10\n"
        "inventory_daily,2026-01-10T07:00:00+00:00,2026-01-10T07:20:00+00:00,,10\n",
        encoding="utf-8",
    )
    settings = Settings(tmp_path / "db.sqlite", tmp_path / "output")
    client = TestClient(create_app(settings))
    client.post("/api/runs", params={"source_file": str(source)})
    dashboard = client.get("/api/dashboard").json()
    run_statuses = {item["dataset_id"]: item["sla_status"] for item in dashboard["runs"][0]["datasets"]}
    assert run_statuses["sales_daily"] == "late"
    assert run_statuses["inventory_daily"] == "incomplete"
    assert run_statuses["customers_daily"] == "missing"


def test_dashboard_scopes_health_to_latest_run(tmp_path: Path):
    settings = Settings(tmp_path / "db.sqlite", tmp_path / "output")
    client = TestClient(create_app(settings))
    first = generate_csv(tmp_path / "first.csv", rows=3)
    client.post("/api/runs", params={"source_file": str(first)})
    second = tmp_path / "second.csv"
    second.write_text(
        "dataset_id,expected_at,arrived_at,completed_at,row_count\n"
        "sales_daily,2026-02-01T06:00:00+00:00,2026-02-01T06:10:00+00:00,2026-02-01T09:00:00+00:00,1\n",
        encoding="utf-8",
    )
    client.post("/api/runs", params={"source_file": str(second)})
    data = client.get("/api/dashboard").json()
    latest_run_statuses = {
        item["dataset_id"]: item["sla_status"] for item in data["runs"][0]["datasets"]
    }
    assert latest_run_statuses["sales_daily"] == "late"
    assert latest_run_statuses["inventory_daily"] == "missing"
    assert latest_run_statuses["customers_daily"] == "missing"


def test_dashboard_summary_is_database_driven(tmp_path: Path):
    settings = Settings(tmp_path / "db.sqlite", tmp_path / "output")
    client = TestClient(create_app(settings))
    empty = client.get("/api/dashboard").json()
    assert empty["summary"]["datasets_monitored"] == 12
    assert empty["summary"]["datasets_needing_attention"] == sum(
        row["sla_status"] in {"late", "missing", "incomplete"}
        for row in empty["datasets"]
    )
    assert empty["summary"]["open_alerts"] == len(empty["alerts"])

    source = generate_csv(tmp_path / "events.csv", rows=12, seed=17)
    client.post("/api/runs", params={"source_file": str(source)})
    populated = client.get("/api/dashboard").json()
    assert populated["summary"]["datasets_monitored"] == len(populated["datasets"])
    assert populated["summary"]["datasets_needing_attention"] == sum(
        row["sla_status"] in {"late", "missing", "incomplete"}
        for row in populated["datasets"]
    )
    assert populated["summary"]["open_alerts"] == len(populated["alerts"])


def test_critical_missing_fixture_creates_critical_alert(tmp_path: Path):
    settings = Settings(tmp_path / "db.sqlite", tmp_path / "output")
    client = TestClient(create_app(settings))
    source = tmp_path / "critical.csv"
    source.write_text(
        "dataset_id,expected_at,arrived_at,completed_at,row_count\n"
        "sales_daily,2026-01-11T06:00:00+00:00,2026-01-11T06:05:00+00:00,2026-01-11T07:00:00+00:00,1\n"
        "inventory_daily,2026-01-11T07:00:00+00:00,2026-01-11T07:10:00+00:00,2026-01-11T08:00:00+00:00,1\n",
        encoding="utf-8",
    )
    client.post("/api/runs", params={"source_file": str(source)})
    alerts = client.get("/api/dashboard").json()["alerts"]
    missing = next(alert for alert in alerts if alert["alert_type"] == "missing_load")
    assert missing["severity"] == "critical"
    assert missing["window_key"]
    assert missing["alert_id"]


def test_combined_dashboard_demo_has_fresh_late_and_critical_missing(tmp_path: Path):
    settings = Settings(tmp_path / "db.sqlite", tmp_path / "output")
    client = TestClient(create_app(settings))
    source = tmp_path / "dashboard_demo.csv"
    source.write_text(
        "dataset_id,expected_at,arrived_at,completed_at,row_count\n"
        "sales_daily,2026-01-12T06:00:00+00:00,2026-01-12T06:15:00+00:00,2026-01-12T09:30:00+00:00,1\n"
        "inventory_daily,2026-01-12T07:00:00+00:00,2026-01-12T07:10:00+00:00,2026-01-12T08:00:00+00:00,1\n",
        encoding="utf-8",
    )
    client.post("/api/runs", params={"source_file": str(source)})
    dashboard = client.get("/api/dashboard").json()
    statuses = {item["dataset_id"]: item["sla_status"] for item in dashboard["runs"][0]["datasets"]}
    assert statuses["sales_daily"] == "late"
    assert statuses["inventory_daily"] == "fresh"
    assert any(
        alert["alert_type"] == "missing_load" and alert["window_key"]
        for alert in dashboard["alerts"]
    )


def test_producer_csv_upload_and_validation(tmp_path: Path):
    settings = Settings(tmp_path / "db.sqlite", tmp_path / "output", tmp_path / "incoming")
    client = TestClient(create_app(settings))
    payload = (
        "dataset_id,expected_at,arrived_at,completed_at,row_count\n"
        "sales_daily,2026-04-15T06:00:00+00:00,2026-04-15T06:05:00+00:00,"
        "2026-04-15T07:00:00+00:00,3\n"
    )
    response = client.post(
        "/api/runs/upload", content=payload, headers={"content-type": "text/csv"}
    )
    assert response.status_code == 200
    assert response.json()["accepted_count"] == 1
    assert list((tmp_path / "incoming").glob("producer_*.csv"))
    rerun = client.post(
        "/api/runs/upload", content=payload, headers={"content-type": "text/csv"}
    )
    assert rerun.status_code == 200
    assert rerun.json()["status"] == "skipped"
    assert client.post("/api/runs/upload", content=payload).status_code == 415
    invalid_schema = client.post(
        "/api/runs/upload", content="dataset_id,unknown\nsales_daily,x\n",
        headers={"content-type": "text/csv"},
    )
    assert invalid_schema.status_code == 422
    with connect(settings.db_path) as connection:
        assert connection.execute("SELECT COUNT(*) FROM runs WHERE status='failed'").fetchone()[0] == 1


def test_generated_run_and_downloadable_outputs(tmp_path: Path):
    settings = Settings(tmp_path / "db.sqlite", tmp_path / "output", tmp_path / "incoming")
    client = TestClient(create_app(settings))
    generated = client.post("/api/runs/generate", params={"rows": 12, "randomize_status": "true"})
    assert generated.status_code == 200
    result = generated.json()
    assert result["accepted_count"] > 0
    assert result["source_file"].startswith(str(tmp_path / "incoming"))
    assert result["download_url"] == "/api/download/events"

    for kind, header in (
        ("events", "event_key"),
        ("observations", "observation_id"),
        ("rejects", "reject_id"),
        ("alerts", "alert_id"),
        ("windows", "window_key"),
    ):
        response = client.get(f"/api/download/{kind}")
        assert response.status_code == 200
        assert response.headers["content-type"].startswith("text/csv")
        assert response.headers["content-disposition"].endswith(f'{kind if kind != "observations" else "event_observations"}.csv"')
        assert response.text.splitlines()[0].startswith(header)
    assert client.get("/api/download/unknown").status_code == 404
    late = client.get("/api/download/events", params={"status": "late"})
    assert late.status_code == 200
    assert "sla_status" in late.text.splitlines()[0]
    missing = client.get("/api/download/windows", params={"status": "missing"})
    assert missing.status_code == 200
    assert "status" in missing.text.splitlines()[0]
    assert client.get("/api/download/events", params={"dataset_id": "not-a-dataset"}).text.count("\n") == 1
    assert client.post("/api/runs/generate", params={"rows": 1_000_001}).status_code == 422


def test_generated_four_rows_include_one_missing_dataset(tmp_path: Path):
    settings = Settings(tmp_path / "db.sqlite", tmp_path / "output", tmp_path / "incoming")
    client = TestClient(create_app(settings))
    response = client.post("/api/runs/generate", params={"rows": 4, "seed": 42})
    assert response.status_code == 200
    with connect(settings.db_path) as connection:
        missing = connection.execute(
            """SELECT COUNT(*) AS count FROM arrival_events
               WHERE run_id=? AND arrived_at IS NULL AND completed_at IS NULL""",
            (response.json()["run_id"],),
        ).fetchone()["count"]
    assert missing == 1


def test_monitor_runs_in_api_lifespan(tmp_path: Path):
    settings = Settings(
        tmp_path / "db.sqlite", tmp_path / "output", tmp_path / "incoming",
        monitor_interval_seconds=1,
    )
    with TestClient(create_app(settings)) as client:
        assert client.get("/api/health").status_code == 200
        deadline = time.monotonic() + 2
        while time.monotonic() < deadline:
            with connect(settings.db_path) as connection:
                count = connection.execute("SELECT COUNT(*) FROM expected_windows").fetchone()[0]
            if count:
                break
            time.sleep(0.01)
        assert count >= 24
        with connect(settings.db_path) as connection:
            assert connection.execute(
                "SELECT COUNT(*) FROM alerts WHERE window_key IS NOT NULL"
            ).fetchone()[0] > 0
        manual = client.post("/api/monitor/run")
        assert manual.status_code == 200
        assert manual.json()["windows_checked"] >= 24


def test_persisted_window_alert_can_be_acknowledged_through_api(tmp_path: Path):
    settings = Settings(tmp_path / "db.sqlite", tmp_path / "output", tmp_path / "incoming")
    with connect(settings.db_path) as connection:
        seed_datasets(connection)
        connection.execute("UPDATE datasets SET active=0 WHERE dataset_id!='sales_daily'")
        connection.commit()
    monitor_expected_windows(
        settings.db_path, datetime(2026, 4, 15, 6, 16, tzinfo=timezone.utc)
    )
    with connect(settings.db_path) as connection:
        alert_id = connection.execute(
            """SELECT alert_id FROM alerts
               WHERE dataset_id='sales_daily' AND alert_type='missing_load'
                 AND window_key IN (
                     SELECT window_key FROM expected_windows
                     WHERE expected_at='2026-04-15T06:00:00+00:00'
                 )"""
        ).fetchone()["alert_id"]

    client = TestClient(create_app(settings))
    acknowledged = client.post(f"/api/alerts/{alert_id}/acknowledge")
    assert acknowledged.status_code == 200
    assert acknowledged.json()["acknowledged"] is True
    assert client.post(f"/api/alerts/{alert_id}/acknowledge").status_code == 404
    with connect(settings.db_path) as connection:
        assert connection.execute(
            "SELECT acknowledged FROM alerts WHERE alert_id=?", (alert_id,)
        ).fetchone()[0] == 1
