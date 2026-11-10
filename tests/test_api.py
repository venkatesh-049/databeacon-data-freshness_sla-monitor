from pathlib import Path

from fastapi.testclient import TestClient

from databeacon.api.app import create_app
from databeacon.pipeline.generator import generate_csv
from databeacon.settings import Settings


def test_api_dashboard_and_trigger(tmp_path: Path):
    settings = Settings(tmp_path / "db.sqlite", tmp_path / "output")
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
    settings = Settings(tmp_path / "db.sqlite", tmp_path / "output")
    client = TestClient(create_app(settings))
    data = client.get("/api/dashboard").json()
    assert len(data["datasets"]) == 12
    assert all(item["sla_status"] in {"missing", "scheduled"} for item in data["datasets"])
    if any(item["sla_status"] == "missing" for item in data["datasets"]):
        assert any(item["alert_type"] == "missing_load" for item in data["alerts"])


def test_dashboard_auto_refreshes_against_latest_run_state():
    response = TestClient(create_app(Settings())).get("/")
    assert response.status_code == 200
    assert "setInterval(load,5000)" in response.text
    assert "Expand any run to inspect dataset health for that run" in response.text
    assert "run.datasets" in response.text
    assert "openRuns" in response.text
    assert "data-run-id" in response.text
    assert "max-width:1600px" in response.text
    assert "overflow-x:hidden" in response.text


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
    statuses = {item["dataset_id"]: item["sla_status"] for item in dashboard["datasets"]}
    assert statuses["sales_daily"] == "late"
    assert statuses["inventory_daily"] == "incomplete"
    assert statuses["customers_daily"] == "missing"


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
    statuses = {item["dataset_id"]: item["sla_status"] for item in data["datasets"]}
    assert statuses["sales_daily"] == "late"
    assert statuses["inventory_daily"] == "missing"
    assert statuses["customers_daily"] == "missing"


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
    assert missing["dataset_id"] == "customers_daily"


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
    statuses = {item["dataset_id"]: item["sla_status"] for item in dashboard["datasets"]}
    assert statuses["sales_daily"] == "late"
    assert statuses["inventory_daily"] == "fresh"
    assert statuses["customers_daily"] == "missing"
    alerts = {(item["dataset_id"], item["severity"], item["alert_type"]) for item in dashboard["alerts"]}
    assert ("sales_daily", "warning", "sla_breach") in alerts
    assert ("customers_daily", "critical", "missing_load") in alerts
