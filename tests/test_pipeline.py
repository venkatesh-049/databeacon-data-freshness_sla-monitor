import csv
from pathlib import Path

from databeacon.db.connection import connect
from databeacon.pipeline.generator import generate_csv
from databeacon.pipeline.ingestion import run_pipeline
from databeacon.pipeline.reports import reconciliation


def test_happy_path_and_idempotency(tmp_path: Path):
    source = generate_csv(tmp_path / "events.csv", rows=6)
    db = tmp_path / "db.sqlite"
    first = run_pipeline(db, source)
    second = run_pipeline(db, source)
    assert first["input_count"] == 6
    assert first["accepted_count"] == 6
    assert second["status"] == "skipped"
    with connect(db) as connection:
        assert connection.execute("SELECT COUNT(*) FROM arrival_events").fetchone()[0] == 6


def test_rejects_malformed_duplicate_and_unknown_reference(tmp_path: Path):
    source = tmp_path / "bad.csv"
    rows = [
        {"dataset_id": "sales_daily", "expected_at": "2026-01-01T06:00:00+00:00",
         "arrived_at": "2026-01-01T06:05:00+00:00", "completed_at": "2026-01-01T07:00:00+00:00", "row_count": "3"},
        {"dataset_id": "sales_daily", "expected_at": "2026-01-01T06:00:00+00:00",
         "arrived_at": "2026-01-01T06:05:00+00:00", "completed_at": "2026-01-01T07:00:00+00:00", "row_count": "3"},
        {"dataset_id": "nope", "expected_at": "bad", "arrived_at": "", "completed_at": "", "row_count": "x"},
    ]
    with source.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=rows[0].keys())
        writer.writeheader()
        writer.writerows(rows)
    result = run_pipeline(tmp_path / "db.sqlite", source)
    assert result["accepted_count"] == 1
    assert result["rejected_count"] == 2


def test_schema_change_fails_explicitly(tmp_path: Path):
    source = tmp_path / "bad.csv"
    source.write_text("dataset_id,unexpected\nsales_daily,x\n", encoding="utf-8")
    try:
        run_pipeline(tmp_path / "db.sqlite", source)
    except ValueError as exc:
        assert "unexpected schema" in str(exc)
    else:
        raise AssertionError("schema change should fail")


def test_empty_batch_is_successful_and_null_or_out_of_order_rows_are_rejected(tmp_path: Path):
    empty = tmp_path / "empty.csv"
    empty.write_text("dataset_id,expected_at,arrived_at,completed_at,row_count\n", encoding="utf-8")
    empty_result = run_pipeline(tmp_path / "empty.sqlite", empty)
    assert empty_result["status"] == "succeeded"
    assert empty_result["input_count"] == 0

    source = tmp_path / "edge.csv"
    source.write_text(
        "dataset_id,expected_at,arrived_at,completed_at,row_count\n"
        "sales_daily,2026-01-01T06:00:00+00:00,,2026-01-01T07:00:00+00:00,1\n"
        "sales_daily,2026-01-02T06:00:00+00:00,2026-01-02T07:00:00+00:00,2026-01-02T06:30:00+00:00,1\n",
        encoding="utf-8",
    )
    result = run_pipeline(tmp_path / "edge.sqlite", source)
    assert result["accepted_count"] == 0
    assert result["rejected_count"] == 2


def test_failed_schema_run_can_be_retried_with_corrected_input(tmp_path: Path):
    source = tmp_path / "retry.csv"
    source.write_text("dataset_id,unexpected\nsales_daily,x\n", encoding="utf-8")
    db = tmp_path / "retry.sqlite"
    try:
        run_pipeline(db, source)
    except ValueError:
        pass
    corrected = generate_csv(tmp_path / "corrected.csv", rows=2)
    result = run_pipeline(db, corrected)
    assert result["status"] == "succeeded"
    with connect(db) as connection:
        assert connection.execute("SELECT COUNT(*) FROM runs WHERE status='failed'").fetchone()[0] == 1


def test_reconciliation_asserts_counts_and_unique_keys(tmp_path: Path):
    source = generate_csv(tmp_path / "events.csv", rows=4)
    run_pipeline(tmp_path / "db.sqlite", source)
    result = reconciliation(tmp_path / "db.sqlite")
    assert result["counts_reconcile"] is True
    assert result["unique_keys_reconcile"] is True


def test_append_generation_creates_new_batch_and_counts_duplicates(tmp_path: Path):
    source = tmp_path / "events.csv"
    generate_csv(source, rows=12, seed=7)
    first = run_pipeline(tmp_path / "db.sqlite", source)
    generate_csv(source, rows=12, seed=8, append=True, randomize_status=True)
    second = run_pipeline(tmp_path / "db.sqlite", source)

    assert second["status"] in {"succeeded", "partial"}
    assert second["duplicate_count"] == first["accepted_count"]
    assert second["accepted_count"] > 0
    result = reconciliation(tmp_path / "db.sqlite")
    assert result["counts_reconcile"] is True
    assert result["published_event_count"] > first["accepted_count"]
