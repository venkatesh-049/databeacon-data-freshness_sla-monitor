import hashlib
import json
import uuid
from datetime import timezone
from pathlib import Path

from databeacon.db import repository
from databeacon.db.connection import connect
from databeacon.domain.rules import alert_for, event_key, minutes_between, sla_status, utc_now
from databeacon.pipeline.validation import raw_json, read_records, validate_record
from databeacon.observability import logger


def run_pipeline(db_path: Path, source_file: Path, now=None, progress=None) -> dict:
    progress = progress or (lambda _message: None)
    progress("Opening source and initializing SQLite target")
    connection = connect(db_path)
    repository.seed_datasets(connection)
    payload = source_file.read_bytes()
    fingerprint = hashlib.sha256(payload).hexdigest()
    if repository.existing_fingerprint(connection, fingerprint):
        logger.info("run_skipped fingerprint=%s source_file=%s", fingerprint, source_file)
        connection.close()
        return {"status": "skipped", "reason": "input_already_processed", "fingerprint": fingerprint}
    run_id = str(uuid.uuid4())
    started = now or utc_now()
    repository.create_run(connection, run_id, fingerprint, str(source_file), started.isoformat())
    accepted = rejected = duplicate_count = updated_count = input_count = 0
    seen: set[str] = set()
    try:
        progress(f"Reading and validating {source_file.name}")
        records = read_records(source_file)
        datasets = repository.dataset_map(connection)
        for source_row, row in records:
            input_count += 1
            parsed, error = validate_record(row, datasets, seen)
            if error:
                rejected += 1
                repository.insert_reject(connection, {
                    "run_id": run_id, "source_file": str(source_file), "source_row": source_row,
                    "reason_code": error[0], "reason_detail": error[1], "raw_payload": raw_json(row),
                })
                continue
            config = datasets[parsed.dataset_id]
            key = event_key(parsed.dataset_id, parsed.expected_at)
            status = sla_status(parsed.arrived_at, parsed.completed_at, parsed.expected_at,
                                config["sla_minutes"], started)
            completion = (minutes_between(parsed.expected_at, parsed.completed_at)
                          if parsed.completed_at else None)
            freshness = (minutes_between(parsed.arrived_at, started)
                         if parsed.arrived_at else None)
            event = {
                "event_key": key, "run_id": run_id, "dataset_id": parsed.dataset_id,
                "source_file": str(source_file), "expected_at": parsed.expected_at.isoformat(),
                "arrived_at": parsed.arrived_at.isoformat() if parsed.arrived_at else None,
                "completed_at": parsed.completed_at.isoformat() if parsed.completed_at else None,
                "row_count": parsed.row_count, "freshness_minutes": freshness,
                "completion_minutes": completion, "sla_status": status, "raw_payload": raw_json(row),
                "observed_at": started.isoformat(), "sla_minutes": config["sla_minutes"],
            }
            outcome = repository.insert_event(connection, event)
            if outcome["action"] == "conflict":
                rejected += 1
                repository.insert_reject(connection, {
                    "run_id": run_id, "source_file": str(source_file), "source_row": source_row,
                    "reason_code": "conflicting_event_update",
                    "reason_detail": "event update would make completion precede arrival",
                    "raw_payload": raw_json(row),
                })
                continue
            if outcome["action"] in {"inserted", "updated"}:
                accepted += 1
                if outcome["action"] == "updated":
                    updated_count += 1
                alert = alert_for(
                    outcome["sla_status"], config["dataset_name"], key,
                    outcome["completion_minutes"], config["sla_minutes"],
                )
                if alert:
                    severity, alert_type, message, action = alert
                    repository.insert_alert(connection, {
                        "run_id": run_id, "event_key": key, "dataset_id": parsed.dataset_id,
                        "severity": severity, "alert_type": alert_type, "message": message,
                        "action": action, "created_at": started.isoformat(),
                    })
            else:
                duplicate_count += 1
                repository.insert_duplicate(connection, {
                    "run_id": run_id, "dataset_id": parsed.dataset_id, "event_key": key,
                    "source_file": str(source_file), "source_row": source_row,
                    "reason": "event key already published",
                })
            if input_count % 1000 == 0:
                progress(f"Processed {input_count:,} rows ({accepted:,} accepted, {rejected:,} rejected)")
        progress("Publishing run metrics, freshness results, and alerts")
        status = "succeeded" if rejected == 0 else "partial"
        repository.finish_run(connection, run_id, utc_now().isoformat(), status,
                              input_count, accepted, rejected, duplicate_count=duplicate_count,
                              updated_count=updated_count)
        connection.commit()
        logger.info(
            "run_finished run_id=%s status=%s input_count=%s accepted_count=%s "
            "rejected_count=%s duplicate_count=%s updated_count=%s",
            run_id, status, input_count, accepted, rejected, duplicate_count, updated_count,
        )
        return {"run_id": run_id, "status": status, "input_count": input_count,
            "accepted_count": accepted, "rejected_count": rejected,
            "duplicate_count": duplicate_count, "updated_count": updated_count}
    except Exception as exc:
        repository.finish_run(connection, run_id, utc_now().isoformat(), "failed",
                              input_count, accepted, rejected, str(exc), duplicate_count, updated_count)
        logger.exception("run_failed run_id=%s input_count=%s error=%s", run_id, input_count, exc)
        raise
    finally:
        connection.close()
