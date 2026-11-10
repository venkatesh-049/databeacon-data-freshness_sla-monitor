import json
from pathlib import Path

from databeacon.pipeline.ingestion import run_pipeline
from databeacon.pipeline.generator import generate_csv
from databeacon.pipeline.reports import reconciliation


ROOT = Path(__file__).resolve().parents[1]
OUTPUT = ROOT / "data" / "test_evidence.json"


def execute_case(name: str, source: str | Path, database: str, expected: dict) -> dict:
    db_path = ROOT / "data" / "evidence" / database
    if db_path.exists():
        db_path.unlink()
    source_path = source if isinstance(source, Path) else ROOT / "data" / "sample" / source
    result = run_pipeline(db_path, source_path)
    evidence = {
        "name": name,
        "input_file": str(source_path.relative_to(ROOT)),
        "database": f"data/evidence/{database}",
        "expected": expected,
        "observed_run": result,
        "reconciliation": reconciliation(db_path),
        "passed": all(result.get(key) == value for key, value in expected.items()),
    }
    return evidence


def main() -> None:
    evidence_dir = ROOT / "data" / "evidence"
    evidence_dir.mkdir(parents=True, exist_ok=True)
    generate_csv(evidence_dir / "happy_path_events.csv", rows=12, seed=42)
    cases = [
        execute_case(
            "happy_path",
            ROOT / "data" / "evidence" / "happy_path_events.csv",
            "happy_path.db",
            {"status": "succeeded", "input_count": 12, "accepted_count": 12, "rejected_count": 0},
        ),
        execute_case(
            "invalid_and_quarantine",
            "invalid_events.csv",
            "invalid_events.db",
            {"status": "partial", "input_count": 5, "accepted_count": 1, "rejected_count": 4},
        ),
        execute_case(
            "attention_states",
            "attention_events.csv",
            "attention_events.db",
            {"status": "succeeded", "input_count": 2, "accepted_count": 2, "rejected_count": 0},
        ),
    ]
    payload = {
        "generated_by": "scripts/build_test_evidence.py",
        "purpose": "Reproducible test data, pipeline outputs, expected results, and reconciliation evidence",
        "test_command": "python -m pytest",
        "cases": cases,
        "automated_test_count": 17,
        "all_cases_passed": all(case["passed"] for case in cases),
    }
    OUTPUT.write_text(json.dumps(payload, indent=2, default=str), encoding="utf-8")
    print(f"Wrote {OUTPUT}")


if __name__ == "__main__":
    main()
