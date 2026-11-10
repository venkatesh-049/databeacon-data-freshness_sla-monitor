import ast
import json
from pathlib import Path
from tempfile import TemporaryDirectory

from databeacon.pipeline.ingestion import run_pipeline
from databeacon.pipeline.reports import reconciliation


ROOT = Path(__file__).resolve().parents[1]
OUTPUT = ROOT / "data" / "test_evidence.json"


def execute_case(name: str, source: str | Path, database: str, expected: dict,
                 evidence_dir: Path) -> dict:
    db_path = evidence_dir / database
    source_path = source if isinstance(source, Path) else ROOT / "data" / "sample" / source
    result = run_pipeline(db_path, source_path)
    evidence = {
        "name": name,
        "input_file": str(source_path.relative_to(ROOT)),
        "database": "temporary database (removed after evidence generation)",
        "expected": expected,
        "observed_run": result,
        "reconciliation": reconciliation(db_path),
        "passed": all(result.get(key) == value for key, value in expected.items()),
    }
    return evidence


def main() -> None:
    with TemporaryDirectory(prefix="databeacon-evidence-") as directory:
        evidence_dir = Path(directory)
        cases = [
            execute_case(
                "happy_path",
                ROOT / "data" / "evidence" / "happy_path_events.csv",
                "happy_path.db",
                {"status": "succeeded", "input_count": 12, "accepted_count": 12, "rejected_count": 0},
                evidence_dir,
            ),
            execute_case(
                "invalid_and_quarantine",
                "invalid_events.csv",
                "invalid_events.db",
                {"status": "partial", "input_count": 5, "accepted_count": 1, "rejected_count": 4},
                evidence_dir,
            ),
            execute_case(
                "attention_states",
                "attention_events.csv",
                "attention_events.db",
                {"status": "succeeded", "input_count": 2, "accepted_count": 2, "rejected_count": 0},
                evidence_dir,
            ),
        ]
    automated_test_count = sum(
        1
        for path in (ROOT / "tests").glob("test_*.py")
        for node in ast.walk(ast.parse(path.read_text(encoding="utf-8")))
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
        and node.name.startswith("test_")
    )
    payload = {
        "generated_by": "scripts/build_test_evidence.py",
        "purpose": "Reproducible test data, pipeline outputs, expected results, and reconciliation evidence",
        "test_command": "python scripts/run_tests.py",
        "cases": cases,
        "automated_test_count": automated_test_count,
        "all_cases_passed": all(case["passed"] for case in cases),
    }
    OUTPUT.write_text(json.dumps(payload, indent=2, default=str), encoding="utf-8")
    print(f"Wrote {OUTPUT}")


if __name__ == "__main__":
    main()
