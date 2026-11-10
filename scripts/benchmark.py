import argparse
import tempfile
import time
from pathlib import Path

from databeacon.pipeline.generator import generate_csv
from databeacon.pipeline.ingestion import run_pipeline


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--rows", type=int, default=100_000)
    args = parser.parse_args()
    with tempfile.TemporaryDirectory() as directory:
        root = Path(directory)
        source = generate_csv(root / "events.csv", args.rows)
        started = time.perf_counter()
        result = run_pipeline(root / "databeacon.db", source)
        elapsed = time.perf_counter() - started
        print({"rows": args.rows, "elapsed_seconds": round(elapsed, 3),
               "rows_per_second": round(args.rows / elapsed, 2), "result": result})


if __name__ == "__main__":
    main()
