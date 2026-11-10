import argparse
from pathlib import Path

from databeacon.pipeline.generator import generate_csv
from databeacon.pipeline.ingestion import run_pipeline
from databeacon.pipeline.reports import write_report
from databeacon.pipeline.reports import reconciliation
from databeacon.settings import Settings


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("command", choices=["generate", "run", "report", "reconcile"])
    parser.add_argument("--file", default="data/sample/events.csv")
    parser.add_argument("--db", default="data/databeacon.db")
    parser.add_argument("--rows", type=int, default=12)
    parser.add_argument("--seed", type=int)
    args = parser.parse_args()
    settings = Settings(Path(args.db))
    if args.command == "generate":
        output = Path(args.file)
        print(f"[1/1] Generating randomized append-only data")
        print(f"      output      : {output}")
        print(f"      rows        : {args.rows}")
        print(f"      seed        : {args.seed or 'random'}")
        print(f"      mode        : append")
        result = generate_csv(output, rows=args.rows, seed=args.seed,
                              append=True, randomize_status=True)
        print(f"      completed   : {result}")
    elif args.command == "run":
        source = Path(args.file)
        print("DataBeacon pipeline run")
        print("=" * 72)
        print(f"source file     : {source}")
        print(f"target database : {settings.db_path}")
        print("run id          : assigned after input fingerprint check")
        print("-" * 72)
        result = run_pipeline(settings.db_path, source, progress=lambda message: print(f"  -> {message}"))
        print("-" * 72)
        print(f"status          : {result['status']}")
        for key in ("run_id", "input_count", "accepted_count", "rejected_count", "duplicate_count"):
            if key in result:
                print(f"{key.replace('_', ' '):16}: {result[key]}")
        if result.get("status") == "skipped":
            print("action          : input fingerprint already processed; no duplicates created")
            print(f"fingerprint     : {result.get('fingerprint')}")
    else:
        if args.command == "report":
            output = settings.output_dir / "validation_report.json"
            print(f"Writing validation report to {output}")
            print(write_report(settings.db_path, output))
        else:
            result = reconciliation(settings.db_path)
            print("DataBeacon reconciliation")
            print("=" * 72)
            print(f"input rows      : {result['input_count']:,}")
            print(f"accepted rows   : {result['accepted_count']:,}")
            print(f"rejected rows   : {result['rejected_count']:,}")
            print(f"published events: {result['published_event_count']:,}")
            print(f"unique event ids: {result['unique_event_key_count']:,}")
            print(f"counts balanced : {'PASS' if result['counts_reconcile'] else 'FAIL'}")
            print(f"keys unique     : {'PASS' if result['unique_keys_reconcile'] else 'FAIL'}")
