import csv
import random
from datetime import datetime, timedelta, timezone
from pathlib import Path

FIELDS = ["dataset_id", "expected_at", "arrived_at", "completed_at", "row_count"]


def generate_csv(path: Path, rows: int = 12, seed: int | None = 42,
                 append: bool = False, randomize_status: bool = False) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    rng = random.Random(seed)
    dataset_ids = [
        "sales_daily", "inventory_daily", "customers_daily", "orders_daily",
        "payments_daily", "products_daily", "shipments_daily", "returns_daily",
        "promotions_daily", "suppliers_daily", "finance_daily", "support_daily",
    ]
    existing = path.exists() and append
    base = datetime(2026, 1, 1, tzinfo=timezone.utc)
    if existing:
        with path.open(newline="", encoding="utf-8") as previous:
            old_rows = list(csv.DictReader(previous))
        if old_rows:
            latest = max(datetime.fromisoformat(row["expected_at"]) for row in old_rows)
            base = latest + timedelta(days=1)
    mode = "a" if existing else "w"
    with path.open(mode, newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=FIELDS)
        if not existing:
            writer.writeheader()
        profiles = (["healthy"] * 7 + ["warning"] * 3 + ["critical"] * 2)
        rng.shuffle(profiles)
        for index in range(rows):
            dataset_id = dataset_ids[index % len(dataset_ids)]
            expected = base + timedelta(days=index // len(dataset_ids), hours=6 + index % 8)
            profile = profiles[index % len(profiles)] if randomize_status else "healthy"
            if profile == "critical":
                continue
            arrived = expected + timedelta(minutes=rng.choice([5, 15, 30]))
            completed = arrived + timedelta(minutes=(
                rng.choice([10, 20, 30]) if profile == "healthy"
                else rng.choice([300, 600, 900])
            ))
            writer.writerow({
                "dataset_id": dataset_id,
                "expected_at": expected.isoformat(),
                "arrived_at": arrived.isoformat(),
                "completed_at": completed.isoformat(),
                "row_count": rng.randint(10, 1000),
            })
    return path
