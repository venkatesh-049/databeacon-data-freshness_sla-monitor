import sqlite3
from pathlib import Path

SCHEMA_PATH = Path(__file__).with_name("schema.sql")


def connect(db_path: Path) -> sqlite3.Connection:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(db_path)
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA foreign_keys = ON")
    connection.executescript(SCHEMA_PATH.read_text(encoding="utf-8"))
    columns = {row[1] for row in connection.execute("PRAGMA table_info(runs)")}
    if "duplicate_count" not in columns:
        connection.execute("ALTER TABLE runs ADD COLUMN duplicate_count INTEGER NOT NULL DEFAULT 0")
        connection.commit()
    return connection
