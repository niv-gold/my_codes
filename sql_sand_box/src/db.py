import sqlite3
from pathlib import Path

DB_PATH = Path(__file__).parent.parent / "db" / "local.db"
print(DB_PATH)
def get_connection() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row  # dict-like row access
    conn.execute("PRAGMA journal_mode=WAL")  # safer concurrent writes
    return conn

def run_query(sql: str, params: tuple = ()) -> list[dict]:
    with get_connection() as conn:
        rows = conn.execute(sql, params).fetchall()
        return [dict(r) for r in rows]