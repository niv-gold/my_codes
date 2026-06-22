from pathlib import Path
from src.db import get_connection

# Anchor to the script's own directory
BASE_DIR = Path(__file__).parent

def init_db():
    ddl = (BASE_DIR / "sql" / "init.sql").read_text()
    with get_connection() as conn:
        conn.executescript(ddl)
    print("DB initialized.")


def seed_db():
    dml = (BASE_DIR / "sql" / "seed.sql").read_text()
    with get_connection() as conn:
        conn.executescript(dml)
    print("DB seeded.")

if __name__ == "__main__":
    init_db()
    seed_db()
    