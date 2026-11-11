"""Print public SQLite views in one read-only transaction, without grading."""
import argparse
import json
from pathlib import Path
import sqlite3


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", required=True)
    args = parser.parse_args()
    path = Path(args.db).resolve()
    if not path.is_file():
        parser.exit(1, "Database does not exist; nothing was created.\n")
    connection = sqlite3.connect(path.as_uri() + "?mode=ro", uri=True, timeout=3)
    connection.row_factory = sqlite3.Row
    try:
        connection.execute("BEGIN")
        output = {}
        for name in ("sync_state", "sync_items", "sync_applied", "sync_quarantine"):
            output[name] = [dict(row) for row in connection.execute(f'SELECT * FROM "{name}"')]
        print(json.dumps(output, ensure_ascii=False, indent=2))
    except sqlite3.Error as exc:
        parser.exit(1, f"Read failed: {exc}\n")
    finally:
        connection.close()


if __name__ == "__main__":
    main()
