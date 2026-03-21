"""
Migration: add bio and accepting_students columns to professors.
Safe to run multiple times (uses IF NOT EXISTS guard).
"""
import logging
from scraper.db.connection import get_connection, put_connection

logger = logging.getLogger(__name__)

_MIGRATIONS = [
    "ALTER TABLE professors ADD COLUMN IF NOT EXISTS bio TEXT",
    "ALTER TABLE professors ADD COLUMN IF NOT EXISTS accepting_students VARCHAR(3)",
]


def run() -> None:
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            for sql in _MIGRATIONS:
                cur.execute(sql)
                logger.info(f"Executed: {sql}")
        conn.commit()
        logger.info("Migration complete.")
    except Exception:
        conn.rollback()
        raise
    finally:
        put_connection(conn)


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="[%(asctime)s] %(levelname)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    run()
