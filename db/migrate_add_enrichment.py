"""
Migration: add qs_ranking, google_scholar_url, cited_by columns to professors.
Safe to run multiple times (uses IF NOT EXISTS guard).
"""
import logging
from scraper.db.connection import get_connection, put_connection

logger = logging.getLogger(__name__)

_MIGRATIONS = [
    "ALTER TABLE professors ADD COLUMN IF NOT EXISTS qs_ranking VARCHAR(20)",
    "ALTER TABLE professors ADD COLUMN IF NOT EXISTS google_scholar_url TEXT",
    "ALTER TABLE professors ADD COLUMN IF NOT EXISTS cited_by INTEGER",
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
