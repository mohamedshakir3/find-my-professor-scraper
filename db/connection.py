import os
import logging
from pathlib import Path
import psycopg2
from psycopg2 import pool
from dotenv import load_dotenv

logger = logging.getLogger(__name__)

# Load .env from scraper directory
_env_path = Path(__file__).parent.parent / ".env"
load_dotenv(_env_path)

_pool: pool.SimpleConnectionPool | None = None


def get_pool() -> pool.SimpleConnectionPool:
    """Get or create the connection pool."""
    global _pool
    if _pool is None or _pool.closed:
        database_url = os.environ.get("DATABASE_URL")
        if not database_url:
            raise RuntimeError(
                "DATABASE_URL not set. Create scraper/.env or set the env var."
            )
        _pool = pool.SimpleConnectionPool(minconn=1, maxconn=5, dsn=database_url)
        logger.info("Database connection pool created.")
    return _pool


def get_connection():
    """Get a connection from the pool. Caller must return it via put_connection()."""
    return get_pool().getconn()


def put_connection(conn):
    """Return a connection to the pool."""
    get_pool().putconn(conn)


def close_pool():
    """Close all connections in the pool."""
    global _pool
    if _pool and not _pool.closed:
        _pool.closeall()
        logger.info("Database connection pool closed.")
        _pool = None
