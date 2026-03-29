"""
Generate embeddings from holistic_string_new using Cohere embed-english-light-v3.0
(384 dims) and store in embedding_new column.

Usage:
    python embed_new.py
"""
import logging
import os
import sys
import time

import cohere
from dotenv import load_dotenv
from psycopg2.extras import execute_batch

from db.connection import get_connection, put_connection, close_pool

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] %(levelname)s: %(message)s",
    datefmt="%H:%M:%S",
    stream=sys.stdout,
)
logger = logging.getLogger("embed_new")

COHERE_BATCH_SIZE = 96  # Cohere allows up to 96 texts per request
DB_CHUNK_SIZE = 500
MODEL = "embed-english-light-v3.0"
MAX_RETRIES = 5


def embed_one_batch(client: cohere.Client, texts: list[str]) -> list[list[float]]:
    """Embed a single batch with retry on 429."""
    for attempt in range(MAX_RETRIES):
        try:
            response = client.embed(
                texts=texts,
                model=MODEL,
                input_type="search_document",
            )
            return response.embeddings
        except cohere.errors.TooManyRequestsError:
            wait = 60 * (attempt + 1)
            logger.warning(f"Rate limited, waiting {wait}s (attempt {attempt + 1}/{MAX_RETRIES})")
            time.sleep(wait)
    raise RuntimeError("Max retries exceeded on Cohere rate limit")


def main():
    client = cohere.Client(os.getenv("COHERE_API_KEY"))

    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("ALTER TABLE professors ADD COLUMN IF NOT EXISTS embedding_new vector(384)")
            conn.commit()
            logger.info("Column embedding_new ready")

            cur.execute("""
                SELECT id, holistic_string_new
                FROM professors
                WHERE holistic_string_new IS NOT NULL
                  AND embedding_new IS NULL
                ORDER BY id
            """)
            rows = cur.fetchall()
    finally:
        put_connection(conn)

    if not rows:
        logger.info("No professors need new embeddings — all up to date!")
        close_pool()
        return

    logger.info(f"Generating Cohere embeddings for {len(rows)} professors...")
    start = time.time()
    total_done = 0

    # Process in chunks: embed + commit each chunk so progress survives crashes
    for i in range(0, len(rows), COHERE_BATCH_SIZE):
        batch_rows = rows[i:i + COHERE_BATCH_SIZE]
        batch_ids = [r[0] for r in batch_rows]
        batch_texts = [r[1] for r in batch_rows]

        embeddings = embed_one_batch(client, batch_texts)

        conn = get_connection()
        try:
            with conn.cursor() as cur:
                execute_batch(
                    cur,
                    "UPDATE professors SET embedding_new = %s WHERE id = %s",
                    [(str(emb), pid) for emb, pid in zip(embeddings, batch_ids)],
                    page_size=100,
                )
            conn.commit()
        finally:
            put_connection(conn)

        total_done += len(batch_rows)
        logger.info(f"  {total_done}/{len(rows)} embedded and stored")
        time.sleep(1)  # pace requests

    close_pool()
    logger.info(f"Done — {total_done} new embeddings in {time.time() - start:.1f}s")


if __name__ == "__main__":
    main()
