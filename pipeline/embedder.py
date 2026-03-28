"""
Embedding generator for professors using sentence-transformers.
Uses all-MiniLM-L6-v2 (384 dimensions) to encode holistic_profile_string
into vector embeddings for semantic search.
"""
import logging
import time
from typing import List, Tuple

from sentence_transformers import SentenceTransformer

from scraper.db.connection import get_connection, put_connection

logger = logging.getLogger(__name__)

# Model is loaded once and cached
_model: SentenceTransformer | None = None


def get_model() -> SentenceTransformer:
    """Load and cache the sentence-transformer model."""
    global _model
    if _model is None:
        logger.info("Loading sentence-transformers model: all-MiniLM-L6-v2")
        _model = SentenceTransformer("all-MiniLM-L6-v2")
        logger.info("Model loaded successfully")
    return _model


def embed_text(text: str) -> List[float]:
    """Embed a single text string and return the 384-dim vector."""
    model = get_model()
    return model.encode(text, normalize_embeddings=True).tolist()


def embed_batch(texts: List[str], batch_size: int = 64) -> List[List[float]]:
    """Embed a batch of texts efficiently."""
    model = get_model()
    embeddings = model.encode(
        texts,
        batch_size=batch_size,
        show_progress_bar=True,
        normalize_embeddings=True,
    )
    return [e.tolist() for e in embeddings]


def fetch_professors_needing_embeddings(
    limit: int = 5000, university_id: int | None = None
) -> List[Tuple[int, str]]:
    """Fetch professors that have a holistic_profile_string but no embedding."""
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            sql = """
                SELECT id, holistic_profile_string
                FROM professors
                WHERE holistic_profile_string IS NOT NULL
                  AND holistic_profile_string NOT IN ('[UNAVAILABLE]', '[ERROR]')
                  AND embedding IS NULL
            """
            params: list = []
            if university_id:
                sql += " AND university_id = %s"
                params.append(university_id)
            sql += " ORDER BY id LIMIT %s"
            params.append(limit)
            cur.execute(sql, params)
            return [(row[0], row[1]) for row in cur.fetchall()]
    finally:
        put_connection(conn)


def update_embedding(professor_id: int, embedding: List[float]):
    """Store a single embedding in the database."""
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE professors SET embedding = %s WHERE id = %s",
                (str(embedding), professor_id),
            )
        conn.commit()
    finally:
        put_connection(conn)


def update_embeddings_batch(updates: List[Tuple[List[float], int]]):
    """Store embeddings in batch (more efficient)."""
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            from psycopg2.extras import execute_batch
            execute_batch(
                cur,
                "UPDATE professors SET embedding = %s WHERE id = %s",
                [(str(emb), pid) for emb, pid in updates],
                page_size=100,
            )
        conn.commit()
    finally:
        put_connection(conn)


def run_embedding_pipeline(
    batch_size: int = 64,
    university_id: int | None = None,
):
    """
    Main embedding pipeline: fetch professors without embeddings,
    generate embeddings in batches, and store them.
    """
    start = time.time()
    rows = fetch_professors_needing_embeddings(limit=10000, university_id=university_id)

    if not rows:
        logger.info("No professors need embeddings — all up to date!")
        return

    logger.info(f"Generating embeddings for {len(rows)} professors...")

    ids = [r[0] for r in rows]
    texts = [r[1] for r in rows]

    # Batch encode
    embeddings = embed_batch(texts, batch_size=batch_size)

    # Batch update DB
    logger.info(f"Storing {len(embeddings)} embeddings in database...")
    updates = list(zip(embeddings, ids))

    # Process in chunks of 500 to avoid huge transactions
    chunk_size = 500
    for i in range(0, len(updates), chunk_size):
        chunk = updates[i : i + chunk_size]
        update_embeddings_batch(chunk)
        logger.info(f"  Stored {min(i + chunk_size, len(updates))}/{len(updates)}")

    elapsed = time.time() - start
    logger.info(
        f"Embedding pipeline complete: {len(embeddings)} embeddings in {elapsed:.1f}s"
    )
