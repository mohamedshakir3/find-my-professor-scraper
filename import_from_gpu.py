"""
Import GPU extraction results back into Supabase.

Reads cloud_output.jsonl, generates embeddings locally, and updates the
professors table with bio, research_interests, accepting_students, and embedding.

Usage:
    python import_from_gpu.py
    python import_from_gpu.py cloud_output.jsonl
"""
import json
import logging
import sys
from pathlib import Path

from db.connection import get_connection, put_connection, close_pool
from pipeline.embedder import embed_batch

logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] %(levelname)s: %(message)s",
    datefmt="%H:%M:%S",
    stream=sys.stdout,
)
logger = logging.getLogger("import_from_gpu")

_SKIP_MARKERS = {"[UNAVAILABLE]", "[ERROR]", ""}


def parse_jsonl(path: Path) -> list[dict]:
    records = []
    with path.open("r") as f:
        for line in f:
            line = line.strip()
            if line:
                records.append(json.loads(line))
    return records


def batch_update(rows: list[dict]) -> None:
    """Update professors in Supabase with extraction results + embeddings."""
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            from psycopg2.extras import execute_batch
            execute_batch(
                cur,
                """
                UPDATE professors SET
                    research_interests = %s,
                    bio = %s,
                    accepting_students = %s,
                    holistic_profile_string = %s,
                    embedding = %s,
                    email = COALESCE(%s, email)
                WHERE id = %s
                """,
                [
                    (
                        r.get("research_interests"),
                        r.get("bio"),
                        r.get("accepting_students"),
                        r.get("holistic_profile_string"),
                        r.get("embedding"),
                        r.get("llm_email"),
                        r["id"],
                    )
                    for r in rows
                ],
                page_size=200,
            )
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        put_connection(conn)


def run_import(input_file: str):
    path = Path(input_file)
    if not path.exists():
        logger.error(f"File not found: {path}")
        return

    records = parse_jsonl(path)
    logger.info(f"Loaded {len(records)} records from {path}")

    # Split into embeddable vs skip
    to_embed = []
    to_skip = []

    for r in records:
        hs = (r.get("holistic_profile_string") or "").strip()
        interests = r.get("unique_interests") or []
        # Clean out any "NONE" entries
        interests = [i.strip() for i in interests if i.strip().upper() != "NONE" and i.strip()] or None

        entry = {
            "id": r["id"],
            "research_interests": interests,
            "bio": r.get("bio"),
            "accepting_students": r.get("accepting_students"),
            "holistic_profile_string": hs or None,
            "llm_email": r.get("llm_email"),
        }

        if hs in _SKIP_MARKERS:
            entry["embedding"] = None
            to_skip.append(entry)
        else:
            to_embed.append(entry)

    logger.info(f"Will embed: {len(to_embed)} | Skip: {len(to_skip)}")

    # Batch embed
    if to_embed:
        texts = [r["holistic_profile_string"] for r in to_embed]
        logger.info(f"Generating {len(texts)} embeddings...")
        embeddings = embed_batch(texts)
        for entry, emb in zip(to_embed, embeddings):
            entry["embedding"] = str(emb)

    all_rows = to_embed + to_skip

    # Batch update in chunks
    chunk_size = 500
    for i in range(0, len(all_rows), chunk_size):
        chunk = all_rows[i : i + chunk_size]
        batch_update(chunk)
        logger.info(f"  Updated {min(i + chunk_size, len(all_rows))}/{len(all_rows)}")

    logger.info(f"Import complete: {len(to_embed)} with embeddings, {len(to_skip)} skipped")
    close_pool()


if __name__ == "__main__":
    input_file = sys.argv[1] if len(sys.argv) > 1 else "cloud_output.jsonl"
    run_import(input_file)
