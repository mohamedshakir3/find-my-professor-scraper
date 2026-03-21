import json
import logging
import sys
from pathlib import Path
from typing import Union

from scraper.db.repositories import batch_update_professor_ai_data
from scraper.pipeline.embedder import embed_batch
from scraper.db.connection import close_pool

logger = logging.getLogger(__name__)

_PROJECT_ROOT = Path(__file__).parents[2]
_DEFAULT_INPUT = _PROJECT_ROOT / "cloud_output.jsonl"

_SKIP_MARKERS = {"[UNAVAILABLE]", "[ERROR]"}


def _parse_jsonl(path: Path) -> list[dict]:
    """Parse a file of concatenated JSON objects — handles compact or pretty-printed."""
    content = path.read_text(encoding="utf-8")
    decoder = json.JSONDecoder(strict=False)
    records, idx = [], 0
    while idx < len(content):
        while idx < len(content) and content[idx] in " \t\n\r":
            idx += 1
        if idx >= len(content):
            break
        obj, idx = decoder.raw_decode(content, idx)
        records.append(obj)
    return records


def _normalize_interests(interests: list[str]) -> list[str] | None:
    """
    Filter out LLM "NONE" responses.
    Returns None if no valid interests remain (triggers null in DB).
    """
    cleaned = [i.strip() for i in interests if i.strip().upper() != "NONE" and i.strip()]
    return cleaned if cleaned else None


def import_from_jsonl(filename: Union[str, Path, None] = None) -> None:
    path = Path(filename) if filename else _DEFAULT_INPUT
    if not path.exists():
        logger.error(f"Input file not found: {path}")
        return

    logger.info(f"Reading briefcase: {path}")
    records = _parse_jsonl(path)
    logger.info(f"Parsed {len(records)} records.")

    # Classify records
    to_embed = []    # (record, cleaned_interests) — valid holistic string, needs embedding
    no_embed = []    # records with [UNAVAILABLE]/[ERROR] holistic strings

    for r in records:
        hs = (r.get("holistic_profile_string") or "").strip()
        interests = _normalize_interests(r.get("unique_interests") or [])

        if hs in _SKIP_MARKERS or not hs:
            no_embed.append({
                "id": r["id"],
                "unique_interests": interests,
                "holistic_profile_string": hs or None,
                "embedding": None,
            })
        else:
            to_embed.append((r["id"], interests, hs))

    logger.info(f"  Will embed: {len(to_embed)} | Skip embedding: {len(no_embed)}")

    # Batch embed all valid holistic strings in one pass
    texts = [hs for _, _, hs in to_embed]
    logger.info(f"Generating {len(texts)} embeddings (this may take a moment)...")
    embeddings = embed_batch(texts)

    rows = [
        {"id": pid, "unique_interests": interests, "holistic_profile_string": hs, "embedding": emb}
        for (pid, interests, hs), emb in zip(to_embed, embeddings)
    ] + no_embed

    # Batch update DB in chunks of 500
    chunk_size = 500
    for i in range(0, len(rows), chunk_size):
        chunk = rows[i: i + chunk_size]
        batch_update_professor_ai_data(chunk)
        logger.info(f"  Updated {min(i + chunk_size, len(rows))}/{len(rows)} rows")

    none_count = sum(1 for r in rows if r["unique_interests"] is None)
    logger.info(
        f"Import complete: {len(rows)} total | "
        f"{len(no_embed)} without embeddings | "
        f"{none_count} with null interests (NONE/empty)"
    )
    close_pool()


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="[%(asctime)s] %(levelname)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    import_from_jsonl(sys.argv[1] if len(sys.argv) > 1 else None)
