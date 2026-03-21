"""
Export professors to JSONL for GPU processing.

Fetches HTML from profile URLs, converts to markdown locally (network-bound,
no GPU needed), and writes a JSONL file ready for the cloud worker.

Usage:
    python export_for_gpu.py --university "McGill University"
    python export_for_gpu.py --university "McGill University" --limit 50 --delay 1.0
"""
import argparse
import json
import logging
import sys
import time

from bs4 import BeautifulSoup
from markdownify import markdownify as md_convert

from db.connection import get_connection, put_connection, close_pool
from pipeline.profile_processor import ProfileProcessor

logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] %(levelname)s: %(message)s",
    datefmt="%H:%M:%S",
    stream=sys.stdout,
)
logger = logging.getLogger("export_for_gpu")

OUTPUT_FILE = "cloud_input.jsonl"


def fetch_professors(university: str | None = None, limit: int = 10000) -> list[dict]:
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            query = """
                SELECT id, name, university, faculty, department, website
                FROM professors
                WHERE website IS NOT NULL
            """
            params: list = []
            if university:
                query += " AND university = %s"
                params.append(university)
            query += " ORDER BY id LIMIT %s"
            params.append(limit)

            cur.execute(query, params)
            cols = [d[0] for d in cur.description]
            return [dict(zip(cols, r)) for r in cur.fetchall()]
    finally:
        put_connection(conn)


# Mutable container so process_one can bump the delay on 429
_state = {"current_delay": 2.0}


def process_one(row: dict, processor: ProfileProcessor, max_retries: int = 3) -> dict | None:
    """Fetch HTML and convert to markdown for a single professor."""
    prof_id = row["id"]
    name = row["name"]
    url = row["website"]

    for attempt in range(max_retries):
        try:
            _, html, _ = processor.fetch_and_hash(url)
            if not html:
                logger.warning(f"  [{prof_id}] {name} — fetch failed")
                return None

            email = processor.extract_email(html)

            soup = BeautifulSoup(html, "html.parser")
            for tag in soup(["script", "style", "nav", "footer", "header", "aside", "meta", "noscript"]):
                tag.decompose()
            markdown = md_convert(str(soup), strip=["a", "img", "table"]).strip()
            if len(markdown) > 10000:
                markdown = markdown[:10000]

            return {
                "id": prof_id,
                "name": name,
                "university": row["university"],
                "faculty": row["faculty"],
                "department": row["department"],
                "email": email,
                "profile_markdown": markdown,
            }

        except ProfileProcessor.RateLimitError:
            # Double the global delay (cap at 30s) and wait before retrying
            _state["current_delay"] = min(30.0, _state["current_delay"] * 2)
            wait = _state["current_delay"] * (attempt + 1)
            logger.warning(f"  [{prof_id}] {name} — 429, waiting {wait:.0f}s, global delay now {_state['current_delay']:.1f}s (attempt {attempt+1}/{max_retries})")
            time.sleep(wait)
        except Exception as e:
            logger.error(f"  [{prof_id}] {name} — error: {e}")
            return None

    logger.error(f"  [{prof_id}] {name} — failed after {max_retries} retries")
    return None


def load_existing_ids() -> set[int]:
    """Load IDs already exported so we can resume."""
    try:
        ids = set()
        with open(OUTPUT_FILE, "r") as f:
            for line in f:
                if line.strip():
                    ids.add(json.loads(line)["id"])
        return ids
    except FileNotFoundError:
        return set()


def run_export(university: str | None, limit: int, delay: float):
    rows = fetch_professors(university, limit)
    if not rows:
        logger.error("No professors found.")
        return

    # Resume support — skip already exported professors
    done_ids = load_existing_ids()
    if done_ids:
        logger.info(f"Resuming — {len(done_ids)} already exported, skipping them")
        rows = [r for r in rows if r["id"] not in done_ids]
        if not rows:
            logger.info("All professors already exported!")
            return

    logger.info(f"Fetching HTML for {len(rows)} professors (initial delay={delay}s)...")
    processor = ProfileProcessor()
    _state["current_delay"] = delay
    ok_streak = 0
    ok_count = 0

    # Append mode so we don't overwrite previous progress
    with open(OUTPUT_FILE, "a") as f:
        for i, row in enumerate(rows, 1):
            result = process_one(row, processor)

            if result:
                f.write(json.dumps(result, ensure_ascii=False) + "\n")
                f.flush()
                ok_count += 1
                ok_streak += 1
                # After 20 consecutive successes, gradually speed back up
                if ok_streak >= 20 and _state["current_delay"] > delay:
                    _state["current_delay"] = max(delay, _state["current_delay"] * 0.8)
                    ok_streak = 0
                    logger.info(f"  Easing delay down to {_state['current_delay']:.1f}s")
            else:
                ok_streak = 0

            if i % 25 == 0 or i == len(rows):
                logger.info(f"  Progress: {i}/{len(rows)} fetched, {ok_count} ok (delay={_state['current_delay']:.1f}s)")

            time.sleep(_state["current_delay"])

    logger.info(f"Exported {ok_count}/{len(rows)} professors to {OUTPUT_FILE}")
    close_pool()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Export professor markdown for GPU processing")
    parser.add_argument("--university", type=str, default=None, help="Filter to one university")
    parser.add_argument("--limit", type=int, default=10000, help="Max professors to export")
    parser.add_argument("--delay", type=float, default=0.5, help="Seconds between requests (default 0.5)")
    args = parser.parse_args()

    run_export(
        university=args.university,
        limit=args.limit,
        delay=args.delay,
    )
