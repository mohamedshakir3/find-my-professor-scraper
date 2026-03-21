"""
Dry-run extraction test.

Fetches a sample of professors from the database, runs Phase 2 (HTML → Markdown)
and Phase 3 (LLM extraction with structured schema) locally, and writes results to
test_output.jsonl for inspection. Does NOT write anything back to the database.

Usage:
    python test_extraction.py                     # 10 random profs
    python test_extraction.py --limit 5           # 5 profs
    python test_extraction.py --university "University of Ottawa" --limit 3
    python test_extraction.py --url "https://example.com/prof-page"   # single URL, no DB needed
"""
import argparse
import json
import logging
import sys
from pathlib import Path

from bs4 import BeautifulSoup
from markdownify import markdownify as md_convert

# Run from project root — db/ and pipeline/ are top-level packages
from db.connection import get_connection, put_connection, close_pool
from pipeline.profile_processor import ProfileProcessor

OUTPUT_FILE = "test_output.jsonl"

logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] %(levelname)s: %(message)s",
    datefmt="%H:%M:%S",
    stream=sys.stdout,
)
logger = logging.getLogger("test_extraction")


def fetch_sample(limit: int, university_name: str | None) -> list[dict]:
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            query = """
                SELECT id, name, university, faculty, department, email, website
                FROM professors
                WHERE website IS NOT NULL
            """
            params: list = []

            if university_name:
                query += " AND university = %s"
                params.append(university_name)

            query += " ORDER BY id LIMIT %s"
            params.append(limit)

            cur.execute(query, params)
            cols = [d[0] for d in cur.description]
            rows = [dict(zip(cols, r)) for r in cur.fetchall()]
    finally:
        put_connection(conn)

    return rows


def process_url(processor: ProfileProcessor, url: str, label: str = "Unknown") -> dict | None:
    """Fetch a single URL, convert to markdown, run LLM extraction. Returns result dict or None."""
    logger.info(f"  Fetching {url}")

    _, html, _ = processor.fetch_and_hash(url)
    if not html:
        logger.warning(f"  Could not fetch page — skipping")
        return None

    email = processor.extract_email(html)

    soup = BeautifulSoup(html, "html.parser")
    for tag in soup(["script", "style", "nav", "footer", "header", "aside", "meta", "noscript"]):
        tag.decompose()
    markdown = md_convert(str(soup), strip=["a", "img", "table"]).strip()
    if len(markdown) > 10000:
        markdown = markdown[:10000]
    logger.info(f"  Markdown: {len(markdown)} chars")

    extraction = processor.extract_with_llm(markdown, label)
    if not extraction:
        logger.warning(f"  LLM returned nothing")
        return None

    return {
        "email_from_html": email,
        **extraction,
    }


def print_result(result: dict, row: dict | None = None) -> None:
    name = row["name"] if row else result.get("name", "Unknown")
    uni  = row["university"] if row else "N/A"
    fac  = row["faculty"] if row else "N/A"
    dept = row["department"] if row else "N/A"

    print(f"\n{'─'*60}")
    print(f"  {name}  |  {uni}")
    print(f"  Faculty: {fac}")
    print(f"  Dept   : {dept}")
    print(f"  Email  : {result.get('email_from_html') or 'N/A'}")
    print(f"  Bio    : {result.get('bio', '')}")
    print(f"  Topics : {', '.join(result.get('unique_interests', []))}")
    print(f"  Accept : {result.get('accepting_students', 'NA')}")


def run_single_url(url: str) -> None:
    """Run extraction on a single URL — no database needed."""
    processor = ProfileProcessor()
    result = process_url(processor, url, label="Unknown")
    if not result:
        logger.error("Extraction failed.")
        return

    print_result(result)

    out = Path(OUTPUT_FILE)
    with out.open("w") as f:
        f.write(json.dumps({"profile_url": url, "status": "ok", **result}, ensure_ascii=False) + "\n")
    logger.info(f"Written to {out.resolve()}")


def run_test(limit: int, university_name: str | None) -> None:
    rows = fetch_sample(limit, university_name)
    if not rows:
        logger.error("No professors found in DB matching your filters.")
        return

    logger.info(f"Running extraction on {len(rows)} professors → {OUTPUT_FILE}")
    processor = ProfileProcessor()
    results = []

    for i, row in enumerate(rows, 1):
        prof_id   = row["id"]
        prof_name = row["name"]
        url       = row["website"]

        logger.info(f"[{i}/{len(rows)}] {prof_name} ({row['university']})")

        result = process_url(processor, url, label=prof_name)
        if not result:
            results.append({"id": prof_id, "name": prof_name, "status": "fetch_failed"})
            continue

        record = {
            "id":                    prof_id,
            "name":                  prof_name,
            "university":            row["university"],
            "faculty":               row["faculty"],
            "department":            row["department"],
            "profile_url":           url,
            "status":                "ok",
            **result,
        }
        results.append(record)
        print_result(result, row)

    # Write JSONL output
    out = Path(OUTPUT_FILE)
    with out.open("w") as f:
        for r in results:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")

    ok      = sum(1 for r in results if r.get("status") == "ok")
    failed  = len(results) - ok
    logger.info(f"\nDone: {ok} extracted, {failed} failed → {out.resolve()}")
    close_pool()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Dry-run extraction test (no DB writes)")
    parser.add_argument("--url",         type=str,  default=None, help="Single profile URL to test (no DB needed)")
    parser.add_argument("--limit",       type=int,  default=10,   help="Number of professors to test")
    parser.add_argument("--university",  type=str,  default=None, help="Filter to one university name")
    args = parser.parse_args()

    if args.url:
        run_single_url(args.url)
    else:
        run_test(
            limit=args.limit,
            university_name=args.university,
        )
