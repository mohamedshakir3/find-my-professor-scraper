"""
Scrape new universities (using GenericDirectoryScraper) and export to JSONL
for LLM processing.

Combines Phase 1 (directory scraping) and Phase 2 (profile fetch → markdown)
into a single pipeline that writes directly to gpu_inputs/, bypassing the DB.

The JSONL records include: name, university, faculty, department, profile_url,
email, and profile_markdown. These feed into the GPU LLM extraction step.

Usage:
    python scrape_new_unis.py --university "York University"
    python scrape_new_unis.py --all
    python scrape_new_unis.py --all --delay 1.0 --workers 5
"""
import argparse
import json
import logging
import os
import re
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

from bs4 import BeautifulSoup
from markdownify import markdownify as md_convert

from pipeline.profile_processor import ProfileProcessor
from universities.generic import GenericDirectoryScraper

logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] %(levelname)s: %(message)s",
    datefmt="%H:%M:%S",
    stream=sys.stdout,
)
logger = logging.getLogger("scrape_new_unis")

OUTPUT_DIR = "gpu_inputs"

# Universities handled by GenericDirectoryScraper — edit this list as needed
GENERIC_UNIS = [
    "Simon Fraser University",
    "Memorial University of Newfoundland",
    "Dalhousie University",
    "University of Manitoba",
    "University of Saskatchewan",
    "York University",
    "University of Victoria",
    "Toronto Metropolitan University",
]


def slugify(name: str) -> str:
    return re.sub(r"[^a-z0-9]+", "_", name.lower()).strip("_")


def output_path(university: str) -> str:
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    return os.path.join(OUTPUT_DIR, f"{slugify(university)}.jsonl")


def load_existing_urls(filepath: str) -> set:
    """Load profile_urls already exported so we can resume."""
    try:
        urls = set()
        with open(filepath) as f:
            for line in f:
                if line.strip():
                    urls.add(json.loads(line)["profile_url"])
        return urls
    except FileNotFoundError:
        return set()


def scrape_directory(university: str, unis_data: dict) -> list[dict]:
    """Run GenericDirectoryScraper over all departments for a university."""
    fac_map = unis_data.get(university, {})
    scraper = GenericDirectoryScraper(university_id=0)
    entries = []

    for fac_name, fac_data in fac_map.items():
        if isinstance(fac_data, str):
            depts = {fac_name: fac_data}
        else:
            depts = fac_data

        for dept_name, url in depts.items():
            logger.info(f"  Scraping {dept_name} ...")
            try:
                results = scraper.scrape_directory(url, faculty_id=0, department_id=0)
            except Exception as e:
                logger.error(f"  Failed to scrape {dept_name}: {e}")
                results = []

            for r in results:
                entries.append({
                    "name": f"{r['first_name']} {r['last_name']}",
                    "first_name": r["first_name"],
                    "last_name": r["last_name"],
                    "university": university,
                    "faculty": fac_name,
                    "department": dept_name,
                    "profile_url": r["profile_url"],
                })

            logger.info(f"    → {len(results)} professors")

    logger.info(f"  Total for {university}: {len(entries)}")
    return entries


# Shared mutable delay state (same pattern as export_for_gpu.py)
_state = {"current_delay": 0.5}


def fetch_profile(entry: dict, processor: ProfileProcessor) -> dict | None:
    """Fetch a profile page and return the full record with markdown."""
    url = entry["profile_url"]
    name = entry["name"]

    for attempt in range(3):
        try:
            _, html, _ = processor.fetch_and_hash(url)
            if not html:
                logger.warning(f"  {name} — fetch returned empty")
                return None

            email = processor.extract_email(html)

            soup = BeautifulSoup(html, "html.parser")
            for tag in soup(["script", "style", "nav", "footer", "header", "aside", "meta", "noscript"]):
                tag.decompose()
            markdown = md_convert(str(soup), strip=["a", "img", "table"]).strip()
            if len(markdown) > 10000:
                markdown = markdown[:10000]

            return {
                "name": entry["name"],
                "university": entry["university"],
                "faculty": entry["faculty"],
                "department": entry["department"],
                "profile_url": url,
                "email": email,
                "profile_markdown": markdown,
            }

        except (ProfileProcessor.RateLimitError, ProfileProcessor.ThrottledError) as e:
            _state["current_delay"] = min(30.0, _state["current_delay"] * 2)
            wait = _state["current_delay"] * (attempt + 1)
            kind = "429" if isinstance(e, ProfileProcessor.RateLimitError) else "connection reset"
            logger.warning(f"  {name} — {kind}, waiting {wait:.0f}s (attempt {attempt+1}/3)")
            time.sleep(wait)
        except Exception as e:
            logger.error(f"  {name} — error: {e}")
            return None

    logger.error(f"  {name} — failed after 3 retries")
    return None


def run_export(university: str, unis_data: dict, delay: float, workers: int):
    out_file = output_path(university)
    done_urls = load_existing_urls(out_file)
    if done_urls:
        logger.info(f"  Resuming — {len(done_urls)} already exported")

    logger.info(f"  Discovering professors via directory scraper...")
    entries = scrape_directory(university, unis_data)

    # Deduplicate profile URLs across departments
    seen: set = set()
    unique_entries = []
    for e in entries:
        if e["profile_url"] not in seen:
            seen.add(e["profile_url"])
            unique_entries.append(e)

    # Skip already exported
    to_fetch = [e for e in unique_entries if e["profile_url"] not in done_urls]
    logger.info(f"  {len(unique_entries)} unique profiles, {len(to_fetch)} to fetch → {out_file}")

    if not to_fetch:
        logger.info("  All already exported, nothing to do.")
        return

    processor = ProfileProcessor()
    _state["current_delay"] = delay
    ok_count = 0
    ok_streak = 0

    with open(out_file, "a") as f:
        if workers > 1:
            with ThreadPoolExecutor(max_workers=workers) as pool:
                futures = {pool.submit(fetch_profile, e, processor): e for e in to_fetch}
                for i, fut in enumerate(as_completed(futures), 1):
                    result = fut.result()
                    if result:
                        f.write(json.dumps(result, ensure_ascii=False) + "\n")
                        f.flush()
                        ok_count += 1
                    if i % 25 == 0 or i == len(to_fetch):
                        logger.info(f"  Progress: {i}/{len(to_fetch)}, {ok_count} ok")
                    time.sleep(_state["current_delay"])
        else:
            for i, entry in enumerate(to_fetch, 1):
                result = fetch_profile(entry, processor)
                if result:
                    f.write(json.dumps(result, ensure_ascii=False) + "\n")
                    f.flush()
                    ok_count += 1
                    ok_streak += 1
                    if ok_streak >= 20 and _state["current_delay"] > delay:
                        _state["current_delay"] = max(delay, _state["current_delay"] * 0.8)
                        ok_streak = 0
                else:
                    ok_streak = 0

                if i % 25 == 0 or i == len(to_fetch):
                    logger.info(f"  Progress: {i}/{len(to_fetch)}, {ok_count} ok (delay={_state['current_delay']:.1f}s)")

                time.sleep(_state["current_delay"])

    logger.info(f"  Done: {ok_count}/{len(to_fetch)} profiles exported")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Scrape new universities and export for LLM")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--university", type=str, help="Single university name (must match universities.json)")
    group.add_argument("--all", action="store_true", help="Export all generic-scraped universities")
    parser.add_argument("--delay", type=float, default=0.5, help="Seconds between requests (default 0.5)")
    parser.add_argument("--workers", type=int, default=1, help="Concurrent fetch workers (default 1)")
    args = parser.parse_args()

    with open("universities.json") as f:
        unis_data = json.load(f)

    if args.all:
        universities = GENERIC_UNIS
    else:
        universities = [args.university]

    for uni in universities:
        if uni not in unis_data:
            logger.warning(f"'{uni}' not found in universities.json, skipping")
            continue
        logger.info(f"\n{'='*60}\n  {uni}\n{'='*60}")
        run_export(uni, unis_data, delay=args.delay, workers=args.workers)

    logger.info("\nAll done.")
