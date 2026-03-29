import json
import logging
import time
from typing import Dict, Type, Optional
import concurrent.futures
from pathlib import Path
from bs4 import BeautifulSoup
from markdownify import markdownify as md

from scraper.core.interfaces import BaseDirectoryScraper
from scraper.universities.uottawa import UOttawaDirectoryScraper
from scraper.universities.carleton import CarletonDirectoryScraper
from scraper.universities.uwaterloo import UWaterlooDirectoryScraper
from scraper.universities.mcgill import McGillDirectoryScraper
from scraper.universities.udem import UdeMDirectoryScraper
from scraper.universities.western import WesternDirectoryScraper
from scraper.universities.concordia import ConcordiaDirectoryScraper
from scraper.universities.queens import QueensDirectoryScraper
from scraper.universities.mcmaster import McMasterDirectoryScraper
from scraper.universities.uoft import UofTDirectoryScraper
from scraper.universities.ubc import UBCDirectoryScraper
from scraper.universities.ualberta import UAlbertaDirectoryScraper
from scraper.universities.ucalgary import UCalgaryDirectoryScraper
from scraper.universities.generic import GenericDirectoryScraper  # works on any directory page

from scraper.pipeline.profile_processor import ProfileProcessor
from scraper.pipeline.embedder import embed_text
from scraper.db.repositories import (
    get_or_create_university,
    get_or_create_faculty,
    get_or_create_department,
    upsert_professor,
    update_professor_profile,
    get_professors_missing_markdown,
    get_professors_ready_for_ai,
    get_counts,
)
from scraper.db.connection import close_pool

logger = logging.getLogger(__name__)

SCRAPER_REGISTRY: Dict[str, Type[BaseDirectoryScraper]] = {
    "University of Ottawa": UOttawaDirectoryScraper,
    "Carleton University": CarletonDirectoryScraper,
    "University of Waterloo": UWaterlooDirectoryScraper,
    "McGill University": McGillDirectoryScraper,
    "University of Montreal": UdeMDirectoryScraper,
    "Western University": WesternDirectoryScraper,
    "Concordia University": ConcordiaDirectoryScraper,
    "Queens University": QueensDirectoryScraper,
    "McMaster University": McMasterDirectoryScraper,
    "University of Toronto": UofTDirectoryScraper,
    "University of British Columbia": UBCDirectoryScraper,
    "University of Alberta": UAlbertaDirectoryScraper,
    "University of Calgary": UCalgaryDirectoryScraper,
    "Simon Fraser University": GenericDirectoryScraper,
    "Memorial University of Newfoundland": GenericDirectoryScraper,
    "Dalhousie University": GenericDirectoryScraper,
    "University of Manitoba": GenericDirectoryScraper,
    "University of Saskatchewan": GenericDirectoryScraper,
    "York University": GenericDirectoryScraper,
    "University of Victoria": GenericDirectoryScraper,
    "Toronto Metropolitan University": GenericDirectoryScraper,
}

class ScraperOrchestrator:
    def __init__(self, universities_json_path: str):
        self.json_path = Path(universities_json_path)
        self.processor = ProfileProcessor()

    # ============================================================
    # PHASE 1: Directory Traversal → DB (Discover URLs)
    # ============================================================
    def run_phase1(self, university_filter: Optional[str] = None):
        """Scrape directories and upsert basic info (names, urls, IDs)."""
        start = time.time()
        with open(self.json_path, "r") as f:
            data = json.load(f)

        total_inserted = 0
        for uni_name, faculties in data.items():
            if university_filter and uni_name != university_filter:
                continue

            scraper_class = SCRAPER_REGISTRY.get(uni_name)
            if not scraper_class:
                logger.warning(f"No scraper for '{uni_name}', skipping.")
                continue

            uni_id = get_or_create_university(uni_name)
            scraper = scraper_class(university_id=uni_id)
            logger.info(f"=== Phase 1: {uni_name} ===")

            for fac_name, fac_data in faculties.items():
                fac_id = get_or_create_faculty(uni_id, fac_name)
                if isinstance(fac_data, str):
                    dept_id = get_or_create_department(uni_id, fac_id, fac_name)
                    total_inserted += self._scrape_and_insert(scraper, fac_data, uni_id, fac_id, dept_id, fac_name)
                elif isinstance(fac_data, dict):
                    for dept_name, dept_url in fac_data.items():
                        dept_id = get_or_create_department(uni_id, fac_id, dept_name)
                        total_inserted += self._scrape_and_insert(scraper, dept_url, uni_id, fac_id, dept_id, dept_name)

        logger.info(f"--- Phase 1 Complete: {total_inserted} inserted in {time.time() - start:.1f}s ---")

    def _scrape_and_insert(self, scraper, url, uni_id, fac_id, dept_id, dept_name) -> int:
        try:
            professors = scraper.scrape_directory(url, faculty_id=fac_id, department_id=dept_id)
        except Exception as e:
            logger.error(f"Scraper error for {dept_name}: {e}")
            return 0

        inserted = 0
        for prof in professors:
            try:
                upsert_professor(
                    first_name=prof["first_name"],
                    last_name=prof["last_name"],
                    profile_url=prof["profile_url"],
                    university_id=uni_id,
                    faculty_id=fac_id,
                    department_id=dept_id,
                )
                inserted += 1
            except Exception as e:
                logger.error(f"DB error for {prof.get('profile_url', '?')}: {e}")
        return inserted

    # ============================================================
    # PHASE 2: Fetch & Markdown (Network Bound)
    # ============================================================
    def _fetch_single_markdown(self, row: dict, force: bool = False) -> dict:
        """Fetches HTML, converts to clean Markdown, saves to DB."""
        prof_id = row["id"]
        prof_name = f"{row['first_name']} {row['last_name']}".strip()
        profile_url = row["profile_url"]
        
        status = {"processed": 0, "skipped": 0, "failed": 0}

        try:
            raw_text, html, new_hash = self.processor.fetch_and_hash(profile_url)
            
            if not html or not new_hash:
                # Page doesn't exist or timed out. Mark it so it leaves the queue.
                update_professor_profile(professor_id=prof_id, profile_markdown="[UNAVAILABLE]")
                logger.warning(f"  → [UNAVAILABLE] {prof_name} ({profile_url})")
                status["failed"] = 1
                return status

            # REMOVED the hash skip check! 
            # If they are in this queue, they are missing Markdown. We MUST process them.

            # Extract email from raw HTML
            email = self.processor.extract_email(html)

            # Clean and convert to Markdown
            soup = BeautifulSoup(html, 'html.parser')
            for tag in soup(['script', 'style', 'nav', 'footer', 'header', 'aside', 'meta', 'noscript']):
                tag.decompose()
            clean_md = md(str(soup), strip=['a', 'img', 'table']).strip()
            
            if len(clean_md) > 10000:
                clean_md = clean_md[:10000]

            # Save everything to the database
            update_professor_profile(
                professor_id=prof_id, 
                content_hash=new_hash,
                profile_markdown=clean_md,
                email=email
            )
            logger.info(f"  → [SUCCESS] Saved Markdown for {prof_name}")
            status["processed"] = 1
            
        except Exception as e:
            logger.error(f"Phase 2 error for {prof_name}: {e}")
            # CRITICAL: Prevent the infinite loop by marking the row as a hard error
            try:
                update_professor_profile(professor_id=prof_id, profile_markdown="[ERROR]")
            except Exception as db_err:
                logger.error(f"  → DB Error while saving failure state: {db_err}")
            status["failed"] = 1

        return status

    def run_phase2(self, batch_size: int = 50, university_filter: Optional[str] = None, force: bool = False, max_workers: int = 10):
        """Downloads profiles and converts them to Markdown concurrently."""
        start = time.time()
        processed = skipped = failed = 0
        uni_id = get_or_create_university(university_filter) if university_filter else None

        while True:
            # You will need a new DB query here: get_professors_missing_markdown()
            rows = get_professors_missing_markdown(limit=batch_size, university_id=uni_id) # Update this repository function
            if not rows:
                break
                
            logger.info(f"Phase 2: Downloading batch of {len(rows)} profiles...")
            
            with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
                futures = [executor.submit(self._fetch_single_markdown, row, force) for row in rows]
                for future in concurrent.futures.as_completed(futures):
                    res = future.result()
                    processed += res["processed"]
                    skipped += res["skipped"]
                    failed += res["failed"]

        logger.info(f"--- Phase 2 Complete: {processed} fetched | {failed} failed in {time.time() - start:.1f}s ---")

    # ============================================================
    # PHASE 3: AI Sprint & Embeddings (Compute Bound)
    # ============================================================
    def _ai_extract_single(self, row: dict) -> dict:
        """Passes Markdown to Ollama and generates the vector embedding."""
        prof_id = row["id"]
        prof_name = f"{row['first_name']} {row['last_name']}".strip()
        markdown = row.get("profile_markdown")
        
        status = {"processed": 0, "failed": 0}

        if not markdown or markdown in ["[UNAVAILABLE]", "[ERROR]"]:
            status["failed"] = 1
            return status

        try:
            # Let the processor handle the Qwen 3.5 call
            result = self.processor.extract_with_llm(markdown, prof_name) 
            
            if result:
                holistic_string = result.get("holistic_profile_string")
                embedding = embed_text(holistic_string) if holistic_string else None

                update_professor_profile(
                    professor_id=prof_id,
                    unique_interests=result.get("unique_interests"),
                    holistic_profile_string=holistic_string,
                    embedding=embedding,
                    bio=result.get("bio"),
                    accepting_students=result.get("accepting_students"),
                )
                logger.info(f"  → Embedded & Processed: {prof_name}")
                status["processed"] = 1
            else:
                status["failed"] = 1
                
        except Exception as e:
            logger.error(f"Phase 3 error for {prof_name}: {e}")
            status["failed"] = 1

        return status

    def run_phase3(self, batch_size: int = 20, university_filter: Optional[str] = None, max_workers: int = 2):
        """Runs the LLM and embedding models on the downloaded Markdown."""
        start = time.time()
        processed = failed = 0
        uni_id = get_or_create_university(university_filter) if university_filter else None

        while True:
            # You will need a DB query: get_professors_ready_for_ai() -> where markdown is not null but interests are null
            rows = get_professors_ready_for_ai(university_id=uni_id) # Update this repository function to fetch AI-ready rows
            if not rows:
                break
                
            logger.info(f"Phase 3: AI Extracting batch of {len(rows)}...")
            
            # Keep max_workers low (1-3) here so you don't overload your GPU VRAM with Ollama requests
            with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
                futures = [executor.submit(self._ai_extract_single, row) for row in rows]
                for future in concurrent.futures.as_completed(futures):
                    res = future.result()
                    processed += res["processed"]
                    failed += res["failed"]

        logger.info(f"--- Phase 3 Complete: {processed} AI extracted | {failed} failed in {time.time() - start:.1f}s ---")


if __name__ == "__main__":
    import argparse

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")

    parser = argparse.ArgumentParser(description="FindMyProfessor 3-Phase Orchestrator")
    parser.add_argument("--phase", choices=["1", "2", "3", "all"], default="all", help="Which phase to run")
    parser.add_argument("--university", type=str, default=None, help="Filter to a single university")
    parser.add_argument("--force", action="store_true", help="Ignore hash caches")
    parser.add_argument("--max-workers", type=int, default=10, help="Workers for Phase 2 (Network)")
    parser.add_argument("--ai-workers", type=int, default=2, help="Workers for Phase 3 (GPU)")

    args = parser.parse_args()
    orchestrator = ScraperOrchestrator("scraper/universities.json")

    if args.phase in ("1", "all"):
        orchestrator.run_phase1(args.university)
    if args.phase in ("2", "all"):
        orchestrator.run_phase2(university_filter=args.university, force=args.force, max_workers=args.max_workers)
    if args.phase in ("3", "all"):
        orchestrator.run_phase3(university_filter=args.university, max_workers=args.ai_workers)

    close_pool()