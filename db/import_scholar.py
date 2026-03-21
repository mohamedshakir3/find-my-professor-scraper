"""
Import Google Scholar citations + profile URLs from scraper/data-dumps/*_authors.json.
Matches scholar entries to DB professors by normalized full name within the same university.
"""
import json
import logging
import re
from pathlib import Path

from scraper.db.connection import get_connection, put_connection
from scraper.db.repositories import batch_update_scholar

logger = logging.getLogger(__name__)

_PROJECT_ROOT = Path(__file__).parents[2]
_DUMPS_DIR = _PROJECT_ROOT / "scraper" / "data-dumps"

# Maps author JSON filename stem → DB university name
_FILE_TO_UNI: dict[str, str] = {
    "carletonu_authors":  "Carleton University",
    "concordia_authors":  "Concordia University",
    "mcgill_authors":     "McGill University",
    "mcmaster_authors":   "McMaster University",
    "queens_authors":     "Queens University",
    "ualberta_authors":   "University of Alberta",
    "ubc_authors":        "University of British Columbia",
    "ucalgary_authors":   "University of Calgary",
    "udem_authors":       "University of Montreal",
    "uoft_authors":       "University of Toronto",
    "uottawa_authors":    "University of Ottawa",
    "uwaterloo_authors":  "University of Waterloo",
    "western_authors":    "Western University",
}

# Suffixes and titles stripped before name comparison
_STRIP_SUFFIXES = re.compile(
    r",.*$"                              # everything after first comma
    r"|\b(dr|prof|professor|mr|mrs|ms|jr|sr|ii|iii|iv)\b\.?",
    re.IGNORECASE,
)


def _normalize_name(name: str) -> str:
    name = _STRIP_SUFFIXES.sub("", name)
    return " ".join(name.split()).lower()


def _load_professors_for_uni(conn, university_name: str) -> list[dict]:
    """Return id, first_name, last_name for all professors at a university."""
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT p.id, p.first_name, p.last_name
            FROM professors p
            JOIN universities u ON u.id = p.university_id
            WHERE u.name ILIKE %s
            """,
            (university_name,),
        )
        return [
            {"id": row[0], "first_name": row[1], "last_name": row[2]}
            for row in cur.fetchall()
        ]


def import_scholar(dumps_dir: Path = _DUMPS_DIR) -> None:
    total_matched = 0
    total_authors = 0

    conn = get_connection()
    try:
        for stem, uni_name in _FILE_TO_UNI.items():
            path = dumps_dir / f"{stem}.json"
            if not path.exists():
                logger.warning(f"Missing file: {path.name} — skipping.")
                continue

            authors = json.loads(path.read_text(encoding="utf-8"))
            logger.info(f"{path.name}: {len(authors)} scholar entries for {uni_name}")
            total_authors += len(authors)

            professors = _load_professors_for_uni(conn, uni_name)
            if not professors:
                logger.warning(f"  No professors found in DB for '{uni_name}' — skipping.")
                continue

            # Build lookup: normalized_full_name -> professor id
            # Two keys per professor: "first last" and "last first", to handle
            # universities where the scraper stored names in reversed order.
            prof_lookup: dict[str, int] = {}
            for p in professors:
                fwd = _normalize_name(f"{p['first_name']} {p['last_name']}")
                rev = _normalize_name(f"{p['last_name']} {p['first_name']}")
                prof_lookup[fwd] = p["id"]
                if rev not in prof_lookup:
                    prof_lookup[rev] = p["id"]

            rows: list[dict] = []
            for author in authors:
                key = _normalize_name(author.get("name", ""))
                prof_id = prof_lookup.get(key)
                if prof_id is None:
                    continue
                try:
                    cited_by = int(author.get("cited_by") or 0) or None
                except (ValueError, TypeError):
                    cited_by = None
                rows.append({
                    "id": prof_id,
                    "google_scholar_url": author.get("profile_url"),
                    "cited_by": cited_by,
                })

            logger.info(f"  Matched {len(rows)}/{len(authors)} authors to DB professors")
            total_matched += len(rows)

            if rows:
                batch_update_scholar(rows)

    finally:
        put_connection(conn)

    logger.info(
        f"Scholar import complete — {total_matched}/{total_authors} total matches across all universities."
    )


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="[%(asctime)s] %(levelname)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    import_scholar()
