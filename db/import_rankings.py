"""
Import QS rankings from scraper/data-dumps/rankings.json into the professors table.
Rankings are applied at the department level (every professor in a department gets
the same QS rank). When a rankings entry has no department, the rank is applied to
all departments within that faculty.
"""
import json
import logging
import re
from pathlib import Path

from scraper.db.connection import get_connection, put_connection
from scraper.db.repositories import batch_update_rankings

logger = logging.getLogger(__name__)

_PROJECT_ROOT = Path(__file__).parents[2]
_RANKINGS_FILE = _PROJECT_ROOT / "scraper" / "data-dumps" / "rankings.json"

# Map ranking university names → DB university names (for mismatches)
_UNI_NAME_ALIASES = {
    "université de montréal": "university of montreal",
}


def _normalize(s: str) -> str:
    """Lowercase, strip punctuation noise for fuzzy name matching."""
    s = s.lower().strip()
    s = re.sub(r"[''`]", "", s)
    return s


def _load_db_departments(conn) -> list[dict]:
    """Return all departments with their university and faculty names."""
    with conn.cursor() as cur:
        cur.execute("""
            SELECT d.id, d.name, f.name AS faculty_name, u.name AS university_name
            FROM departments d
            JOIN faculties f ON f.id = d.faculty_id
            JOIN universities u ON u.id = d.university_id
        """)
        cols = [desc[0] for desc in cur.description]
        return [dict(zip(cols, row)) for row in cur.fetchall()]


def import_rankings(path: Path = _RANKINGS_FILE) -> None:
    if not path.exists():
        logger.error(f"Rankings file not found: {path}")
        return

    rankings = json.loads(path.read_text(encoding="utf-8"))
    logger.info(f"Loaded {len(rankings)} ranking entries.")

    conn = get_connection()
    try:
        departments = _load_db_departments(conn)
    finally:
        put_connection(conn)

    # Build lookup: (normalized_uni, normalized_dept) -> dept_id
    # and          (normalized_uni, normalized_faculty) -> [dept_id, ...]
    dept_by_name: dict[tuple, int] = {}
    depts_by_faculty: dict[tuple, list[int]] = {}
    for d in departments:
        uni = _normalize(d["university_name"])
        dept = _normalize(d["name"])
        fac = _normalize(d["faculty_name"])
        dept_by_name[(uni, dept)] = d["id"]
        depts_by_faculty.setdefault((uni, fac), []).append(d["id"])

    rows: list[dict] = []
    unmatched: list[dict] = []

    for entry in rankings:
        uni_raw = _normalize(entry["university"])
        uni = _UNI_NAME_ALIASES.get(uni_raw, uni_raw)
        dept_raw = _normalize(entry.get("department", ""))
        fac_raw = _normalize(entry.get("faculty", ""))
        ranking = entry["ranking"]

        if dept_raw:
            dept_id = dept_by_name.get((uni, dept_raw))
            if dept_id:
                rows.append({"department_id": dept_id, "qs_ranking": ranking})
            else:
                unmatched.append(entry)
        elif fac_raw:
            dept_ids = depts_by_faculty.get((uni, fac_raw), [])
            if dept_ids:
                for dept_id in dept_ids:
                    rows.append({"department_id": dept_id, "qs_ranking": ranking})
            else:
                unmatched.append(entry)
        else:
            unmatched.append(entry)

    logger.info(f"Matched {len(rows)} dept→ranking pairs | Unmatched: {len(unmatched)}")
    if unmatched:
        for u in unmatched:
            logger.warning(f"  No match: {u}")

    if rows:
        batch_update_rankings(rows)
        logger.info(f"Rankings import complete — updated {len(rows)} department rows.")


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="[%(asctime)s] %(levelname)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    import_rankings()
