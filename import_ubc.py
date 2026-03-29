"""
One-off script to import UBC professors from data-dumps/ubc.json
and attach Google Scholar links from data-dumps/ubc_authors.json.
"""
import json
import logging
import sys

from db.connection import get_connection, put_connection, close_pool

logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] %(levelname)s: %(message)s",
    datefmt="%H:%M:%S",
    stream=sys.stdout,
)
logger = logging.getLogger("import_ubc")


def normalize(name: str) -> str:
    """Lowercase, strip titles/suffixes for fuzzy matching."""
    n = name.lower().strip()
    for prefix in ("dr. ", "dr ", "prof. ", "prof ", "professor "):
        if n.startswith(prefix):
            n = n[len(prefix):]
    return n


def main():
    with open("data-dumps/ubc.json") as f:
        profs = json.load(f)
    with open("data-dumps/ubc_authors.json") as f:
        authors = json.load(f)

    # Build a lookup from normalized name -> best author (highest cited_by)
    author_lookup: dict[str, dict] = {}
    for a in authors:
        key = normalize(a["name"])
        cited = int(a.get("cited_by", 0) or 0)
        if key not in author_lookup or cited > int(author_lookup[key].get("cited_by", 0) or 0):
            author_lookup[key] = a

    conn = get_connection()
    try:
        with conn.cursor() as cur:
            inserted = 0
            matched = 0

            for p in profs:
                name = p["name"]
                university = p["university"]
                scholar_url = None
                cited_by = None

                # Try to match with Google Scholar data
                key = normalize(name)
                if key in author_lookup:
                    a = author_lookup[key]
                    scholar_url = a.get("profile_url")
                    cited_by = int(a.get("cited_by", 0) or 0)
                    matched += 1

                # Skip if already exists
                cur.execute("SELECT id FROM professors WHERE name = %s AND university = %s", (name, university))
                if cur.fetchone():
                    continue

                cur.execute("""
                    INSERT INTO professors (name, university, faculty, department, email, website, research_interests, google_scholar, cited_by)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    name,
                    university,
                    p.get("faculty"),
                    p.get("department"),
                    p.get("email"),
                    p.get("website"),
                    p.get("research_interests"),
                    scholar_url,
                    cited_by,
                ))
                inserted += 1

            conn.commit()
            logger.info(f"Inserted/updated {inserted} UBC professors, {matched} matched with Google Scholar")

    finally:
        put_connection(conn)
        close_pool()


if __name__ == "__main__":
    main()
