"""
Import new LLM-extracted data from new_dumps/ into new columns:
  - research_interests_new (text[])
  - holistic_string_new (text)
  - accepting_students (text)

Matches by professor ID. Skips University of Montreal.
"""
import json
import logging
import os
import sys

from db.connection import get_connection, put_connection, close_pool

logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] %(levelname)s: %(message)s",
    datefmt="%H:%M:%S",
    stream=sys.stdout,
)
logger = logging.getLogger("import_new_dumps")

DUMPS_DIR = "new_dumps"
SKIP_FILES = {"udem-qwen3.5-27b-Opus-Reasoning.jsonl"}


def main():
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            # Add new columns if they don't exist
            cur.execute("""
                ALTER TABLE professors
                ADD COLUMN IF NOT EXISTS research_interests_new text[],
                ADD COLUMN IF NOT EXISTS holistic_string_new text,
                ADD COLUMN IF NOT EXISTS accepting_students text
            """)
            conn.commit()
            logger.info("Columns ready")

            total = 0
            updated = 0

            for fname in sorted(os.listdir(DUMPS_DIR)):
                if not fname.endswith(".jsonl") or fname in SKIP_FILES:
                    if fname in SKIP_FILES:
                        logger.info(f"Skipping {fname}")
                    continue

                filepath = os.path.join(DUMPS_DIR, fname)
                file_count = 0

                with open(filepath) as f:
                    for line in f:
                        line = line.strip()
                        if not line:
                            continue
                        row = json.loads(line)
                        prof_id = row.get("id")
                        if not prof_id:
                            continue

                        total += 1
                        interests = row.get("unique_interests", [])
                        holistic = row.get("holistic_profile_string")
                        accepting = row.get("accepting_students")

                        cur.execute("""
                            UPDATE professors
                            SET research_interests_new = %s,
                                holistic_string_new = %s,
                                accepting_students = %s
                            WHERE id = %s
                        """, (interests, holistic, accepting, prof_id))

                        if cur.rowcount > 0:
                            updated += 1
                            file_count += 1

                logger.info(f"  {fname}: {file_count} updated")

            conn.commit()
            logger.info(f"Done — {updated}/{total} professors updated across all files")

    finally:
        put_connection(conn)
        close_pool()


if __name__ == "__main__":
    main()
