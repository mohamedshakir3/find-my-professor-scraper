import logging
from typing import Optional, List, Dict, Any
from .connection import get_connection, put_connection

logger = logging.getLogger(__name__)


# ============================================================
# Taxonomy CRUD
# ============================================================

def get_or_create_university(name: str, short_name: Optional[str] = None) -> int:
    """Insert university if not exists, return its id."""
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO universities (name, short_name)
                VALUES (%s, %s)
                ON CONFLICT (name) DO UPDATE SET name = EXCLUDED.name
                RETURNING id
                """,
                (name, short_name),
            )
            row = cur.fetchone()
            conn.commit()
            return row[0]
    except Exception:
        conn.rollback()
        raise
    finally:
        put_connection(conn)


def get_or_create_faculty(university_id: int, name: str) -> int:
    """Insert faculty if not exists, return its id."""
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO faculties (university_id, name)
                VALUES (%s, %s)
                ON CONFLICT (university_id, name) DO UPDATE SET name = EXCLUDED.name
                RETURNING id
                """,
                (university_id, name),
            )
            row = cur.fetchone()
            conn.commit()
            return row[0]
    except Exception:
        conn.rollback()
        raise
    finally:
        put_connection(conn)


def get_or_create_department(university_id: int, faculty_id: int, name: str) -> int:
    """Insert department if not exists, return its id."""
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO departments (university_id, faculty_id, name)
                VALUES (%s, %s, %s)
                ON CONFLICT (faculty_id, name) DO UPDATE SET name = EXCLUDED.name
                RETURNING id
                """,
                (university_id, faculty_id, name),
            )
            row = cur.fetchone()
            conn.commit()
            return row[0]
    except Exception:
        conn.rollback()
        raise
    finally:
        put_connection(conn)


# ============================================================
# Professor CRUD
# ============================================================

def upsert_professor(
    first_name: str,
    last_name: str,
    profile_url: str,
    university_id: int,
    faculty_id: int,
    department_id: int,
) -> int:
    """
    Insert professor or update if profile_url already exists (Phase 1).
    Returns the professor id.
    """
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO professors
                    (first_name, last_name, profile_url, university_id, faculty_id, department_id)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (profile_url) DO UPDATE SET
                    first_name = EXCLUDED.first_name,
                    last_name = EXCLUDED.last_name,
                    university_id = EXCLUDED.university_id,
                    faculty_id = EXCLUDED.faculty_id,
                    department_id = EXCLUDED.department_id
                RETURNING id
                """,
                (first_name, last_name, profile_url, university_id, faculty_id, department_id),
            )
            row = cur.fetchone()
            conn.commit()
            return row[0]
    except Exception:
        conn.rollback()
        raise
    finally:
        put_connection(conn)


def update_professor_profile(
    professor_id: int,
    email: Optional[str] = None,
    unique_interests: Optional[List[str]] = None,
    holistic_profile_string: Optional[str] = None,
    embedding: Optional[List[float]] = None,
    content_hash: Optional[str] = None,
    profile_markdown: Optional[str] = None,
    bio: Optional[str] = None,
    accepting_students: Optional[str] = None,
) -> None:
    """
    Update a professor row with Phase 2 (Markdown) or Phase 3 (NLP) data.
    Uses COALESCE so we only update the fields that are passed in.
    """
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE professors SET
                    email = COALESCE(%s, email),
                    unique_interests = COALESCE(%s, unique_interests),
                    holistic_profile_string = COALESCE(%s, holistic_profile_string),
                    embedding = COALESCE(%s, embedding),
                    content_hash = COALESCE(%s, content_hash),
                    profile_markdown = COALESCE(%s, profile_markdown),
                    bio = COALESCE(%s, bio),
                    accepting_students = COALESCE(%s, accepting_students),
                    search_vector = CASE
                        WHEN %s IS NOT NULL
                        THEN to_tsvector('english', %s)
                        ELSE search_vector
                    END
                WHERE id = %s
                """,
                (
                    email,
                    unique_interests,
                    holistic_profile_string,
                    str(embedding) if embedding else None,
                    content_hash,
                    profile_markdown,
                    bio,
                    accepting_students,
                    holistic_profile_string,
                    holistic_profile_string,
                    professor_id,
                ),
            )
            conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        put_connection(conn)


def batch_update_professor_ai_data(rows: list[dict]) -> None:
    """
    Bulk force-overwrite AI extraction results (Phase 3 import).
    Each dict: {id, unique_interests, holistic_profile_string, embedding, bio, accepting_students}
    All fields are optional except id.
    """
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            from psycopg2.extras import execute_batch
            execute_batch(
                cur,
                """
                UPDATE professors SET
                    unique_interests        = %s,
                    holistic_profile_string = %s,
                    embedding               = %s,
                    bio                     = %s,
                    accepting_students      = %s,
                    search_vector           = CASE
                        WHEN %s IS NOT NULL
                        THEN to_tsvector('english', %s)
                        ELSE search_vector
                    END
                WHERE id = %s
                """,
                [
                    (
                        r.get("unique_interests"),
                        r.get("holistic_profile_string"),
                        str(r["embedding"]) if r.get("embedding") else None,
                        r.get("bio"),
                        r.get("accepting_students"),
                        r.get("holistic_profile_string"),
                        r.get("holistic_profile_string"),
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


def get_professors_missing_markdown(limit: int = 100, university_id: Optional[int] = None) -> List[Dict[str, Any]]:
    """Fetch professors that haven't had their HTML downloaded yet (Phase 2 worker)."""
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            query = """
                SELECT p.id, p.first_name, p.last_name, p.profile_url,
                       p.university_id, p.faculty_id, p.department_id,
                       d.name as department_name, p.content_hash
                FROM professors p
                JOIN departments d ON d.id = p.department_id
                WHERE p.profile_markdown IS NULL
            """
            params: list = []
            if university_id is not None:
                query += " AND p.university_id = %s"
                params.append(university_id)
            query += " ORDER BY p.id LIMIT %s"
            params.append(limit)

            cur.execute(query, params)
            columns = [desc[0] for desc in cur.description]
            return [dict(zip(columns, row)) for row in cur.fetchall()]
    finally:
        put_connection(conn)


def get_professors_ready_for_ai(limit: int = 100, university_id: Optional[int] = None) -> List[Dict[str, Any]]:
    """
    Fetch professors that have Markdown, but haven't been processed by the LLM yet (Phase 3 worker).
    Excludes profiles where the Markdown fetch failed (marked as [UNAVAILABLE] or [ERROR]).
    """
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            query = """
                SELECT p.id, p.first_name, p.last_name, p.profile_url,
                       p.university_id, p.faculty_id, p.department_id,
                       d.name as department_name, p.profile_markdown
                FROM professors p
                JOIN departments d ON d.id = p.department_id
                WHERE p.profile_markdown IS NOT NULL
                  AND p.profile_markdown NOT IN ('[UNAVAILABLE]', '[ERROR]')
                  AND p.unique_interests IS NULL
            """
            params: list = []
            if university_id is not None:
                query += " AND p.university_id = %s"
                params.append(university_id)
            query += " ORDER BY p.id LIMIT %s"
            params.append(limit)

            cur.execute(query, params)
            columns = [desc[0] for desc in cur.description]
            return [dict(zip(columns, row)) for row in cur.fetchall()]
    finally:
        put_connection(conn)


def batch_update_rankings(rows: list[dict]) -> None:
    """
    Bulk update qs_ranking for a list of professors by department.
    Each dict: {department_id: int, qs_ranking: str}
    Updates all professors belonging to the given department_id.
    """
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            from psycopg2.extras import execute_batch
            execute_batch(
                cur,
                "UPDATE professors SET qs_ranking = %s WHERE department_id = %s",
                [(r["qs_ranking"], r["department_id"]) for r in rows],
                page_size=200,
            )
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        put_connection(conn)


def batch_update_scholar(rows: list[dict]) -> None:
    """
    Bulk update google_scholar_url and cited_by for matched professors.
    Each dict: {id: int, google_scholar_url: str, cited_by: int}
    """
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            from psycopg2.extras import execute_batch
            execute_batch(
                cur,
                "UPDATE professors SET google_scholar_url = %s, cited_by = %s WHERE id = %s",
                [(r["google_scholar_url"], r["cited_by"], r["id"]) for r in rows],
                page_size=200,
            )
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        put_connection(conn)


def get_counts() -> Dict[str, int]:
    """Quick count of all tables for verification."""
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            counts = {}
            for table in ["universities", "faculties", "departments", "professors"]:
                cur.execute(f"SELECT count(*) FROM {table}")
                counts[table] = cur.fetchone()[0]
            
            # Add specific pipeline counts to help you debug
            cur.execute("SELECT count(*) FROM professors WHERE profile_markdown IS NOT NULL")
            counts["markdown_downloaded"] = cur.fetchone()[0]
            
            cur.execute("SELECT count(*) FROM professors WHERE unique_interests IS NOT NULL")
            counts["ai_processed"] = cur.fetchone()[0]
            
            return counts
    finally:
        put_connection(conn)