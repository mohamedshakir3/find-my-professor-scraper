"""
Seed the taxonomy tables (universities, faculties, departments) from universities.json.

Usage:
    python -m scraper.db.seed_taxonomy
"""

import json
import logging
from pathlib import Path
from .repositories import get_or_create_university, get_or_create_faculty, get_or_create_department, get_counts
from .connection import close_pool

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
logger = logging.getLogger(__name__)

# Short name lookup for universities
SHORT_NAMES = {
    "University of Ottawa": "uOttawa",
    "Carleton University": "Carleton",
    "University of Waterloo": "Waterloo",
    "McMaster University": "McMaster",
    "University of Toronto": "UofT",
    "McGill University": "McGill",
    "University of British Columbia": "UBC",
    "University of Montreal": "UdeM",
    "University of Alberta": "UAlberta",
    "University of Calgary": "UCalgary",
    "Queens University": "Queens",
    "Western University": "Western",
    "Concordia University": "Concordia",
}


def seed():
    json_path = Path(__file__).parent.parent / "universities.json"
    with open(json_path, "r") as f:
        data = json.load(f)

    for uni_name, faculties in data.items():
        short = SHORT_NAMES.get(uni_name)
        uni_id = get_or_create_university(uni_name, short)
        logger.info(f"University: {uni_name} (id={uni_id})")

        for fac_name, fac_data in faculties.items():
            fac_id = get_or_create_faculty(uni_id, fac_name)

            if isinstance(fac_data, str):
                # Faculty-level URL, no sub-departments — create department = faculty
                get_or_create_department(uni_id, fac_id, fac_name)
            elif isinstance(fac_data, dict):
                for dept_name in fac_data.keys():
                    get_or_create_department(uni_id, fac_id, dept_name)

    counts = get_counts()
    logger.info(f"Seeded: {counts}")
    close_pool()


if __name__ == "__main__":
    seed()
