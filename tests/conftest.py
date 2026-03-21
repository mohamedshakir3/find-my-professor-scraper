import json
import pytest
from pathlib import Path
from typing import Dict, Type

from scraper.core.interfaces import BaseDirectoryScraper
from scraper.orchestrator import SCRAPER_REGISTRY

UNIVERSITIES_JSON_PATH = Path(__file__).parent.parent / "universities.json"


@pytest.fixture(scope="session")
def universities_data():
    """Loads universities.json once for the entire test session."""
    with open(UNIVERSITIES_JSON_PATH, "r") as f:
        return json.load(f)


def _flatten_urls(data: dict) -> list[tuple[str, str, str, str]]:
    """
    Walks the universities.json structure and returns a flat list of
    (university_name, faculty_name, department_name, url) tuples.
    For faculties with a direct URL (no departments), department_name == faculty_name.
    """
    urls = []
    for uni_name, faculties in data.items():
        for faculty_name, faculty_data in faculties.items():
            if isinstance(faculty_data, str):
                urls.append((uni_name, faculty_name, faculty_name, faculty_data))
            elif isinstance(faculty_data, dict):
                for dept_name, dept_url in faculty_data.items():
                    urls.append((uni_name, faculty_name, dept_name, dept_url))
    return urls


@pytest.fixture(scope="session")
def all_urls(universities_data):
    """Returns a flat list of all (uni, faculty, dept, url) tuples."""
    return _flatten_urls(universities_data)


def _get_implemented_urls(data: dict) -> list[tuple[str, str, str, str]]:
    """Returns only URLs for universities that have a scraper in the registry."""
    all_urls = _flatten_urls(data)
    return [entry for entry in all_urls if entry[0] in SCRAPER_REGISTRY]


@pytest.fixture(scope="session")
def implemented_urls(universities_data):
    """Returns URLs only for universities with implemented scrapers."""
    return _get_implemented_urls(universities_data)


# --- Parametrize helpers ---
# These load at import time so pytest can parametrize test IDs

def _load_json():
    with open(UNIVERSITIES_JSON_PATH, "r") as f:
        return json.load(f)

_ALL_URLS = _flatten_urls(_load_json())
_IMPLEMENTED_URLS = _get_implemented_urls(_load_json())
