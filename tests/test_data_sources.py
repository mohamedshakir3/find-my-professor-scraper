"""
Data Source Health Check Tests
==============================
These tests validate that all university directory URLs in universities.json
are reachable and that implemented scrapers can still extract professor data.

Run quick health checks:
    pytest scraper/tests/test_data_sources.py -m health -v

Run all data source tests:
    pytest scraper/tests/test_data_sources.py -v
"""

import json
import random
import re
import time
import requests
import pytest
from pathlib import Path
from urllib.parse import urlparse
from collections import defaultdict

from scraper.tests.conftest import _ALL_URLS, _IMPLEMENTED_URLS, SCRAPER_REGISTRY

# Shared session with browser-like User-Agent to avoid 403s
_SESSION = requests.Session()
_SESSION.headers.update({
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko)"
})

# Track last request time per domain to throttle and avoid 429s
_LAST_REQUEST_TIME: dict[str, float] = defaultdict(float)
_DOMAIN_DELAY = 1.0  # seconds between requests to the same domain


def _throttled_get(url: str, **kwargs):
    """GET with per-domain rate limiting and retry on timeout."""
    domain = urlparse(url).netloc
    elapsed = time.time() - _LAST_REQUEST_TIME[domain]
    if elapsed < _DOMAIN_DELAY:
        time.sleep(_DOMAIN_DELAY - elapsed)
    _LAST_REQUEST_TIME[domain] = time.time()

    try:
        return _SESSION.get(url, **kwargs)
    except requests.exceptions.ReadTimeout:
        # Retry once with a longer timeout for slow servers (e.g. schulich.ucalgary.ca)
        time.sleep(2)
        _LAST_REQUEST_TIME[domain] = time.time()
        return _SESSION.get(url, timeout=45, allow_redirects=True)


# ============================================================
# Test 1: URL Reachability
# ============================================================

@pytest.mark.health
@pytest.mark.parametrize(
    "uni_name, faculty_name, dept_name, url",
    _ALL_URLS,
    ids=[f"{u[0]}::{u[2]}" for u in _ALL_URLS]
)
def test_url_reachable(uni_name, faculty_name, dept_name, url):
    """Every directory URL in universities.json should return HTTP 200."""
    response = _throttled_get(url, timeout=30, allow_redirects=True)
    assert response.status_code == 200, (
        f"[{uni_name}] {dept_name} returned HTTP {response.status_code}: {url}"
    )


# ============================================================
# Test 2: Scraper Output Validation
# ============================================================

@pytest.mark.slow
@pytest.mark.parametrize(
    "uni_name, faculty_name, dept_name, url",
    _IMPLEMENTED_URLS,
    ids=[f"{u[0]}::{u[2]}" for u in _IMPLEMENTED_URLS]
)
def test_scraper_returns_results(uni_name, faculty_name, dept_name, url):
    """
    For universities with implemented scrapers, scrape_directory()
    should return a non-empty list with the required fields.
    """
    scraper_class = SCRAPER_REGISTRY[uni_name]
    scraper = scraper_class(university_id=1)
    
    results = scraper.scrape_directory(url, faculty_id=1, department_id=1)
    
    assert len(results) > 0, (
        f"[{uni_name}] {dept_name} scraper returned 0 professors from {url}"
    )
    
    # Validate structure of each result
    for prof in results:
        assert "first_name" in prof, f"Missing 'first_name' in result: {prof}"
        assert "profile_url" in prof, f"Missing 'profile_url' in result: {prof}"
        assert prof["profile_url"].startswith("http"), (
            f"Invalid profile_url: {prof['profile_url']}"
        )


# ============================================================
# Test 3: Scraper Registry Coverage
# ============================================================

def test_all_universities_have_scrapers(universities_data):
    """
    Track which universities still need scraper implementations.
    This test is marked xfail so it doesn't block CI, but serves
    as a living TODO list.
    """
    missing = []
    for uni_name in universities_data.keys():
        if uni_name not in SCRAPER_REGISTRY:
            missing.append(uni_name)
    
    if missing:
        pytest.xfail(
            f"{len(missing)} universities without scrapers: {', '.join(missing)}"
        )


# ============================================================
# Test 4: universities.json Schema Validation  
# ============================================================

@pytest.mark.health
def test_json_schema_valid(universities_data):
    """
    Validates universities.json structure:
    - No empty URL strings
    - All leaf values are valid URLs
    - No duplicate URLs across the entire file
    """
    errors = []
    seen_urls = {}
    
    for uni_name, faculties in universities_data.items():
        assert isinstance(faculties, dict), f"{uni_name} should map to a dict of faculties"
        
        for faculty_name, faculty_data in faculties.items():
            if isinstance(faculty_data, str):
                # Direct faculty URL
                _validate_url(faculty_data, f"{uni_name} > {faculty_name}", errors, seen_urls)
            elif isinstance(faculty_data, dict):
                for dept_name, dept_url in faculty_data.items():
                    _validate_url(dept_url, f"{uni_name} > {faculty_name} > {dept_name}", errors, seen_urls)
            else:
                errors.append(f"{uni_name} > {faculty_name}: unexpected type {type(faculty_data)}")
    
    assert not errors, "Schema validation errors:\n" + "\n".join(f"  - {e}" for e in errors)


def _validate_url(url: str, context: str, errors: list, seen_urls: dict):
    """Helper to validate a single URL entry."""
    if not url or not url.strip():
        errors.append(f"{context}: empty URL string")
        return
    
    parsed = urlparse(url)
    if not parsed.scheme or not parsed.netloc:
        errors.append(f"{context}: invalid URL format: {url}")
        return
    
    if url in seen_urls:
        errors.append(f"{context}: duplicate URL (also in {seen_urls[url]}): {url}")
    else:
        seen_urls[url] = context


# ============================================================
# Test 5: Profile URL Sampling
# ============================================================

@pytest.mark.slow
@pytest.mark.parametrize(
    "uni_name, faculty_name, dept_name, url",
    # Sample a subset of implemented URLs to keep runtime reasonable
    _IMPLEMENTED_URLS[:3] if len(_IMPLEMENTED_URLS) >= 3 else _IMPLEMENTED_URLS,
    ids=[f"{u[0]}::{u[2]}" for u in (_IMPLEMENTED_URLS[:3] if len(_IMPLEMENTED_URLS) >= 3 else _IMPLEMENTED_URLS)]
)
def test_profile_urls_reachable(uni_name, faculty_name, dept_name, url):
    """
    After scraping a directory, sample a few profile URLs from the results
    and verify they are reachable. Catches broken link generation.
    """
    scraper_class = SCRAPER_REGISTRY[uni_name]
    scraper = scraper_class(university_id=1)
    
    results = scraper.scrape_directory(url, faculty_id=1, department_id=1)
    if not results:
        pytest.skip(f"No results from {dept_name}, cannot sample profile URLs")
    
    # Sample up to 3 profile URLs
    sample_size = min(3, len(results))
    sample = random.sample(results, sample_size)
    
    for prof in sample:
        profile_url = prof.get("profile_url")
        assert profile_url, f"Professor missing profile_url: {prof}"
        
        response = _throttled_get(profile_url, timeout=20, allow_redirects=True)
        assert response.status_code == 200, (
            f"Profile URL returned HTTP {response.status_code}: "
            f"{prof.get('first_name', '?')} {prof.get('last_name', '?')} -> {profile_url}"
        )
