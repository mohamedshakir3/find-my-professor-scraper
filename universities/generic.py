"""
Generic directory scraper that works on any university faculty page.

Instead of hand-crafted CSS selectors per university, this scraper:
1. Extracts all <a> tags from the page
2. Filters out obvious noise (social media, navigation, anchors, files)
3. Clusters links by URL template (replacing the variable slug with a placeholder)
4. Picks the largest cluster as the professor profile links
5. Parses names from the anchor text
"""

import re
import logging
import time
from typing import List, Dict, Any, Optional
from urllib.parse import urljoin, urlparse
from collections import Counter
from bs4 import BeautifulSoup

from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager

try:
    from ..core.interfaces import BaseDirectoryScraper
except ImportError:
    from core.interfaces import BaseDirectoryScraper

logger = logging.getLogger(__name__)

# Domains that are never professor profiles
SOCIAL_DOMAINS = {
    "facebook.com", "twitter.com", "x.com", "instagram.com", "linkedin.com",
    "youtube.com", "tiktok.com", "reddit.com", "github.com", "scholar.google.com",
    "orcid.org", "researchgate.net", "academia.edu",
}

# Path segments that indicate non-profile pages
# Keep this tight — many universities put profiles under /about/people/ etc.
NOISE_PATH_PATTERNS = re.compile(
    r"/(login|logout|search|sitemap|privacy|terms-and-conditions|"
    r"admission|emergency|donate|give|maps|directions|"
    r"cookie|accessibility|copyright|disclaimer|rss|feed|wp-admin|wp-content|"
    r"cdn-cgi)(/|$)",
    re.IGNORECASE,
)

# File extensions that aren't profile pages
FILE_EXTENSIONS = re.compile(r"\.(pdf|jpg|jpeg|png|gif|svg|doc|docx|xls|xlsx|zip|tar|gz)$", re.IGNORECASE)


def _normalize_url(url: str, base_url: str) -> Optional[str]:
    """Resolve relative URLs and normalize."""
    if not url or url.startswith(("#", "javascript:", "mailto:", "tel:")):
        return None
    full = urljoin(base_url, url)
    parsed = urlparse(full)
    if parsed.scheme not in ("http", "https"):
        return None
    return full


def _is_noise_domain(url: str) -> bool:
    """Check if URL belongs to a known non-profile domain."""
    host = urlparse(url).hostname or ""
    return any(social in host for social in SOCIAL_DOMAINS)


def _is_noise_path(url: str) -> bool:
    """Check if URL path contains noise indicators."""
    path = urlparse(url).path
    if FILE_EXTENSIONS.search(path):
        return False  # .html is fine, actual files are filtered by extension
    if NOISE_PATH_PATTERNS.search(path):
        return True
    return False


def _url_template(url: str) -> str:
    """
    Convert a URL to a template by replacing the last path segment with a placeholder.
    e.g. /people/faculty/john-doe.html -> /people/faculty/{}.html
         /profiles?id=123 -> /profiles?id={}
    """
    parsed = urlparse(url)
    path = parsed.path

    # Replace the last path segment (the variable part — usually the professor slug)
    parts = path.rstrip("/").rsplit("/", 1)
    if len(parts) == 2:
        template_path = parts[0] + "/{}"
    else:
        template_path = "/{}"

    # Preserve extension if present
    ext_match = re.search(r"\.\w+$", path)
    if ext_match:
        template_path += ext_match.group()

    return f"{parsed.scheme}://{parsed.netloc}{template_path}"


def _url_template_coarse(url: str) -> str:
    """
    Like _url_template but replaces the last TWO path segments with placeholders.
    e.g. /our-faculty/professors/john-doe.html -> /our-faculty/{}/{}.html
    This catches sibling categories like /professors/X and /adjunct-professors/X.
    """
    parsed = urlparse(url)
    path = parsed.path.rstrip("/")

    parts = path.rsplit("/", 2)
    if len(parts) == 3:
        template_path = parts[0] + "/{}/{}"
    elif len(parts) == 2:
        template_path = parts[0] + "/{}"
    else:
        template_path = "/{}"

    ext_match = re.search(r"\.\w+$", path)
    if ext_match:
        template_path += ext_match.group()

    return f"{parsed.scheme}://{parsed.netloc}{template_path}"


def _looks_like_name(text: str) -> bool:
    """Check if text plausibly contains a person's name (2-5 capitalized-ish words)."""
    text = text.strip()
    if not text or len(text) < 3 or len(text) > 80:
        return False
    # Must have at least 2 word-like tokens
    words = text.split()
    if len(words) < 2 or len(words) > 6:
        return False
    # At least half should start with a capital letter (allows for particles like "de", "van")
    capitalized = sum(1 for w in words if w[0].isupper())
    return capitalized >= len(words) / 2


# Words that never appear in a person's name — used to reject false positives
NON_NAME_WORDS = {
    "faculty", "staff", "students", "news", "events", "alumni", "friends",
    "about", "programs", "home", "research", "contact", "directory",
    "current", "career", "openings", "department", "school", "engineering",
    "science", "associate", "assistant", "adjunct", "adjuncts", "emeritus",
    "cross", "listed", "appointed", "cross-appointed", "cross-listed",
    "hall", "fame", "professor", "professors", "instructors",
    "our", "the", "for", "and", "of", "in", "at", "to", "us",
    "view", "profile", "more", "all", "see", "visit", "search",
    "overview", "welcome", "information", "privacy", "terms", "statement",
}


def _parse_name(text: str) -> Optional[Dict[str, str]]:
    """
    Parse a name string into first_name and last_name.
    Handles formats: "Last, First", "First Last", "Last, First Middle"
    """
    text = text.strip()
    # Reject if contains &, |, or / — not a person name
    if re.search(r"[&|/]", text):
        return None
    # Remove titles/suffixes/credentials
    text = re.sub(r"\b(Dr|Prof|PhD|Ph\.D|MSc|M\.Sc|P\.Eng|EIT|Mr|Mrs|Ms|Jr|Sr|III|II)\b\.?", "", text, flags=re.IGNORECASE).strip()
    # Remove trailing commas and extra whitespace
    text = text.rstrip(",").strip()
    text = re.sub(r"\s+", " ", text)

    if not text:
        return None

    # Reject if most words are common non-name words
    words_lower = {w.lower() for w in text.split()}
    if len(words_lower & NON_NAME_WORDS) > len(words_lower) / 2:
        return None

    if "," in text:
        # "Last, First" format
        parts = [p.strip() for p in text.split(",", 1)]
        if len(parts) == 2 and parts[0] and parts[1]:
            return {"first_name": parts[1].split()[0], "last_name": parts[0]}

    # "First Last" format
    words = text.split()
    if len(words) >= 2:
        return {"first_name": words[0], "last_name": " ".join(words[1:])}

    return None


class GenericDirectoryScraper(BaseDirectoryScraper):
    """
    A generic scraper that uses URL template clustering to identify
    professor profile links from any directory page.

    Tries plain requests first; falls back to Selenium for JS-rendered pages.
    """

    def __init__(self, university_id: int, min_cluster_size: int = 3):
        super().__init__(university_id)
        self.min_cluster_size = min_cluster_size

    def _fetch_with_selenium(self, url: str) -> Optional[BeautifulSoup]:
        """Fetch a page using headless Chrome for JS-rendered content."""
        opts = Options()
        opts.add_argument("--headless")
        driver = webdriver.Chrome(
            service=Service(ChromeDriverManager().install()),
            options=opts,
        )
        try:
            driver.get(url)
            time.sleep(4)
            # Scroll to trigger lazy-loading
            driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(2)
            return BeautifulSoup(driver.page_source, "html.parser")
        except Exception as e:
            logger.error(f"Selenium error fetching {url}: {e}")
            return None
        finally:
            driver.quit()

    def _extract_links(self, soup: BeautifulSoup, url: str) -> List[tuple]:
        """Extract all (full_url, anchor_text) pairs from a parsed page."""
        links = []
        for a_tag in soup.find_all("a", href=True):
            full_url = _normalize_url(a_tag["href"], url)
            if full_url:
                links.append((full_url, a_tag.get_text(strip=True)))
        return links

    def _classify_links(self, raw_links: List[tuple], url: str) -> List[tuple]:
        """Filter noise from raw links."""
        filtered = []
        for link_url, anchor_text in raw_links:
            if _is_noise_domain(link_url):
                continue
            if _is_noise_path(link_url):
                continue
            if link_url.rstrip("/") == url.rstrip("/"):
                continue
            if len(anchor_text) < 2:
                continue
            filtered.append((link_url, anchor_text))
        return filtered

    @staticmethod
    def _score_clusters(
        template_groups: Dict[str, List[tuple]], url: str, min_size: int,
    ) -> List[tuple]:
        source_domain = urlparse(url).netloc
        scored = []
        for tmpl, links in template_groups.items():
            if len(links) < min_size:
                continue
            tmpl_domain = urlparse(tmpl).netloc
            same_domain = 1 if tmpl_domain == source_domain else 0
            name_ratio = sum(1 for _, txt in links if _parse_name(txt) is not None) / len(links)
            profile_keywords = re.search(
                r"/(people|faculty|professors?|profiles?|directory|staff|members?|users?)/",
                tmpl, re.IGNORECASE,
            )
            profile_bonus = 30 if profile_keywords else 0
            score = name_ratio * 100 + same_domain * 50 + profile_bonus + len(links)
            scored.append((score, tmpl, links))
            logger.debug(
                f"  Cluster ({len(links)} links, name_ratio={name_ratio:.2f}, "
                f"same_domain={same_domain}, score={score:.1f}): {tmpl}"
            )
        scored.sort(key=lambda x: -x[0])
        return scored

    def _find_best_cluster(self, filtered: List[tuple], url: str) -> Optional[tuple]:
        """
        Cluster links by URL template and return (score, template, links) for best cluster.

        Uses two-level clustering: first by exact template (last path segment replaced),
        then by coarse template (last two segments replaced) to catch cases like
        /faculty/professors/name.html + /faculty/adjunct-professors/name.html.
        """
        # Fine-grained clustering: replace last path segment
        fine_groups: Dict[str, List[tuple]] = {}
        for link_url, anchor_text in filtered:
            template = _url_template(link_url)
            fine_groups.setdefault(template, []).append((link_url, anchor_text))

        scored = self._score_clusters(fine_groups, url, self.min_cluster_size)

        # If the fine-grained best cluster has a high parseable-name ratio, use it
        if scored:
            _, _, best_links = scored[0]
            parseable = sum(1 for _, txt in best_links if _parse_name(txt) is not None)
            if parseable / len(best_links) >= 0.5:
                return scored[0]

        # Otherwise, try coarse clustering (replace last 2 segments)
        coarse_groups: Dict[str, List[tuple]] = {}
        for link_url, anchor_text in filtered:
            template = _url_template_coarse(link_url)
            coarse_groups.setdefault(template, []).append((link_url, anchor_text))

        coarse_scored = self._score_clusters(coarse_groups, url, self.min_cluster_size)

        # Pick the best across both, preferring the one with more parseable names
        all_scored = scored + coarse_scored
        if not all_scored:
            return None

        # Re-rank: prefer clusters with many parseable names AND high ratio
        def effective_score(entry):
            _, _, links = entry
            parseable = sum(1 for _, txt in links if _parse_name(txt) is not None)
            ratio = parseable / len(links)
            # Must have at least 50% parseable to be considered, then rank by count
            if ratio < 0.5:
                return (0, 0)
            return (parseable, ratio)

        all_scored.sort(key=effective_score, reverse=True)
        return all_scored[0]

    def scrape_directory(self, url: str, faculty_id: int, department_id: int) -> List[Dict[str, Any]]:
        soup = self.fetch_page(url)
        if not soup:
            return []

        # Step 1: Try with plain requests first
        raw_links = self._extract_links(soup, url)
        filtered = self._classify_links(raw_links, url)
        best = self._find_best_cluster(filtered, url)

        # Step 2: If no good cluster found, retry with Selenium
        # "Good" = at least min_cluster_size parseable names with high ratio
        if best is not None:
            _, _, candidate_links = best
            # Count unique parseable URLs to avoid duplicates inflating the count
            seen_urls: set = set()
            parseable = 0
            for link_url, txt in candidate_links:
                if link_url not in seen_urls and _parse_name(txt) is not None:
                    seen_urls.add(link_url)
                    parseable += 1
        else:
            parseable = 0
        needs_selenium = parseable < self.min_cluster_size
        if needs_selenium:
            score_str = f"{best[0]:.0f}" if best else "N/A"
            logger.info(
                f"No strong profile cluster via requests "
                f"(best score: {score_str}, parseable: {parseable}), retrying with Selenium: {url}"
            )
            soup = self._fetch_with_selenium(url)
            if soup:
                raw_links = self._extract_links(soup, url)
                filtered = self._classify_links(raw_links, url)
                best = self._find_best_cluster(filtered, url)

        logger.info(f"Extracted {len(raw_links)} raw links from {url}")

        if not best:
            logger.warning(f"No profile cluster found for {url}")
            return []

        best_score, best_template, best_links = best
        logger.info(
            f"Selected cluster with {len(best_links)} links (score={best_score:.1f}): {best_template}"
        )

        # Step 3: Parse names and build results
        results = []
        visited = set()
        for link_url, anchor_text in best_links:
            if link_url in visited:
                continue
            visited.add(link_url)

            name = _parse_name(anchor_text)
            if not name:
                # Try to extract name from URL slug as fallback
                slug = urlparse(link_url).path.rstrip("/").rsplit("/", 1)[-1]
                slug = re.sub(r"\.\w+$", "", slug)  # remove extension
                slug_name = slug.replace("-", " ").replace("_", " ").title()
                name = _parse_name(slug_name)

            if not name:
                logger.debug(f"Could not parse name from '{anchor_text}' or URL, skipping")
                continue

            results.append({
                "first_name": name["first_name"],
                "last_name": name["last_name"],
                "profile_url": link_url,
                "university_id": self.university_id,
                "faculty_id": faculty_id,
                "department_id": department_id,
            })

        logger.info(f"Extracted {len(results)} professors from {url}")
        return results
