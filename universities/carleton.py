from typing import List, Dict, Any
import logging
import time
from urllib.parse import urljoin
from bs4 import BeautifulSoup
from ..core.interfaces import BaseDirectoryScraper

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options

logger = logging.getLogger(__name__)


class CarletonDirectoryScraper(BaseDirectoryScraper):
    """
    Carleton University directory scraper.
    Uses Selenium since pages render dynamically.
    Carleton uses a consistent "People Card" layout with:
      - Name in h3/h2 heading
      - "View Profile" button: a.cu-button--red
    """
    def __init__(self, university_id: int):
        super().__init__(university_id)
        self.options = Options()
        self.options.add_argument("--headless")

    def _scrape_with_selenium(self, url: str) -> str:
        driver = webdriver.Chrome(
            service=Service(ChromeDriverManager().install()),
            options=self.options)
        try:
            driver.get(url)
            time.sleep(4)  # Wait for page load
            # Scroll down to trigger lazy-loading
            driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(2)
            return driver.page_source
        except Exception as e:
            logger.error(f"Selenium error fetching {url}: {e}")
            return ""
        finally:
            driver.quit()

    def scrape_directory(self, url: str, faculty_id: int, department_id: int) -> List[Dict[str, Any]]:
        html_source = self._scrape_with_selenium(url)
        page = BeautifulSoup(html_source, 'html.parser')

        professors = []
        visited = set()

        # Strategy 1: Look for "View Profile" buttons (cu-button--red)
        # Walk upward from each button to find the name
        view_profile_links = page.find_all("a", class_=lambda c: c and "cu-button" in c, href=True)
        if not view_profile_links:
            # Also try finding by link text
            view_profile_links = page.find_all("a", href=True, string=lambda s: s and "view profile" in s.lower())

        for link_tag in view_profile_links:
            href = link_tag["href"]
            if not href.startswith("http"):
                href = urljoin(url, href)
            if href in visited:
                continue

            # Find name: look for nearest h2/h3 preceding this link
            parent = link_tag.parent
            name = ""
            for _ in range(5):  # Walk up max 5 levels
                if parent is None:
                    break
                heading = parent.find(["h2", "h3"])
                if heading:
                    name = heading.get_text(strip=True)
                    break
                parent = parent.parent

            if not name:
                # Try extracting from link text or URL
                name = href.rstrip("/").split("/")[-1].replace("-", " ").title()

            # Skip non-person entries
            if not name or name.lower() in ("view profile", "learn more"):
                continue

            visited.add(href)
            name_parts = name.split(" ")
            first_name = name_parts[0] if name_parts else ""
            last_name = " ".join(name_parts[1:]) if len(name_parts) > 1 else ""

            professors.append({
                "first_name": first_name,
                "last_name": last_name,
                "profile_url": href,
                "university_id": self.university_id,
                "faculty_id": faculty_id,
                "department_id": department_id
            })

        # Strategy 2: If no View Profile buttons, look for listing-item pattern
        if not professors:
            listings = page.find_all("li", class_="listing-item")
            for listing in listings:
                a = listing.find("a", href=True)
                if not a:
                    continue
                heading = a.find(["h3", "h2"])
                if not heading:
                    continue

                name = heading.get_text(strip=True)
                href = a["href"]
                if not href.startswith("http"):
                    href = urljoin(url, href)
                if href in visited:
                    continue
                visited.add(href)

                name_parts = name.split(" ")
                first_name = name_parts[0] if name_parts else ""
                last_name = " ".join(name_parts[1:]) if len(name_parts) > 1 else ""

                professors.append({
                    "first_name": first_name,
                    "last_name": last_name,
                    "profile_url": href,
                    "university_id": self.university_id,
                    "faculty_id": faculty_id,
                    "department_id": department_id
                })

        # Strategy 3: Generic fallback — find all links to /people/ paths
        if not professors:
            for a in page.find_all("a", href=True):
                href = a["href"]
                if "/people/" in href and href not in visited:
                    if not href.startswith("http"):
                        href = urljoin(url, href)
                    name = a.get_text(strip=True)
                    if not name or len(name) > 60:
                        name = href.rstrip("/").split("/")[-1].replace("-", " ").title()

                    visited.add(href)
                    name_parts = name.split(" ")
                    first_name = name_parts[0] if name_parts else ""
                    last_name = " ".join(name_parts[1:]) if len(name_parts) > 1 else ""

                    professors.append({
                        "first_name": first_name,
                        "last_name": last_name,
                        "profile_url": href,
                        "university_id": self.university_id,
                        "faculty_id": faculty_id,
                        "department_id": department_id
                    })

        return professors
