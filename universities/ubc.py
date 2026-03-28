import time
import logging
from typing import List, Dict, Any
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


class UBCDirectoryScraper(BaseDirectoryScraper):
    """
    University of British Columbia directory scraper.
    Engineering faculty uses Selenium pagination (rel=next).
    Other departments use standard requests with various HTML patterns.
    """
    def __init__(self, university_id: int):
        super().__init__(university_id)
        self.options = Options()
        self.options.add_argument("--headless")

    def scrape_directory(self, url: str, faculty_id: int, department_id: int) -> List[Dict[str, Any]]:
        # Engineering faculty needs Selenium pagination
        if "engineering.ubc.ca" in url:
            return self._scrape_engineering(url, faculty_id, department_id)

        # All other departments use requests
        page = self.fetch_page(url)
        if not page:
            return []

        professors = []
        visited = set()

        # Try multiple patterns for different department pages
        # Pattern 1: Links in profile listings (common across many UBC depts)
        for a in page.find_all("a", href=True):
            href = a["href"]
            text = a.get_text(strip=True)

            # Look for profile-like links with names
            if not text or len(text) < 3:
                continue

            # Skip navigation/utility links
            if any(skip in text.lower() for skip in [
                "home", "about", "contact", "search", "menu", "skip",
                "read more", "view", "http", "www", "page"
            ]):
                continue

            if href in visited:
                continue

            # Department-specific profile URL patterns
            is_profile = False

            # EOAS, Chemistry, Stats, Zoology, IRES
            if "/people/" in href or "/personnel/" in href or "/faculty/" in href:
                is_profile = True
            # Physics
            elif "/researchers" in url and "/profile/" in href:
                is_profile = True
            # Math
            elif "math.ubc.ca" in url and "/user/" in href:
                is_profile = True

            if is_profile:
                visited.add(href)
                if not href.startswith("http"):
                    # Build absolute URL from the department URL
                    from urllib.parse import urljoin
                    href = urljoin(url, href)

                name_parts = text.split(" ")
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

    def _scrape_engineering(self, url: str, faculty_id: int, department_id: int) -> List[Dict[str, Any]]:
        """Engineering faculty — Selenium pagination with rel=next buttons."""
        driver = webdriver.Chrome(
            service=Service(ChromeDriverManager().install()),
            options=self.options)

        professors = []
        visited = set()

        try:
            driver.get(url)
            wait = WebDriverWait(driver, 10)

            while True:
                soup = BeautifulSoup(driver.page_source, 'html.parser')
                profiles = soup.find_all("li", class_="my-atom-4")

                for li in profiles:
                    a = li.find("a", href=True)
                    if not a:
                        continue

                    href = a["href"]
                    if href in visited:
                        continue
                    visited.add(href)

                    name = a.get_text(strip=True)
                    # Get department from sibling <p> if available
                    h3 = a.find_parent()
                    dept_text = ""
                    if h3:
                        p = h3.find_next_sibling("p")
                        if p:
                            dept_text = p.get_text(strip=True)

                    # Skip nursing faculty
                    if "nursing" in dept_text.lower():
                        continue

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

                # Try to click "next" pagination
                try:
                    next_button = wait.until(
                        EC.element_to_be_clickable(
                            (By.CSS_SELECTOR, 'a[rel="next"]')))
                    next_button.click()
                    time.sleep(2)
                except Exception:
                    break
        finally:
            driver.quit()

        return professors
