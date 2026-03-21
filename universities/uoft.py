import re
import time
import logging
from typing import List, Dict, Any
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


class UofTDirectoryScraper(BaseDirectoryScraper):
    """
    University of Toronto directory scraper.
    Has 6+ department-specific HTML patterns. Some departments require
    Selenium for pagination (ECE). Others use plain requests.
    """
    def __init__(self, university_id: int):
        super().__init__(university_id)
        self.options = Options()
        self.options.add_argument("--headless")
        self.base_url_map = {
            "Department of Chemical Engineering and Applied Chemistry": "https://chem-eng.utoronto.ca",
            "Department of Mathematics": "http://mathematics.utoronto.ca",
        }

    def scrape_directory(self, url: str, faculty_id: int, department_id: int) -> List[Dict[str, Any]]:
        professors = []

        # ECE needs Selenium pagination
        if "ece.utoronto.ca" in url:
            return self._scrape_ece(url, faculty_id, department_id)

        page = self.fetch_page(url)
        if not page:
            return []

        links = []

        # Pattern 1: Chemical Engineering — fl-post-feed-post divs
        if "chem-eng" in url:
            divs = page.find_all("div", class_="fl-post-feed-post")
            base = self.base_url_map.get(
                "Department of Chemical Engineering and Applied Chemistry", "")
            for div in divs:
                content = div.find("div", class_="fl-post-feed-content")
                if not content:
                    continue
                for a in content.find_all("a", href=True):
                    href = a["href"]
                    if (href.startswith(base) or "faculty" in href) and "jpg" not in href:
                        links.append(href)
                        break

        # Pattern 2: Civil Engineering — table with column-1/column-2
        elif "civmin" in url:
            table = page.find("table")
            if table:
                for row in table.find_all("tr"):
                    col2 = row.find("td", class_="column-2")
                    if not col2 or "professor" not in col2.get_text(strip=True).lower():
                        continue
                    col1 = row.find("td", class_="column-1")
                    if col1:
                        a = col1.find("a", href=True)
                        if a:
                            links.append(a["href"])

        # Pattern 3: Materials Science — fl-col divs
        elif "mse.utoronto.ca" in url:
            for div in page.find_all("div", class_="fl-col"):
                a = div.find("a", href=True)
                if a:
                    links.append(a["href"])

        # Pattern 4: Mechanical Engineering — pp-post-link
        elif "mie.utoronto.ca" in url:
            links = [a["href"] for a in page.find_all("a", class_="pp-post-link", href=True)]

        # Pattern 5: CS — research table with inline data
        elif "cs.toronto" in url or "web.cs" in url:
            return self._scrape_cs(page, faculty_id, department_id)

        # Pattern 6: Math — pane-node-people divs
        elif "math" in url:
            divs = page.find_all("div", class_="pane-node-people-uoft-people-title")
            base = self.base_url_map.get("Department of Mathematics", "")
            for div in divs:
                a = div.find("a", href=True)
                if a:
                    href = a["href"]
                    if not href.startswith("http"):
                        href = base + href
                    links.append(href)

        # Convert links to professor dicts
        visited = set()
        for link in links:
            if link in visited:
                continue
            visited.add(link)

            # Try to extract name from link text if available
            name = link.rstrip("/").split("/")[-1].replace("-", " ").title()
            name_parts = name.split(" ")
            first_name = name_parts[0] if name_parts else ""
            last_name = " ".join(name_parts[1:]) if len(name_parts) > 1 else ""

            # Resolve relative URLs
            if not link.startswith("http"):
                link = urljoin(url, link)

            professors.append({
                "first_name": first_name,
                "last_name": last_name,
                "profile_url": link,
                "university_id": self.university_id,
                "faculty_id": faculty_id,
                "department_id": department_id
            })

        return professors

    def _scrape_ece(self, url: str, faculty_id: int, department_id: int) -> List[Dict[str, Any]]:
        """ECE department uses Selenium pagination with 'next' button."""
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

                # Look for the Faculty Directory section
                div = None
                for span in soup.find_all("span"):
                    if span.text == "Faculty Directory":
                        div = span.find_next("div", class_="fl-module-pp-content-grid")
                        break

                if div:
                    for a in div.find_all("a", class_="pp-post-link", href=True):
                        href = a["href"]
                        if href not in visited:
                            visited.add(href)
                            name = href.rstrip("/").split("/")[-1].replace("-", " ").title()
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

                try:
                    next_button = wait.until(
                        EC.element_to_be_clickable((By.CLASS_NAME, "next")))
                    next_button.click()
                    time.sleep(2)
                except Exception:
                    break
        finally:
            driver.quit()

        return professors

    def _scrape_cs(self, page: BeautifulSoup, faculty_id: int, department_id: int) -> List[Dict[str, Any]]:
        """CS department has a table with inline research areas — extract directly."""
        professors = []
        research_header = page.find("h2", id="researchstream")
        if not research_header:
            return []

        table = research_header.find_next("table")
        if not table:
            return []

        for tr in table.find_all("tr"):
            tds = tr.find_all("td")
            if not tds:
                continue

            prof_td = tds[0]
            link_tag = prof_td.find("a")
            if not link_tag:
                continue

            name = link_tag.text.strip()
            link = link_tag.get("href", "")

            name_parts = name.split(" ")
            first_name = name_parts[0] if name_parts else ""
            last_name = " ".join(name_parts[1:]) if len(name_parts) > 1 else ""

            professors.append({
                "first_name": first_name,
                "last_name": last_name,
                "profile_url": link,
                "university_id": self.university_id,
                "faculty_id": faculty_id,
                "department_id": department_id
            })

        return professors
