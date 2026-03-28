import re
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


class QueensDirectoryScraper(BaseDirectoryScraper):
    """
    Queens University directory scraper.
    Uses Selenium for initial page load (JS-rendered content),
    then parses with BeautifulSoup. Three department-specific patterns.
    """
    def __init__(self, university_id: int):
        super().__init__(university_id)
        self.BASE_URL = "https://www.queensu.ca"
        self.options = Options()
        self.options.add_argument("--headless")

    def _get_page_with_selenium(self, url: str) -> str:
        driver = webdriver.Chrome(
            service=Service(ChromeDriverManager().install()),
            options=self.options)
        try:
            driver.get(url)
            time.sleep(2)
            return driver.page_source
        except Exception as e:
            logger.error(f"Selenium error fetching {url}: {e}")
            return ""
        finally:
            driver.quit()

    def scrape_directory(self, url: str, faculty_id: int, department_id: int) -> List[Dict[str, Any]]:
        html = self._get_page_with_selenium(url)
        if not html:
            return []

        soup = BeautifulSoup(html, 'html.parser')
        professors = []

        # Pattern 1: col-sm divs with h3 name + profile link (Geology, Math/Stats)
        col_divs = soup.find_all("div", class_="col-sm")
        if col_divs:
            for div in col_divs:
                name_header = div.find("h3")
                if not name_header:
                    continue

                name = name_header.text.strip()
                # Strip "Dr." prefix
                if name.startswith("Dr"):
                    name = name.split(".", 1)[-1].strip()

                a = div.find("a", string=re.compile(r'profile', re.IGNORECASE))
                if not a:
                    continue

                link = a.get("href", "")
                if link.startswith("http") and not link.startswith(self.BASE_URL):
                    continue
                if not link.startswith("http"):
                    link = self.BASE_URL + link

                # Extract email
                email = ""
                for a_tag in div.find_all("a", href=True):
                    if "mailto" in a_tag["href"]:
                        email = a_tag.text.strip()
                        break

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

        # Pattern 2: views-row divs (Physics)
        views_rows = soup.find_all("div", class_="views-row")
        if views_rows:
            for div in views_rows:
                name_tag = div.find("p", class_="directory-list-name")
                if not name_tag:
                    continue
                a = name_tag.find("a", href=True)
                if not a:
                    continue

                name = a.text.strip()
                link = a["href"]
                if not link.startswith("http"):
                    link = self.BASE_URL + link

                email_tag = div.find("p", class_="directory-list-email")
                email = email_tag.get_text(strip=True) if email_tag else ""

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

            # Handle pagination for views-row pattern
            next_page_li = soup.find("li", class_="pager__item pager__item--next")
            if next_page_li:
                next_a = next_page_li.find("a", href=True)
                if next_a:
                    next_url = url + next_a["href"]
                    professors.extend(self.scrape_directory(next_url, faculty_id, department_id))

            return professors

        # Pattern 3: dirItem divs (Engineering departments — default)
        dir_items = soup.find_all("div", class_="dirItem")
        for div in dir_items:
            title = div.find("div", class_="panel-title")
            if not title:
                continue
            a = title.find("a")
            if not a:
                continue

            name = a.text.strip()
            link = a["href"]

            # Filter to professors only
            body = div.find("div", class_="text-muted depts")
            if body and "professor" not in body.get_text(strip=True).lower():
                continue

            email_div = div.find("div", class_="email")
            email = email_div.get_text(strip=True) if email_div else ""

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
