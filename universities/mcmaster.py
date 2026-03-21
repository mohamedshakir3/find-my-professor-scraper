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


class McMasterDirectoryScraper(BaseDirectoryScraper):
    """
    McMaster University directory scraper.
    Uses requests with recursive pagination via 'next page-numbers' links.
    Extracts professors from faculty-card__link elements.
    """
    def __init__(self, university_id: int):
        super().__init__(university_id)
        self.visited_pages = set()

    def scrape_directory(self, url: str, faculty_id: int, department_id: int) -> List[Dict[str, Any]]:
        self.visited_pages = set()
        links = self._collect_links(url)

        professors = []
        visited_profiles = set()
        for link in links:
            if link in visited_profiles:
                continue
            visited_profiles.add(link)

            # Extract name from the profile URL slug
            # e.g. https://engineering.mcmaster.ca/faculty/john-smith/ -> John Smith
            slug = link.rstrip("/").split("/")[-1]
            name_parts = slug.replace("-", " ").title().split(" ")
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

    def _collect_links(self, url: str) -> list:
        """Recursively collect all faculty-card links, following pagination."""
        if url in self.visited_pages:
            return []
        self.visited_pages.add(url)

        page = self.fetch_page(url)
        if not page:
            return []

        links = [a["href"] for a in page.find_all("a", class_="faculty-card__link", href=True)]

        # Check for next page
        next_link = page.find("a", class_="next page-numbers", href=True)
        if next_link:
            links.extend(self._collect_links(next_link["href"]))

        return links
