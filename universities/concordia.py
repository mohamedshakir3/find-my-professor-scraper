from bs4 import BeautifulSoup
from typing import List, Dict, Any
import logging
from ..core.interfaces import BaseDirectoryScraper

logger = logging.getLogger(__name__)


class ConcordiaDirectoryScraper(BaseDirectoryScraper):
    def __init__(self, university_id: int):
        super().__init__(university_id)
        self.BASE_URL = "https://www.concordia.ca"

    def scrape_directory(self, url: str, faculty_id: int, department_id: int) -> List[Dict[str, Any]]:
        page = self.fetch_page(url)
        if not page:
            return []

        professors = []
        uls = page.find_all(class_="c-faculty-profile-list__list")

        for ul in uls:
            for li in ul.find_all("li", class_="c-faculty-profile-list__list-item"):
                a = li.find("a", class_="c-faculty-profile-list__name")
                if not a:
                    continue

                name = a.text.strip()
                profile_url = a["href"]
                if not profile_url.startswith("http"):
                    profile_url = self.BASE_URL + profile_url

                name_parts = name.split(" ")
                first_name = name_parts[0] if len(name_parts) > 0 else ""
                last_name = " ".join(name_parts[1:]) if len(name_parts) > 1 else ""

                professors.append({
                    "first_name": first_name,
                    "last_name": last_name,
                    "profile_url": profile_url,
                    "university_id": self.university_id,
                    "faculty_id": faculty_id,
                    "department_id": department_id
                })

        return professors
