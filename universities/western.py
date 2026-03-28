from bs4 import BeautifulSoup
from typing import List, Dict, Any
import logging
from ..core.interfaces import BaseDirectoryScraper

logger = logging.getLogger(__name__)


class WesternDirectoryScraper(BaseDirectoryScraper):
    def __init__(self, university_id: int):
        super().__init__(university_id)
        self.BASE_URL_MAP = {
            "Department of Chemical and Biochemical Engineering":
                "https://www.eng.uwo.ca/chemical",
            "Department of Civil and Environmental Engineering":
                "https://www.eng.uwo.ca/civil",
            "Department of Electrical and Computer Engineering":
                "https://www.eng.uwo.ca/electrical",
            "Department of Mechanical and Materials Engineering":
                "https://www.eng.uwo.ca/mechanical",
            "School of Biomedical Engineering":
                "https://www.eng.uwo.ca/biomed/"
        }

    def scrape_directory(self, url: str, faculty_id: int, department_id: int) -> List[Dict[str, Any]]:
        page = self.fetch_page(url)
        if not page:
            return []

        professors = []
        divs = page.find_all("div", class_="teamgrid")

        for div in divs:
            infoleft = div.find("div", class_="infoleft")
            inforight = div.find("div", class_="inforight")
            if not infoleft or not inforight:
                continue

            h2 = infoleft.find("h2")
            if not h2:
                continue
            name = h2.get_text(strip=True)

            # Extract profile URL from the second link in inforight
            a_tags = inforight.find_all("a", href=True)
            if len(a_tags) < 2:
                continue

            profile_url = a_tags[1]["href"]
            if not profile_url.startswith("http"):
                # Resolve relative URLs using department base
                # URL format is usually "../../people/name.html"
                for dept_name, base in self.BASE_URL_MAP.items():
                    if dept_name.lower() in url.lower() or base in url:
                        profile_url = base + profile_url.split("..")[-1]
                        break

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
