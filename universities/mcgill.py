from bs4 import BeautifulSoup
from typing import List, Dict, Any
import logging
from ..core.interfaces import BaseDirectoryScraper

logger = logging.getLogger(__name__)


class McGillDirectoryScraper(BaseDirectoryScraper):
    def __init__(self, university_id: int):
        super().__init__(university_id)
        self.base_url = "https://www.mcgill.ca"

    def scrape_directory(self, url: str, faculty_id: int, department_id: int) -> List[Dict[str, Any]]:
        page = self.fetch_page(url)
        if not page:
            return []

        professors = []
        visited = set()

        # Pattern 1: Engineering faculty — profile display names
        headers = page.find_all("h2", class_="mcgill-profiles-display-name")
        if headers:
            for header in headers:
                a = header.find("a", href=True)
                if not a:
                    continue
                href = a["href"]
                if not href.startswith("http"):
                    href = self.base_url + href
                if href in visited:
                    continue
                visited.add(href)

                name = a.get_text(strip=True)
                name_parts = name.split(" ")
                first_name = name_parts[0] if len(name_parts) > 0 else ""
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

        # Pattern 2: Math/Stats — table under "Professors" header
        professor_header = page.find("h2", string="Professors")
        if professor_header:
            table = professor_header.find_next("table")
            if table:
                rows = table.find_all("tr")
                for row in rows:
                    a = row.find("a", href=True)
                    if not a:
                        continue
                    href = a["href"]
                    if not href.startswith("http"):
                        href = self.base_url + href
                    if href in visited:
                        continue
                    visited.add(href)

                    name = a.get_text(strip=True)
                    name_parts = name.split(" ")
                    first_name = name_parts[0] if len(name_parts) > 0 else ""
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

            # Pattern 3: CS — col-md-6 divs after "Professors" header
            for sibling in professor_header.find_next_siblings():
                if sibling.name == "h2":
                    break
                rows = sibling.find_all("div", class_="col-md-6")
                for row in rows:
                    name_tag = row.find("h4")
                    name = name_tag.get_text(strip=True) if name_tag else ""
                    if not name:
                        continue

                    # Try to find a profile/website link
                    website_tag = row.find("a", href=True, string="Website")
                    href = website_tag["href"] if website_tag else ""
                    if not href:
                        # Fall back to any link
                        a = row.find("a", href=True)
                        href = a["href"] if a else ""
                    if not href.startswith("http"):
                        href = self.base_url + href

                    if href in visited or not href:
                        continue
                    visited.add(href)

                    name_parts = name.split(" ")
                    first_name = name_parts[0] if len(name_parts) > 0 else ""
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
