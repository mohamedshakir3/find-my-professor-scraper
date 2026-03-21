from bs4 import BeautifulSoup
from typing import List, Dict, Any
import logging
from ..core.interfaces import BaseDirectoryScraper

logger = logging.getLogger(__name__)


class UdeMDirectoryScraper(BaseDirectoryScraper):
    """
    University of Montreal (Polytechnique Montréal) directory scraper.
    Directory pages use tables with French headers (professeurs, chercheurs).
    """
    def __init__(self, university_id: int):
        super().__init__(university_id)

    def scrape_directory(self, url: str, faculty_id: int, department_id: int) -> List[Dict[str, Any]]:
        page = self.fetch_page(url)
        if not page:
            return []

        professors = []
        visited = set()

        table = page.find("table")
        if not table:
            logger.warning(f"No table found at {url}")
            return []

        rows = table.find_all("tr")
        prof_header = None

        for row in rows:
            th = row.find("th")

            # Look for the "professors" section header
            if not th and not prof_header:
                continue
            if th:
                header_text = th.text.lower()
                if ("professors" in header_text or
                    "researchers" in header_text or
                    "professeurs" in header_text or
                    "chercheurs" in header_text):
                    prof_header = row
                    continue
                elif prof_header:
                    # Hit a different section header, stop
                    break

            if prof_header:
                a_tags = row.find_all("a", href=True)
                link = ""
                name = ""
                for a in a_tags:
                    if "mailto" not in a["href"]:
                        link = a["href"]
                    else:
                        name = a.get_text(strip=True)

                if link and link not in visited:
                    # Rewrite URL to English version if possible
                    parts = link.rsplit('/', 1)
                    if len(parts) == 2:
                        link = f"{parts[0]}/en/{parts[1]}"

                    visited.add(link)

                    if not name:
                        # Try getting name from the row text
                        name = row.get_text(strip=True).split("\n")[0].strip()

                    name_parts = name.split(" ")
                    first_name = name_parts[0] if len(name_parts) > 0 else ""
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
