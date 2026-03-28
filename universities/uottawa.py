from bs4 import BeautifulSoup
from typing import List, Dict, Any
import logging
from ..core.interfaces import BaseDirectoryScraper

logger = logging.getLogger(__name__)

class UOttawaDirectoryScraper(BaseDirectoryScraper):
    def __init__(self, university_id: int):
        super().__init__(university_id)

    def scrape_directory(self, url: str, faculty_id: int, department_id: int) -> List[Dict[str, Any]]:
        page = self.fetch_page(url)
        if not page:
            return []

        professors = []
        visited = set()
        p_tags = page.find_all("p")
        for p in p_tags: 
            a = p.find("a", class_="link", href=True)
            if a and a["href"].startswith("https://www.uottawa.ca"):
                link = a["href"]
                if link in visited:
                    continue
                visited.add(link)
                
                # The text inside 'a' is usually the name
                name = a.get_text(strip=True)
                if ", " in name:
                    parts = name.split(", ", 1)
                    last_name = parts[0].strip()
                    first_name = parts[1].strip()
                else:
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
