from bs4 import BeautifulSoup
from typing import List, Dict, Any
import logging
from ..core.interfaces import BaseDirectoryScraper

logger = logging.getLogger(__name__)


class UWaterlooDirectoryScraper(BaseDirectoryScraper):
    def __init__(self, university_id: int):
        super().__init__(university_id)

    def scrape_directory(self, url: str, faculty_id: int, department_id: int) -> List[Dict[str, Any]]:
        BASE_URL = "https://uwaterloo.ca"
        # CS department lives on a different domain
        if "cs.uwaterloo.ca" in url:
            BASE_URL = "https://cs.uwaterloo.ca"

        page = self.fetch_page(url)
        if not page:
            return []

        professors = []
        visited = set()
        links = []

        # Pattern 1: views-row divs with contact profiles or h2 headers
        if page.find_all("div", class_="views-row"):
            divs = page.find_all("div", class_="views-row")
            for div in divs:
                if div.find("div", class_="uw-contact__profile"):
                    a = div.find("div", class_="uw-contact__profile").find("a", href=True)
                    if a:
                        links.append(a)
                elif div.find("h2", class_="uw-contact__h2"):
                    a = div.find("h2", class_="uw-contact__h2").find("a")
                    if a:
                        links.append(a)

        # Pattern 2: uw-contact divs (alternate layout)
        elif page.find_all("div", class_="uw-contact"):
            divs = page.find_all("div", class_="uw-contact")
            for div in divs:
                if div.find("div", class_="uw-contact__profile"):
                    a = div.find("div", class_="uw-contact__profile").find("a", href=True)
                    if a:
                        links.append(a)
                elif div.find("div", class_="uw-contact__website"):
                    a = div.find("div", class_="uw-contact__website").find("a", href=True)
                    if a:
                        links.append(a)

        # Pattern 3: card titles
        elif page.find_all("h2", class_="card__title"):
            for h in page.find_all("h2", class_="card__title"):
                a = h.find("a", href=True)
                if a:
                    links.append(a)

        # Pattern 4: Faculty header with article/ul listing
        elif page.find("h3", string="Faculty"):
            header = page.find("h3", string="Faculty")
            article = header.find_next("article")
            if article:
                ul = article.find("ul")
                if ul:
                    for li in ul.find_all("li"):
                        a = li.find("a", href=True)
                        if a:
                            links.append(a)

        # Pattern 5: table-based listing
        elif page.find("table"):
            links = page.find("table").find_all("a", href=True)

        for link in links:
            if not link or not link.get("href"):
                continue
            href = link["href"]
            if not href.startswith("http"):
                href = BASE_URL + href
            if href in visited:
                continue
            visited.add(href)

            name = link.get_text(strip=True)
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
