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


class UCalgaryDirectoryScraper(BaseDirectoryScraper):
    """
    University of Calgary (Schulich School of Engineering) directory scraper.
    Uses Selenium to paginate through faculty member listings.
    Pages use pager__item--next for navigation.
    """
    def __init__(self, university_id: int):
        super().__init__(university_id)
        self.options = Options()
        self.options.add_argument("--headless")

    def scrape_directory(self, url: str, faculty_id: int, department_id: int) -> List[Dict[str, Any]]:
        driver = webdriver.Chrome(
            service=Service(ChromeDriverManager().install()),
            options=self.options)

        professors = []
        visited = set()

        try:
            driver.get(url)
            wait = WebDriverWait(driver, 60)  # UCalgary can be slow
            time.sleep(3)

            while True:
                current_source = driver.page_source
                soup = BeautifulSoup(current_source, 'html.parser')

                ol = soup.find("ol", class_="profile-items-list")
                if not ol:
                    logger.warning(f"No profile-items-list found at {url}")
                    break

                for li in ol.find_all("li", class_="profile"):
                    a = li.find("a", href=True)
                    if not a:
                        continue

                    link = a["href"]
                    name = a.text.strip()

                    if link in visited:
                        continue
                    visited.add(link)

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

                # Try to click the "next page" link
                try:
                    next_li = wait.until(
                        EC.presence_of_element_located(
                            (By.CSS_SELECTOR, "li.pager__item--next")))

                    li_classes = next_li.get_attribute("class")
                    if "disabled" in li_classes:
                        break

                    next_link = next_li.find_element(By.TAG_NAME, "a")
                    driver.execute_script(
                        "arguments[0].scrollIntoView(true);", next_link)
                    driver.execute_script(
                        "arguments[0].click();", next_link)

                    # Wait for page content to change
                    wait.until(lambda d: d.page_source != current_source)
                    time.sleep(1)
                except Exception:
                    break
        finally:
            driver.quit()

        return professors
