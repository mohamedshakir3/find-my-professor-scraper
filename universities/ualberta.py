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


class UAlbertaDirectoryScraper(BaseDirectoryScraper):
    """
    University of Alberta directory scraper.
    Uses Selenium to paginate through Coveo search results.
    Result cards use div.CoveoResult with a.CoveoResultLink for name+link.
    """
    def __init__(self, university_id: int):
        super().__init__(university_id)
        self.options = Options()
        self.options.add_argument("--headless=new")
        self.options.add_argument("--no-sandbox")
        self.options.add_argument("--disable-dev-shm-usage")
        self.options.add_argument("--window-size=1920,1080")
        self.options.add_argument("user-agent=Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")

    def scrape_directory(self, url: str, faculty_id: int, department_id: int) -> List[Dict[str, Any]]:
        driver = webdriver.Chrome(
            service=Service(ChromeDriverManager().install()),
            options=self.options)

        professors = []
        visited = set()

        try:
            driver.get(url)
            wait = WebDriverWait(driver, 20)

            # Wait explicitly for Coveo results to appear
            try:
                wait.until(EC.presence_of_element_located(
                    (By.CSS_SELECTOR, "a.CoveoResultLink")))
                logger.info("Coveo results loaded successfully")
            except Exception:
                logger.warning("Timed out waiting for CoveoResultLink — trying longer wait")
                time.sleep(10)

            page_num = 0
            max_pages = 100  # Safety limit

            while page_num < max_pages:
                page_num += 1
                source = driver.page_source
                soup = BeautifulSoup(source, 'html.parser')

                # Find result cards — try multiple selectors
                results = soup.find_all("div", class_="CoveoResult")
                logger.info(f"Page source length: {len(source)}, CoveoResult divs: {len(results)}")
                if not results:
                    results = soup.find_all("div", class_="coveo-list-layout")
                if not results:
                    # Try generic result container
                    results = soup.find_all("div", class_=lambda c: c and "result" in c.lower() and "coveo" in c.lower())

                found_new = False
                for result in results:
                    link_tag = None
                    name = ""

                    # Look for CoveoResultLink
                    for a in result.find_all("a", href=True):
                        classes = a.get("class", [])
                        if any("CoveoResultLink" in c for c in classes):
                            link_tag = a
                            name = a.get_text(strip=True)
                            break

                    if not link_tag:
                        # Fallback: first link with a person-like URL
                        for a in result.find_all("a", href=True):
                            href = a["href"]
                            if "directory" in href or "person" in href or "profile" in href:
                                link_tag = a
                                name = a.get_text(strip=True)
                                break

                    if not link_tag or not name:
                        continue

                    link = link_tag["href"]
                    if not link.startswith("http"):
                        link = f"https://www.ualberta.ca{link}"

                    if link in visited:
                        continue

                    visited.add(link)
                    found_new = True

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

                logger.info(f"UAlberta page {page_num}: found {len(results)} results, {len(professors)} total profs")

                if not found_new:
                    logger.info("No new results found, stopping pagination")
                    break

                # Try clicking next page
                try:
                    # Get reference to first result to detect page change
                    first_result = driver.find_element(By.CSS_SELECTOR, "a.CoveoResultLink")
                    
                    next_button = driver.find_element(
                        By.CSS_SELECTOR,
                        "span[title='Next'], "
                        "li.coveo-pager-next-icon, "
                        ".coveo-pager-next .coveo-accessible-button"
                    )
                    driver.execute_script("arguments[0].scrollIntoView(true);", next_button)
                    time.sleep(0.5)
                    driver.execute_script("arguments[0].click();", next_button)
                    
                    # Wait for the old results to become stale (new page loads)
                    try:
                        WebDriverWait(driver, 10).until(
                            EC.staleness_of(first_result))
                        logger.info("Page results changed after clicking next")
                    except Exception:
                        logger.info("Results didn't change — might be last page")
                    time.sleep(2)  # Extra wait for new results to render
                    
                except Exception as e:
                    logger.info(f"No more pages to navigate: {e}")
                    break

        except Exception as e:
            logger.error(f"UAlberta scraper error: {e}")
        finally:
            driver.quit()

        return professors
