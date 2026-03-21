import sys
import logging
from pathlib import Path

# Add the parent directory so we can import scraper as a package
sys.path.append(str(Path(__file__).parent.parent))

from scraper.universities.uottawa import UOttawaDirectoryScraper

logging.basicConfig(level=logging.INFO)

def test_uottawa_scraper():
    print("Testing UOttawa Directory Scraper (Phase 1)...")
    
    # Mock IDs
    university_id = 1
    faculty_id = 1
    department_id = 1
    
    scraper = UOttawaDirectoryScraper(university_id)
    
    # Test Biology department
    test_url = "https://www.uottawa.ca/faculty-science/biology/professor-directory"
    
    print(f"Scraping {test_url}")
    results = scraper.scrape_directory(test_url, faculty_id, department_id)
    
    print(f"\\nFound {len(results)} professors!")
    if results:
        print("First 3 results:")
        for r in results[:3]:
            print(f"  - {r['first_name']} {r['last_name']}: {r['profile_url']}")

if __name__ == "__main__":
    test_uottawa_scraper()
