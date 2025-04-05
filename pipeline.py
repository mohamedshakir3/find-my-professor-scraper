from ScraperFactory import ScraperFactory
from Database import Database
import json
import logging

logger = logging.getLogger(__name__)



def main():
    logging.basicConfig(level=logging.INFO)
    logging.info("Starting pipeline...")
    logging.info("Loading university data")
    universities = open("universities.json")
    universities = json.load(universities)
    factory = ScraperFactory("Concordia University", universities["Concordia University"])
    scraper = factory.getScraper()
    data = scraper.run()
    with open("data-dumps/concordia.json", "w", encoding="utf-8") as f:
        json.dump(data, f, indent=4, ensure_ascii=False)
    # database = Database()
    # McGill = open("McGill.json")
    # McGill = json.load(McGill)
    # database.update_professors(McGill)
    # database.update_research_interests("McGill University")
    
    

if __name__ == "__main__":
    main()