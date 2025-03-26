from ScraperFactory import ScraperFactory
from Database import Database
import json
import logging

logger = logging.getLogger(__name__)


def main():
    logging.info("Starting pipeline...")
    logging.info("Loading university data")
    # universities = open("universities.json")
    # universities = json.load(universities)
    # factory = ScraperFactory("McMaster University", universities["McMaster University"])
    # scraper = factory.getScraper()
    # data = scraper.run()
    # with open("mcmaster.json", "w", encoding="utf-8") as f:
    #     json.dump(data, f, indent=4, ensure_ascii=False)
    database = Database()
    mcmaster = open("mcmaster.json")
    mcmaster = json.load(mcmaster)
    database.update_professors(mcmaster)
    database.update_research_interests("McMaster University")
    # factory = ScraperFactory("McMaster University", universities["University of Waterloo"])
    
    
    

if __name__ == "__main__":
    main()