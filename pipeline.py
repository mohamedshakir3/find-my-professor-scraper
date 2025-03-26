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
    factory = ScraperFactory("McGill University", universities["McGill University"])
    scraper = factory.getScraper()
    data = scraper.run()
    with open("mcgill.json", "w", encoding="utf-8") as f:
        json.dump(data, f, indent=4, ensure_ascii=False)
    # database = Database()
    # uoft = open("uoft_clean.json")
    # uoft = json.load(uoft)
    # database.update_professors(uoft)
    # database.update_research_interests("University of Toronto")
    # mcmaster = open("mcmaster.json")
    # mcmaster = json.load(mcmaster)
    # database.update_professors(mcmaster)
    # database.update_research_interests("McMaster University")
    # factory = ScraperFactory("McMaster University", universities["University of Waterloo"])
    
    
    

if __name__ == "__main__":
    main()