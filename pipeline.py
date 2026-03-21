from ScraperFactory import ScraperFactory
from Database import Database
import json
import logging

logger = logging.getLogger(__name__)



def main():
    logging.basicConfig(level=logging.INFO)
    logging.info("Starting pipeline...")
    logging.info("Loading university data")
    # universities = open("universities.json")
    # universities = json.load(universities)
    # factory = ScraperFactory("University of Ottawa", 
    #                          universities["University of Ottawa"])
    # scraper = factory.getScraper()
    # data = scraper.run()
    # with open("data-dumps/uottawa.json", "w", encoding="utf-8") as f:
    #     json.dump(data, f, indent=4, ensure_ascii=False)

    # for university in universities:
    #     factory = ScraperFactory(university, universities[university])
    #     scraper = factory.getScraper()
    #     data = scraper.run()
    #     with open("data-dumps/all_unis.json", "w", encoding="utf-8") as f:
    #         json.dump(data, f, indent=4, ensure_ascii=False)
    database = Database()
    database.update_google_scholar()
    # queens = open("data-dumps/all_uni's.json")
    # queens = json.load(queens)
    # database.update_professors(queens)
    # database.update_research_interests("Queens University")
    
    # western = open("data-dumps/western.json")
    # western = json.load(western)
    # database.update_professors(western)
    # database.update_research_interests("Western University")
    
    # concordia = open("data-dumps/concordia.json")
    # concordia = json.load(concordia)
    # database.update_professors(concordia)
    # database.update_research_interests("Concordia University")
    
    

if __name__ == "__main__":
    main()