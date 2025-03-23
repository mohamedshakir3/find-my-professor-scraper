from ScraperFactory import ScraperFactory
from UpdateDB import DatabaseUploader
import json

def main():
    # universities = open("universities.json")
    # universities = json.load(universities)
    # factory = ScraperFactory("Carleton University", universities["Carleton University"])
    # scraper = factory.getScraper()
    # data = scraper.run()
    # with open("carleton_final.json", "w", encoding="utf-8") as f:
    #     json.dump(data, f, indent=4, ensure_ascii=False)
    carleton = open("carleton_final.json")
    carleton = json.load(carleton)
    uottawa = open("uottawa.json")
    uottawa = json.load(uottawa)
    database = DatabaseUploader()
    database.update_research_interests()
    # database.update_professors(uottawa)
    # database.update_professors(carleton)
    
    

if __name__ == "__main__":
    main()