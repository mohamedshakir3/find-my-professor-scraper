from UniversityScrapers import uOttawaScraper, CarletonScraper, uWaterlooScraper, McMasterScraper, uoftScraper, McGillScraper, UBCScraper
import logging

logger = logging.getLogger(__name__)


class ScraperFactory:
    def __init__(self, university, directory):
        self.university = university
        self.directory = directory
    
    def getScraper(self):
        if self.university == "University of Ottawa":
            logging.info("Initializing uOttawa scraper.")
            return uOttawaScraper(self.directory)
        elif self.university == "Carleton University":
            logging.info("Initializing Carleton scraper.")
            return CarletonScraper(self.directory)
        elif self.university == "University of Waterloo":
            logging.info("Initializing uWaterloo scraper.")
            return uWaterlooScraper(self.directory)
        elif self.university == "McMaster University":
            logging.info("Initializing McMaster scraper.")
            return McMasterScraper(self.directory)
        elif self.university == "University of Toronto":
            logging.info("Intializing uoft scraper.")
            return uoftScraper(self.directory)
        elif self.university == "McGill University":
            logging.info("Intializing McGill scraper.")
            return McGillScraper(self.directory)
        elif self.university == "University of British Columbia":
            logging.info("Intializing UBC scraper.")
            return UBCScraper(self.directory)
        
        else:
            raise Exception("Invalid University")