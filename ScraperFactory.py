from UniversityScrapers import uOttawaScraper, CarletonScraper, uWaterlooScraper
from UniversityScrapers import McMasterScraper, uoftScraper, McGillScraper
from UniversityScrapers import UBCScraper, UdemScraper, uAlbertaScraper
from UniversityScrapers import uCalgaryScraper, QueensScraper, WesternScraper
from UniversityScrapers import ConcordiaScraper
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
        elif self.university == "University of Montreal":
            logging.info("Initializing UdeM scraper.")
            return UdemScraper(self.directory)
        elif self.university == "University of Alberta":
            logging.info("Initializing uAlberta scraper.")
            return uAlbertaScraper(self.directory)
        elif self.university == "University of Calgary":
            logging.info("Initializing uCalgary scraper")
            return uCalgaryScraper(self.directory)
        elif self.university == "Queens University":
            logging.info("Initializing Queens scraper.")
            return QueensScraper(self.directory)
        elif self.university == "Western University":
            logging.info("Initializing Western scraper.")
            return WesternScraper(self.directory)
        elif self.university == "Concordia University":
            logging.info("Initializing Concordia scraper.")
            return ConcordiaScraper(self.directory)
        else:
            raise Exception("Invalid University")