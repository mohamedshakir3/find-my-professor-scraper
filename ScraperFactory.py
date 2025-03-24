from UniversityScrapers import uOttawaScraper, CarletonScraper, uWaterlooScraper
class ScraperFactory:
    def __init__(self, university, directory):
        self.university = university
        self.directory = directory
    
    def getScraper(self):
        if self.university == "University of Ottawa":
            return uOttawaScraper(self.directory)
        elif self.university == "Carleton University":
            return CarletonScraper(self.directory)
        elif self.university == "University of Waterloo":
            return uWaterlooScraper(self.directory)
        else:
            raise Exception("Invalid University")