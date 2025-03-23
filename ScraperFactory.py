from UniversityScrapers import uOttawaScraper, CarletonScraper
class ScraperFactory:
    def __init__(self, university, directory):
        self.university = university
        self.directory = directory
    
    def getScraper(self):
        if (self.university == "University of Ottawa"):
            return uOttawaScraper(self.directory)
        elif (self.university == "Carleton University"):
            return CarletonScraper(self.directory)
        else:
            raise Exception("Invalid University")