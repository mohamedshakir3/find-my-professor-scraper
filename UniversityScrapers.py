import requests
import re
from bs4 import BeautifulSoup
from AIScraper import LLMScraper
from abc import ABC, abstractmethod
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.options import Options
import logging
import time

logger = logging.getLogger(__name__)

class Scraper(ABC):
    def __init__(self, university):
        self.university = university
        self.ai_scraper = LLMScraper()
        self.header = {"user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/88.0.4324.182 Safari/537.36"}
        self.faculty_count = 0
        self.department_count = 0
        self.prof_count = 0
        self.failed_count = 0
        self.skipped_count = 0
    
    def fetch_url(self, url):
        try:
            response = requests.get(url, headers=self.header, timeout=10)
            response.raise_for_status()
        except requests.RequestException as e:
            print(f"Failed to fetch {url}: {e}")
            return None

        return response.text
            

    def decode_email(self, protected_url):
        """Decodes Cloudflare-protected email addresses"""
        encoded = protected_url.split("#")[1]
        hex_bytes = bytes.fromhex(encoded)
        key = hex_bytes[0]
        decoded_email = ''.join(chr(b ^ key) for b in hex_bytes[1:])
        return decoded_email

    @abstractmethod
    def directory_scraper(self, url: str, faculty: str, department = ""):
        """Scrapes university directory """
        pass

    @abstractmethod
    def profile_scraper(self, url):
        """Scrapes professor profile page"""
        pass


    def run(self):
        profile_data = []
        # faculty, department = "Faculty of Science", "Department of Biology"
        # profile_data = self.directory_scraper(self.university[faculty][department], faculty, department)
        for faculty in self.university:
            logging.info(f"Processing {faculty}")
            if type(self.university[faculty]) is str:
                profile_data.extend(
                    self.directory_scraper(self.university[faculty], faculty))
            else:
                for department in self.university[faculty]:
                    logging.info(f"Processing {department}")
                    profile_data.extend(
                        self.directory_scraper(
                            self.university[faculty][department],
                            faculty, department))
        return profile_data
    

class uOttawaScraper(Scraper):
    def __init__(self, university):
        super().__init__(university)

    def directory_scraper(self, url: str, faculty: str, department = ""):
        BASE_URL = "https://www.uottawa.ca"
        # TODO: Create uniweb scraper
        response = requests.get(url)
        soup = BeautifulSoup(response.text, 'html.parser')
        
        profiles = []
        visited = set()
        p_tags = soup.find_all("p")
        for p in p_tags: 
            a = p.find("a", class_="link", href=True)
            if a and a["href"].startswith(BASE_URL):
                if a["href"] in visited:
                    continue
                research_interests, email, name = self.profile_scraper(a["href"])
                if not research_interests:
                    research_interests = self.ai_scraper.scrape(a["href"])
                if research_interests:
                    profiles.append({"name": name,
                                "university": "University of Ottawa", 
                                "faculty": faculty,
                                "department": department,
                                "website": a["href"],
                                "email": email,
                                "research_interests": research_interests})
        return profiles
    
    def profile_scraper(self, url):
        response = requests.get(url)
        soup = BeautifulSoup(response.text, 'html.parser')
        
        strong = soup.find("strong")
        name = ""
        if strong:
            name = strong.text.strip()
            name = re.sub(r'\s+', ' ', name)

        email_div = soup.find("div", class_="field field--name-field-business-card__email field--type-email field--label-inline clearfix")
        email = ""
        if email_div:
            email_link = email_div.find("a")
            if email_link:
                email = email_link["href"].split(":")[-1]
                email = self.decode_email(email_link["href"])
        
        research_heading = soup.find('h2', string="Research interests")
        research_interests = []
        if research_heading:
            container = research_heading.find_parent('section').find_next_sibling('section')
            if container:
                ul = container.find('ul')
                if ul:
                    research_interests = [li.text.strip() for li in ul.find_all('li')]
        return research_interests, email, name


class CarletonScraper(Scraper):
    def __init__(self, university):
        super().__init__(university)
        self.options = Options()
        self.options.add_argument("--headless")
    
    def directory_scraper(self, url, faculty, department=""):
        try:
            response = requests.get(url, timeout=10)
            response.raise_for_status()
        except requests.RequestException as e:
            print(f"Failed to fetch {url}: {e}")
            return []

        visited = set()
        profiles = []
        soup = BeautifulSoup(response.text, 'html.parser')
        

        if soup.find("button", class_="loadMore"):
            links = self.scrape_with_selenium(url, "load")
        elif soup.find_all("a", class_="card__link"):
            links = soup.find_all("a", class_="card__link")
        elif soup.find_all("a", class_="c-list-item--people"):
            links = soup.find_all("a", class_="c-list-item--people")
        elif soup.find_all(lambda tag:tag.name=="a" and "View Profile" in tag.text):
            links = soup.find_all(lambda tag:tag.name=="a" and "View Profile" in tag.text)
        elif soup.find_all("span", class_="person-name"):
            links = [span.find("a", href=True) for span in soup.find_all("span", class_="person-name")]
        else:
            links = []
            
            
        for link in links:
            if link and link["href"]:
                if link["href"] in visited:
                    continue
                link = link["href"]
                if "#new_tab" in link:
                    link = link.split("#")[0]
                link = link.strip()
                if department == "School of Information Technology":
                    research_interests, email, name = self.scrape_with_selenium(link, "obfuscation")
                else:
                    research_interests, email, name = self.profile_scraper(link)
                if not research_interests:
                    research_interests = self.ai_scraper.scrape(link)
                if research_interests:
                    profiles.append({"name": name,
                                "university": "Carleton University", 
                                "faculty": faculty,
                                "department": department,
                                "website": link,
                                "email": email,
                                "research_interests": research_interests})
        
        return profiles


    def profile_scraper(self, url):
        page = self.fetch_url(url)
        if not page:
            print(f"Failed to scrape {url}")
            return [], "", ""
        soup = BeautifulSoup(page, 'html.parser')
        
        interests = []
        if soup.find(lambda tag:tag.name in ["h1", "h2", "h3", "h4", "h5", "h6"] and "research" in tag.text.lower()):
            research_header = soup.find(lambda tag:tag.name in ["h1", "h2", "h3", "h4", "h5", "h6"] and "research" in tag.text.lower())
            if research_header.find_next_sibling('ul'):
                ul = research_header.find_next_sibling('ul')
                interests = [li.get_text(strip=True) for li in ul.find_all("li")]
        elif soup.find(lambda tag:tag.name=="strong" and "research interests" in tag.text.lower()):
            research_header = soup.find(lambda tag:tag.name=="strong" and "research interests" in tag.text.lower())
            ul = research_header.find_parent('p').find_next_sibling("ul")
            if ul:
                interests = [li.get_text(strip=True) for li in ul.find_all("li")]
        elif soup.find(lambda tag:tag.name=="b" and "research interests" in tag.text.lower()):
            research_header = soup.find(lambda tag:tag.name=="b" and "research interests" in tag.text.lower())
            ul = research_header.find_parent('p').find_next_sibling("ul")
            if ul:
                interests = [li.get_text(strip=True) for li in ul.find_all("li")]
        
        email = ""
        name = ""        
        
        if soup.find("div", class_="people__details"):
            people_details = soup.find("div", class_="people__details")
            email_link = people_details.find("a", href=lambda href: href and href.startswith("mailto:"))
            if email_link:
                email = email_link["href"].split(":")[-1]
            name_header = people_details.find("h2", class_="people__heading")
            if name_header:
                name = name_header.text
        elif soup.find("h1", class_="cu-prose-first-last"):
            name_header = soup.find("h1", class_="cu-prose-first-last")
            name = name_header.get_text(strip=True)
            ul = soup.find("ul", class_="cu-details")
            if ul:
                email_link = ul.find("a")
                if email_link:
                    email = email_link["href"].split(":")[-1]
        elif soup.find("h1", class_="l-post--people-title"):
            name_header = soup.find("h1", class_="l-post--people-title")
            name = name_header.get_text(strip=True)
            email_link = name_header.find_next("a")
            if email_link:
                email = email_link["href"].split(":")[-1]
        
        return interests, email, name
            
    
    def scrape_with_selenium(self, url, case):
        try:
            self.driver = webdriver.Chrome(
                service=Service(ChromeDriverManager().install()), 
                options=self.options)
            self.driver.get(url)
        except Exception as e:
            print(f"Failed to fetch {url}, failure {e}")
            return [] if case == "load" else [], "", ""
        time.sleep(1)
        if case == "load":
            while True:
                try:
                    load_more_button = self.driver.find_element(By.CLASS_NAME, "loadMore")
                    if load_more_button.get_attribute("disabled"):
                        print("No more 'Load More' buttons found.")
                        break
                
                    load_more_button.click()

                    time.sleep(1)
                except Exception as e:
                    print("No more 'Load More' buttons found.")
                    break

            page_source = self.driver.page_source
            self.driver.quit()
            soup = BeautifulSoup(page_source, 'html.parser')

            return [profile for profile in soup.find_all("a") if "View Profile" in profile.text]
        elif case == "obfuscation":
            page_source = self.driver.page_source
            soup = BeautifulSoup(page_source, 'html.parser')
            interests, email, name = [], "", ""
            if soup.find("div", class_="profile-name-desktop"):
                div = soup.find("div", class_="profile-name-desktop")
                head = div.find("h1")
                if head:
                    name = head.text
                li = soup.find("li", class_="email")
                if li:
                    span = li.find("span")
                    if span:
                        email = span.text
                if soup.find(lambda tag:tag.name in ["h1", "h2", "h3", "h4", "h5", "h6"] and "research" in tag.text.lower()):
                    research_header = soup.find(lambda tag:tag.name in ["h1", "h2", "h3", "h4", "h5", "h6"] and "research" in tag.text.lower())
                    if research_header.find_next_sibling('ul'):
                        ul = research_header.find_next_sibling('ul')
                        interests = [li.get_text(strip=True) for li in ul.find_all("li")]

            return interests, email, name


class uWaterlooScraper(Scraper):
    def is_research_header(self, text):
        return (
            ("research" in text.lower() and "interests" in text.lower()) or
            ("research" in text.lower() and "areas" in text.lower()) or
            "areas of supervision" in text.lower() or
            "areas of graduate supervision" in text.lower()
        )
    def directory_scraper(self, url, faculty, department=""):
        BASE_URL = "https://uwaterloo.ca"
        if department == "David R. Cheriton School of Computer Science":
            BASE_URL = "https://cs.uwaterloo.ca"
        page = self.fetch_url(url)
        if not page:
            print(f"Failed to scrape {url}")
            return []

        soup = BeautifulSoup(page, 'html.parser')
        profiles = []
        links = []
        if soup.find_all("div", class_="views-row"):
            divs = soup.find_all("div", class_="views-row")
            for div in divs:
                if div.find("div", class_="uw-contact__profile"):
                    links.append(div.find("div", class_="uw-contact__profile").find("a", href=True))
                elif div.find("h2", class_="uw-contact__h2"):
                    links.append(div.find("h2", class_="uw-contact__h2").find("a"))
        elif soup.find_all("div", class_="uw-contact"):
            divs = soup.find_all("div", class_="uw-contact")
            for div in divs:
                if div.find("div", class_="uw-contact__profile"):
                    links.append(div.find("div", class_="uw-contact__profile").find("a", href=True))
                elif div.find("div", class_="uw-contact__website"):
                    links.append(div.find("div", class_="uw-contact__website").find("a", href=True))
        elif soup.find_all("h2", class_="card__title"):
            links = [h.find("a", href=True) for h in soup.find_all("h2", class_="card__title")]
        elif soup.find("h3", string="Faculty"):
            header = soup.find('h3', string="Faculty")
            article = header.find_next("article")
            ul = article.find("ul")
            links = [li.find("a", href=True) for li in ul.find_all("li")]
        elif soup.find("table"):
            links = soup.find("table").find_all("a", href=True)
        
        for link in links:
            url = link["href"]
            if not url.startswith("http"):
                url = BASE_URL + url
            research_interests, name, email = self.profile_scraper(url)
            if not research_interests:
                research_interests = self.ai_scraper.scrape(url)
            if research_interests:
                profiles.append({"name": name,
                                    "university": "University of Waterloo", 
                                    "faculty": faculty,
                                    "department": department,
                                    "website": url,
                                    "email": email,
                                    "research_interests": research_interests})

        return profiles

    def profile_scraper(self, url):
        page = self.fetch_url(url)
        if not page: 
            print(f"Can't find page {url}")
            return [], "", ""
        
        soup = BeautifulSoup(page, 'html.parser')
       
        interests, name, email = [], "", ""
        headers = soup.find_all(["h2", "h3", "strong"])
        for header in headers:
            if self.is_research_header(header.text):
                next_elem = header.find_next_sibling()
                while next_elem:
                    if next_elem.name == 'ul':
                        interests = [li.text for li in next_elem.find_all("li")]
                        break
                    elif next_elem.name == 'h2' or next_elem.name == "strong":
                        break
                    elif next_elem.name == 'div':
                        if next_elem.find("ul"):
                            interests = [li.text for li in next_elem.find("ul").find_all("li")]
                    next_elem = next_elem.find_next_sibling()
                if interests:
                    break
        
        if soup.find("span", class_="field--name-title"):
            span = soup.find("span", class_="field--name-title")
            name = span.text.strip()
        elif soup.find("h1", class_="card__title"):
            h1 = soup.find("h1", class_="card__title")
            name = h1.text.strip()
        elif soup.find("h1", class_="page-title"):
            h1 = soup.find("h1", class_="page-title")
            if h1:
                name = h1.text.strip()
        elif soup.find("div", class_="boxes-box-content"):
            div = soup.find("div", class_="boxes-box-content")
            a = div.find("a")
            if a:
                name = a.text.strip()
            
        email_links = soup.find_all("a", href=True)
        for email_link in email_links:
            if "mailto" in email_link["href"]:
                email = email_link["href"].split(":")[-1]
                break
        
        return interests, name, email


class McMasterScraper(Scraper):
    def __init__(self, university):
        super().__init__(university)
        self.options = Options()
        self.options.add_argument("--headless")
        self.visited = set()
        self.names = set()

    def directory_scraper(self, url, faculty, department=""):
        page = self.fetch_url(url)
        if not page:
            logging.error(f"Failed to scrape {faculty} {department}, trying with selenium...")
            page = self.scrape_with_selenium(url)
            if not page:
                logging.error(f"Still failed lol")
                return []
        links = []
        soup = BeautifulSoup(page, 'html.parser')
        profiles = []
        
        def innerscraper(url, faculty, department):
            soup = BeautifulSoup(page, 'html.parser')
            if soup.find_all("a", class_="faculty-card__link", href=True):
                links.extend(a["href"] for a in soup.find_all("a", class_="faculty-card__link", href=True))
            if soup.find("a", class_="next page-numbers", href=True) and url not in self.visited:
                self.visited.add(url)
                next_page = soup.find("a", class_="next page-numbers") 
                innerscraper(next_page["href"], faculty, department)
        
        if soup.find_all("a", class_="faculty-card__link", href=True):
            innerscraper(url, faculty, department)
        elif soup.find_all("div", class_="modal-dialog"):
            self.visited = set()
            divs = soup.find_all("div", class_="modal-dialog")
            for div in divs:
                if div in self.visited:
                    continue
                self.visited.add(div)
                name_h3 = div.find("h3", "card-title no-line p-0 pb-2")
                if name_h3:
                    name = name_h3.text.strip()

                strong = div.find("strong")
                
                research_interests = []
                if strong:
                    p = strong.find_parent("p")
                    if p:
                        text = ''.join(str(content) for content in p.contents if content.name != "strong").strip()
                        research_interests = [
                            ri.strip() for ri in text.split(",")
                        ]
                else:
                    if div.find("div", class_="more-info-text"):
                        p = div.find("div", class_="more-info-text").find("p")
                        content = p.get_text()
                        if len(content) > 50:
                            response = self.ai_scraper.prompt(content)
                        if response:
                            research_interests = response.split(",")
                    
                a_tags = div.find_all("a", class_="dropdown-item", href=True)
                for a_tag in a_tags:
                    if "mailto" in a_tag["href"]:
                        email = a_tag["href"].split(":")[-1]
                    if "experts" in a_tag["href"]:
                        website = a_tag["href"]
                if research_interests:
                    profiles.append({"name": name,
                                    "university": "McMaster University", 
                                    "faculty": faculty,
                                    "department": department,
                                    "website": website,
                                    "email": email,
                                    "research_interests": research_interests})


        
        for link in links:
            interests, name, email = self.profile_scraper(link)
            profiles.append({"name": name,
                             "university": "McMaster University", 
                             "faculty": faculty,
                             "department": department,
                             "website": link,
                             "email": email,
                             "research_interests": interests})


        return profiles
    
    def profile_scraper(self, url):
        page = self.fetch_url(url)
        if not page:
            logging.error(f"Failed to fetch profile {url}")
            return [], "", ""

        soup = BeautifulSoup(page, 'html.parser')
        interests, name, email = [], "", ""
        if soup.find("div", class_="single-faculty__taxs-terms--expertise"):
            div = soup.find("div", class_="single-faculty__taxs-terms--expertise")
            interests = [ri.strip() for ri in div.text.strip().split(",")]
        
        if soup.find("h1", class_="faculty-hero__title-heading"):
            header = soup.find("h1", class_="faculty-hero__title-heading")
            name = header.text.strip()
        
        if soup.find("div", class_="single-faculty__contact__option-content"):
            div = soup.find("div", class_="single-faculty__contact__option-content")
            if div.find("a", href=True):
                email = div.find("a", href=True)["href"].split(":")[-1]
            elif div.find("p"):
                email = div.find("p").text.strip()
        
        
        return interests, name, email
        
    def scrape_with_selenium(self, url):
        try:
            self.driver = webdriver.Chrome(
                service=Service(ChromeDriverManager().install()), 
                options=self.options)
            self.driver.get(url)
        except Exception as e:
            print(f"Failed to fetch {url}, failure {e}")
            return []
        time.sleep(1)
        page_source = self.driver.page_source
        self.driver.quit()
        return page_source

class uoftScraper(Scraper):
    def directory_scraper(self, url, faculty, department=""):
        return super().directory_scraper(url, faculty, department)
    def profile_scraper(self, url):
        return super().profile_scraper(url)