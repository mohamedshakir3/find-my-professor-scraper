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

import time

class Scraper(ABC):
    def __init__(self, university):
        self.university = university
        self.ai_scraper = LLMScraper()
        self.faculty_count = 0
        self.department_count = 0
        self.prof_count = 0
        self.failed_count = 0
        self.skipped_count = 0
    
    def fetch_url(self, url):
        try:
            response = requests.get(url, timeout=10)
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
        # faculty, department = "Faculty of Math", "David R. Cheriton School of Computer Science"
        # self.directory_scraper(self.university[faculty][department], faculty, department)
        for faculty in self.university:
            print(f"Processing {faculty}")
            if type(self.university[faculty]) is str:
                profile_data.extend(
                    self.directory_scraper(self.university[faculty], faculty))
            else:
                for department in self.university[faculty]:
                    print(f"Processing {department}")
                    profile_data.extend(
                        self.directory_scraper(
                            self.university[faculty][department],
                            faculty, department))
            break
        
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
            "research interests" in text.lower() or 
            "research areas" in text.lower() or
            "areas of supervision" in text.lower() or
            "areas of graduate supervision" in text.lower()
        )
    def directory_scraper(self, url, faculty, department=""):
        BASE_URL = "https://uwaterloo.ca"
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
                elif div.find("div", class_="uw-contact__website"):
                    links.append(div.find("div", class_="uw-contact__website").find("a", href=True))
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
            if not url.startswith("https://"):
                url = BASE_URL + url
            research_interests, name, email = self.profile_scraper(url)
            print(name, email)
            
        
        return []

    def profile_scraper(self, url):
        page = self.fetch_url(url)
        if not page: 
            print(f"Can't find page {url}")
            return [], "", ""
        
        soup = BeautifulSoup(page, 'html.parser')
       
        interests, name, email = [], "", ""
        headers = soup.find_all("h2")
        res = []
        for header in headers:
            if self.is_research_header(header.text):
                next_elem = header.find_next_sibling()
                while next_elem:
                    if next_elem.name == 'ul':
                        interests = [li.text for li in next_elem.find_all("li")]
                    elif next_elem.name == 'h2' or next_elem.name == "strong":
                        break
                    next_elem = next_elem.find_next_sibling()
        
        if soup.find("span", class_="field--name-title"):
            span = soup.find("span", class_="field--name-title")
            name = span.text.strip()
            
        email_links = soup.find_all("a", href=True)
        for email_link in email_links:
            if "mailto" in email_link["href"]:
                email = email_link["href"]
                break
        
        return interests, name, email
            

            
        
