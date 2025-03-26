import requests
import re
from bs4 import BeautifulSoup
from AIScraper import LLMScraper
from abc import ABC, abstractmethod
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
import logging
from typing import overload
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
    def profile_scraper(self, url, department=None):
        """Scrapes professor profile page"""
        pass


    def run(self):
        profile_data = []
        faculty, department = "Faculty of Engineering", "Department of Bioengineering"
        profile_data = self.directory_scraper(self.university[faculty][department], faculty, department)
        # for faculty in self.university:
        #     logging.info(f"Processing {faculty}")
        #     if type(self.university[faculty]) is str:
        #         profile_data.extend(
        #             self.directory_scraper(self.university[faculty], faculty))
        #     else:
        #         for department in self.university[faculty]:
        #             logging.info(f"Processing {department}")
        #             profile_data.extend(
        #                 self.directory_scraper(
        #                     self.university[faculty][department],
        #                     faculty, department))
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
            logging.error(f"Failed to scrape {faculty} {department}")
            return []
        links = []
        soup = BeautifulSoup(page, 'html.parser')
        profiles = []
        def innerscraper(url, faculty, department):
            page = self.fetch_url(url)
            if not page:
                print(f"Failed to load {url}")
            soup = BeautifulSoup(page, 'html.parser')
            if soup.find_all("a", class_="faculty-card__link"):
                links.extend(a["href"] for a in soup.find_all("a", class_="faculty-card__link"))
            if soup.find("a", class_="next page-numbers", href=True) and url not in self.visited:
                self.visited.add(url)
                next_page = soup.find("a", class_="next page-numbers") 
                innerscraper(next_page["href"], faculty, department)


        if soup.find_all("a", class_="faculty-card__link"):
            innerscraper(url, faculty, department)

        for link in links:
            if link in self.visited:
                continue
            self.visited.add(link)
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
    
    def __init__(self, university):
        super().__init__(university)
        self.options = Options()
        self.options.add_argument("--headless")
        self.base_url_map = {
            "Department of Chemical Engineering and Applied Chemistry": "https://chem-eng.utoronto.ca",
            "Department of Mathematics": "http://mathematics.utoronto.ca"
        }
        self.driver = webdriver.Chrome(
            service=Service(ChromeDriverManager().install()), 
            options=self.options)
    
    def is_research_header(self, header):
        return (
            ("research" in header and "interests" in header) or
            header == "research" or header == "research:" or
            "research in" in header or
            ("research" in header and "areas" in header)
        )

    def prefer_research_interests(self, header):
        return (
            "research" in header and "interests" in header
        )
    
    def get_next_page(self, url):
        count = 1
        try:
            self.driver.get(url)
            wait = WebDriverWait(self.driver, 10)
            while True:
                logging.info(f"Found page number {count}")
                count += 1
                yield self.driver.page_source

                try:
                    next_button = wait.until(
                        EC.element_to_be_clickable((By.CLASS_NAME, "next"))
                    )
                    next_button.click()

                    time.sleep(2)
                except Exception:
                    print("No more pages.")
                    break
        finally:
            self.driver.quit()
        
        

    def directory_scraper(self, url, faculty, department=""):
        page = self.fetch_url(url)
        if not page:
            logging.error(f"Failed to fetch {url}")
            return []
        soup = BeautifulSoup(page, 'html.parser')
        
        links = []
        profiles = []
        if department == "Department of Chemical Engineering and Applied Chemistry":
            divs = soup.find_all("div", class_="fl-post-feed-post")
            for div in divs:
                content = div.find("div", class_="fl-post-feed-content")
                a_tags = content.find_all("a")
                if a_tags:
                    for a in a_tags:
                        if ((a["href"].startswith(self.base_url_map[department])
                            or "faculty" in a["href"]) and "jpg" not in a["href"]):
                                links.append(a["href"])
                                break
        elif department == "Department of Civil and Mineral Engineering":
            table = soup.find("table")
            rows = table.find_all("tr")
            for row in rows:
                col2 = row.find("td", class_="column-2")
                if not col2 or not "professor" in col2.get_text(strip=True).lower():
                    continue
                col1 = row.find("td", class_="column-1")
                if not col1:
                    continue
                a = col1.find("a", href=True)
                if a:
                    links.append(a["href"])
        
        elif department == "Department of Materials Science and Engineering":
            divs = soup.find_all("div", class_="fl-col")
            for div in divs:
                a = div.find("a", href=True)
                if a:
                    links.append(a["href"])
        elif department == "Department of Mechanical and Industrial Engineering":
            links = [a["href"] for a in soup.find_all("a", class_="pp-post-link", href=True)]
        elif department == "The Edward S. Rogers Sr. Department of Electrical and Computer Engineering":
            page_generator = self.get_next_page(url)
            for page_source in page_generator:
                soup = BeautifulSoup(page_source, 'html.parser')
                spans = soup.find_all("span")
                div = None
                for span in spans:
                    if span.text == "Faculty Directory":
                        div = span.find_next("div", class_="fl-module-pp-content-grid")
                        break
                if not div:
                    logging.error("Did not find faculty directory")
                    return []
                links.extend(
                    [a["href"] for a in div.find_all("a", class_="pp-post-link", href=True)]
                )
                if not page:
                    logging.error(f"Failed to find next page {a['href']}")
                    return []
        elif department == "Department of Computer Science":
            research_header = soup.find("h2", id="researchstream")
            if not research_header:
                logging.error("Failed to find research table")
                return []
            table = research_header.find_next("table")
            if not table:
                logging.error("Failed to find research table")
                return []
            rows = table.find_all("tr")
            for tr in rows:
                research_interests, name, email = [], "", ""
                tds = tr.find_all("td")
                if not tds:
                    continue
                prof = tds[0]
                link = prof.find("a")
                if link:
                    name = link.text.strip()
                    link = link["href"]
                email = tds[1]
                if email.find("a", href=True):
                    email = email.find("a", href=True)["href"].split(":")[-1]

                td_text = tds[2].get_text(separator=" ", strip=True)
                areas_match = re.search(r'Research Areas:(.*?)(Research Interests:|$)', td_text, re.IGNORECASE)
                interests_match = re.search(r'Research Interests:(.*)', td_text, re.IGNORECASE)
                delims = r",|;|\band\b"

                if areas_match:
                    areas = areas_match.group(1).strip()
                    research_interests.extend([area.strip() for area in re.split(delims, areas) if area.strip()] )

                if interests_match:
                    interests = interests_match.group(1).strip()
                    research_interests.extend([i.strip() for i in re.split(delims, interests) if i.strip()])

                if name and email and research_interests:
                    profiles.append({"name": name,
                                    "university": "University of Toronto", 
                                    "faculty": faculty,
                                    "department": department,
                                    "website": link,
                                    "email": email,
                                    "research_interests": research_interests})

        elif (department == "Department of Mathematics" or
              department == "Department of Chemistry"):     
            divs = soup.find_all("div", class_="pane-node-people-uoft-people-title")
            if not divs:
                logging.error("Failed to find professors.")
                return []

            links = [div.find("a", href=True)["href"] for div in divs]
                    
        print(len(links))
        for link in links:
            if not link.startswith("http"):
                link = f"{self.base_url_map[department]}{link}"
            interests, name, email = self.profile_scraper(link, department)
            if interests and name and email:
                profiles.append({"name": name,
                                "university": "University of Toronto", 
                                "faculty": faculty,
                                "department": department,
                                "website": link,
                                "email": email,
                                "research_interests": interests})
        return profiles

    def profile_scraper(self, url, department):
        page = self.fetch_url(url)
        
        if not page:
            logging.error(f"Failed to fetch {url}")
            return [], "", ""

        soup = BeautifulSoup(page, 'html.parser')
        
        research_interests, email, name = [], "", ""
        
        if (department == "Department of Chemical Engineering and Applied Chemistry" or
            department == "Department of Materials Science and Engineering" or
            department == "Department of Mechanical and Industrial Engineering" or
            department == "The Edward S. Rogers Sr. Department of Electrical and Computer Engineering"):
            entry_header = soup.find("section", class_="entry-header")
            if entry_header:
                h1 = entry_header.find("h1")
                if h1:                                            
                    name = h1.text.strip()
            scrape_with_ai = True

            research_header = None
            headers = soup.find_all("h3")
            headers.extend(soup.find_all("h2"))
            headers.extend(soup.find_all("h4"))
            headers.extend(soup.find_all("h5"))
            
            ps = soup.find_all("p")
            
            for p in ps:
                if "E:" in p.text:
                    email = p.text.split(":")[-1].strip()
                    break
            
            for header in headers:
                if self.prefer_research_interests(header.get_text().lower()):
                    research_header = header
                    break
                elif self.is_research_header(header.get_text().lower()):
                    research_header = header
            else:
                for header in headers:
                    if "bio" in header.text.lower():
                        p = header.find_next_sibling("p")
                        if p:
                            research_interests = [p.text]
                        if not p:
                            div = header.find_next_sibling("div")
                            if div:
                                research_interests = [div.get_text()]

            if not research_header:
                strongs = soup.find_all("strong")
                for strong in strongs:
                    if self.is_research_header(strong.get_text().lower()):
                        research_header = strong
            if not research_header:
                panels = soup.find_all("div", class_="pp-tabs-panel")
                if panels:
                    for panel in panels:
                        title = panel.find("div", class_="pp-tab-title")
                        if title and "biography" in title.text.strip().lower():
                            ps = panel.find_all("p")
                            if ps:
                                for p in ps:
                                    text = p.get_text().strip()
                                    if len(text) > 0:
                                        research_interests.append(text)                            

            if not research_header and not research_interests:
                logging.info(f"Found no research header for {url}")
                return [], "", ""
            if not research_interests:
                if research_header.find_next_sibling("ul"):
                    ul = research_header.find_next_sibling("ul")
                    research_interests = [li.text.strip() for li in ul.find_all("li")]
                    scrape_with_ai = False
                else:
                    if research_header.name == "strong":
                        curr = research_header = research_header.find_parent()
                    curr = research_header.find_next_sibling()
                    next_header = ["h3", "h2"] if research_header.name == "h3" else ["strong"]
                    if not curr:
                        curr = research_header.find_parent().find_parent()
                        if curr:
                            curr = curr.find_next_sibling()
                            if curr:
                                curr = curr.find("p")   
                    flag = False
                    while curr and curr.name not in next_header:
                        if curr.name == "p" or curr.name == "div":
                            for head in next_header:
                                if curr.find(head):
                                    flag = True
                            if flag: break
                            text = curr.get_text(strip=True)
                            if len(text) > 0:
                                research_interests.append(text)
                        elif curr.name == "ol":
                            for li in curr.find_all("li"):
                                text = li.get_text(strip=True)
                                if len(text) > 0:
                                    research_interests.append(text)

                        curr = curr.find_next_sibling()
                if len(research_interests) == 1:
                    research_interests = [ri.strip() for ri in research_interests[0].split(";")]
                    if len(research_interests) == 1:
                        research_interests = [ri.strip() for ri in research_interests[0].split(",")]
            if scrape_with_ai:
                logging.info("Scraping interests using ai")
                temp_interests = []
                for research_interest in research_interests:
                    temp_interests.extend(self.ai_scraper.prompt(research_interest))
                
                research_interests = temp_interests
            
        elif department == "Department of Civil and Mineral Engineering":
            name_header = soup.find("div", class_="fl-rich-text")
            if name_header:
                while name_header and not name_header.find("h2"):
                    name_header = name_header.find_next("div", class_="fl-rich-text")
                name_header = name_header.find("h2")
                if name_header:
                    name = name_header.get_text(strip=True)
                    if not name:
                        print(name_header)
                        print(f"no name for {url}")
                else:
                    print(f"no name header for {url}")

            tabs = soup.find_all("div", class_="fl-tabs-panel")
            if not tabs:
                logging.error(f"Failed to find tabs for {url}")
                return [], "", ""
            for tab in tabs:
                label = tab.find("div", class_="fl-tabs-label")
                if not label:
                    continue
                text = label.get_text(strip=True).lower()
                if self.is_research_header(text):
                    div = label.find_next_sibling("div")
                    if div:
                        ul = div.find("ul")
                        if ul:
                            research_interests = [
                                li.text.strip() for li in ul.find_all("li")]
                        else:
                            ps = div.find_all("p")
                            if ps:
                                for p in ps:
                                    research_interests.append(p.text.strip())
                            if len(research_interests) == 1:
                                logging.info("Scraping info using AI...")
                                temp_interests = []
                                for ri in research_interests:
                                    temp_interests.extend(self.ai_scraper.prompt(ri))
                                research_interests = temp_interests            
        elif department == "Department of Mathematics":
            h1 = soup.find("h1", class_="page-header")
            if not h1:
                logging.error(f"Failed to find name for {url}")            
                return [], "", ""
            name = h1.text.strip()
            headers = soup.find_all("h4", class_="pane-title")
            if not headers:
                logging.error(f"Failed to find research interests for {url}")
                return [], "", ""
            research_header = None
            for header in headers:
                if "areas of interest" in header.text.lower():
                    research_header = header
                    break
            if not research_header:
                logging.error(f"Failed to find research interests for {url}")
                return [], "", ""
            div = research_header.find_next("div", class_="pane-content")
            if not div:
                logging.error(f"Failed to find research interests for {url}")
                return [], "", ""
            ul = div.find("ul")
            if not ul:
                logging.error(f"Failed to find research interests for {url}")
                return [], "", ""
            research_interests = [li.text.strip() for li in ul.find_all("li")]
            
        a_tags = soup.find_all("a", href=True)
        if not email:
            for a in a_tags:
                if "mailto" in a["href"]:
                    email = a["href"].split(":")[-1]
                    break
                
        return research_interests, name, email

class McGillScraper(Scraper):
    def __init__(self, university):
        super().__init__(university)
        self.base_url_mapping = {
            "Department of Bioengineering": "https://www.mcgill.ca/bioengineering"
        }
    
    def directory_scraper(self, url, faculty, department=""):
        page = self.fetch_url(url)
        if not page:
            logging.error(f"Failed to fetch {url}")
            return []
        profiles = []
        links = []
        
        soup = BeautifulSoup(page, 'html.parser')
        
        if department == "Department of Bioengineering":
            headers = soup.find_all("h2", class_="mcgill-profiles-display-name")
            if not headers:
                logging.error(f"Failed to scrape {department}")
                return []
            links = [header.find("a", href=True)["href"] for header in headers]
        
        for link in links:
            print(link)
        
        
        
        return profiles
    
    def profile_scraper(self, url, department=None):
        return super().profile_scraper(url, department)