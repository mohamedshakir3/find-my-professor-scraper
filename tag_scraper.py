import requests
from bs4 import BeautifulSoup
from typing import List
import json
import re
from AIScraper import LLMScraper
def carleton_scraper(url: str):
    """Scrapes profile links from different faculty card formats on the page."""
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
    except requests.RequestException as e:
        print(f"Failed to fetch {url}: {e}")
        return []

    soup = BeautifulSoup(response.text, 'html.parser')
    links = []

    cards1 = soup.find_all("div", class_="card card--no-underline card--people-list")
    links.extend(
        [a["href"] for card in cards1 if (a := card.find("a", class_="card__link"))]
    )

    cards2 = soup.find_all("div", class_="cu-card")
    for card in cards2:
        a_tag = card.find("a", class_="cu-button")
        if a_tag and "href" in a_tag.attrs:
            links.append(a_tag["href"])
        else:
            footer = card.find("footer")
            if footer:
                a_tag = footer.find("a", class_="cu-button")
                if a_tag and "href" in a_tag.attrs:
                    links.append(a_tag["href"])

    cards3 = soup.find_all("div", class_="c-list-item--people")
    links.extend(
        [a["href"] for card in cards3 if (a := card.find("a", class_="c-list-item--people"))]
    )
    
    cards4 = soup.find_all("div", class_="person")
    links.extend(
        [a["href"] for card in cards4 if (a := card.find("a")) and a.has_attr("href")]
    )

    cards5 = soup.find_all("article")
    links.extend(
        [a["href"] for card in cards5 if (a := card.find("a")) and a.has_attr("href")]
    )
    
    return links


def ottawa_scraper(url: str, faculty: str, department = ""):
    BASE_URL = "https://www.uottawa.ca"
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
    except requests.RequestException as e:
        print(f"Failed to fetch {url}: {e}")
        return []
    soup = BeautifulSoup(response.text, 'html.parser')
    
    links = []
    visited = set()
    p_tags = soup.find_all("p")
    for p in p_tags: 
        a = p.find("a", class_="link", href=True)
        if a and a["href"].startswith(BASE_URL):
            if a["href"] in visited:
                continue
            visited.add(a["href"])
            research_interests, email, name, bio = uottawa_scrape_profile(a["href"])
            if not research_interests:
                print(f"No research interests found for {name} at {a["href"]}")
                if bio:
                    research_interests = ai_scraper.scrape(a["href"])
                    print(research_interests)
                else:
                    print("No bio found either.. skipping")
            if research_interests:
                links.append({"name": name,
                              "university": "University of Ottawa", 
                              "faculty": faculty,
                              "department": department,
                              "website": a["href"],
                              "email": email,
                              "research_interests": research_interests})

    return links
def uottawa_scrape_profile(url):
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
    except requests.RequestException as e:
        print(f"Failed to fetch {url}: {e}")
        return []
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
            email = decode_email(email_link["href"])

    body_content = soup.find('div', class_="uottawa-default--title")
    bio = False
    if body_content and body_content.get_text(strip=True):
        bio = True
    
    research_heading = soup.find('h2', string="Research interests")
    research_interests = []
    if research_heading:
        container = research_heading.find_parent('section').find_next_sibling('section')
        if container:
            ul = container.find('ul')
            if ul:
                research_interests = [li.text.strip() for li in ul.find_all('li')]
    return research_interests, email, name, bio

def scrape_multiple_pages(urls: List[str]) -> List[str]:
    all_links = []
    for url in urls:
        links = carleton_scraper(url)
        print(f"Found {len(links)} links in {url}")
        all_links.extend(links)
    
    return all_links

def decode_email(protected_url):
    """Decodes Cloudflare-protected email addresses"""
    encoded = protected_url.split("#")[1]
    hex_bytes = bytes.fromhex(encoded)
    key = hex_bytes[0]
    decoded_email = ''.join(chr(b ^ key) for b in hex_bytes[1:])
    return decoded_email

if __name__ == "__main__": 
    file = open("universities.json")
    university_data = json.load(file)
    uottawa = university_data["University of Ottawa"]
    links = []
    ai_scraper = LLMScraper()
    for faculty in uottawa:
        print(f"Processing {faculty}")
        if type(uottawa[faculty]) is str:
            links.extend(ottawa_scraper(uottawa[faculty], faculty))
        else:
            for department in uottawa[faculty]:
                print(f"Processing {department}")
                links.extend(ottawa_scraper(uottawa[faculty][department],
                                            faculty, department))
    
    with open("uottawa.json", "w", encoding="utf-8") as f:
        json.dump(links, f, indent=4, ensure_ascii=False)
