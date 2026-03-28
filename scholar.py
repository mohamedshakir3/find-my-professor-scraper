from playwright.sync_api import sync_playwright
from bs4 import BeautifulSoup
import json
import time, random

STATE_FILE = "scholar_session.json"
PAGE_SIZE  = 10
MAX_PAGES  = 10000

def login_and_save_state():
    with sync_playwright() as pw:
        browser = pw.chromium.launch(
            headless=False,
            args=[
                '--disable-blink-features=AutomationControlled',
                '--no-sandbox',
                '--disable-web-security',
                '--disable-infobars',
                '--disable-extensions',
                '--start-maximized',
                '--window-size=1280,720'
            ]
        )
        context = browser.new_context()
        page = context.new_page()
        
        page.goto("https://scholar.google.com/citations?hl=en&view_op=search_authors")
        print("⚠️  Please complete the Google login (including any 2FA)…")
        page.wait_for_url("https://scholar.google.com/citations?hl=en&view_op=search_authors", timeout=180000)  # 3 minutes timeout
        context.storage_state(path=STATE_FILE)
        print(f"✅  Session saved to {STATE_FILE}")
        browser.close()

def fetch_with_saved_state(url: str):
    """Fetch one search-results page, given the full query string
       e.g. 'University+of+Ottawa&cstart=10&pagesize=10'"""
    with sync_playwright() as pw:
        browser = pw.chromium.launch(
            headless=True,
            args=[
                '--disable-blink-features=AutomationControlled',
                '--no-sandbox',
                '--disable-web-security',
                '--disable-infobars',
                '--disable-extensions',
                '--window-size=1280,720'
            ]
        )
        context = browser.new_context(storage_state=STATE_FILE)
        page = context.new_page()
        page.goto(url)
        page.wait_for_selector(".gs_ai_t", timeout=10000)
        html = page.content()
        browser.close()
        return html

def parse_authors(html: str):
    soup = BeautifulSoup(html, 'html.parser')
    authors = []
    
    for author in soup.select('div.gsc_1usr'):
        name_elem = author.select_one('.gs_ai_name a')
        if not name_elem:
            continue

        interests = [i.text.strip()
                     for i in author.select('.gs_ai_int a')]

        authors.append({
            'name':         name_elem.text.strip(),
            'profile_url':  'https://scholar.google.com' + name_elem['href'],
            'affiliation':  (author.select_one('.gs_ai_aff') or '').text.strip(),
            'email':        (author.select_one('.gs_ai_eml') or '').text.strip(),
            'cited_by':     (author.select_one('.gs_ai_cby') or '').text.replace('Cited by ', '').strip(),
            'interests':    interests
        })
    
    # Find next page button
    next_button = soup.select_one('.gs_btnPR')
    next_page_url = None
    if next_button and 'onclick' in next_button.attrs:
        onclick_value = next_button['onclick']
        if "window.location='" in onclick_value:
            url_part = onclick_value.split("window.location='")[1].split("'")[0]
            url_part = url_part.replace('\\x3d', '=').replace('\\x26', '&')
            next_page_url = f"https://scholar.google.com{url_part}"
    
    return authors, next_page_url

if __name__ == "__main__":
    login_and_save_state()

    all_profiles = []
    
    org_ids = {
        # "uottawa": "5757600927927532557",
        # "University of Waterloo": "12436753108180256887",
        "uoft": "8515235176732148308",
        # "carletonu": "2572996909411545503",
        # "mcmaster": "16902803553507995100",
        # "mcgill": "13784427342582529234",
        # "ubc": "13655899619131983200",
        # "udem": "4964519586676348649",
        # "ualberta": "16627554827500071773",
        # "ucalgary": "2186568608501296974",
        # "queens": "15288470216663349706",
        # "western": "4065822778065209794",
        # "concordia": "9771069054884662907"
    }
    
    
    for uni in org_ids:
        url = "https://scholar.google.com/citations?view_op=view_org&hl=en&org=" + org_ids[uni]
        all_profiles = []
        
        for page_num in range(MAX_PAGES):
            print(f"Scraping page {page_num+1}: {url}")
            html = fetch_with_saved_state(url)
            batch, next_url = parse_authors(html)
            if not batch:
                print("No profiles found. Stopping.")
                break
            print(batch)
            all_profiles.extend(batch)
            print(f"Page {page_num+1}: fetched {len(batch)} profiles (total: {len(all_profiles)})")
            
            if not next_url:
                print("No next page URL found. Stopping.")
                break
                
            url = next_url
            print(f"Next page: {url}")
            time.sleep(random.uniform(2, 5))

        # Dump to JSON
        with open(f"data-dumps/{uni}_authors.json", "w", encoding="utf-8") as f:
            json.dump(all_profiles, f, indent=2, ensure_ascii=False)

    print(f"✅  Saved {len(all_profiles)} profiles to authors.json")
