import requests
from bs4 import BeautifulSoup

url = "https://www.uottawa.ca/faculty-science/professors/rafal-kulik"
response = requests.get(url)
soup = BeautifulSoup(response.content, 'html.parser')

print("Extracting MAIN text using BS4:")
print("=" * 40)

main_content = soup.find('main')
if main_content:
    print(main_content.get_text(separator='\n', strip=True))
else:
    print("NO MAIN TAG FOUND")
print("=" * 40)
