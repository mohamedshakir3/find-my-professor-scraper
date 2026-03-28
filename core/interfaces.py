from abc import ABC, abstractmethod
from typing import List, Dict, Any, Optional
from bs4 import BeautifulSoup
import requests
import logging

logger = logging.getLogger(__name__)

class BaseDirectoryScraper(ABC):
    """
    Abstract base class for Phase 1: Directory Traversal.
    The goal is to navigate a university department's directory page
    and extract a raw, incomplete list of professors containing at minimum
    their Name and Profile URL.
    """
    
    def __init__(self, university_id: int):
        self.university_id = university_id

    def fetch_page(self, url: str) -> Optional[BeautifulSoup]:
        """Utility method to fetch and parse a directory page."""
        headers = {
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko)"
        }
        try:
            response = requests.get(url, timeout=20, headers=headers)
            response.raise_for_status()
            return BeautifulSoup(response.text, 'html.parser')
        except requests.RequestException as e:
            logger.error(f"Failed to fetch {url}: {e}")
            return None

    @abstractmethod
    def scrape_directory(self, url: str, faculty_id: int, department_id: int) -> List[Dict[str, Any]]:
        """
        Scrapes a directory URL and returns a list of dictionaries.
        
        Expected output format:
        [
            {
                "first_name": "Jane",
                "last_name": "Doe",
                "profile_url": "https://...",
                "university_id": 1,
                "faculty_id": 2,
                "department_id": 3
            }, ...
        ]
        """
        pass
