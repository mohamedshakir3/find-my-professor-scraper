import sys
import logging
from pathlib import Path
import json

# Add the parent directory so we can import scraper as a package
sys.path.append(str(Path(__file__).parent.parent))

from scraper.pipeline.profile_processor import ProfileProcessor

logging.basicConfig(level=logging.INFO)

def test_phase_2():
    print("Testing Phase 2 Profile Processor (Trafilatura + KeyBERT)...")
    
    # Instantiate the processor
    processor = ProfileProcessor()
    
    # Test a known professor profile
    test_url = "https://www.uottawa.ca/faculty-medicine/dr-richard-naud"
    print(f"\nProcessing profile: {test_url}\n")
    
    result = processor.process_profile(test_url, prof_name="Richard Naud", department_name="Medicine")
    
    print("----- Extraction Results -----")
    print(f"Email: {result.get('email')}")
    print(f"Holistic Profile String preview (first 250 chars):")
    
    holistic_string = result.get('holistic_profile_string', '')
    if len(holistic_string) > 250:
        print(f"{holistic_string[:250]}...")
    else:
        print(holistic_string)
        
    print("\\nExtracting keywords specifically for display:")
    import trafilatura
    downloaded = trafilatura.fetch_url(test_url)
    raw_text = trafilatura.extract(downloaded) if downloaded else ""
    keywords = processor.extract_keywords(raw_text)
    print(json.dumps(keywords, indent=2))

if __name__ == "__main__":
    test_phase_2()
