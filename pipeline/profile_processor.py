import re
import json
import hashlib
import logging
import os
from typing import Dict, Any, Optional, Tuple
from bs4 import BeautifulSoup
from markdownify import markdownify as md

import requests as http_requests
import trafilatura

logger = logging.getLogger(__name__)

LLAMA_BASE_URL = os.environ.get("LLAMA_BASE_URL", "http://localhost:6969")

EXTRACTION_SCHEMA = {
    "type": "object",
    "properties": {
        "name": {
            "type": "string",
            "description": "The professor's full name."
        },
        "email": {
            "type": "string",
            "description": "The professor's email address if found in the text, otherwise 'NA'."
        },
        "bio": {
            "type": "string",
            "description": "A concise 2-3 sentence summary of their academic background and core focus."
        },
        "interests": {
            "type": "array",
            "items": {"type": "string"},
            "maxItems": 10,
            "description": "A list of 1 to 4 word research interests."
        },
        "accepting_students": {
            "type": "string",
            "enum": ["Yes", "No", "NA"],
            "description": "Whether the professor explicitly mentions accepting new graduate students. 'Yes' if actively recruiting, 'No' if explicitly full, 'NA' if completely unmentioned."
        }
    },
    "required": ["name", "email", "bio", "interests", "accepting_students"]
}


def _llm_chat(messages: list[dict], json_schema: dict | None = None, temperature: float = 0.1) -> str:
    """Send a chat completion request to the local llama.cpp server."""
    payload: dict = {
        "messages": messages,
        "temperature": temperature,
    }
    if json_schema:
        payload["response_format"] = {
            "type": "json_schema",
            "json_schema": {"name": "extraction", "strict": True, "schema": json_schema},
        }

    resp = http_requests.post(
        f"{LLAMA_BASE_URL}/v1/chat/completions",
        json=payload,
        timeout=600,
    )
    resp.raise_for_status()
    return resp.json()["choices"][0]["message"]["content"]


class ProfileProcessor:
    """
    Universal Spoke for Phase 2: NLP Processing.
    Fetches raw text using Trafilatura, uses Regex for emails/phones,
    and KeyBERT for research interests.
    """

    def __init__(self):
        # Ollama manages its own model lifecycle externally, no heavy lifting here.
        pass

    def extract_email(self, html_content: str) -> Optional[str]:
        """
        Extracts email using the original BeautifulSoup method,
        looking for mailto links and decoding Cloudflare protection.
        """
        if not html_content:
            return None
            
        soup = BeautifulSoup(html_content, 'html.parser')
        email_links = soup.find_all("a", href=True)
        
        for link in email_links:
            href = link['href']
            
            # 1. Standard mailto
            if "mailto:" in href.lower():
                return href.split(":", 1)[-1].strip()
                
            # 2. Cloudflare encoded
            elif "email-protection#" in href:
                try:
                    encoded = href.split("#")[1]
                    hex_bytes = bytes.fromhex(encoded)
                    key = hex_bytes[0]
                    decoded_email = ''.join(chr(b ^ key) for b in hex_bytes[1:])
                    return decoded_email
                except Exception as e:
                    logger.debug(f"Failed to decode cloudflare email: {e}")
                    
        return None


    def extract_with_llm(self, markdown: str, prof_name: str) -> Optional[Dict[str, Any]]:
        """
        Structured extraction using Ollama's JSON schema format.
        Returns a dict with bio, unique_interests, accepting_students,
        holistic_profile_string — or None on failure.
        """
        if not markdown or markdown in ["[UNAVAILABLE]", "[ERROR]"]:
            return None

        prompt = (
            f"Extract structured information about Professor {prof_name} "
            f"from the following profile page content.\n\n"
            f"Profile:\n{markdown[:10000]}"
        )

        try:
            raw = _llm_chat(
                messages=[
                    {
                        "role": "system",
                        "content": (
                            "You are a precise academic profile extractor. "
                            "Return only valid JSON matching the provided schema. "
                            "For interests, use 1-4 word phrases only. "
                            "For accepting_students, output exactly 'Yes', 'No', or 'NA'."
                        ),
                    },
                    {"role": "user", "content": prompt},
                ],
                json_schema=EXTRACTION_SCHEMA,
            )

            data = json.loads(raw)

            interests = [i.strip() for i in data.get("interests", []) if i.strip()][:10]
            bio = data.get("bio", "").strip()
            accepting = data.get("accepting_students", "NA")

            if not interests and not bio:
                logger.warning(f"Empty extraction for {prof_name}")
                return None

            kw_str = ", ".join(interests)
            holistic = f"Professor {prof_name}. Research interests: {kw_str}."

            return {
                "bio": bio,
                "unique_interests": interests,
                "accepting_students": accepting,
                "holistic_profile_string": holistic,
            }

        except Exception as e:
            logger.error(f"extract_with_llm failed for {prof_name}: {e}")
            return None

    def extract_keywords(self, raw_html: str, top_n: int = 10) -> list[str]:
        """Extracts top research interests from raw HTML using Qwen 3.5 4B."""
        if not raw_html:
            return []
            
        try:
            # --- STEP 1: Prune HTML and Convert to Markdown FIRST ---
            soup = BeautifulSoup(raw_html, 'html.parser')
            
            # Destroy useless layout tags to save massive amounts of tokens
            for tag in soup(['script', 'style', 'nav', 'footer', 'header', 'aside', 'meta', 'noscript']):
                tag.decompose()
            
            # Convert to Markdown (BeautifulSoup also automatically fixes HTML entities like &amp;)
            clean_markdown = md(str(soup), strip=['a', 'img']).strip()

            # --- STEP 2: Safe Truncation ---
            # Now that it is pure Markdown, it is safe to truncate if it's a massive page
            if len(clean_markdown) > 10000:
                clean_markdown = clean_markdown[:10000]

            # --- STEP 3: The Merged Prompt ---
            prompt = f"""
            You are an expert in extracting detailed research interests from professor biographies.

            INSTRUCTIONS:
            1. Scan the markdown for explicit headers (e.g., "Research Interests", "Research Areas", "Areas of Interest", "Research Focus", "Research Expertise") followed by a bulleted list. 
            If found, ONLY extract those exact points and do not infer bullet points.
            2. If no list exists, infer the core research interests from the biographical text.
            3. Group similar areas into concise terms but avoid losing important details.
            
            STRICT FORMATTING RULES:
            - Extract a maximum of {top_n} distinct research interests.
            - Output ONLY a single, semicolon-separated list.
            - Each interest MUST be strictly 1 to 4 words long (e.g., "neuronal dynamics", "machine learning").
            - Do NOT return long summaries, sentences, explanations, or conversational filler.
            - If no explicit research interests are mentioned, return an empty string.

            Input Markdown:  
            {clean_markdown}

            Output:
            """
            
            logger.info("Querying llama.cpp for keywords...")

            raw_content = _llm_chat(
                messages=[
                    {"role": "system", "content": "You are a strict data extractor. Output only the requested semicolon-separated list."},
                    {"role": "user",   "content": prompt.strip()},
                ],
            ).strip()
            
            if not raw_content:
                return []
                
            # --- STEP 4: Parse the Results ---
            keywords = [item.strip() for item in raw_content.split(";") if item.strip()]
            
            # Enforce the top_n limit just in case the LLM gets overly enthusiastic
            keywords = keywords[:top_n] 
            
            logger.info(f"Ollama extracted: {keywords}")
            return keywords
            
        except Exception as e:
            logger.error(f"Ollama extraction failed: {e}")
            return []

    class RateLimitError(Exception):
        """Raised when the server returns 429 Too Many Requests."""
        pass

    class ThrottledError(Exception):
        """Raised on connection resets or similar throttling signals."""
        pass

    def _fetch_html(self, url: str, timeout: int = 30) -> Optional[str]:
        """Fetch HTML with a hard timeout to prevent indefinite hangs."""
        import requests
        try:
            headers = {
                "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                              "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
            }
            resp = requests.get(url, timeout=timeout, headers=headers, allow_redirects=True)
            if resp.status_code == 429:
                raise self.RateLimitError(f"429 Too Many Requests for {url}")
            resp.raise_for_status()
            return resp.text
        except (self.RateLimitError, self.ThrottledError):
            raise
        except ConnectionError as e:
            raise self.ThrottledError(f"Connection reset for {url}: {e}") from e
        except Exception as e:
            logger.warning(f"Failed to fetch {url}: {e}")
            return None

    def fetch_and_hash(self, profile_url: str) -> Tuple[Optional[str], Optional[str], Optional[str]]:
        """
        Cheap step: fetch HTML, extract text, compute SHA-256 hash.
        Returns (raw_text, html, content_hash) or (None, None, None) on failure.
        """
        html = self._fetch_html(profile_url)
        if not html:
            return None, None, None

        raw_text = trafilatura.extract(html, favor_recall=True)
        if not raw_text:
            return None, html, None

        content_hash = hashlib.sha256(raw_text.encode("utf-8")).hexdigest()
        return raw_text, html, content_hash

    def process_profile(self, profile_url: str, prof_name: str, department_name: str,
                        raw_text: Optional[str] = None, html: Optional[str] = None) -> Dict[str, Any]:
        """
        Main pipeline method to extract unstructured data.
        If raw_text and html are provided (from fetch_and_hash), skips re-fetching.
        """
        if raw_text is None or html is None:
            html = self._fetch_html(profile_url)
            if not html:
                logger.warning(f"Could not fetch HTML for {profile_url}")
                return {}
            raw_text = trafilatura.extract(html, favor_recall=True)
            if not raw_text:
                logger.warning(f"Could not extract text for {profile_url}")
                return {}

        email = self.extract_email(html)
        keywords = self.extract_keywords(raw_text)
        
        # Fallback: Trafilatura sometimes strips out "Research Interests" if they 
        # look like navigation links or are in sidebars. If Ollama returns nothing,
        # we try again using a raw BeautifulSoup text dump of the entire page.
        # if not keywords and html:
        #     from bs4 import BeautifulSoup
        #     logger.info(f"Trafilatura text yielded no keywords for {profile_url}. Falling back to raw HTML extraction...")
        #     soup = BeautifulSoup(html, "html.parser")
        #     raw_bs4_text = soup.get_text(separator=' ', strip=True)
        #     keywords = self.extract_keywords(raw_bs4_text)
        
        content_hash = hashlib.sha256(raw_text.encode("utf-8")).hexdigest()
        
        # Build the holistic string
        kw_str = ", ".join(keywords)
        
        # If the LLM returned nothing, flag this profile as unavailable so the orchestrator
        # handles it as an error/missing data, rather than inserting an empty profile.
        if not keywords:
            logger.warning(f"Ollama returned empty keywords for {profile_url}. Flagging as missing data.")
            return {}

        holistic_string = f"Professor {prof_name}, {department_name}. Research interests: {kw_str}."
        
        return {
            "email": email,
            "unique_interests": keywords,
            "holistic_profile_string": holistic_string,
            "content_hash": content_hash,
        }
