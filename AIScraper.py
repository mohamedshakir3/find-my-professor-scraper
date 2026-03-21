from ollama import chat, ChatResponse
import json
import os
import asyncio
from openai import OpenAI
from crawl4ai import AsyncWebCrawler, BrowserConfig, CacheMode, CrawlerRunConfig, LLMConfig
from crawl4ai.extraction_strategy import LLMExtractionStrategy
from pydantic import BaseModel, Field
from typing import List

class Professor(BaseModel):
    research_interests: List[str] = Field(..., description="Research interests of the professor")

class LLMScraper:
    INSTRUCTION_TO_LLM = "Extract the research interests from the professors profile. Place each professors' research interests as an object with that data."

    def __init__(self):
        pass
        """Initialize LLM Scraper with config"""
        # self.deepseek = OpenAI(
        #     api_key=os.getenv("DEEPSEEK_API"),
        #     base_url="https://api.deepseek.com")
        # self.llm_strategy = LLMExtractionStrategy(
        #     llm_config=LLMConfig(
        #         provider="deepseek/deepseek-chat",
        #         api_token=os.getenv("DEEPSEEK_API")
        #     ),
        #     schema=Professor.model_json_schema(),
        #     extraction_type="schema",
        #     instruction=self.INSTRUCTION_TO_LLM,
        #     chunk_token_threshold=1000,
        #     overlap_rate=0.0,
        #     apply_chunking=True,
        #     input_format="markdown",
        #     extra_args={"temperature": 0.0, "max_tokens": 2500}
        # )

        # self.crawl_config = CrawlerRunConfig(
        #     extraction_strategy=self.llm_strategy,
        #     cache_mode=CacheMode.BYPASS,
        #     process_iframes=False,
        #     remove_overlay_elements=True,
        #     exclude_external_links=True
        # )

        # self.browser_cfg = BrowserConfig(headless=True, verbose=True)

    def scrape(self, url):
        """Run async scrape in a synchronous way"""
        return asyncio.run(self._async_scrape(url))
    
    def prompt(self, content):
        print("Processing LLM prompt...")
        prompt = "Given a professor biography, extract a comma separated list of research interests for this professor. Return only a comma separated list of research interests, if no interests are found, return nothing."
        response = self.deepseek.chat.completions.create(
            model="deepseek-chat",
            messages=[
                {"role": "system", "content": prompt},
                {"role": "user", "content": content},
            ],
            stream=False
        )
        return response.choices[0].message.content
    
    def mistral(self,  content):
        prompt = """
        You are an expert in extracting and summarizing research interests from professor biographies.

        Given a professor biography, extract the research interests as a concise, **semicolon-separated list**.
        - **Summarize slightly** but not too much, only enough clear phrases (no more than 10 words each).
        - **Preserve important technical terms** and compound phrases.
        - **Avoid duplication** and redundant phrases.
        - If no explicit research interests are mentioned, return an empty string (no text or explanation).

        **Input:**  
        {content}

        **Output:**  
        A semicolon-separated list of summarized research interests or an empty string.
        """
        print("Processing prompt with Mistral...")
        response: ChatResponse = chat(
            model = "mistral",
            messages = [
                 {"role": "system", "content": "You are an expert in extracting and summarizing research interests from professor biographies."},
                {"role": "user", "content": prompt.format(content=content)},
            ],
            options = {
                "num_ctx": 30000,
                "temperature": 0.4
            },
            stream=False
        )
        return response.message.content

    def qwen(self, content):
        prompt = f"""
        You are an expert in **extracting detailed research interests** from professor biographies.

        Given a professor biography, **extract all mentioned research interests** as a concise, **semicolon-separated list**.
        - **Include all technical terms** and multi-word phrases exactly as they appear.
        - **Do not split phrases** that naturally belong together (for example, ensure "Gait & Posture" remains intact).
        - If any HTML entities (like "&amp;") appear, convert them to their correct characters.
        - **Group similar areas** into concise terms but avoid losing important details.
        - **Do not truncate or oversimplify** complex phrases.
        - If no explicit research interests are mentioned, return an empty string (no extra text or explanation).

        **Input:**  
        {content}

        **Output:**  
        A semicolon-separated list of all relevant research interests or an empty string.
        """

        response: ChatResponse = chat(
            model="qwen2.5:14b",
            messages=[
                {"role": "system", "content": "You are an expert in extracting detailed research interests from professor biographies."},
                {"role": "user", "content": prompt},
            ],
            options={"num_ctx": 50000, "temperature": 0}
        )
        return response.message.content
    
    def qwen2(self, content):
        system = "You are an expert extractor.  When asked for a semicolon-separated list, you MUST output only the list items, separated by semicolons, with NO markdown, NO code fences, NO commentary, and NO quotes.  If there are no items, output an empty string. /nothink"
        user = f"""Extract all research interests (exact phrases) from the following HTML/text.  Output only a semicolon-separated list:

        {content}
        """
        response: ChatResponse = chat(
            model="qwen2.5:7b",
            messages=[
                {"role": "system", "content": system},
                {"role": "user", "content": user},
            ],
            options={"num_ctx": 50000, "temperature": 0.1}
        )
        return response.message.content.strip()
    
    def qwen3(self, content):
        prompt = f"""
        You are an expert in **extracting detailed research interests** from professor biographies.

        Given a professor biography, **extract all mentioned research interests** as a concise, **semicolon-separated list**.
        - **Include all technical terms** and multi-word phrases exactly as they appear.
        - **Do not split phrases** that naturally belong together (for example, ensure "Gait & Posture" remains intact).
        - If any HTML entities (like "&amp;") appear, convert them to their correct characters.
        - **Group similar areas** into concise terms but avoid losing important details.
        - **Do not truncate or oversimplify** complex phrases.
        - If no explicit research interests are mentioned, return an empty string (no extra text or explanation).
        - Combine and summarize the text/bio in no more than 10 research interests

        **Input:**  
        {content}

        **Output:**  
        A semicolon-separated list of all relevant research interests or an empty string.
        """
        response = chat(
            model="qwen2.5:7b",
            messages=[
                {"role": "system", "content": "You are an expert in extracting detailed research interests from professor biographies. /no_think"},
                {"role": "user",   "content": prompt.strip()},
            ],
            options={"num_ctx": 50000, "temperature": 0.1}
        )
        return response.message.content.strip()

    def qwen3_post_process(self, raw_str: str) -> list[str]:
        system_prompt = """
        You are a data-cleaning assistant.  You receive a JSON array of possible research interests extracted by another model.  Your job is to:
        1. Remove any entry that is:
            • Longer than 10 words
            • Contains a 4-digit year or biographical verbs (“received”, “taught”, “joined”)
            • Is a known country name (e.g. “United States”, “Canada”)
            • Is all-caps with more than one word
            • Contains markdown characters (like underscores, asterisks)
        2. Trim whitespace and punctuation; normalize casing (title case or sentence case).
        3. Deduplicate, preserving original order.
        4. Return a JSON array of cleaned strings, nothing else.
        """
        raw_list = [item.strip() for item in raw_str.split(";") if item.strip()]
        clean_prompt = f"""
        Here is the raw list:
        {json.dumps(raw_list)}
        """
        response = chat(
            model="qwen3:4b",
            messages=[
                {"role": "system", "content": system_prompt.strip()},
                {"role": "user",   "content": clean_prompt.strip()},
            ],
            options={"num_ctx": 50000, "temperature": 0.1}
        )
        try:
            cleaned = json.loads(response.message.content)
            if isinstance(cleaned, list):
                return cleaned
        except json.JSONDecodeError:
            pass
        return raw_list
        
    def deepseek_coder(self, content):
        prompt = f"""
        You are an expert in extracting and summarizing research interests from professor biographies.

        Given a professor biography, extract the research interests as a concise, **semicolon-separated list**.
        - **Preserve important technical terms** and compound phrases.
        - **Avoid duplication** and redundant phrases.
        - If no explicit research interests are mentioned, return an empty string (no text or explanation).

        **Input:**  
        {content}

        **Output:**  
        A semicolon-separated list of summarized research interests or an empty string.
        """
        
        response: ChatResponse = chat(
            model = "deepseek-coder:6.7b",
            messages = [
                {"role": "system", "content": "You are an expert in extracting and summarizing research interests from professor biographies."},
                {"role": "user", "content": prompt},
            ],
            options = {"num_ctx": 30000, "temperature": 0.1, "top_p": 0.8}
        )
        
        return response.message.content

    def qwen_paraphrase(self, content):
        prompt = f"""
        You are an expert in paraphrasing and summarizing research interests.

        Given a research interest, summarize and paraphrase it as much as possible, maintaining technical keywords. If there are multiple interests in one string return a **semicolon-separated list**.
        - **Preserve important technical terms** and compound phrases
        - **Avoid duplication** and redundant phrases.
        - If no explicit research interests are mentioned, return an empty string (no text or explanation).

        **Input:**  
        {content}

        **Output:**  
        A semicolon-separated list or individual string of summarized research interests or an empty string.
        """

        response: ChatResponse = chat(
            model = "deepseek-coder:6.7b",
            messages = [
                {"role": "system", "content": "You are an expert in extracting and summarizing research interests from professor biographies."},
                {"role": "user", "content": prompt},
            ],
            options = {"num_ctx": 30000, "temperature": 0.1, "top_p": 0.8}
        )
        
        return response.message.content

    async def _async_scrape(self, url):
        """Internal async scraping logic"""
        research_interests = []

        async with AsyncWebCrawler(config=self.browser_cfg) as crawler:
            result = await crawler.arun(url=url, config=self.crawl_config)

            if result.success:
                data = json.loads(result.extracted_content)
                if isinstance(data, list):
                    if len(data) > 0 and isinstance(data[0], dict):
                        research_interests = data[0].get("research_interests", [])
                    else:
                        research_interests = []
                elif isinstance(data, dict):
                    research_interests = data.get("research_interests", [])
                else:
                    research_interests = []

        self.llm_strategy.show_usage()
        return research_interests
