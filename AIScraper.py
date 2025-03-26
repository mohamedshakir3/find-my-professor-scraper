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
        """Initialize LLM Scraper with config"""
        self.deepseek = OpenAI(
            api_key=os.getenv("DEEPSEEK_API"),
            base_url="https://api.deepseek.com")
        self.llm_strategy = LLMExtractionStrategy(
            llm_config=LLMConfig(
                provider="deepseek/deepseek-chat",
                api_token=os.getenv("DEEPSEEK_API")
            ),
            schema=Professor.model_json_schema(),
            extraction_type="schema",
            instruction=self.INSTRUCTION_TO_LLM,
            chunk_token_threshold=1000,
            overlap_rate=0.0,
            apply_chunking=True,
            input_format="markdown",
            extra_args={"temperature": 0.0, "max_tokens": 2500}
        )

        self.crawl_config = CrawlerRunConfig(
            extraction_strategy=self.llm_strategy,
            cache_mode=CacheMode.BYPASS,
            process_iframes=False,
            remove_overlay_elements=True,
            exclude_external_links=True
        )

        self.browser_cfg = BrowserConfig(headless=True, verbose=True)

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
