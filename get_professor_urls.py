import asyncio
import json
import os
from typing import List

from crawl4ai import AsyncWebCrawler, BrowserConfig, CacheMode, CrawlerRunConfig, LLMConfig
from crawl4ai.extraction_strategy import LLMExtractionStrategy
from pydantic import BaseModel, Field

class ProfessorURL(BaseModel): 
    url: str = Field(..., description="URL to professors webpage.")
URL_TO_SCRAPE = "https://www.uottawa.ca/faculty-science/mathematics-statistics/professor-directory"

INSTRUCTION_TO_LLM = "Extract all professors website URLS, and return just a list of all professor URLs."

async def main():

    llm_strategy = LLMExtractionStrategy(
        llm_config = LLMConfig(provider="deepseek/deepseek-chat",
                               api_token=os.getenv("DEEPSEEK_API")),
        schema=ProfessorURL.model_json_schema(),
        extraction_type="schema",
        instruction=INSTRUCTION_TO_LLM,
        chunk_token_threshold=1000,
        overlap_rate=0.0,
        apply_chunking=True,
        input_format="markdown",
        extra_args={"temperature": 0.0, "max_tokens": 800},
    )

    crawl_config = CrawlerRunConfig(
        extraction_strategy=llm_strategy,
        cache_mode=CacheMode.BYPASS,
        process_iframes=False,
        remove_overlay_elements=True,
        exclude_external_links=True,
    )

    browser_cfg = BrowserConfig(headless=True, verbose=True)
    file = open("universities.json")
    urls = json.load(file)
    urls = urls["uOttawa"]
    prof_urls = []
    async with AsyncWebCrawler(config=browser_cfg) as crawler:

        results = await crawler.arun_many(urls=urls, config=crawl_config)
        for result in results:
            if result.success:
                data = json.loads(result.extracted_content)
                prof_urls.extend(data)
                print("Extracted items:", data)

            else:
                print("Error:", result.error_message)
        llm_strategy.show_usage()
        with open("prof_urls", "w", encoding="utf-8") as f:
            json.dump(prof_urls, f, indent=4, ensure_ascii=False)


if __name__ == "__main__":
    asyncio.run(main())
