import asyncio
import json
import os
from crawl4ai import AsyncWebCrawler, BrowserConfig, CacheMode, CrawlerRunConfig, LLMConfig
from crawl4ai.extraction_strategy import LLMExtractionStrategy
from typing import List, Optional
from pydantic import BaseModel, HttpUrl, EmailStr, Field

class Professor(BaseModel):
    name: str = Field(..., description="Full name of the professor")
    email: Optional[EmailStr] = Field(None, description="Email address, if available")
    research_interests: List[str] = Field(..., description="Research interests of the professor")

class ProfessorList(BaseModel):
    professors: List[Professor]


URL_TO_SCRAPE = "https://www.uottawa.ca/faculty-science/mathematics-statistics/professor-directory"

INSTRUCTION_TO_LLM = "Extract the professors profile information with their name, email address, research interests. Place each professor as an object with that data."


async def main():
    file = open("carleton_profs.json")
    urls = json.load(file)
    llm_strategy = LLMExtractionStrategy(
        llm_config = LLMConfig(provider="deepseek/deepseek-chat",
                               api_token=os.getenv("DEEPSEEK_API")),
        schema=Professor.model_json_schema(),
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

    async with AsyncWebCrawler(config=browser_cfg) as crawler:

        results = await crawler.arun_many(urls=urls, config=crawl_config)
        prof_data = []
        for result in results:
            if result.success:
                data = json.loads(result.extracted_content)
                prof_data.extend(data)
            else:
                print("Error:", result.error_message)

        llm_strategy.show_usage()
        with open("carleton_data.json", "w", encoding="utf-8") as f:
            json.dump(prof_data, f, indent=4, ensure_ascii=False)

if __name__ == "__main__":
    asyncio.run(main())
