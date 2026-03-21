"""
GPU cloud worker for LLM extraction (Asyncio Version).

Reads cloud_input.jsonl (professor markdown exported locally),
runs structured extraction via llama.cpp server, writes cloud_output.jsonl.

Usage on GPU machine:
    1. pip install aiohttp
    2. Start llama-server with your model (ensure -np matches --parallel)
    3. python -m pipeline.cloud_worker --input cloud_input.jsonl --output cloud_output.jsonl --parallel 24
"""
import json
import os
import time
import logging
import sys
import argparse
import asyncio
import aiohttp

LLAMA_BASE_URL = os.environ.get("LLAMA_BASE_URL", "http://localhost:8000") # Defaulted to 8000 for llama.cpp

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

def setup_logger():
    logger = logging.getLogger("CloudWorker")
    logger.setLevel(logging.INFO)
    formatter = logging.Formatter('[%(asctime)s] %(levelname)s: %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)
    file_handler = logging.FileHandler('worker_sprint.log', mode='w')
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
    return logger

logger = setup_logger()

def _empty_result(prof_id, status="[UNAVAILABLE]"):
    return {
        "id": prof_id,
        "bio": None,
        "unique_interests": [],
        "accepting_students": "NA",
        "holistic_profile_string": status,
        "llm_email": None
    }

async def process_single_row(session, semaphore, data_line, current_idx, total_lines):
    # The semaphore acts as a strict bottleneck, ensuring we NEVER exceed your --parallel limit
    async with semaphore:
        try:
            data = json.loads(data_line) if isinstance(data_line, str) else data_line
            prof_id = data["id"]
            prof_name = data["name"]
            faculty = data.get("faculty", "")
            department = data.get("department", "")
            clean_markdown = data["profile_markdown"]
        except KeyError as e:
            logger.error(f"Malformed JSON line, missing key: {e}")
            return None

        if not clean_markdown or clean_markdown in ["[UNAVAILABLE]", "[ERROR]"]:
            logger.warning(f"Skipping ID {prof_id} ({prof_name}) - Markdown unavailable.")
            return _empty_result(prof_id)

        prompt = (
            f"Extract structured information about Professor {prof_name} "
            f"from the following profile page content.\n\n"
            f"Profile:\n{clean_markdown[:10000]}"
        )

        payload = {
            "messages": [
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
            "temperature": 0.0, # Dropped to 0.0 for max speed/determinism
            "response_format": {
                "type": "json_schema",
                "json_schema": {"name": "extraction", "strict": True, "schema": EXTRACTION_SCHEMA},
            },
        }

        try:
            # We use aiohttp instead of requests here to keep it entirely asynchronous
            async with session.post(
                f"{LLAMA_BASE_URL}/v1/chat/completions",
                json=payload,
                timeout=aiohttp.ClientTimeout(total=600)
            ) as resp:
                
                # Catch actual server overload errors before attempting to parse JSON
                if resp.status != 200:
                    error_text = await resp.text()
                    logger.error(f"Server returned {resp.status} for ID {prof_id}. Server said: {error_text}")
                    return _empty_result(prof_id, "[ERROR]")

                raw_json = await resp.json()
                raw = raw_json["choices"][0]["message"]["content"]
                data_out = json.loads(raw)

                interests = [i.strip() for i in data_out.get("interests", []) if i.strip()][:10]
                bio = data_out.get("bio", "").strip()
                accepting = data_out.get("accepting_students", "NA")
                llm_email = data_out.get("email", "").strip()

                if not interests and not bio:
                    logger.warning(f"ID {prof_id} ({prof_name}) - Empty extraction.")
                    return _empty_result(prof_id)

                kw_str = ", ".join(interests)
                holistic = f"Professor {prof_name}, {faculty}, {department}. Research interests: {kw_str}."

                logger.info(f"[{current_idx}/{total_lines}] ID {prof_id} ({prof_name}) — {len(interests)} interests, accepting={accepting}")

                return {
                    "id": prof_id,
                    "bio": bio,
                    "unique_interests": interests,
                    "accepting_students": accepting,
                    "holistic_profile_string": holistic,
                    "llm_email": llm_email if llm_email and llm_email != "NA" else None,
                }

        except Exception as e:
            logger.error(f"Inference failed for ID {prof_id} ({prof_name}): {e}")
            return _empty_result(prof_id, "[ERROR]")

async def run_cloud_sprint(input_file="cloud_input.jsonl", output_file="cloud_output.jsonl", parallel=24):
    try:
        with open(input_file, 'r') as f:
            lines = f.readlines()
    except FileNotFoundError:
        logger.critical(f"Input file '{input_file}' not found.")
        return

    total_lines = len(lines)
    logger.info(f"Starting Async GPU Sprint on {total_lines} profiles (parallel={parallel})...")
    start_time = time.time()

    # The semaphore restricts concurrent connections to perfectly match your server capacity
    semaphore = asyncio.Semaphore(parallel)
    
    # We configure the TCPConnector to allow the high parallel limit without throttling
    connector = aiohttp.TCPConnector(limit=parallel)
    
    async with aiohttp.ClientSession(connector=connector) as session:
        # Fire off all tasks. The semaphore ensures only 'parallel' number run at once.
        tasks = [
            process_single_row(session, semaphore, line, i+1, total_lines) 
            for i, line in enumerate(lines)
        ]
        
        # Wait for everything to finish
        raw_results = await asyncio.gather(*tasks)

    # Filter out Nones and sort
    results = [r for r in raw_results if r is not None]
    results.sort(key=lambda r: r["id"])

    try:
        with open(output_file, 'w') as f:
            for res in results:
                f.write(json.dumps(res, ensure_ascii=False) + "\n")
    except Exception as e:
        logger.critical(f"Failed to write output file: {e}")
        return

    elapsed = time.time() - start_time
    rate = len(results) / elapsed if elapsed > 0 else 0
    logger.info(f"Done! {len(results)} profiles in {elapsed:.1f}s ({rate:.1f} profiles/sec)")
    logger.info(f"Output: {output_file}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Async GPU cloud worker for LLM extraction")
    parser.add_argument("--input", default="cloud_input.jsonl", help="Input JSONL file")
    parser.add_argument("--output", default="cloud_output.jsonl", help="Output JSONL file")
    parser.add_argument("--parallel", type=int, default=24, help="Parallel requests to llama-server")
    args = parser.parse_args()

    # Execute the async event loop
    asyncio.run(run_cloud_sprint(args.input, args.output, args.parallel))