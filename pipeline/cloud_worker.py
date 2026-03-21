"""
GPU cloud worker for LLM extraction.

Reads cloud_input.jsonl (professor markdown exported locally),
runs structured extraction via llama.cpp server, writes cloud_output.jsonl.

Usage on GPU machine:
    1. Start llama-server with your model
    2. python -m pipeline.cloud_worker
    3. Or: python -m pipeline.cloud_worker --input cloud_input.jsonl --output cloud_output.jsonl --parallel 4
"""
import json
import os
import requests
import time
import logging
import sys
import argparse
import concurrent.futures

LLAMA_BASE_URL = os.environ.get("LLAMA_BASE_URL", "http://localhost:8080")

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
    }


def process_single_row(data_line):
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

    try:
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
            "temperature": 0.1,
            "response_format": {
                "type": "json_schema",
                "json_schema": {"name": "extraction", "strict": True, "schema": EXTRACTION_SCHEMA},
            },
        }
        resp = requests.post(
            f"{LLAMA_BASE_URL}/v1/chat/completions",
            json=payload,
            timeout=600,
        )
        resp.raise_for_status()
        raw = resp.json()["choices"][0]["message"]["content"]
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


def run_cloud_sprint(input_file="cloud_input.jsonl", output_file="cloud_output.jsonl", parallel=1):
    try:
        with open(input_file, 'r') as f:
            lines = f.readlines()
    except FileNotFoundError:
        logger.critical(f"Input file '{input_file}' not found.")
        return

    logger.info(f"Starting GPU Sprint on {len(lines)} profiles (parallel={parallel})...")
    start_time = time.time()
    results = []

    if parallel <= 1:
        for i, line in enumerate(lines):
            result = process_single_row(line)
            if result:
                results.append(result)
                logger.info(f"[{i+1}/{len(lines)}] ID {result['id']} — {len(result['unique_interests'])} interests, accepting={result['accepting_students']}")
    else:
        with concurrent.futures.ThreadPoolExecutor(max_workers=parallel) as executor:
            futures = {executor.submit(process_single_row, line): i for i, line in enumerate(lines)}
            for future in concurrent.futures.as_completed(futures):
                idx = futures[future]
                result = future.result()
                if result:
                    results.append(result)
                    logger.info(f"[{idx+1}/{len(lines)}] ID {result['id']} — {len(result['unique_interests'])} interests")

    # Sort by ID for deterministic output
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
    parser = argparse.ArgumentParser(description="GPU cloud worker for LLM extraction")
    parser.add_argument("--input", default="cloud_input.jsonl", help="Input JSONL file")
    parser.add_argument("--output", default="cloud_output.jsonl", help="Output JSONL file")
    parser.add_argument("--parallel", type=int, default=1, help="Parallel requests to llama-server")
    args = parser.parse_args()

    run_cloud_sprint(args.input, args.output, args.parallel)
