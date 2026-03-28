# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

A web scraping and NLP pipeline for aggregating Canadian university professor information. It scrapes faculty directories from 13+ Canadian universities, converts profiles to markdown, uses a local LLM (Ollama) to extract research interests, and generates vector embeddings for semantic search — all stored in PostgreSQL.

## Setup

```bash
source venv/bin/activate
```

Requires a `.env` file with:
```
DATABASE_URL=postgresql://user:password@localhost:5432/findmyprofessor
```

Also requires Ollama running locally with the Qwen 3.5 4B model for Phase 3.

## Running the Pipeline

```bash
# Run all phases
python orchestrator.py

# Run a specific phase
python orchestrator.py --phase 1          # Scrape directory URLs → DB
python orchestrator.py --phase 2          # Fetch profiles → markdown
python orchestrator.py --phase 3          # LLM extraction + embeddings

# Filter by university
python orchestrator.py --university "University of Ottawa"

# Control concurrency
python orchestrator.py --phase 2 --max-workers 5
python orchestrator.py --phase 3 --ai-workers 1   # Keep low to avoid VRAM exhaustion

# Force reprocessing (skip content hash check)
python orchestrator.py --phase 2 --force
```

## Running Tests

```bash
pytest tests/

# Fast URL health checks only
pytest tests/test_data_sources.py -m health -v

# Full data source tests (hits live websites, slow)
pytest tests/test_data_sources.py -v

# Single test file
pytest tests/test_uottawa.py -v
```

## Architecture

### Three-Phase Pipeline

**Phase 1 — Directory Traversal** (`orchestrator.py` + `universities/*.py`):
Reads `universities.json` (three-level hierarchy: University → Faculty → Department → URL), routes each URL to a university-specific scraper, stores discovered professors (name + profile URL) in PostgreSQL.

**Phase 2 — Fetch & Markdown** (`pipeline/profile_processor.py`):
Downloads HTML profiles concurrently (default 10 workers), extracts emails, converts HTML to markdown via `markdownify`/`trafilatura`, detects changes via content hash to avoid redundant work.

**Phase 3 — AI Extraction & Embeddings** (`pipeline/embedder.py` + Ollama):
Sends markdown to a local Ollama/Qwen model to extract research interests, then generates 384-dim vector embeddings using `sentence-transformers` (`all-MiniLM-L6-v2`). Kept at low concurrency (default 2 workers) to avoid GPU memory issues.

### Adding a New University Scraper

1. Create `universities/<university_slug>.py`
2. Inherit from `BaseDirectoryScraper` in `core/interfaces.py`
3. Implement `scrape_directory(self, url: str, faculty_id: int, department_id: int) -> List[Dict]`
4. Each returned dict must include: `first_name`, `last_name`, `profile_url`, `university_id`, `faculty_id`, `department_id`
5. Register the scraper in `SCRAPER_REGISTRY` in `orchestrator.py`
6. Add directory URLs to `universities.json`

### Database Layer (`db/`)

- `connection.py` — PostgreSQL connection pool; always use `get_connection()` / `put_connection()` in a try/finally
- `repositories.py` — Upsert operations for professors, faculties, departments; handles ON CONFLICT resolution

### Key Design Decisions

- Content hashing in Phase 2 prevents redundant re-fetching of unchanged profiles
- Phase 3 concurrency is intentionally low (`--ai-workers 2` default) to prevent Ollama VRAM exhaustion
- Each university gets a custom scraper because faculty directory HTML structures vary widely
