# scripts/run_scrape_once.py
from scraper.pipeline.orchestrator import run_once

if __name__ == "__main__":
    total, kept = run_once()
    print(f"Scraped {total} postings, kept {kept}.")
