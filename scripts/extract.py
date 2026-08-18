"""
extract.py
----------
EXTRACT step of the JobPulse ETL pipeline.

Pulls remote job listings from the Remotive public API (no API key required):
https://remotive.com/api/remote-jobs

Docs: https://remotive.com/api-documentation
"""

import requests
import json
import logging
from pathlib import Path
from datetime import datetime

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

REMOTIVE_API_URL = "https://remotive.com/api/remote-jobs"
RAW_DATA_DIR = Path(__file__).parent.parent / "data" / "raw"


def extract_jobs(limit: int = 200, category: str = None) -> list[dict]:
    """
    Extract raw job postings from the Remotive API.

    Args:
        limit: max number of jobs to fetch
        category: optional Remotive category filter, e.g. "software-dev"

    Returns:
        List of raw job dicts as returned by the API.
    """
    params = {"limit": limit}
    if category:
        params["category"] = category

    logger.info(f"Extracting jobs from Remotive API (limit={limit}, category={category})")
    response = requests.get(REMOTIVE_API_URL, params=params, timeout=30)
    response.raise_for_status()

    payload = response.json()
    jobs = payload.get("jobs", [])
    logger.info(f"Extracted {len(jobs)} raw job records")
    return jobs


def save_raw(jobs: list[dict]) -> Path:
    """Persist the raw extracted payload to disk (the 'landing zone')."""
    RAW_DATA_DIR.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    out_path = RAW_DATA_DIR / f"jobs_raw_{timestamp}.json"

    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(jobs, f, indent=2)

    logger.info(f"Saved raw data to {out_path}")
    return out_path


if __name__ == "__main__":
    jobs = extract_jobs(limit=200)
    save_raw(jobs)
