"""
transform.py
------------
TRANSFORM step of the JobPulse ETL pipeline.

Takes raw job records (from extract.py) and:
  - Deduplicates on job id
  - Parses/normalizes salary text into salary_min / salary_max / currency
  - Normalizes location text
  - Extracts a clean skill-tag list per job by keyword-matching the description
  - Flags and drops records with missing critical fields (title, id)

Produces a clean pandas DataFrame ready for loading into Postgres.
"""

import re
import logging
import pandas as pd

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

# A small, extensible keyword list used to tag jobs with skills mentioned
# in their description. This is intentionally simple (keyword match) --
# good enough for a demo pipeline, easy to explain, easy to extend.
SKILL_KEYWORDS = [
    "python", "sql", "java", "javascript", "typescript", "react", "node.js",
    "aws", "azure", "gcp", "docker", "kubernetes", "airflow", "spark",
    "kafka", "postgresql", "mongodb", "machine learning", "deep learning",
    "pandas", "numpy", "tensorflow", "pytorch", "flask", "django", "fastapi",
    "excel", "power bi", "tableau", "etl", "data warehouse", "redshift",
]

SALARY_RANGE_RE = re.compile(
    r"(?P<currency>[$€£₹])?\s*(?P<min>[\d,]+)\s*(?:-|to)\s*(?P<currency2>[$€£₹])?\s*(?P<max>[\d,]+)",
    re.IGNORECASE,
)


def _parse_salary(salary_text: str):
    """Extract (min, max, currency) from a free-text salary string."""
    if not salary_text or not isinstance(salary_text, str):
        return None, None, None

    match = SALARY_RANGE_RE.search(salary_text)
    if not match:
        return None, None, None

    currency = match.group("currency") or match.group("currency2") or None
    try:
        salary_min = float(match.group("min").replace(",", ""))
        salary_max = float(match.group("max").replace(",", ""))
    except (TypeError, ValueError):
        return None, None, currency

    return salary_min, salary_max, currency


def _extract_skills(description: str) -> list[str]:
    """Keyword-match a job description against the known skill list."""
    if not description or not isinstance(description, str):
        return []
    text = description.lower()
    return [skill for skill in SKILL_KEYWORDS if skill in text]


def transform_jobs(raw_jobs: list[dict]) -> pd.DataFrame:
    """
    Clean and structure raw job records into a tidy DataFrame.

    Returns a DataFrame with columns:
        job_id, title, company_name, location_name, category, job_type,
        salary_min, salary_max, salary_currency, published_date, skills (list)
    """
    logger.info(f"Transforming {len(raw_jobs)} raw job records")
    df = pd.DataFrame(raw_jobs)

    if df.empty:
        logger.warning("No jobs to transform — empty DataFrame returned")
        return df

    # --- Deduplicate on job id ---
    before = len(df)
    df = df.drop_duplicates(subset="id")
    duplicate_count = before - len(df)
    logger.info(f"Removed {duplicate_count} duplicate records")

    # --- Drop rows missing critical fields ---
    null_title_count = df["title"].isna().sum()
    df = df.dropna(subset=["id", "title"])

    # --- Rename / select relevant columns ---
    df = df.rename(columns={
        "id": "job_id",
        "company_name": "company_name",
        "candidate_required_location": "location_name",
        "job_type": "job_type",
        "publication_date": "published_date",
        "salary": "salary_text",
        "description": "description",
    })

    keep_cols = [
        "job_id", "title", "company_name", "location_name", "category",
        "job_type", "salary_text", "published_date", "description",
    ]
    df = df[[c for c in keep_cols if c in df.columns]]

    # --- Normalize location text (basic cleanup) ---
    df["location_name"] = (
        df["location_name"]
        .fillna("Not Specified")
        .str.strip()
        .replace("", "Not Specified")
    )

    # --- Parse salary text into structured min/max/currency ---
    parsed = df["salary_text"].apply(_parse_salary)
    df["salary_min"] = parsed.apply(lambda x: x[0])
    df["salary_max"] = parsed.apply(lambda x: x[1])
    df["salary_currency"] = parsed.apply(lambda x: x[2])

    # --- Parse published_date into a proper date ---
    df["published_date"] = pd.to_datetime(df["published_date"], errors="coerce").dt.date

    # --- Extract skill tags from description ---
    df["skills"] = df["description"].apply(_extract_skills)

    df = df.drop(columns=["salary_text", "description"])

    # Stash quality-check numbers as DataFrame attrs so load.py can log them
    df.attrs["duplicate_count"] = duplicate_count
    df.attrs["null_title_count"] = int(null_title_count)
    df.attrs["rows_extracted"] = before

    logger.info(f"Transform complete: {len(df)} clean rows remaining")
    return df


if __name__ == "__main__":
    import json
    from pathlib import Path

    raw_dir = Path(__file__).parent.parent / "data" / "raw"
    latest_raw = sorted(raw_dir.glob("jobs_raw_*.json"))[-1]

    with open(latest_raw) as f:
        raw_jobs = json.load(f)

    clean_df = transform_jobs(raw_jobs)
    out_path = Path(__file__).parent.parent / "data" / "clean_jobs.csv"
    clean_df.to_csv(out_path, index=False)
    logger.info(f"Saved clean data to {out_path}")
