"""
load.py
-------
LOAD step of the JobPulse ETL pipeline.

Takes the clean DataFrame (from transform.py) and loads it into the
PostgreSQL star-schema warehouse defined in sql/schema.sql:

    fact_jobs  <-- fact_job_skills -->  dim_skill
    fact_jobs  -->  dim_company
    fact_jobs  -->  dim_location

Also writes a row to pipeline_run_log for monitoring/troubleshooting.
"""

import os
import logging
from pathlib import Path
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv

# Load .env file from project root
load_dotenv(Path(__file__).parent.parent / ".env")
load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

DB_CONFIG = {
    "host": os.environ.get("JOBPULSE_DB_HOST", "localhost"),
    "port": os.environ.get("JOBPULSE_DB_PORT", "5432"),
    "dbname": os.environ.get("JOBPULSE_DB_NAME", "jobpulse"),
    "user": os.environ.get("JOBPULSE_DB_USER", "postgres"),
    "password": os.environ.get("JOBPULSE_DB_PASSWORD", "postgres"),
}


def get_connection():
    return psycopg2.connect(**DB_CONFIG)


def _get_or_create_id(cur, table: str, id_col: str, name_col: str, value: str) -> int:
    """Upsert a dimension row and return its surrogate id."""
    cur.execute(f"SELECT {id_col} FROM {table} WHERE {name_col} = %s", (value,))
    row = cur.fetchone()
    if row:
        return row[0]

    cur.execute(
        f"INSERT INTO {table} ({name_col}) VALUES (%s) RETURNING {id_col}",
        (value,),
    )
    return cur.fetchone()[0]


def load_jobs(df: pd.DataFrame) -> dict:
    """
    Load a clean jobs DataFrame into the warehouse.

    Returns a dict of run stats (used for the pipeline_run_log entry).
    """
    if df.empty:
        logger.warning("No rows to load — skipping load step")
        return {"rows_loaded": 0}

    conn = get_connection()
    cur = conn.cursor()
    rows_loaded = 0

    try:
        for _, row in df.iterrows():
            company_id = _get_or_create_id(
                cur, "dim_company", "company_id", "company_name",
                row.get("company_name") or "Unknown"
            )
            location_id = _get_or_create_id(
                cur, "dim_location", "location_id", "location_name",
                row.get("location_name") or "Not Specified"
            )

            def _clean(val):
                return None if pd.isna(val) else val

            cur.execute(
                """
                INSERT INTO fact_jobs
                    (job_id, title, company_id, location_id, category, job_type,
                     salary_min, salary_max, salary_currency, published_date, source)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (job_id) DO UPDATE SET
                    title = EXCLUDED.title,
                    company_id = EXCLUDED.company_id,
                    location_id = EXCLUDED.location_id,
                    category = EXCLUDED.category,
                    job_type = EXCLUDED.job_type,
                    salary_min = EXCLUDED.salary_min,
                    salary_max = EXCLUDED.salary_max,
                    salary_currency = EXCLUDED.salary_currency,
                    published_date = EXCLUDED.published_date
                """,
                (
                    int(row["job_id"]), _clean(row["title"]), company_id, location_id,
                    _clean(row.get("category")), _clean(row.get("job_type")),
                    _clean(row.get("salary_min")), _clean(row.get("salary_max")),
                    _clean(row.get("salary_currency")), _clean(row.get("published_date")),
                    "remotive",
                ),
            )

            # Skills bridge table
            skills = row.get("skills") or []
            for skill in skills:
                skill_id = _get_or_create_id(
                    cur, "dim_skill", "skill_id", "skill_name", skill
                )
                cur.execute(
                    """
                    INSERT INTO fact_job_skills (job_id, skill_id)
                    VALUES (%s, %s)
                    ON CONFLICT DO NOTHING
                    """,
                    (int(row["job_id"]), skill_id),
                )

            rows_loaded += 1

        conn.commit()
        logger.info(f"Loaded {rows_loaded} rows into fact_jobs")

    except Exception as e:
        conn.rollback()
        logger.error(f"Load failed, rolled back transaction: {e}")
        raise
    finally:
        cur.close()
        conn.close()

    return {"rows_loaded": rows_loaded}


def log_pipeline_run(stats: dict):
    """Write a monitoring row to pipeline_run_log."""
    conn = get_connection()
    cur = conn.cursor()
    try:
        cur.execute(
            """
            INSERT INTO pipeline_run_log
                (rows_extracted, rows_loaded, null_title_count, duplicate_count, status, error_message)
            VALUES (%s, %s, %s, %s, %s, %s)
            """,
            (
                stats.get("rows_extracted", 0),
                stats.get("rows_loaded", 0),
                stats.get("null_title_count", 0),
                stats.get("duplicate_count", 0),
                stats.get("status", "SUCCESS"),
                stats.get("error_message"),
            ),
        )
        conn.commit()
        logger.info("Pipeline run logged for monitoring")
    finally:
        cur.close()
        conn.close()


if __name__ == "__main__":
    from pathlib import Path

    clean_path = Path(__file__).parent.parent / "data" / "clean_jobs.csv"
    df = pd.read_csv(clean_path)
    # skills column comes back as a string repr of a list from CSV; eval safely
    df["skills"] = df["skills"].apply(eval)

    stats = load_jobs(df)
    log_pipeline_run(stats)
