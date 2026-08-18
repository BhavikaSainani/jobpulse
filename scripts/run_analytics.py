import os
from pathlib import Path
from dotenv import load_dotenv
import psycopg2
import pandas as pd
import warnings

warnings.filterwarnings("ignore", category=UserWarning)

load_dotenv(Path(__file__).parent.parent / ".env")

conn = psycopg2.connect(
    host=os.environ.get("JOBPULSE_DB_HOST", "localhost"),
    port=os.environ.get("JOBPULSE_DB_PORT", "5432"),
    dbname=os.environ.get("JOBPULSE_DB_NAME", "jobpulse"),
    user=os.environ.get("JOBPULSE_DB_USER", "postgres"),
    password=os.environ.get("JOBPULSE_DB_PASSWORD", "postgres"),
)

queries = [
    ("1. Top In-Demand Skills", """
        SELECT s.skill_name, COUNT(*) AS job_count
        FROM fact_job_skills fjs
        JOIN dim_skill s ON fjs.skill_id = s.skill_id
        GROUP BY s.skill_name
        ORDER BY job_count DESC
        LIMIT 10;
    """),
    ("2. Average Salary by Location", """
        SELECT l.location_name,
               ROUND(AVG((f.salary_min + f.salary_max) / 2.0), 2) AS avg_salary,
               COUNT(*) AS job_count
        FROM fact_jobs f
        JOIN dim_location l ON f.location_id = l.location_id
        WHERE f.salary_min IS NOT NULL AND f.salary_max IS NOT NULL
        GROUP BY l.location_name
        ORDER BY avg_salary DESC
        LIMIT 10;
    """),
    ("3. Job Postings Trend Over Time", """
        SELECT published_date, COUNT(*) AS jobs_posted
        FROM fact_jobs
        WHERE published_date IS NOT NULL
        GROUP BY published_date
        ORDER BY published_date DESC
        LIMIT 10;
    """),
    ("4. Top Hiring Companies", """
        SELECT c.company_name, COUNT(*) AS job_count
        FROM fact_jobs f
        JOIN dim_company c ON f.company_id = c.company_id
        GROUP BY c.company_name
        ORDER BY job_count DESC
        LIMIT 10;
    """),
    ("5. Skill Co-occurrence (Pairs most frequently requested together)", """
        SELECT s1.skill_name AS skill_a, s2.skill_name AS skill_b, COUNT(*) AS pair_count
        FROM fact_job_skills fjs1
        JOIN fact_job_skills fjs2 ON fjs1.job_id = fjs2.job_id AND fjs1.skill_id < fjs2.skill_id
        JOIN dim_skill s1 ON fjs1.skill_id = s1.skill_id
        JOIN dim_skill s2 ON fjs2.skill_id = s2.skill_id
        GROUP BY s1.skill_name, s2.skill_name
        ORDER BY pair_count DESC
        LIMIT 10;
    """),
    ("6. Pipeline Run Logs (Monitoring)", """
        SELECT run_id, run_started_at, rows_extracted, rows_loaded,
               duplicate_count, null_title_count, status
        FROM pipeline_run_log
        ORDER BY run_started_at DESC
        LIMIT 5;
    """)
]

for title, sql in queries:
    print(f"\n=======================================================")
    print(f" {title}")
    print(f"=======================================================")
    df = pd.read_sql(sql, conn)
    if df.empty:
        print(" (No data matching criteria)")
    else:
        print(df.to_string(index=False))

conn.close()
