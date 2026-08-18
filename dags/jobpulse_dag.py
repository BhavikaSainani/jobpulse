"""
jobpulse_dag.py
----------------
Airflow DAG that orchestrates the JobPulse ETL pipeline daily.

Steps: extract -> transform -> load, each as a separate task so failures
are isolated and retried independently. Airflow handles scheduling,
retries, and gives a UI for monitoring pipeline health.

To use: copy this file (and the scripts/ folder) into your Airflow
DAGs folder (e.g. ~/airflow/dags/), or mount it via docker-compose
(see docker-compose.yml in this repo).
"""

from datetime import datetime, timedelta
import sys
import os

from airflow import DAG
from airflow.operators.python import PythonOperator

# Make scripts/ importable from within the DAG
sys.path.append(os.path.join(os.path.dirname(__file__), "..", "scripts"))

from extract import extract_jobs, save_raw
from transform import transform_jobs
from load import load_jobs, log_pipeline_run

default_args = {
    "owner": "bhavika",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


def _extract_task(**context):
    jobs = extract_jobs(limit=200)
    save_raw(jobs)
    # Push raw jobs to XCom for the next task
    context["ti"].xcom_push(key="raw_jobs", value=jobs)


def _transform_task(**context):
    raw_jobs = context["ti"].xcom_pull(key="raw_jobs", task_ids="extract")
    df = transform_jobs(raw_jobs)

    if df.empty:
        raise ValueError("Transform produced zero clean rows — failing task to trigger alert/retry")

    # Store as records (JSON-serializable) for the next task via XCom
    context["ti"].xcom_push(key="clean_records", value=df.to_dict(orient="records"))
    context["ti"].xcom_push(key="quality_stats", value={
        "rows_extracted": df.attrs.get("rows_extracted", len(raw_jobs)),
        "duplicate_count": df.attrs.get("duplicate_count", 0),
        "null_title_count": df.attrs.get("null_title_count", 0),
    })


def _load_task(**context):
    import pandas as pd

    records = context["ti"].xcom_pull(key="clean_records", task_ids="transform")
    quality_stats = context["ti"].xcom_pull(key="quality_stats", task_ids="transform")
    df = pd.DataFrame(records)

    load_stats = load_jobs(df)

    full_stats = {**quality_stats, **load_stats, "status": "SUCCESS", "error_message": None}
    log_pipeline_run(full_stats)


with DAG(
    dag_id="jobpulse_daily_etl",
    description="Daily ETL pipeline: extract job listings, clean, and load into Postgres warehouse",
    default_args=default_args,
    schedule_interval="@daily",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["jobpulse", "etl", "data-engineering"],
) as dag:

    extract = PythonOperator(
        task_id="extract",
        python_callable=_extract_task,
    )

    transform = PythonOperator(
        task_id="transform",
        python_callable=_transform_task,
    )

    load = PythonOperator(
        task_id="load",
        python_callable=_load_task,
    )

    extract >> transform >> load
