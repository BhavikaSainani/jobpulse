"""
pipeline.py
-----------
End-to-end orchestration of the JobPulse ETL pipeline:

    extract() -> transform() -> load() -> log_pipeline_run()

This is the script Airflow's DAG calls (see dags/jobpulse_dag.py), and is
also runnable standalone for local testing:

    python scripts/pipeline.py
"""

import logging
from extract import extract_jobs, save_raw
from transform import transform_jobs
from load import load_jobs, log_pipeline_run

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)


def run_pipeline(limit: int = 200):
    logger.info("=== JobPulse pipeline run started ===")

    stats = {"status": "SUCCESS", "error_message": None}

    try:
        # EXTRACT
        raw_jobs = extract_jobs(limit=limit)
        save_raw(raw_jobs)

        # TRANSFORM
        clean_df = transform_jobs(raw_jobs)
        stats["rows_extracted"] = clean_df.attrs.get("rows_extracted", len(raw_jobs))
        stats["duplicate_count"] = clean_df.attrs.get("duplicate_count", 0)
        stats["null_title_count"] = clean_df.attrs.get("null_title_count", 0)

        # --- Simple data-quality validation gate ---
        if clean_df.empty:
            raise ValueError("Transform produced zero clean rows — aborting load")

        # LOAD
        load_stats = load_jobs(clean_df)
        stats["rows_loaded"] = load_stats["rows_loaded"]

    except Exception as e:
        stats["status"] = "FAILED"
        stats["error_message"] = str(e)
        stats.setdefault("rows_extracted", 0)
        stats.setdefault("rows_loaded", 0)
        stats.setdefault("duplicate_count", 0)
        stats.setdefault("null_title_count", 0)
        logger.error(f"Pipeline run FAILED: {e}")
        log_pipeline_run(stats)
        raise

    log_pipeline_run(stats)
    logger.info(f"=== JobPulse pipeline run finished: {stats} ===")
    return stats


if __name__ == "__main__":
    run_pipeline(limit=200)
