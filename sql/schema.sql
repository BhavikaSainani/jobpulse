-- JobPulse Data Warehouse Schema (Star Schema)
-- fact_jobs is the central fact table; dim_* are dimension/reference tables

CREATE TABLE IF NOT EXISTS dim_company (
    company_id      SERIAL PRIMARY KEY,
    company_name    TEXT UNIQUE NOT NULL
);

CREATE TABLE IF NOT EXISTS dim_location (
    location_id     SERIAL PRIMARY KEY,
    location_name   TEXT UNIQUE NOT NULL
);

CREATE TABLE IF NOT EXISTS dim_skill (
    skill_id        SERIAL PRIMARY KEY,
    skill_name      TEXT UNIQUE NOT NULL
);

CREATE TABLE IF NOT EXISTS fact_jobs (
    job_id          BIGINT PRIMARY KEY,        -- source API's unique job id
    title           TEXT NOT NULL,
    company_id      INTEGER REFERENCES dim_company(company_id),
    location_id     INTEGER REFERENCES dim_location(location_id),
    category        TEXT,
    job_type        TEXT,                      -- full_time / contract / etc.
    salary_min       NUMERIC,
    salary_max       NUMERIC,
    salary_currency   TEXT,
    published_date   DATE,
    ingested_at       TIMESTAMP DEFAULT NOW(),
    source           TEXT DEFAULT 'remotive'
);

-- Many-to-many bridge: a job can require multiple skills
CREATE TABLE IF NOT EXISTS fact_job_skills (
    job_id      BIGINT REFERENCES fact_jobs(job_id) ON DELETE CASCADE,
    skill_id    INTEGER REFERENCES dim_skill(skill_id),
    PRIMARY KEY (job_id, skill_id)
);

-- Helpful indexes for the analytics queries we'll run
CREATE INDEX IF NOT EXISTS idx_fact_jobs_location ON fact_jobs(location_id);
CREATE INDEX IF NOT EXISTS idx_fact_jobs_company ON fact_jobs(company_id);
CREATE INDEX IF NOT EXISTS idx_fact_jobs_published ON fact_jobs(published_date);

-- Simple pipeline run log for monitoring (row counts, null checks, etc.)
CREATE TABLE IF NOT EXISTS pipeline_run_log (
    run_id          SERIAL PRIMARY KEY,
    run_started_at  TIMESTAMP DEFAULT NOW(),
    rows_extracted  INTEGER,
    rows_loaded     INTEGER,
    null_title_count INTEGER,
    duplicate_count INTEGER,
    status          TEXT,   -- SUCCESS / FAILED
    error_message   TEXT
);
