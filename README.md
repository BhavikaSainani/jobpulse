# 📊 JobPulse — Automated Job Market Intelligence Pipeline

[![Python 3.10+](https://img.shields.io/badge/python-3.10+-blue.svg)](https://www.python.org/downloads/)
[![PostgreSQL 16+](https://img.shields.io/badge/PostgreSQL-16+-336791.svg?logo=postgresql&logoColor=white)](https://www.postgresql.org/)
[![Apache Airflow](https://img.shields.io/badge/Apache%20Airflow-2.8+-017CEE.svg?logo=apache-airflow&logoColor=white)](https://airflow.apache.org/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

**JobPulse** is an end-to-end automated data engineering pipeline that extracts remote job listings daily from public APIs, cleans and transforms the unstructured data, loads it into an analytical **PostgreSQL Star-Schema Warehouse**, and surfaces actionable insights regarding in-demand skills, salary distributions, and hiring trends via SQL analytics.

---

## 🏗️ System Architecture

```text
┌────────────────┐      ┌─────────────────┐      ┌──────────────────┐      ┌─────────────────────────┐
│  Remotive API  │ ───► │  Extract Stage  │ ───► │ Transform Stage  │ ───► │  PostgreSQL Warehouse   │
│ (Job Listings) │      │  (extract.py)   │      │  (transform.py)  │      │      (Star Schema)      │
└────────────────┘      └─────────────────┘      └──────────────────┘      └────────────┬────────────┘
                                                                                        │
                             ┌───────────────────────────────────────┐                  │
                             │  Apache Airflow DAG (Daily Schedule,  │ ◄────────────────┘
                             │  Retries, Monitoring via Run Log)     │
                             └───────────────────────────────────────┘
                                                 │
                                       ┌─────────▼─────────┐
                                       │   SQL Analytics   │
                                       │   & BI Insights   │
                                       └───────────────────┘
```

---

## 🌟 Key Features

- **Automated Extraction**: Fetches job listings reliably from the [Remotive Public API](https://remotive.com/api-documentation) and lands raw JSON payloads with timestamped audit logs.
- **Data Transformation & Cleansing**:
  - Deduplicates records based on unique job IDs.
  - Regex-based free-text salary parsing into structured `salary_min`, `salary_max`, and `salary_currency`.
  - Normalizes location descriptions and standardizes missing metadata.
  - Keyword-based skill extraction tagging technical competencies (Python, React, Docker, Kubernetes, AWS, SQL, Machine Learning, etc.).
- **Star Schema Warehouse Design**:
  - **Fact Table**: `fact_jobs`
  - **Dimension Tables**: `dim_company`, `dim_location`, `dim_skill`
  - **Bridge Table**: `fact_job_skills` (many-to-many relationship mapping)
- **Data Quality & Idempotency**:
  - Implements `ON CONFLICT` upserts so pipeline re-runs are completely idempotent.
  - Converts invalid types and NaNs to SQL `NULL`s.
  - Enforces schema constraints and relational foreign keys with indexing.
- **Pipeline Observability**: Every execution logs rows extracted, rows loaded, duplicate counts, and null counts into `pipeline_run_log`.
- **Orchestration Ready**: Pre-configured Apache Airflow DAG (`dags/jobpulse_dag.py`) for daily execution and failure retry handling.

---

## 🗄️ Star Schema Diagram

```text
       ┌────────────────────────┐             ┌────────────────────────┐
       │      dim_company       │             │      dim_location      │
       ├────────────────────────┤             ├────────────────────────┤
       │ PK  company_id (INT)   │             │ PK  location_id (INT)  │
       │     company_name (TEXT)│             │     location_name(TEXT)│
       └───────────┬────────────┘             └───────────┬────────────┘
                   │                                      │
                   │ 1                                    │ 1
                   │                                      │
                   │ ∞                                    │ ∞
       ┌───────────┴──────────────────────────────────────┴────────────┐
       │                           fact_jobs                           │
       ├───────────────────────────────────────────────────────────────┤
       │ PK  job_id              (BIGINT)                              │
       │     title               (TEXT)                                │
       │ FK  company_id          (INTEGER)                             │
       │ FK  location_id         (INTEGER)                             │
       │     category            (TEXT)                                │
       │     job_type            (TEXT)                                │
       │     salary_min          (NUMERIC)                             │
       │     salary_max          (NUMERIC)                             │
       │     salary_currency     (TEXT)                                │
       │     published_date      (DATE)                                │
       │     ingested_at         (TIMESTAMP)                           │
       │     source              (TEXT)                                │
       └───────────────────────────────┬───────────────────────────────┘
                                       │ 1
                                       │
                                       │ ∞
       ┌───────────────────────────────┴───────────────┐
       │                fact_job_skills                │
       ├───────────────────────────────────────────────┤
       │ PK, FK  job_id          (BIGINT)              │
       │ PK, FK  skill_id        (INTEGER)             │
       └───────────────────────────────┬───────────────┘
                                       │ ∞
                                       │
                                       │ 1
       ┌───────────────────────────────┴───────────────┐
       │                   dim_skill                   │
       ├───────────────────────────────────────────────┤
       │ PK  skill_id            (INT)                 │
       │     skill_name          (TEXT)                │
       └───────────────────────────────────────────────┘
```

---

## 📁 Repository Structure

```text
jobpulse/
├── dags/
│   └── jobpulse_dag.py         # Apache Airflow DAG for daily scheduling & retries
├── data/
│   └── raw/                    # Raw JSON landing zone (.gitkeep preserved)
├── scripts/
│   ├── extract.py              # Pulls listings from Remotive API to data/raw/
│   ├── transform.py            # Data cleaning, salary parsing, skill tagging
│   ├── load.py                 # Upserts to Postgres star schema & logs runs
│   ├── pipeline.py             # End-to-end pipeline runner
│   └── run_analytics.py        # Executes SQL analytics queries & prints tables
├── sql/
│   ├── schema.sql              # Star-schema DDL and index creation script
│   └── analytics_queries.sql   # SQL queries for skills, salaries & trends
├── .env.example                # Template for database configuration
├── docker-compose.yml          # Postgres service setup
├── requirements.txt            # Python dependencies
└── README.md                   # Project documentation
```

---

## 🚀 Quickstart & Setup

### 1. Clone the Repository
```bash
git clone https://github.com/BhavikaSainani/jobpulse.git
cd jobpulse
```

### 2. Configure Environment Variables
Copy `.env.example` to `.env` and fill in your PostgreSQL credentials:
```bash
cp .env.example .env
```
Example `.env`:
```env
JOBPULSE_DB_HOST=localhost
JOBPULSE_DB_PORT=5432
JOBPULSE_DB_NAME=jobpulse
JOBPULSE_DB_USER=postgres
JOBPULSE_DB_PASSWORD=your_password
```

### 3. Start PostgreSQL
You can run Postgres via Docker or use your local PostgreSQL instance:

**Via Docker:**
```bash
docker compose up -d
```

**Via Local PostgreSQL:**
Make sure PostgreSQL is running, create the `jobpulse` database, and apply the schema:
```bash
psql -U postgres -d jobpulse -f sql/schema.sql
```

### 4. Install Dependencies
```bash
pip install -r requirements.txt
```

---

## ⚡ Running the Pipeline

### Run the Full ETL Pipeline
```bash
python scripts/pipeline.py
```
**Sample Pipeline Execution Output:**
```text
[INFO] === JobPulse pipeline run started ===
[INFO] Extracting jobs from Remotive API (limit=200, category=None)
[INFO] Extracted 16 raw job records
[INFO] Saved raw data to data/raw/jobs_raw_20260818_094947.json
[INFO] Transforming 16 raw job records
[INFO] Removed 0 duplicate records
[INFO] Transform complete: 16 clean rows remaining
[INFO] Loaded 16 rows into fact_jobs
[INFO] Pipeline run logged for monitoring
[INFO] === JobPulse pipeline run finished: {'status': 'SUCCESS', 'rows_extracted': 16, 'rows_loaded': 16} ===
```

### Run Analytics Queries
```bash
python scripts/run_analytics.py
```
Or execute [`sql/analytics_queries.sql`](sql/analytics_queries.sql) via `psql` or any BI tool.

### 🌐 Launch Interactive Web Dashboard
To view live charts, metrics, and filter job postings in your browser:
```bash
python scripts/dashboard.py
```
Then open **[http://localhost:8000](http://localhost:8000)** in your browser.

---

## 📈 Analytics & SQL Insights

Here are examples of business questions answered by the warehouse:

### 1. Top In-Demand Skills
```sql
SELECT s.skill_name, COUNT(*) AS job_count
FROM fact_job_skills fjs
JOIN dim_skill s ON fjs.skill_id = s.skill_id
GROUP BY s.skill_name
ORDER BY job_count DESC
LIMIT 10;
```

### 2. Salary Analysis by Location
```sql
SELECT l.location_name,
       ROUND(AVG((f.salary_min + f.salary_max) / 2.0), 2) AS avg_salary,
       COUNT(*) AS job_count
FROM fact_jobs f
JOIN dim_location l ON f.location_id = l.location_id
WHERE f.salary_min IS NOT NULL AND f.salary_max IS NOT NULL
GROUP BY l.location_name
ORDER BY avg_salary DESC;
```

### 3. Skill Co-occurrence
```sql
SELECT s1.skill_name AS skill_a, s2.skill_name AS skill_b, COUNT(*) AS pair_count
FROM fact_job_skills fjs1
JOIN fact_job_skills fjs2 ON fjs1.job_id = fjs2.job_id AND fjs1.skill_id < fjs2.skill_id
JOIN dim_skill s1 ON fjs1.skill_id = s1.skill_id
JOIN dim_skill s2 ON fjs2.skill_id = s2.skill_id
GROUP BY s1.skill_name, s2.skill_name
ORDER BY pair_count DESC
LIMIT 10;
```

---

## ⏱️ Airflow Orchestration

For automated daily scheduling:
1. Place `dags/jobpulse_dag.py` and the `scripts/` directory into your Airflow DAGs directory.
2. Set the environment variables in your Airflow worker environment.
3. Enable the `jobpulse_daily_etl` DAG from the Airflow web interface.

---

## 💡 Key Engineering Decisions

- **Star Schema vs Flat Table**: Normalizes company, location, and skill dimensions to reduce storage redundancy and provide fast analytical query execution.
- **Idempotency**: All load operations use `ON CONFLICT` upserts to allow safe pipeline reruns without generating duplicate rows.
- **Observability**: The `pipeline_run_log` table tracks execution metrics across runs to surface failures and data quality changes over time.

---

## 📄 License
This project is licensed under the [MIT License](LICENSE).
