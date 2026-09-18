"""
dashboard.py
------------
JobPulse Interactive Web Dashboard.
Serves a modern web analytics UI and REST API for job market intelligence.
"""

import os
import sys
import logging
from pathlib import Path
from fastapi import FastAPI, HTTPException
from fastapi.responses import HTMLResponse, JSONResponse
import uvicorn
import psycopg2
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv

# Ensure scripts directory is in sys.path
sys.path.insert(0, str(Path(__file__).parent))
from pipeline import run_pipeline

load_dotenv(Path(__file__).parent.parent / ".env")
load_dotenv()

app = FastAPI(title="JobPulse Dashboard", version="1.0.0")

def get_db():
    return psycopg2.connect(
        host=os.environ.get("JOBPULSE_DB_HOST", "localhost"),
        port=os.environ.get("JOBPULSE_DB_PORT", "5432"),
        dbname=os.environ.get("JOBPULSE_DB_NAME", "jobpulse"),
        user=os.environ.get("JOBPULSE_DB_USER", "postgres"),
        password=os.environ.get("JOBPULSE_DB_PASSWORD", "postgres"),
        cursor_factory=RealDictCursor
    )

@app.get("/api/summary")
def get_summary():
    conn = get_db()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) AS total_jobs FROM fact_jobs;")
            total_jobs = cur.fetchone()["total_jobs"]

            cur.execute("SELECT COUNT(*) AS total_companies FROM dim_company;")
            total_companies = cur.fetchone()["total_companies"]

            cur.execute("SELECT COUNT(*) AS total_skills FROM dim_skill;")
            total_skills = cur.fetchone()["total_skills"]

            cur.execute("""
                SELECT ROUND(AVG((salary_min + salary_max) / 2.0), 0) AS avg_salary
                FROM fact_jobs
                WHERE salary_min IS NOT NULL AND salary_max IS NOT NULL;
            """)
            avg_salary_row = cur.fetchone()
            avg_salary = avg_salary_row["avg_salary"] if avg_salary_row and avg_salary_row["avg_salary"] else None

            cur.execute("""
                SELECT run_id, run_started_at, rows_extracted, rows_loaded, status
                FROM pipeline_run_log
                ORDER BY run_started_at DESC
                LIMIT 1;
            """)
            last_run = cur.fetchone()

            return {
                "total_jobs": total_jobs,
                "total_companies": total_companies,
                "total_skills": total_skills,
                "avg_salary": avg_salary,
                "last_run": last_run
            }
    finally:
        conn.close()

@app.get("/api/skills")
def get_skills():
    conn = get_db()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT s.skill_name, COUNT(*) AS count
                FROM fact_job_skills fjs
                JOIN dim_skill s ON fjs.skill_id = s.skill_id
                GROUP BY s.skill_name
                ORDER BY count DESC
                LIMIT 12;
            """)
            return cur.fetchall()
    finally:
        conn.close()

@app.get("/api/companies")
def get_companies():
    conn = get_db()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT c.company_name, COUNT(*) AS count
                FROM fact_jobs f
                JOIN dim_company c ON f.company_id = c.company_id
                GROUP BY c.company_name
                ORDER BY count DESC
                LIMIT 8;
            """)
            return cur.fetchall()
    finally:
        conn.close()

@app.get("/api/trends")
def get_trends():
    conn = get_db()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT TO_CHAR(published_date, 'YYYY-MM-DD') AS date, COUNT(*) AS count
                FROM fact_jobs
                WHERE published_date IS NOT NULL
                GROUP BY published_date
                ORDER BY published_date ASC;
            """)
            return cur.fetchall()
    finally:
        conn.close()

@app.get("/api/salaries")
def get_salaries():
    conn = get_db()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT l.location_name,
                       ROUND(AVG((f.salary_min + f.salary_max) / 2.0), 2) AS avg_salary,
                       COUNT(*) AS count
                FROM fact_jobs f
                JOIN dim_location l ON f.location_id = l.location_id
                WHERE f.salary_min IS NOT NULL AND f.salary_max IS NOT NULL
                GROUP BY l.location_name
                ORDER BY avg_salary DESC
                LIMIT 8;
            """)
            return cur.fetchall()
    finally:
        conn.close()

@app.get("/api/cooccurrence")
def get_cooccurrence():
    conn = get_db()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT s1.skill_name AS skill_a, s2.skill_name AS skill_b, COUNT(*) AS count
                FROM fact_job_skills fjs1
                JOIN fact_job_skills fjs2 ON fjs1.job_id = fjs2.job_id AND fjs1.skill_id < fjs2.skill_id
                JOIN dim_skill s1 ON fjs1.skill_id = s1.skill_id
                JOIN dim_skill s2 ON fjs2.skill_id = s2.skill_id
                GROUP BY s1.skill_name, s2.skill_name
                ORDER BY count DESC
                LIMIT 8;
            """)
            return cur.fetchall()
    finally:
        conn.close()

@app.get("/api/jobs")
def get_jobs(search: str = "", limit: int = 50):
    conn = get_db()
    try:
        with conn.cursor() as cur:
            query = """
                SELECT 
                    f.job_id,
                    f.title,
                    c.company_name,
                    l.location_name,
                    f.category,
                    f.job_type,
                    f.salary_min,
                    f.salary_max,
                    f.salary_currency,
                    TO_CHAR(f.published_date, 'YYYY-MM-DD') AS published_date,
                    COALESCE(
                        (SELECT ARRAY_AGG(s.skill_name)
                         FROM fact_job_skills fjs
                         JOIN dim_skill s ON fjs.skill_id = s.skill_id
                         WHERE fjs.job_id = f.job_id), 
                        ARRAY[]::TEXT[]
                    ) AS skills
                FROM fact_jobs f
                LEFT JOIN dim_company c ON f.company_id = c.company_id
                LEFT JOIN dim_location l ON f.location_id = l.location_id
                WHERE 1=1
            """
            params = []
            if search:
                query += " AND (f.title ILIKE %s OR c.company_name ILIKE %s OR l.location_name ILIKE %s)"
                search_param = f"%{search}%"
                params.extend([search_param, search_param, search_param])
            query += " ORDER BY f.published_date DESC NULLS LAST LIMIT %s;"
            params.append(limit)

            cur.execute(query, tuple(params))
            return cur.fetchall()
    finally:
        conn.close()

@app.get("/api/logs")
def get_logs():
    conn = get_db()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT run_id, TO_CHAR(run_started_at, 'YYYY-MM-DD HH24:MI:SS') AS run_time,
                       rows_extracted, rows_loaded, duplicate_count, null_title_count, status, error_message
                FROM pipeline_run_log
                ORDER BY run_started_at DESC
                LIMIT 10;
            """)
            return cur.fetchall()
    finally:
        conn.close()

@app.post("/api/run-pipeline")
def trigger_pipeline():
    try:
        stats = run_pipeline(limit=200)
        return {"success": True, "stats": stats}
    except Exception as e:
        return JSONResponse(status_code=500, content={"success": False, "error": str(e)})

@app.get("/", response_class=HTMLResponse)
def index():
    html_content = """<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>JobPulse — Job Market Intelligence Dashboard</title>
    <link rel="preconnect" href="https://fonts.googleapis.com">
    <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
    <link href="https://fonts.googleapis.com/css2?family=Plus+Jakarta+Sans:wght@300;400;500;600;700;800&display=swap" rel="stylesheet">
    <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
    <link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.4.0/css/all.min.css">
    <style>
        :root {
            --bg-primary: #0b0f19;
            --bg-secondary: #111827;
            --bg-card: rgba(17, 24, 39, 0.7);
            --border-color: rgba(255, 255, 255, 0.08);
            --border-hover: rgba(99, 102, 241, 0.4);
            --primary: #6366f1;
            --primary-gradient: linear-gradient(135deg, #6366f1 0%, #a855f7 100%);
            --accent-cyan: #06b6d4;
            --accent-emerald: #10b981;
            --accent-amber: #f59e0b;
            --accent-rose: #f43f5e;
            --text-main: #f3f4f6;
            --text-muted: #9ca3af;
            --text-subtle: #6b7280;
            --glass-shadow: 0 8px 32px 0 rgba(0, 0, 0, 0.37);
        }

        * {
            box-sizing: border-box;
            margin: 0;
            padding: 0;
            font-family: 'Plus Jakarta Sans', -apple-system, BlinkMacSystemFont, sans-serif;
        }

        body {
            background-color: var(--bg-primary);
            color: var(--text-main);
            min-height: 100vh;
            display: flex;
            flex-direction: column;
            overflow-x: hidden;
            background-image: 
                radial-gradient(at 0% 0%, rgba(99, 102, 241, 0.12) 0px, transparent 50%),
                radial-gradient(at 100% 100%, rgba(168, 85, 247, 0.1) 0px, transparent 50%);
        }

        /* Header */
        header {
            background: rgba(11, 15, 25, 0.85);
            backdrop-filter: blur(12px);
            border-bottom: 1px solid var(--border-color);
            position: sticky;
            top: 0;
            z-index: 100;
            padding: 1rem 2rem;
            display: flex;
            align-items: center;
            justify-content: space-between;
        }

        .brand {
            display: flex;
            align-items: center;
            gap: 0.75rem;
        }

        .brand-icon {
            width: 40px;
            height: 40px;
            background: var(--primary-gradient);
            border-radius: 10px;
            display: flex;
            align-items: center;
            justify-content: center;
            font-size: 1.25rem;
            color: #fff;
            box-shadow: 0 0 15px rgba(99, 102, 241, 0.5);
        }

        .brand-title h1 {
            font-size: 1.35rem;
            font-weight: 700;
            letter-spacing: -0.02em;
            background: linear-gradient(to right, #ffffff, #cbd5e1);
            -webkit-background-clip: text;
            -webkit-text-fill-color: transparent;
        }

        .brand-title p {
            font-size: 0.75rem;
            color: var(--text-muted);
            font-weight: 500;
        }

        .header-actions {
            display: flex;
            align-items: center;
            gap: 1rem;
        }

        .btn {
            display: inline-flex;
            align-items: center;
            gap: 0.5rem;
            padding: 0.6rem 1.2rem;
            border-radius: 8px;
            font-size: 0.875rem;
            font-weight: 600;
            cursor: pointer;
            transition: all 0.2s ease;
            border: 1px solid transparent;
            text-decoration: none;
        }

        .btn-primary {
            background: var(--primary-gradient);
            color: #fff;
            box-shadow: 0 4px 12px rgba(99, 102, 241, 0.3);
        }

        .btn-primary:hover {
            transform: translateY(-1px);
            box-shadow: 0 6px 16px rgba(99, 102, 241, 0.45);
        }

        .btn-secondary {
            background: rgba(255, 255, 255, 0.05);
            color: var(--text-main);
            border-color: var(--border-color);
        }

        .btn-secondary:hover {
            background: rgba(255, 255, 255, 0.1);
            border-color: var(--border-hover);
        }

        .btn:disabled {
            opacity: 0.6;
            cursor: not-allowed;
            transform: none !important;
        }

        /* Container */
        .main-content {
            max-width: 1440px;
            margin: 0 auto;
            padding: 2rem;
            width: 100%;
            display: flex;
            flex-direction: column;
            gap: 2rem;
        }

        /* Metric Grid */
        .metrics-grid {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(240px, 1fr));
            gap: 1.25rem;
        }

        .metric-card {
            background: var(--bg-card);
            border: 1px solid var(--border-color);
            border-radius: 16px;
            padding: 1.5rem;
            backdrop-filter: blur(16px);
            box-shadow: var(--glass-shadow);
            position: relative;
            overflow: hidden;
            transition: all 0.3s cubic-bezier(0.4, 0, 0.2, 1);
        }

        .metric-card:hover {
            transform: translateY(-3px);
            border-color: var(--border-hover);
            box-shadow: 0 12px 24px -10px rgba(99, 102, 241, 0.3);
        }

        .metric-header {
            display: flex;
            align-items: center;
            justify-content: space-between;
            margin-bottom: 0.75rem;
        }

        .metric-title {
            font-size: 0.85rem;
            color: var(--text-muted);
            font-weight: 600;
            text-transform: uppercase;
            letter-spacing: 0.05em;
        }

        .metric-icon {
            width: 38px;
            height: 38px;
            border-radius: 10px;
            display: flex;
            align-items: center;
            justify-content: center;
            font-size: 1rem;
        }

        .icon-purple { background: rgba(99, 102, 241, 0.15); color: #818cf8; }
        .icon-cyan { background: rgba(6, 182, 212, 0.15); color: #22d3ee; }
        .icon-emerald { background: rgba(16, 185, 129, 0.15); color: #34d399; }
        .icon-amber { background: rgba(245, 158, 11, 0.15); color: #fbbf24; }

        .metric-value {
            font-size: 2rem;
            font-weight: 800;
            color: #ffffff;
            letter-spacing: -0.03em;
        }

        .metric-subtitle {
            font-size: 0.8rem;
            color: var(--text-subtle);
            margin-top: 0.25rem;
            display: flex;
            align-items: center;
            gap: 0.35rem;
        }

        .badge-status {
            padding: 0.2rem 0.6rem;
            border-radius: 9999px;
            font-size: 0.75rem;
            font-weight: 700;
            display: inline-flex;
            align-items: center;
            gap: 0.3rem;
        }
        .badge-success { background: rgba(16, 185, 129, 0.2); color: #34d399; border: 1px solid rgba(16, 185, 129, 0.4); }

        /* Charts Grid */
        .charts-grid {
            display: grid;
            grid-template-columns: repeat(12, 1fr);
            gap: 1.5rem;
        }

        .chart-card {
            background: var(--bg-card);
            border: 1px solid var(--border-color);
            border-radius: 16px;
            padding: 1.5rem;
            backdrop-filter: blur(16px);
            box-shadow: var(--glass-shadow);
        }

        .col-8 { grid-column: span 8; }
        .col-4 { grid-column: span 4; }
        .col-6 { grid-column: span 6; }
        .col-12 { grid-column: span 12; }

        @media (max-width: 1024px) {
            .col-8, .col-4, .col-6 { grid-column: span 12; }
        }

        .card-header {
            display: flex;
            align-items: center;
            justify-content: space-between;
            margin-bottom: 1.25rem;
        }

        .card-title {
            font-size: 1.05rem;
            font-weight: 700;
            color: #ffffff;
            display: flex;
            align-items: center;
            gap: 0.5rem;
        }

        .card-subtitle {
            font-size: 0.8rem;
            color: var(--text-muted);
        }

        .chart-container {
            position: relative;
            height: 280px;
            width: 100%;
        }

        /* Co-occurrence chips list */
        .cooccur-list {
            display: flex;
            flex-direction: column;
            gap: 0.75rem;
            height: 280px;
            overflow-y: auto;
        }

        .cooccur-item {
            display: flex;
            align-items: center;
            justify-content: space-between;
            padding: 0.75rem 1rem;
            background: rgba(255, 255, 255, 0.03);
            border: 1px solid rgba(255, 255, 255, 0.05);
            border-radius: 10px;
            transition: 0.2s ease;
        }

        .cooccur-item:hover {
            background: rgba(255, 255, 255, 0.06);
            border-color: var(--border-hover);
        }

        .pair-tags {
            display: flex;
            align-items: center;
            gap: 0.5rem;
        }

        .skill-tag {
            background: rgba(99, 102, 241, 0.18);
            color: #a5b4fc;
            padding: 0.25rem 0.6rem;
            border-radius: 6px;
            font-size: 0.75rem;
            font-weight: 600;
            border: 1px solid rgba(99, 102, 241, 0.3);
        }

        .pair-count {
            font-size: 0.85rem;
            font-weight: 700;
            color: var(--accent-cyan);
        }

        /* Tables section */
        .table-section {
            background: var(--bg-card);
            border: 1px solid var(--border-color);
            border-radius: 16px;
            padding: 1.5rem;
            backdrop-filter: blur(16px);
            box-shadow: var(--glass-shadow);
        }

        .table-controls {
            display: flex;
            align-items: center;
            justify-content: space-between;
            gap: 1rem;
            margin-bottom: 1.25rem;
            flex-wrap: wrap;
        }

        .search-box {
            position: relative;
            min-width: 280px;
        }

        .search-box input {
            width: 100%;
            background: rgba(255, 255, 255, 0.04);
            border: 1px solid var(--border-color);
            border-radius: 8px;
            padding: 0.6rem 1rem 0.6rem 2.25rem;
            color: #fff;
            font-size: 0.875rem;
            outline: none;
            transition: 0.2s;
        }

        .search-box input:focus {
            border-color: var(--primary);
            box-shadow: 0 0 0 2px rgba(99, 102, 241, 0.25);
        }

        .search-box i {
            position: absolute;
            left: 0.8rem;
            top: 50%;
            transform: translateY(-50%);
            color: var(--text-subtle);
            font-size: 0.85rem;
        }

        .custom-table {
            width: 100%;
            border-collapse: collapse;
            font-size: 0.875rem;
        }

        .custom-table th {
            text-align: left;
            padding: 0.85rem 1rem;
            color: var(--text-muted);
            font-weight: 600;
            border-bottom: 1px solid var(--border-color);
            background: rgba(0, 0, 0, 0.2);
            font-size: 0.8rem;
            text-transform: uppercase;
            letter-spacing: 0.05em;
        }

        .custom-table td {
            padding: 1rem;
            border-bottom: 1px solid rgba(255, 255, 255, 0.04);
            color: var(--text-main);
            vertical-align: middle;
        }

        .custom-table tr:hover td {
            background: rgba(255, 255, 255, 0.02);
        }

        .job-title-col {
            font-weight: 600;
            color: #ffffff;
        }

        .company-pill {
            display: inline-flex;
            align-items: center;
            gap: 0.35rem;
            font-weight: 500;
            color: #e2e8f0;
        }

        .salary-badge {
            background: rgba(16, 185, 129, 0.15);
            color: #34d399;
            padding: 0.25rem 0.5rem;
            border-radius: 6px;
            font-weight: 600;
            font-size: 0.8rem;
            display: inline-block;
        }

        .skills-cell {
            display: flex;
            flex-wrap: wrap;
            gap: 0.35rem;
        }

        /* Logs Table */
        .logs-section {
            background: var(--bg-card);
            border: 1px solid var(--border-color);
            border-radius: 16px;
            padding: 1.5rem;
            backdrop-filter: blur(16px);
        }

        /* Toast notification */
        #toast {
            position: fixed;
            bottom: 2rem;
            right: 2rem;
            background: rgba(17, 24, 39, 0.95);
            border: 1px solid var(--primary);
            padding: 1rem 1.5rem;
            border-radius: 12px;
            box-shadow: 0 10px 30px rgba(0, 0, 0, 0.5);
            backdrop-filter: blur(12px);
            color: #fff;
            font-size: 0.9rem;
            display: flex;
            align-items: center;
            gap: 0.75rem;
            transform: translateY(150%);
            transition: transform 0.3s cubic-bezier(0.4, 0, 0.2, 1);
            z-index: 1000;
        }

        #toast.show {
            transform: translateY(0);
        }

        /* Footer */
        footer {
            margin-top: auto;
            border-top: 1px solid var(--border-color);
            padding: 1.5rem 2rem;
            text-align: center;
            color: var(--text-subtle);
            font-size: 0.8rem;
        }
    </style>
</head>
<body>

    <header>
        <div class="brand">
            <div class="brand-icon">
                <i class="fa-solid fa-bolt"></i>
            </div>
            <div class="brand-title">
                <h1>JobPulse</h1>
                <p>Automated Job Market Intelligence & Warehouse Analytics</p>
            </div>
        </div>
        <div class="header-actions">
            <button class="btn btn-secondary" onclick="loadAllData()" id="refreshBtn">
                <i class="fa-solid fa-arrows-rotate"></i> Refresh
            </button>
            <button class="btn btn-primary" onclick="triggerETL()" id="etlBtn">
                <i class="fa-solid fa-play"></i> Trigger ETL Run
            </button>
        </div>
    </header>

    <div class="main-content">
        <!-- Top KPI Cards -->
        <div class="metrics-grid">
            <div class="metric-card">
                <div class="metric-header">
                    <span class="metric-title">Total Ingested Jobs</span>
                    <div class="metric-icon icon-purple"><i class="fa-solid fa-briefcase"></i></div>
                </div>
                <div class="metric-value" id="totalJobs">--</div>
                <div class="metric-subtitle">
                    <i class="fa-solid fa-database"></i> Fact Jobs Table
                </div>
            </div>

            <div class="metric-card">
                <div class="metric-header">
                    <span class="metric-title">Hiring Companies</span>
                    <div class="metric-icon icon-cyan"><i class="fa-solid fa-building"></i></div>
                </div>
                <div class="metric-value" id="totalCompanies">--</div>
                <div class="metric-subtitle">
                    <i class="fa-solid fa-network-wired"></i> Unique Remote Employers
                </div>
            </div>

            <div class="metric-card">
                <div class="metric-header">
                    <span class="metric-title">Skills Tracked</span>
                    <div class="metric-icon icon-emerald"><i class="fa-solid fa-code"></i></div>
                </div>
                <div class="metric-value" id="totalSkills">--</div>
                <div class="metric-subtitle">
                    <i class="fa-solid fa-tags"></i> Tagged Tech Competencies
                </div>
            </div>

            <div class="metric-card">
                <div class="metric-header">
                    <span class="metric-title">Pipeline Health</span>
                    <div class="metric-icon icon-amber"><i class="fa-solid fa-heart-pulse"></i></div>
                </div>
                <div class="metric-value" id="pipelineStatus">
                    <span class="badge-status badge-success">ACTIVE</span>
                </div>
                <div class="metric-subtitle" id="lastRunTime">
                    Last Run: Checking...
                </div>
            </div>
        </div>

        <!-- Charts Row 1 -->
        <div class="charts-grid">
            <div class="chart-card col-8">
                <div class="card-header">
                    <div>
                        <div class="card-title"><i class="fa-solid fa-chart-column" style="color:#818cf8;"></i> Top In-Demand Tech Skills</div>
                        <div class="card-subtitle">Frequency of skills extracted from remote job descriptions</div>
                    </div>
                </div>
                <div class="chart-container">
                    <canvas id="skillsChart"></canvas>
                </div>
            </div>

            <div class="chart-card col-4">
                <div class="card-header">
                    <div>
                        <div class="card-title"><i class="fa-solid fa-diagram-project" style="color:#22d3ee;"></i> Skill Co-occurrence</div>
                        <div class="card-subtitle">Pairs most frequently required together</div>
                    </div>
                </div>
                <div class="cooccur-list" id="cooccurList">
                    <!-- Populated via JS -->
                </div>
            </div>
        </div>

        <!-- Charts Row 2 -->
        <div class="charts-grid">
            <div class="chart-card col-6">
                <div class="card-header">
                    <div>
                        <div class="card-title"><i class="fa-solid fa-chart-line" style="color:#34d399;"></i> Job Postings Velocity</div>
                        <div class="card-subtitle">Number of job postings over publication dates</div>
                    </div>
                </div>
                <div class="chart-container">
                    <canvas id="trendChart"></canvas>
                </div>
            </div>

            <div class="chart-card col-6">
                <div class="card-header">
                    <div>
                        <div class="card-title"><i class="fa-solid fa-building-user" style="color:#f59e0b;"></i> Top Hiring Companies</div>
                        <div class="card-subtitle">Companies with most active remote openings</div>
                    </div>
                </div>
                <div class="chart-container">
                    <canvas id="companiesChart"></canvas>
                </div>
            </div>
        </div>

        <!-- Job Listings Table -->
        <div class="table-section">
            <div class="table-controls">
                <div>
                    <h2 class="card-title"><i class="fa-solid fa-table-list" style="color:#818cf8;"></i> Live Job Market Catalog</h2>
                    <p class="card-subtitle">Cleaned and structured records from PostgreSQL fact_jobs</p>
                </div>
                <div class="search-box">
                    <i class="fa-solid fa-magnifying-glass"></i>
                    <input type="text" id="searchInput" placeholder="Search by title, company, location..." onkeyup="debounceSearch()">
                </div>
            </div>

            <div style="overflow-x: auto;">
                <table class="custom-table">
                    <thead>
                        <tr>
                            <th>Job Title</th>
                            <th>Company</th>
                            <th>Location</th>
                            <th>Category</th>
                            <th>Salary</th>
                            <th>Extracted Skills</th>
                            <th>Published</th>
                        </tr>
                    </thead>
                    <tbody id="jobsTableBody">
                        <tr><td colspan="7" style="text-align: center; color: var(--text-muted); padding: 2rem;">Loading jobs...</td></tr>
                    </tbody>
                </table>
            </div>
        </div>

        <!-- Pipeline Run History -->
        <div class="logs-section">
            <div class="card-header">
                <div>
                    <h2 class="card-title"><i class="fa-solid fa-clock-rotate-left" style="color:#a855f7;"></i> Pipeline Run Audit Logs</h2>
                    <p class="card-subtitle">Automated ETL execution telemetry stored in pipeline_run_log</p>
                </div>
            </div>
            <div style="overflow-x: auto;">
                <table class="custom-table">
                    <thead>
                        <tr>
                            <th>Run ID</th>
                            <th>Timestamp</th>
                            <th>Status</th>
                            <th>Extracted</th>
                            <th>Loaded</th>
                            <th>Duplicates</th>
                            <th>Null Titles</th>
                        </tr>
                    </thead>
                    <tbody id="logsTableBody">
                        <tr><td colspan="7" style="text-align: center; color: var(--text-muted); padding: 2rem;">Loading audit logs...</td></tr>
                    </tbody>
                </table>
            </div>
        </div>
    </div>

    <!-- Toast -->
    <div id="toast">
        <i class="fa-solid fa-circle-check" style="color: #34d399; font-size: 1.2rem;"></i>
        <span id="toastMsg">Action completed successfully</span>
    </div>

    <footer>
        <p>JobPulse Data Pipeline &bull; Star Schema PostgreSQL &bull; Remotive Public API &bull; Built with FastAPI & Chart.js</p>
    </footer>

    <script>
        let skillsChartInstance = null;
        let trendChartInstance = null;
        let companiesChartInstance = null;

        function showToast(msg, isError = false) {
            const toast = document.getElementById("toast");
            const toastMsg = document.getElementById("toastMsg");
            const icon = toast.querySelector("i");
            
            toastMsg.innerText = msg;
            if (isError) {
                icon.className = "fa-solid fa-circle-xmark";
                icon.style.color = "#f43f5e";
                toast.style.borderColor = "#f43f5e";
            } else {
                icon.className = "fa-solid fa-circle-check";
                icon.style.color = "#34d399";
                toast.style.borderColor = "var(--primary)";
            }
            toast.classList.add("show");
            setTimeout(() => toast.classList.remove("show"), 4000);
        }

        async function fetchSummary() {
            try {
                const res = await fetch("/api/summary");
                const data = await res.json();
                document.getElementById("totalJobs").innerText = data.total_jobs;
                document.getElementById("totalCompanies").innerText = data.total_companies;
                document.getElementById("totalSkills").innerText = data.total_skills;

                if (data.last_run) {
                    document.getElementById("pipelineStatus").innerHTML = 
                        `<span class="badge-status badge-success">${data.last_run.status}</span>`;
                    const dateStr = new Date(data.last_run.run_started_at).toLocaleString();
                    document.getElementById("lastRunTime").innerText = `Last Run: ${dateStr}`;
                }
            } catch (e) {
                console.error("Error fetching summary:", e);
            }
        }

        async function fetchSkillsChart() {
            try {
                const res = await fetch("/api/skills");
                const data = await res.json();
                const labels = data.map(d => d.skill_name.toUpperCase());
                const counts = data.map(d => d.count);

                const ctx = document.getElementById("skillsChart").getContext("2d");
                if (skillsChartInstance) skillsChartInstance.destroy();

                skillsChartInstance = new Chart(ctx, {
                    type: "bar",
                    data: {
                        labels: labels,
                        datasets: [{
                            label: "Mentions in Jobs",
                            data: counts,
                            backgroundColor: "rgba(99, 102, 241, 0.75)",
                            hoverBackgroundColor: "rgba(168, 85, 247, 0.9)",
                            borderRadius: 6,
                            borderWidth: 0
                        }]
                    },
                    options: {
                        responsive: true,
                        maintainAspectRatio: false,
                        plugins: { legend: { display: false } },
                        scales: {
                            x: { grid: { display: false }, ticks: { color: "#9ca3af" } },
                            y: { grid: { color: "rgba(255,255,255,0.05)" }, ticks: { color: "#9ca3af", stepSize: 1 } }
                        }
                    }
                });
            } catch (e) {
                console.error("Error fetching skills chart:", e);
            }
        }

        async function fetchCooccurrence() {
            try {
                const res = await fetch("/api/cooccurrence");
                const data = await res.json();
                const container = document.getElementById("cooccurList");
                if (data.length === 0) {
                    container.innerHTML = '<div style="color:var(--text-muted);text-align:center;padding:2rem;">No skill pairs available</div>';
                    return;
                }
                container.innerHTML = data.map(item => `
                    <div class="cooccur-item">
                        <div class="pair-tags">
                            <span class="skill-tag">${item.skill_a}</span>
                            <i class="fa-solid fa-plus" style="font-size: 0.65rem; color: var(--text-subtle);"></i>
                            <span class="skill-tag">${item.skill_b}</span>
                        </div>
                        <span class="pair-count">${item.count} jobs</span>
                    </div>
                `).join("");
            } catch (e) {
                console.error("Error fetching cooccurrence:", e);
            }
        }

        async function fetchTrendChart() {
            try {
                const res = await fetch("/api/trends");
                const data = await res.json();
                const labels = data.map(d => d.date);
                const counts = data.map(d => d.count);

                const ctx = document.getElementById("trendChart").getContext("2d");
                if (trendChartInstance) trendChartInstance.destroy();

                trendChartInstance = new Chart(ctx, {
                    type: "line",
                    data: {
                        labels: labels,
                        datasets: [{
                            label: "Jobs Posted",
                            data: counts,
                            borderColor: "#34d399",
                            backgroundColor: "rgba(16, 185, 129, 0.15)",
                            fill: true,
                            tension: 0.35,
                            pointBackgroundColor: "#34d399",
                            pointRadius: 4
                        }]
                    },
                    options: {
                        responsive: true,
                        maintainAspectRatio: false,
                        plugins: { legend: { display: false } },
                        scales: {
                            x: { grid: { display: false }, ticks: { color: "#9ca3af" } },
                            y: { grid: { color: "rgba(255,255,255,0.05)" }, ticks: { color: "#9ca3af", stepSize: 1 } }
                        }
                    }
                });
            } catch (e) {
                console.error("Error fetching trends:", e);
            }
        }

        async function fetchCompaniesChart() {
            try {
                const res = await fetch("/api/companies");
                const data = await res.json();
                const labels = data.map(d => d.company_name);
                const counts = data.map(d => d.count);

                const ctx = document.getElementById("companiesChart").getContext("2d");
                if (companiesChartInstance) companiesChartInstance.destroy();

                companiesChartInstance = new Chart(ctx, {
                    type: "bar",
                    data: {
                        labels: labels,
                        datasets: [{
                            label: "Job Openings",
                            data: counts,
                            backgroundColor: "rgba(245, 158, 11, 0.75)",
                            borderRadius: 6
                        }]
                    },
                    options: {
                        indexAxis: 'y',
                        responsive: true,
                        maintainAspectRatio: false,
                        plugins: { legend: { display: false } },
                        scales: {
                            x: { grid: { color: "rgba(255,255,255,0.05)" }, ticks: { color: "#9ca3af", stepSize: 1 } },
                            y: { grid: { display: false }, ticks: { color: "#9ca3af" } }
                        }
                    }
                });
            } catch (e) {
                console.error("Error fetching companies:", e);
            }
        }

        async function fetchJobs(search = "") {
            try {
                const res = await fetch(`/api/jobs?search=${encodeURIComponent(search)}`);
                const jobs = await res.json();
                const tbody = document.getElementById("jobsTableBody");

                if (jobs.length === 0) {
                    tbody.innerHTML = '<tr><td colspan="7" style="text-align: center; color: var(--text-muted); padding: 2rem;">No matching jobs found</td></tr>';
                    return;
                }

                tbody.innerHTML = jobs.map(j => {
                    let salaryText = '<span style="color:var(--text-subtle);">Unspecified</span>';
                    if (j.salary_min && j.salary_max) {
                        salaryText = `<span class="salary-badge">${j.salary_currency || '$'}${j.salary_min.toLocaleString()} - ${j.salary_max.toLocaleString()}</span>`;
                    }

                    const skillsHtml = j.skills && j.skills.length > 0 
                        ? j.skills.map(s => `<span class="skill-tag">${s}</span>`).join(" ")
                        : '<span style="color:var(--text-subtle);font-size:0.8rem;">-</span>';

                    return `
                        <tr>
                            <td class="job-title-col">${j.title}</td>
                            <td><span class="company-pill"><i class="fa-regular fa-building" style="color:var(--text-subtle);"></i> ${j.company_name || 'Unknown'}</span></td>
                            <td><i class="fa-solid fa-location-dot" style="color:var(--text-subtle);font-size:0.75rem;"></i> ${j.location_name || 'Worldwide'}</td>
                            <td><span style="color:var(--text-muted);font-size:0.85rem;">${j.category || 'General'}</span></td>
                            <td>${salaryText}</td>
                            <td><div class="skills-cell">${skillsHtml}</div></td>
                            <td><span style="color:var(--text-subtle);font-size:0.8rem;">${j.published_date || 'Recent'}</span></td>
                        </tr>
                    `;
                }).join("");
            } catch (e) {
                console.error("Error fetching jobs:", e);
            }
        }

        async function fetchLogs() {
            try {
                const res = await fetch("/api/logs");
                const logs = await res.json();
                const tbody = document.getElementById("logsTableBody");
                if (logs.length === 0) {
                    tbody.innerHTML = '<tr><td colspan="7" style="text-align:center;padding:2rem;">No logs found</td></tr>';
                    return;
                }
                tbody.innerHTML = logs.map(l => `
                    <tr>
                        <td>#${l.run_id}</td>
                        <td style="color:var(--text-muted);">${l.run_time}</td>
                        <td><span class="badge-status badge-success">${l.status}</span></td>
                        <td><strong>${l.rows_extracted}</strong></td>
                        <td><strong>${l.rows_loaded}</strong></td>
                        <td>${l.duplicate_count}</td>
                        <td>${l.null_title_count}</td>
                    </tr>
                `).join("");
            } catch (e) {
                console.error("Error fetching logs:", e);
            }
        }

        let debounceTimer;
        function debounceSearch() {
            clearTimeout(debounceTimer);
            debounceTimer = setTimeout(() => {
                const val = document.getElementById("searchInput").value;
                fetchJobs(val);
            }, 300);
        }

        async function triggerETL() {
            const btn = document.getElementById("etlBtn");
            btn.disabled = true;
            btn.innerHTML = '<i class="fa-solid fa-spinner fa-spin"></i> Running ETL...';
            try {
                const res = await fetch("/api/run-pipeline", { method: "POST" });
                const result = await res.json();
                if (result.success) {
                    showToast(`Pipeline Run Complete: ${result.stats.rows_loaded} jobs loaded!`);
                    loadAllData();
                } else {
                    showToast("Pipeline Run Failed: " + (result.error || "Unknown error"), true);
                }
            } catch (e) {
                showToast("Failed to communicate with ETL worker", true);
            } finally {
                btn.disabled = false;
                btn.innerHTML = '<i class="fa-solid fa-play"></i> Trigger ETL Run';
            }
        }

        function loadAllData() {
            fetchSummary();
            fetchSkillsChart();
            fetchCooccurrence();
            fetchTrendChart();
            fetchCompaniesChart();
            fetchJobs(document.getElementById("searchInput").value);
            fetchLogs();
        }

        // Initialize on load
        window.addEventListener("DOMContentLoaded", () => {
            loadAllData();
        });
    </script>
</body>
</html>
"""
    return HTMLResponse(content=html_content)

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8000))
    print(f"Starting JobPulse Dashboard at http://localhost:{port}")
    uvicorn.run(app, host="0.0.0.0", port=port)
