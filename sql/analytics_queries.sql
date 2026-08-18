-- JobPulse Analytics Queries
-- Run these against the warehouse to answer real questions from the data.

-- 1. Top 10 most in-demand skills across all job postings
SELECT s.skill_name, COUNT(*) AS job_count
FROM fact_job_skills fjs
JOIN dim_skill s ON fjs.skill_id = s.skill_id
GROUP BY s.skill_name
ORDER BY job_count DESC
LIMIT 10;

-- 2. Average salary by location (only where salary data exists)
SELECT l.location_name,
       ROUND(AVG((f.salary_min + f.salary_max) / 2.0), 2) AS avg_salary,
       COUNT(*) AS job_count
FROM fact_jobs f
JOIN dim_location l ON f.location_id = l.location_id
WHERE f.salary_min IS NOT NULL AND f.salary_max IS NOT NULL
GROUP BY l.location_name
ORDER BY avg_salary DESC
LIMIT 15;

-- 3. Job postings trend over time (by publish date)
SELECT published_date, COUNT(*) AS jobs_posted
FROM fact_jobs
WHERE published_date IS NOT NULL
GROUP BY published_date
ORDER BY published_date DESC
LIMIT 30;

-- 4. Top hiring companies
SELECT c.company_name, COUNT(*) AS job_count
FROM fact_jobs f
JOIN dim_company c ON f.company_id = c.company_id
GROUP BY c.company_name
ORDER BY job_count DESC
LIMIT 10;

-- 5. Skill co-occurrence: which skills are most often requested together
SELECT s1.skill_name AS skill_a, s2.skill_name AS skill_b, COUNT(*) AS pair_count
FROM fact_job_skills fjs1
JOIN fact_job_skills fjs2 ON fjs1.job_id = fjs2.job_id AND fjs1.skill_id < fjs2.skill_id
JOIN dim_skill s1 ON fjs1.skill_id = s1.skill_id
JOIN dim_skill s2 ON fjs2.skill_id = s2.skill_id
GROUP BY s1.skill_name, s2.skill_name
ORDER BY pair_count DESC
LIMIT 10;

-- 6. Pipeline health check — recent run history (for the "monitoring" story)
SELECT run_id, run_started_at, rows_extracted, rows_loaded,
       duplicate_count, null_title_count, status
FROM pipeline_run_log
ORDER BY run_started_at DESC
LIMIT 10;
