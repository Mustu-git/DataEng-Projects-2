# Observability Runbook

How to respond when the monitoring tables flag an issue.

---

## Alert 1: `dq_table_freshness` shows STALE

**What it means:** A table hasn't received new data in more than 25 hours.
The pipeline likely failed silently or the ingestion job didn't run.

**How to investigate:**
```sql
-- See which tables are stale and by how much
SELECT table_name, last_record_ts, hours_since_last_record
FROM observability.dq_table_freshness
WHERE freshness_status = 'STALE'
ORDER BY hours_since_last_record DESC;
```

**Steps to resolve:**
1. Check if the Prefect flow ran — look at the Prefect logs or run `python3 orchestration/pipeline.py`
2. Check if Docker is running — `docker ps` should show `taxi_postgres`
3. If the container is down — `docker compose -f docker/docker-compose.yml up -d`
4. Re-run the pipeline — `python3 orchestration/pipeline.py`
5. Re-run the observability models — `dbt run` from `monitoring/dbt_observability/`

---

## Alert 2: `dq_rowcount_drift` shows DRIFT_DETECTED

**What it means:** Today's row count is more than 20% above or below the 7-day average.
Could mean duplicate ingestion (spike) or missed records (drop).

**How to investigate:**
```sql
-- See which tables drifted and by how much
SELECT
    table_name,
    checked_date,
    row_count,
    avg_7day_row_count,
    round(
        (row_count - avg_7day_row_count) / nullif(avg_7day_row_count, 0) * 100,
        1
    ) as pct_change
FROM observability.dq_rowcount_drift
WHERE drift_status = 'DRIFT_DETECTED'
ORDER BY checked_date DESC;
```

**If row count dropped (possible missed records):**
1. Check the source Parquet file was downloaded — `ls data/`
2. Check ingest logs for errors
3. Re-run `python3 src/ingest_raw.py`

**If row count spiked (possible duplicate ingestion):**
1. Check if `ingest_raw.py` ran twice — look at timestamps in `raw.taxi_trips`
2. `ingest_raw.py` uses `if_exists="replace"` so duplicates shouldn't happen — if they do, check if the schema changed

---

## Alert 3: `dq_run_results` shows failures

**What it means:** One or more dbt tests failed on the last run.

**How to investigate:**
```sql
-- See which tests failed
SELECT test_name, model_name, column_name, failures, run_at
FROM observability.dq_run_results
WHERE status = 'fail'
ORDER BY run_at DESC;
```

**Steps to resolve:**
1. Run dbt tests directly to see the full error:
   ```bash
   cd ~/DataEng-Projects-1/warehouse/dbt_taxi
   dbt test --select <model_name>
   ```
2. Query the failing column for nulls or unexpected values:
   ```sql
   SELECT <column_name>, count(*)
   FROM staging.<model_name>
   WHERE <column_name> IS NULL
   GROUP BY 1;
   ```
3. Fix the upstream data issue or update the test threshold
4. Re-run `python3 scripts/load_dbt_run_results.py` to refresh the results table

---

## Alert 4: `schema_snapshots` shows COLUMN_ADDED or TYPE_CHANGED

**What it means:** A column appeared, disappeared, or changed type since yesterday.
This can silently break downstream models that depend on that column.

**How to investigate:**
```sql
-- See what changed
SELECT table_name, column_name, data_type, schema_change, snapshot_date
FROM observability.schema_snapshots
WHERE schema_change != 'UNCHANGED'
AND schema_change != 'BASELINE'
ORDER BY snapshot_date DESC;
```

**Steps to resolve:**
1. If `COLUMN_ADDED` — check if any downstream models need updating to use the new column
2. If `TYPE_CHANGED` — find all models that cast or use that column and verify they still work
3. If a column was removed — this is critical, find every model that references it:
   ```bash
   grep -r "column_name" ~/DataEng-Projects-1/warehouse/dbt_taxi/models/
   ```
4. Run `dbt run && dbt test` to confirm nothing is broken

---

## Daily health check (run all observability models)

```bash
# From Project 1 — run tests and capture results
cd ~/DataEng-Projects-1/warehouse/dbt_taxi
dbt test

# Load test results into observability DB
cd ~/DataEng-Projects-2
python3 scripts/load_dbt_run_results.py

# Run observability models
cd monitoring/dbt_observability
dbt run

# Quick status check
PGPASSWORD=taxi_pass psql -h localhost -p 5433 -U taxi_user -d taxi_db -c "
SELECT 'freshness' as check, freshness_status as status, table_name as detail
FROM observability.dq_table_freshness
UNION ALL
SELECT 'rowcount', drift_status, table_name
FROM observability.dq_rowcount_drift
WHERE checked_date = current_date
UNION ALL
SELECT 'tests', status, count(*)::text
FROM observability.dq_run_results
WHERE run_date = current_date
GROUP BY status;"
```
