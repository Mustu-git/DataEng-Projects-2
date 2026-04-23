# Data Quality + Observability Layer

A production-style data quality monitoring system built on top of the NYC Taxi pipeline (Project 1).
Tracks freshness, row-count drift, dbt test results, and schema changes — the layer that tells you when your pipeline breaks silently.

---

## What this builds

| Table | Purpose |
|-------|---------|
| `dq_run_results` | Log of every dbt test — pass/fail per model per run |
| `dq_table_freshness` | Flags tables that haven't received new data recently (FRESH / STALE) |
| `dq_rowcount_drift` | Flags sudden drops or spikes in row volume (DRIFT_DETECTED / NORMAL) |
| `schema_snapshots` | Tracks column-level schema changes over time (COLUMN_ADDED / TYPE_CHANGED) |

Slack alerts fire automatically when `DRIFT_DETECTED` or `STALE` states are detected.

---

## Stack

| Tool | Purpose |
|---|---|
| PostgreSQL 15 | Shared warehouse with Project 1 (Docker) |
| dbt 1.7 | Observability models (incremental + snapshot) |
| Soda Core | Data quality checks on source tables |
| Python 3.11 | dbt results loader + Slack alerting |
| Prefect 3 | Orchestration |
| GitHub Actions | CI — dbt run + test on every push |

---

## How to run

### Prerequisites
- Project 1 must be set up and running (PostgreSQL + dbt models loaded)
- Slack webhook URL (optional — for alerting)

### 1. Start PostgreSQL (shared with Project 1)
```bash
docker compose -f ../DataEng-Projects-1/docker/docker-compose.yml up -d
```

### 2. Install dependencies
```bash
pip install dbt-postgres prefect soda-core-postgres psycopg2-binary requests
```

### 3. Configure dbt profile
Add to `~/.dbt/profiles.yml`:
```yaml
dbt_observability:
  target: dev
  outputs:
    dev:
      type: postgres
      host: localhost
      port: 5432
      user: postgres
      password: postgres
      dbname: nyc_taxi
      schema: observability
      threads: 4
```

### 4. Run the observability models
```bash
cd monitoring/dbt_observability
dbt deps
dbt run
dbt test
```

### 5. Load dbt test results into dq_run_results
```bash
python3 scripts/load_dbt_run_results.py
```

### 6. Run Soda Core checks
```bash
soda scan -d nyc_taxi -c soda/configuration.yml soda/checks.yml
```

### 7. Run Slack alerting (optional)
```bash
export SLACK_WEBHOOK_URL="https://hooks.slack.com/services/..."
python3 scripts/alert_on_drift.py
```

### 8. Run full pipeline with Prefect
```bash
python3 orchestration/pipeline.py
```

---

## Project structure

```
monitoring/
  dbt_observability/
    models/observability/
      dq_run_results.sql       # dbt test pass/fail log
      dq_table_freshness.sql   # Freshness check (FRESH / STALE)
      dq_rowcount_drift.sql    # Row count drift detection (DRIFT_DETECTED / NORMAL)
      schema_snapshots.sql     # Column-level schema change tracking

scripts/
  load_dbt_run_results.py      # Parses dbt run_results.json → dq_run_results table
  alert_on_drift.py            # Queries observability tables and fires Slack alerts

soda/
  checks.yml                   # Soda Core data quality checks
  configuration.yml            # Soda connection config
```

---

## Alerting logic

`scripts/alert_on_drift.py` queries the observability tables after each run and sends Slack alerts when:

| Condition | Alert |
|---|---|
| `dq_rowcount_drift.status = DRIFT_DETECTED` | Row count changed >20% from 7-day average |
| `dq_table_freshness.status = STALE` | Table hasn't received new data in >25 hours |
| `schema_snapshots.change_type != UNCHANGED` | Column added or type changed |
