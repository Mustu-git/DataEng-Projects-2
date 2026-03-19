# Data Quality + Observability Layer

A production-style data quality monitoring system built on top of the NYC Taxi pipeline.
Tracks freshness, row count drift, test results, and schema changes — the layer that tells you when your pipeline breaks silently.

## What this builds

| Table | Purpose |
|-------|---------|
| `dq_run_results` | Log of every dbt test — pass/fail per model per run |
| `dq_table_freshness` | Flags tables that haven't received new data recently |
| `dq_rowcount_drift` | Flags sudden drops or spikes in row volume |
| `schema_snapshots` | Tracks column-level schema changes over time |

## Stack
- PostgreSQL 15 (shared with Project 1)
- dbt 1.11.7
- Python 3.11
- Prefect 3

## How to run
```bash
# Coming soon
```
