"""
Reads dbt's run_results.json after a dbt test run and loads
actual pass/fail results into observability.dq_run_results.

Usage:
    python3 scripts/load_dbt_run_results.py

Run this after: dbt test (in Project 1's dbt_taxi directory)
"""

import json
import re
from datetime import datetime, timezone
from pathlib import Path
from sqlalchemy import create_engine, text

DB_URL = "postgresql://taxi_user:taxi_pass@localhost:5433/taxi_db"
RUN_RESULTS_PATH = Path.home() / "DataEng-Projects-1/warehouse/dbt_taxi/target/run_results.json"

CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS observability.dq_run_results (
    run_date        date,
    run_at          timestamptz,
    project         text,
    test_name       text,
    test_type       text,
    model_name      text,
    column_name     text,
    status          text,
    failures        integer,
    execution_time  numeric
);
"""

def parse_unique_id(unique_id: str) -> dict:
    """Extract test_type, model_name, column_name from dbt unique_id."""
    # Format: test.project.test_type_model_column.hash
    parts = unique_id.split(".")
    test_full = parts[2] if len(parts) > 2 else unique_id

    # Common test prefixes
    for prefix in ["not_null", "unique", "accepted_values", "relationships"]:
        if test_full.startswith(prefix):
            remainder = test_full[len(prefix) + 1:]
            # remainder is model_column or just model
            model_parts = remainder.split("_")
            model_name = model_parts[0] if model_parts else remainder
            column_name = "_".join(model_parts[1:]) if len(model_parts) > 1 else None
            return {"test_type": prefix, "model_name": model_name, "column_name": column_name}

    return {"test_type": "custom", "model_name": test_full, "column_name": None}


def load_results():
    with open(RUN_RESULTS_PATH) as f:
        data = json.load(f)

    run_at = datetime.fromisoformat(
        data["metadata"]["generated_at"].replace("Z", "+00:00")
    )
    run_date = run_at.date()
    project = data["metadata"].get("dbt_schema_version", "dbt_taxi")

    rows = []
    for result in data["results"]:
        parsed = parse_unique_id(result["unique_id"])
        rows.append({
            "run_date": run_date,
            "run_at": run_at,
            "project": "dbt_taxi",
            "test_name": result["unique_id"].split(".")[2],
            "test_type": parsed["test_type"],
            "model_name": parsed["model_name"],
            "column_name": parsed["column_name"],
            "status": result["status"],
            "failures": result.get("failures", 0),
            "execution_time": round(result.get("execution_time", 0), 4),
        })

    engine = create_engine(DB_URL)
    with engine.begin() as conn:
        conn.execute(text(CREATE_TABLE_SQL))
        # Remove today's rows before inserting (idempotent)
        conn.execute(
            text("DELETE FROM observability.dq_run_results WHERE run_date = :d"),
            {"d": run_date}
        )
        conn.execute(
            text("""
                INSERT INTO observability.dq_run_results
                    (run_date, run_at, project, test_name, test_type,
                     model_name, column_name, status, failures, execution_time)
                VALUES
                    (:run_date, :run_at, :project, :test_name, :test_type,
                     :model_name, :column_name, :status, :failures, :execution_time)
            """),
            rows
        )

    print(f"Loaded {len(rows)} test results for {run_date}")
    passed = sum(1 for r in rows if r["status"] == "pass")
    failed = sum(1 for r in rows if r["status"] == "fail")
    print(f"  PASS: {passed}  FAIL: {failed}")


if __name__ == "__main__":
    load_results()
