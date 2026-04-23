"""
alert_on_drift.py

Queries the observability tables and sends Slack alerts when:
  - dq_rowcount_drift has DRIFT_DETECTED rows
  - dq_table_freshness has STALE rows
  - schema_snapshots has COLUMN_ADDED or TYPE_CHANGED rows

Usage:
    export SLACK_WEBHOOK_URL="https://hooks.slack.com/services/..."
    python3 scripts/alert_on_drift.py
"""

import os
import requests
import psycopg2
from datetime import date

# --- Config ---
DB_CONFIG = {
    "host":     os.getenv("DB_HOST",     "localhost"),
    "port":     int(os.getenv("DB_PORT", "5432")),
    "dbname":   os.getenv("DB_NAME",     "nyc_taxi"),
    "user":     os.getenv("DB_USER",     "postgres"),
    "password": os.getenv("DB_PASSWORD", "postgres"),
}
WEBHOOK_URL = os.getenv("SLACK_WEBHOOK_URL")
TODAY = date.today().isoformat()


def send_slack(message: str, is_error: bool = True) -> None:
    if not WEBHOOK_URL:
        print(f"[alert] No SLACK_WEBHOOK_URL set. Message: {message}")
        return
    icon = ":red_circle:" if is_error else ":large_green_circle:"
    payload = {"text": f"{icon} *NYC Taxi Observability*\n{message}"}
    try:
        resp = requests.post(WEBHOOK_URL, json=payload, timeout=5)
        resp.raise_for_status()
        print(f"[alert] Slack message sent.")
    except Exception as e:
        print(f"[alert] Failed to send Slack message: {e}")


def check_alerts(conn) -> list[str]:
    alerts = []
    cur = conn.cursor()

    # Check row count drift
    cur.execute("""
        SELECT table_name, current_count, avg_7d_count, pct_change
        FROM observability.dq_rowcount_drift
        WHERE status = 'DRIFT_DETECTED'
          AND checked_date = %s
    """, (TODAY,))
    for row in cur.fetchall():
        table, current, avg7d, pct = row
        alerts.append(
            f"Row count drift detected in `{table}`: "
            f"today={current:,}, 7d avg={avg7d:,.0f}, change={pct:+.1f}%"
        )

    # Check freshness
    cur.execute("""
        SELECT table_name, hours_since_last_record
        FROM observability.dq_table_freshness
        WHERE status = 'STALE'
          AND checked_at::date = %s
    """, (TODAY,))
    for row in cur.fetchall():
        table, hours = row
        alerts.append(
            f"Stale table detected: `{table}` — last record {hours:.1f} hours ago (threshold: 25h)"
        )

    # Check schema changes
    cur.execute("""
        SELECT table_schema, table_name, column_name, change_type
        FROM observability.schema_snapshots
        WHERE change_type != 'UNCHANGED'
          AND snapshot_date = %s
    """, (TODAY,))
    for row in cur.fetchall():
        schema, table, column, change = row
        alerts.append(
            f"Schema change in `{schema}.{table}`: column `{column}` — {change}"
        )

    cur.close()
    return alerts


def main():
    print(f"[alert] Running observability alert check for {TODAY}...")
    try:
        conn = psycopg2.connect(**DB_CONFIG)
    except Exception as e:
        send_slack(f"Could not connect to database: {e}")
        raise

    alerts = check_alerts(conn)
    conn.close()

    if alerts:
        print(f"[alert] {len(alerts)} alert(s) found:")
        for a in alerts:
            print(f"  - {a}")
        message = f"*{len(alerts)} issue(s) detected on {TODAY}:*\n" + "\n".join(f"• {a}" for a in alerts)
        send_slack(message, is_error=True)
    else:
        print("[alert] All checks passed. No alerts.")
        send_slack(f"All observability checks passed for {TODAY}.", is_error=False)


if __name__ == "__main__":
    main()
