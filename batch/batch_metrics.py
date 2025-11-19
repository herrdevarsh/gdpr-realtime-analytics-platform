import sys
from datetime import date, datetime, timedelta

import psycopg2

from config import POSTGRES_DSN


def compute_for_date(target_date: date):
    start = datetime.combine(target_date, datetime.min.time())
    end = start + timedelta(days=1)

    conn = psycopg2.connect(POSTGRES_DSN)
    cur = conn.cursor()

    cur.execute(
        """
        INSERT INTO metrics_batch_daily (
            event_date, event_type, total_events, total_amount
        )
        SELECT
            DATE(event_timestamp) AS event_date,
            event_type,
            COUNT(*) AS total_events,
            COALESCE(SUM(amount), 0) AS total_amount
        FROM events_raw
        WHERE event_timestamp >= %s
          AND event_timestamp < %s
        GROUP BY 1, 2
        ON CONFLICT (event_date, event_type)
        DO UPDATE SET
            total_events = EXCLUDED.total_events,
            total_amount = EXCLUDED.total_amount;
        """,
        (start, end),
    )

    conn.commit()
    cur.close()
    conn.close()
    print(f"Batch metrics computed for {target_date.isoformat()}")


if __name__ == "__main__":
    if len(sys.argv) > 1:
        target = date.fromisoformat(sys.argv[1])
    else:
        target = date.today()
    compute_for_date(target)
