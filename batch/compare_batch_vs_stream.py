import sys
from datetime import date

import psycopg2

from config import POSTGRES_DSN


def compare_for_date(target_date: date):
    conn = psycopg2.connect(POSTGRES_DSN)
    cur = conn.cursor()

    cur.execute(
        """
        SELECT
            b.event_type,
            b.total_events AS batch_events,
            COALESCE(s.total_events, 0) AS streaming_events,
            b.total_amount AS batch_amount,
            COALESCE(s.total_amount, 0) AS streaming_amount
        FROM metrics_batch_daily b
        LEFT JOIN metrics_streaming_daily s
          ON s.event_date = b.event_date
         AND s.event_type = b.event_type
        WHERE b.event_date = %s
        ORDER BY b.event_type;
        """,
        (target_date,),
    )

    rows = cur.fetchall()
    cur.close()
    conn.close()

    print(f"Comparison for {target_date.isoformat()}:")
    print(
        "event_type | batch_events | streaming_events | batch_amount | streaming_amount"
    )
    for r in rows:
        print(
            f"{r[0]:<10} | {r[1]:<12} | {r[2]:<16} | {float(r[3]):<12.2f} | {float(r[4]):<16.2f}"
        )


if __name__ == "__main__":
    if len(sys.argv) > 1:
        target = date.fromisoformat(sys.argv[1])
    else:
        target = date.today()
    compare_for_date(target)
