from datetime import datetime

import psycopg2

from config import POSTGRES_DSN


def main():
    conn = psycopg2.connect(POSTGRES_DSN)
    cur = conn.cursor()

    # total requests
    cur.execute("SELECT COUNT(*) FROM privacy_requests;")
    total_requests = cur.fetchone()[0]

    # processed vs pending
    cur.execute("SELECT COUNT(*) FROM privacy_requests WHERE processed_at IS NOT NULL;")
    processed = cur.fetchone()[0]

    cur.execute("SELECT COUNT(*) FROM privacy_requests WHERE processed_at IS NULL;")
    pending = cur.fetchone()[0]

    # average processing time
    cur.execute(
        """
        SELECT AVG(processed_at - requested_at)
        FROM privacy_requests
        WHERE processed_at IS NOT NULL;
        """
    )
    avg_delta = cur.fetchone()[0]
    avg_hours = None
    if avg_delta is not None:
        # avg_delta is a timedelta
        avg_hours = avg_delta.total_seconds() / 3600.0

    # number of anonymized users
    cur.execute("SELECT COUNT(*) FROM users_pii WHERE email IS NULL;")
    anonymized_users = cur.fetchone()[0]

    cur.close()
    conn.close()

    print("\n=== GDPR / Privacy Audit Report ===\n")
    print(f"- Total privacy requests: {total_requests}")
    print(f"- Processed requests:    {processed}")
    print(f"- Pending requests:      {pending}")
    if avg_hours is not None:
        print(f"- Avg processing time:   {avg_hours:.2f} hours")
    else:
        print("- Avg processing time:   N/A (no processed requests yet)")
    print(f"- Users with anonymized PII: {anonymized_users}\n")


if __name__ == "__main__":
    main()
