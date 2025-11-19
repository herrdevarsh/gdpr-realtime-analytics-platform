import psycopg2

from config import POSTGRES_DSN


def process_privacy_requests():
    conn = psycopg2.connect(POSTGRES_DSN)
    cur = conn.cursor()

    # Select unprocessed requests
    cur.execute(
        """
        SELECT id, email
        FROM privacy_requests
        WHERE processed_at IS NULL
        ORDER BY requested_at
        FOR UPDATE;
        """
    )
    requests = cur.fetchall()

    if not requests:
        print("No pending privacy requests.")
        cur.close()
        conn.close()
        return

    for req_id, email in requests:
        # Find user(s) with that email
        cur.execute(
            "SELECT user_id FROM users_pii WHERE email = %s;",
            (email,),
        )
        users = cur.fetchall()

        if not users:
            print(f"[req {req_id}] No user found for email {email}")
        else:
            for (user_id,) in users:
                # Anonymize PII (don't touch analytics tables)
                cur.execute(
                    """
                    UPDATE users_pii
                    SET email = NULL,
                        full_name = NULL,
                        address = NULL
                    WHERE user_id = %s;
                    """,
                    (user_id,),
                )
                print(f"[req {req_id}] Anonymized PII for user_id={user_id}")

        # Mark request as processed
        cur.execute(
            "UPDATE privacy_requests SET processed_at = NOW() WHERE id = %s;",
            (req_id,),
        )

    conn.commit()
    cur.close()
    conn.close()
    print("Finished processing privacy requests.")


if __name__ == "__main__":
    process_privacy_requests()
