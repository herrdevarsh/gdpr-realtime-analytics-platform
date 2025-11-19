import uuid

import psycopg2
import pytest

from config import POSTGRES_DSN
from gdpr.process_privacy_requests import process_privacy_requests


def test_gdpr_anonymizes_pii():
    # Try to connect to Postgres; if it's not up, skip the test (for CI)
    try:
        conn = psycopg2.connect(POSTGRES_DSN)
    except Exception:
        pytest.skip("Postgres is not available")

    cur = conn.cursor()

    # insert fake user
    user_id = str(uuid.uuid4())
    email = f"test_{user_id[:8]}@example.com"

    cur.execute(
        """
        INSERT INTO users_pii (user_id, email, full_name, address)
        VALUES (%s, %s, 'Test User', 'Some Address')
        ON CONFLICT (user_id) DO NOTHING;
        """,
        (user_id, email),
    )

    cur.execute(
        "INSERT INTO privacy_requests (email) VALUES (%s);",
        (email,),
    )

    conn.commit()
    cur.close()
    conn.close()

    # run processor
    process_privacy_requests()

    # verify anonymized
    conn = psycopg2.connect(POSTGRES_DSN)
    cur = conn.cursor()
    cur.execute(
        "SELECT email, full_name, address FROM users_pii WHERE user_id = %s;",
        (user_id,),
    )
    row = cur.fetchone()
    cur.close()
    conn.close()

    assert row is not None
    assert row[0] is None
    assert row[1] is None
    assert row[2] is None
