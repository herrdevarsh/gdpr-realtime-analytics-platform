import uuid
import random
from datetime import date, timedelta

import psycopg2
from faker import Faker

from config import POSTGRES_DSN


def create_users(n_users: int = 500):
    fake = Faker()
    conn = psycopg2.connect(POSTGRES_DSN)
    cur = conn.cursor()

    countries = ["DE", "FR", "IT", "ES", "NL", "PL", "SE", "AT"]

    for _ in range(n_users):
        user_id = uuid.uuid4()
        email = fake.unique.email()
        full_name = fake.name()
        address = fake.address().replace("\n", ", ")
        country = random.choice(countries)
        signup_date = date.today() - timedelta(days=random.randint(0, 365))

        cur.execute(
            """
            INSERT INTO users_pii (user_id, email, full_name, address)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (user_id) DO NOTHING;
            """,
            (str(user_id), email, full_name, address),
        )

        cur.execute(
            """
            INSERT INTO users_public (user_id, country, signup_date)
            VALUES (%s, %s, %s)
            ON CONFLICT (user_id) DO NOTHING;
            """,
            (str(user_id), country, signup_date),
        )

    conn.commit()
    cur.close()
    conn.close()
    print(f"Inserted {n_users} users.")


if __name__ == "__main__":
    create_users()
