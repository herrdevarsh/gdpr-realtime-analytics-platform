import json
import random
import time
import uuid
from datetime import datetime, timezone

import psycopg2
from kafka import KafkaProducer

from config import POSTGRES_DSN, KAFKA_BOOTSTRAP_SERVERS, EVENTS_TOPIC


def load_user_ids():
    conn = psycopg2.connect(POSTGRES_DSN)
    cur = conn.cursor()
    cur.execute("SELECT user_id FROM users_public;")
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return [r[0] for r in rows]


def create_producer():
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )


def generate_event(user_id: str):
    event_type = random.choices(
        ["page_view", "add_to_cart", "purchase"],
        weights=[0.7, 0.2, 0.1],
    )[0]

    now = datetime.now(timezone.utc)
    amount = 0.0

    if event_type == "purchase":
        amount = round(random.uniform(5, 200), 2)

    metadata = {}
    if event_type == "page_view":
        metadata["page"] = random.choice(
            ["/home", "/product", "/search", "/cart", "/checkout"]
        )

    return {
        "event_id": str(uuid.uuid4()),
        "user_id": user_id,
        "event_type": event_type,
        "event_timestamp": now.isoformat(),
        "amount": amount,
        "metadata": metadata,
    }


def main(events_per_second: float = 5.0):
    user_ids = load_user_ids()
    if not user_ids:
        raise RuntimeError("No users found. Run generate_users.py first.")

    producer = create_producer()
    delay = 1.0 / events_per_second

    print(f"Producing events to topic '{EVENTS_TOPIC}'...")
    while True:
        user_id = random.choice(user_ids)
        event = generate_event(user_id)
        producer.send(EVENTS_TOPIC, value=event)
        print(f"Produced event {event['event_id']} type={event['event_type']}")
        time.sleep(delay)


if __name__ == "__main__":
    main()
