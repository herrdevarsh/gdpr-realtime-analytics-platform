import json
from datetime import datetime
from uuid import UUID

import psycopg2
from kafka import KafkaConsumer

from config import POSTGRES_DSN, KAFKA_BOOTSTRAP_SERVERS, EVENTS_TOPIC


def main():
    consumer = KafkaConsumer(
        EVENTS_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        group_id="raw_events_consumer",
        auto_offset_reset="earliest",
        enable_auto_commit=True,
    )

    conn = psycopg2.connect(POSTGRES_DSN)
    cur = conn.cursor()

    print("Consuming events and writing to events_raw...")

    try:
        for msg in consumer:
            event = msg.value
            event_id = UUID(event["event_id"])
            user_id = UUID(event["user_id"])
            event_type = event["event_type"]
            event_timestamp = datetime.fromisoformat(
                event["event_timestamp"].replace("Z", "+00:00")
            )
            amount = float(event.get("amount") or 0.0)
            metadata = json.dumps(event.get("metadata") or {})

            cur.execute(
                """
                INSERT INTO events_raw (
                    event_id, user_id, event_type, event_timestamp, amount, metadata
                )
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (event_id) DO NOTHING;
                """,
                (str(event_id), str(user_id), event_type, event_timestamp, amount, metadata),
            )
            conn.commit()
            print(f"Stored raw event {event_id} type={event_type}")
    finally:
        cur.close()
        conn.close()


if __name__ == "__main__":
    main()
