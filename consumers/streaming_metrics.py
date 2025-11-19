import json
import time
from collections import defaultdict
from datetime import datetime
from typing import Dict, Tuple

import psycopg2
from kafka import KafkaConsumer

from config import (
    POSTGRES_DSN,
    KAFKA_BOOTSTRAP_SERVERS,
    EVENTS_TOPIC,
    STREAMING_FLUSH_INTERVAL_SECONDS,
    STREAMING_FLUSH_BATCH_SIZE,
)


Key = Tuple[str, str]  # (event_date, event_type)


def flush_to_db(conn, buffer: Dict[Key, Dict[str, float]]):
    if not buffer:
        return

    cur = conn.cursor()
    for (event_date, event_type), agg in buffer.items():
        total_events = int(agg["total_events"])
        total_amount = float(agg["total_amount"])

        cur.execute(
            """
            INSERT INTO metrics_streaming_daily (
                event_date, event_type, total_events, total_amount
            )
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (event_date, event_type)
            DO UPDATE SET
                total_events = metrics_streaming_daily.total_events + EXCLUDED.total_events,
                total_amount = metrics_streaming_daily.total_amount + EXCLUDED.total_amount;
            """,
            (event_date, event_type, total_events, total_amount),
        )
    conn.commit()
    cur.close()
    buffer.clear()
    print("Flushed streaming metrics to DB.")


def main():
    consumer = KafkaConsumer(
        EVENTS_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        group_id="streaming_metrics_consumer",
        auto_offset_reset="earliest",
        enable_auto_commit=True,
    )

    conn = psycopg2.connect(POSTGRES_DSN)

    buffer: Dict[Key, Dict[str, float]] = defaultdict(
        lambda: {"total_events": 0, "total_amount": 0.0}
    )

    last_flush = time.time()
    events_since_flush = 0

    print("Consuming events and updating streaming metrics...")

    try:
        for msg in consumer:
            event = msg.value
            event_type = event["event_type"]
            ts = datetime.fromisoformat(
                event["event_timestamp"].replace("Z", "+00:00")
            )
            event_date = ts.date().isoformat()
            amount = float(event.get("amount") or 0.0)

            key = (event_date, event_type)
            buffer[key]["total_events"] += 1
            buffer[key]["total_amount"] += amount
            events_since_flush += 1

            now = time.time()
            if (
                now - last_flush >= STREAMING_FLUSH_INTERVAL_SECONDS
                or events_since_flush >= STREAMING_FLUSH_BATCH_SIZE
            ):
                flush_to_db(conn, buffer)
                last_flush = now
                events_since_flush = 0
    finally:
        flush_to_db(conn, buffer)
        conn.close()


if __name__ == "__main__":
    main()
