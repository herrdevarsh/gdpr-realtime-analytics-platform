# GDPR-Compliant Real-Time Analytics Pipeline

End-to-end data engineering project:

- Real-time stream (Kafka → Python consumer) computing daily metrics
- Batch pipeline aggregating from raw events
- Lambda-style batch vs streaming comparison
- GDPR-aware design with separate PII store and "right to be forgotten" flow

## Tech Stack

- Python (3.12)
- Kafka (event streaming)
- Postgres (OLTP + metrics store)
- Docker Compose (local infra)
- kafka-python-ng, psycopg2

## Architecture

1. **User Data**
   - `users_pii`: PII only (email, name, address)
   - `users_public`: non-PII user attributes (country, signup date)

2. **Streaming Path**
   - Producer sends `user_events` to Kafka
   - `store_raw_events` consumer stores every event to `events_raw`
   - `streaming_metrics` consumer aggregates to `metrics_streaming_daily`

3. **Batch Path**
   - `batch_metrics.py` aggregates from `events_raw` to `metrics_batch_daily`
   - `compare_batch_vs_stream.py` compares batch vs streaming metrics

4. **GDPR / Privacy**
   - `privacy_requests` table stores deletion requests by email
   - `process_privacy_requests.py` anonymizes PII in `users_pii`
   - Analytics tables never store PII (only `user_id`)

## How to Run

```bash
# 1. Start infra
docker compose up -d

# 2. Create venv + install deps
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt

# 3. Seed users
python -m data_generators.generate_users

# 4. Start consumers (separate terminals)
python -m consumers.store_raw_events
python -m consumers.streaming_metrics

# 5. Start event generator
python -m data_generators.generate_events

# 6. Run batch + compare
python -m batch.batch_metrics
python -m batch.compare_batch_vs_stream

# 7. GDPR request (inside Postgres via psql), then:
python -m gdpr.process_privacy_requests
