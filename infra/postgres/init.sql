-- PII and public user data
CREATE TABLE IF NOT EXISTS users_pii (
    user_id UUID PRIMARY KEY,
    email TEXT UNIQUE NOT NULL,
    full_name TEXT,
    address TEXT,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS users_public (
    user_id UUID PRIMARY KEY,
    country VARCHAR(2),
    signup_date DATE,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

-- Privacy / GDPR requests
CREATE TABLE IF NOT EXISTS privacy_requests (
    id SERIAL PRIMARY KEY,
    email TEXT NOT NULL,
    requested_at TIMESTAMPTZ DEFAULT NOW(),
    processed_at TIMESTAMPTZ
);

-- Raw events (batch input)
CREATE TABLE IF NOT EXISTS events_raw (
    event_id UUID PRIMARY KEY,
    user_id UUID NOT NULL,
    event_type VARCHAR(50) NOT NULL,
    event_timestamp TIMESTAMPTZ NOT NULL,
    amount NUMERIC(10,2),
    metadata JSONB,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_events_raw_ts ON events_raw(event_timestamp);

-- Streaming metrics (daily)
CREATE TABLE IF NOT EXISTS metrics_streaming_daily (
    event_date DATE NOT NULL,
    event_type VARCHAR(50) NOT NULL,
    total_events BIGINT NOT NULL,
    total_amount NUMERIC(10,2) NOT NULL,
    PRIMARY KEY (event_date, event_type)
);

-- Batch metrics (daily)
CREATE TABLE IF NOT EXISTS metrics_batch_daily (
    event_date DATE NOT NULL,
    event_type VARCHAR(50) NOT NULL,
    total_events BIGINT NOT NULL,
    total_amount NUMERIC(10,2) NOT NULL,
    PRIMARY KEY (event_date, event_type)
);
