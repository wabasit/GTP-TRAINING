CREATE TABLE IF NOT EXISTS heartbeats (
    id SERIAL PRIMARY KEY,
    customer_id INTEGER,
    timestamp TIMESTAMPTZ NOT NULL,
    heart_rate INTEGER NOT NULL,
    anomaly BOOLEAN DEFAULT FALSE
);
CREATE INDEX idx_timestamp ON heartbeats (timestamp);