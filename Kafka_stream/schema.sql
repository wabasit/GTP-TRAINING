CREATE TABLE IF NOT EXISTS heartbeats (
    id SERIAL PRIMARY KEY,
    customer_id INTEGER,
    timestamp BIGINT,
    heart_rate INTEGER,
    anomaly BOOLEAN DEFAULT FALSE
);
CREATE INDEX idx_timestamp ON heartbeats (timestamp);