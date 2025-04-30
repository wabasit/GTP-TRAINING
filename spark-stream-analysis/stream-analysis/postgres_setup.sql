-- Create database
CREATE DATABASE ecommerce_db
    WITH 
    OWNER = postgres
    ENCODING = 'UTF8'
    CONNECTION LIMIT = -1;

-- Connect to the database
\c ecommerce_db

-- Main events table with partitioning by event_time
CREATE TABLE user_events (
    event_id UUID PRIMARY KEY,
    user_id INT NOT NULL,
    product_id VARCHAR(20) NOT NULL,
    event_type VARCHAR(20) CHECK (event_type IN ('view', 'purchase')),
    event_time TIMESTAMP NOT NULL,
    price NUMERIC(10,2),
    category VARCHAR(50),
    user_device VARCHAR(20),
    user_location CHAR(3),
    processed_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
) PARTITION BY RANGE (event_time);

-- Optimize for common queries
CREATE INDEX idx_user_id ON user_events(user_id);
CREATE INDEX idx_event_time ON user_events USING BRIN(event_time);
CREATE INDEX idx_category ON user_events(category);

-- Error handling table
CREATE TABLE error_events (
    error_id SERIAL PRIMARY KEY,
    error_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    raw_data JSONB,
    error_message TEXT,
    batch_id BIGINT
);

-- Connection optimization
ALTER SYSTEM SET max_connections = 100;
ALTER SYSTEM SET shared_buffers = '4GB';
ALTER SYSTEM SET work_mem = '16MB';