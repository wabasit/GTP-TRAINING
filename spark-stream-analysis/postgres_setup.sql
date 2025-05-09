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
);

-- Optimize for common queries
CREATE INDEX idx_user_id ON user_events(user_id);
CREATE INDEX idx_category ON user_events(category);