CREATE SCHEMA IF NOT EXISTS analytics;

CREATE TABLE IF NOT EXISTS analytics.flight_prices (
    id SERIAL PRIMARY KEY,
    airline VARCHAR(100) NOT NULL,
    source VARCHAR(100) NOT NULL,
    destination VARCHAR(100) NOT NULL,
    base_fare NUMERIC(10, 2) NOT NULL,
    tax_surcharge NUMERIC(10, 2) NOT NULL,
    total_fare NUMERIC(10, 2) NOT NULL,
    booking_date DATE NOT NULL,
    travel_date DATE NOT NULL
);

CREATE TABLE IF NOT EXISTS analytics.kpi_metrics (
    id SERIAL PRIMARY KEY,
    metric_name VARCHAR(100) NOT NULL,
    metric_value NUMERIC(10, 2),
    airline VARCHAR(100),
    source VARCHAR(100),
    destination VARCHAR(100),
    season VARCHAR(50),
    calculation_date DATE NOT NULL
);