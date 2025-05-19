CREATE DATABASE IF NOT EXISTS flight_staging_db;
USE flight_staging_db;

CREATE TABLE IF NOT EXISTS raw_flight_prices (
    id INT AUTO_INCREMENT PRIMARY KEY,
    airline VARCHAR(100),
    source VARCHAR(100),
    destination VARCHAR(100),
    base_fare DECIMAL(10, 2),
    tax_surcharge DECIMAL(10, 2),
    total_fare DECIMAL(10, 2),
    booking_date DATE,
    travel_date DATE,
    is_valid BOOLEAN DEFAULT TRUE,
    validation_error VARCHAR(255)
);