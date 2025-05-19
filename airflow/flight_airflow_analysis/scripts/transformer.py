import mysql.connector
import psycopg2
import logging
import os
from datetime import datetime

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def transform_and_compute_kpis():
    try:
        mysql_conn = mysql.connector.connect(
            host="flight_mysql",
            user="user",
            password="password",
            database="flight_staging_db"
        )
        pg_conn = psycopg2.connect(
            host="flight_postgres",
            user="postgres",
            password="postgres",
            database="flight_analytics_db"
        )
        mysql_cursor = mysql_conn.cursor()
        pg_cursor = pg_conn.cursor()

        # Compute Total Fare if missing
        mysql_cursor.execute("""
        UPDATE raw_flight_prices
        SET total_fare = base_fare + tax_surcharge
        WHERE total_fare IS NULL AND is_valid = TRUE
        """)
        mysql_conn.commit()

        # Transfer valid data to PostgreSQL
        mysql_cursor.execute("""
        SELECT airline, source, destination, base_fare, tax_surcharge, total_fare,
               booking_date, travel_date
        FROM raw_flight_prices
        WHERE is_valid = TRUE
        """)
        records = mysql_cursor.fetchall()

        insert_query = """
        INSERT INTO analytics.flight_prices (
            airline, source, destination, base_fare, tax_surcharge, total_fare,
            booking_date, travel_date
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """
        psycopg2.extras.execute_batch(pg_cursor, insert_query, records)
        pg_conn.commit()
        logging.info(f"Transferred {len(records)} valid records to PostgreSQL")

        # Compute KPIs
        calculation_date = datetime.now().date()

        # Average Fare by Airline
        pg_cursor.execute("""
        INSERT INTO analytics.kpi_metrics (
            metric_name, metric_value, airline, calculation_date
        )
        SELECT 'avg_fare', AVG(total_fare), airline, %s
        FROM analytics.flight_prices
        GROUP BY airline
        """, (calculation_date,))

        # Seasonal Fare Variation (Eid: Apr-May, Winter: Dec-Jan)
        pg_cursor.execute("""
        INSERT INTO analytics.kpi_metrics (
            metric_name, metric_value, season, calculation_date
        )
        SELECT 'seasonal_fare', AVG(total_fare), 
               CASE 
                   WHEN EXTRACT(MONTH FROM travel_date) IN (4, 5) THEN 'Eid'
                   WHEN EXTRACT(MONTH FROM travel_date) IN (12, 1) THEN 'Winter'
                   ELSE 'Off-Season'
               END, %s
        FROM analytics.flight_prices
        GROUP BY CASE 
                   WHEN EXTRACT(MONTH FROM travel_date) IN (4, 5) THEN 'Eid'
                   WHEN EXTRACT(MONTH FROM travel_date) IN (12, 1) THEN 'Winter'
                   ELSE 'Off-Season'
               END
        """, (calculation_date,))

        # Booking Count by Airline
        pg_cursor.execute("""
        INSERT INTO analytics.kpi_metrics (
            metric_name, metric_value, airline, calculation_date
        )
        SELECT 'booking_count', COUNT(*), airline, %s
        FROM analytics.flight_prices
        GROUP BY airline
        """, (calculation_date,))

        # Most Popular Routes
        pg_cursor.execute("""
        INSERT INTO analytics.kpi_metrics (
            metric_name, metric_value, source, destination, calculation_date
        )
        SELECT 'route_popularity', COUNT(*), source, destination, %s
        FROM analytics.flight_prices
        GROUP BY source, destination
        ORDER BY COUNT(*) DESC
        LIMIT 5
        """, (calculation_date,))

        pg_conn.commit()
        logging.info("Computed and stored KPIs")
    except Exception as e:
        logging.error(f"Error in transformation/KPI computation: {e}")
        raise
    finally:
        mysql_cursor.close()
        pg_cursor.close()
        mysql_conn.close()
        pg_conn.close()