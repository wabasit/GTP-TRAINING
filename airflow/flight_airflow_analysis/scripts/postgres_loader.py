import psycopg2
import logging
import os

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def verify_postgres_data():
    try:
        conn = psycopg2.connect(
            host="flight_postgres",
            user="postgres",
            password="postgres",
            database="flight_analytics_db"
        )
        cursor = conn.cursor()

        cursor.execute("SELECT COUNT(*) FROM analytics.flight_prices")
        flight_count = cursor.fetchone()[0]
        cursor.execute("SELECT COUNT(*) FROM analytics.kpi_metrics")
        kpi_count = cursor.fetchone()[0]
        logging.info(f"PostgreSQL verification: {flight_count} flight records, {kpi_count} KPI records")
    except Exception as e:
        logging.error(f"Error verifying PostgreSQL data: {e}")
        raise
    finally:
        cursor.close()
        conn.close()