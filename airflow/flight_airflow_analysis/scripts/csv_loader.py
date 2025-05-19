import pandas as pd
import mysql.connector
import logging
import os

logpath = os.path.join(os.path.dirname(__file__), 'logs/csv_loader.log')
os.makedirs(os.path.dirname(logpath), exist_ok=True)  # Ensure logs directory exists
logging.basicConfig(filename=logpath, level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def load_csv_to_mysql(csv_path):
    try:
        conn = mysql.connector.connect(
            host=os.getenv("MYSQL_HOST", "localhost"),
            port=int(os.getenv("MYSQL_PORT", 3306)),
            user=os.getenv("MYSQL_USER", "user"),
            password=os.getenv("MYSQL_PASSWORD", "password"),
            database=os.getenv("MYSQL_DB", "flight_staging_db")
        )
        cursor = conn.cursor()

        df = pd.read_csv(csv_path)
        df = df.where(pd.notnull(df), None)  # Convert NaN to None for MySQL
        logging.info(f"Read {len(df)} records from {csv_path}")

        insert_query = """
        INSERT INTO raw_flight_prices (
            airline, source, destination, base_fare, tax_surcharge, total_fare,
            booking_date, travel_date
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """
        for _, row in df.iterrows():
            cursor.execute(insert_query, (
                row.get('Airline'), row.get('Source'), row.get('Destination'),
                row.get('Base Fare'), row.get('Tax & Surcharge'), row.get('Total Fare'),
                row.get('Booking Date'), row.get('Travel Date')
            ))
        conn.commit()
        logging.info(f"Loaded {len(df)} records into MySQL")
    except Exception as e:
        logging.error(f"Error loading CSV to MySQL: {e}")
        raise
    finally:
        cursor.close()
        conn.close()