import mysql.connector
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def validate_data():
    conn = None
    cursor = None
    try:
        conn = mysql.connector.connect(
            host="flight_mysql",
            user="user",
            password="password",
            database="flight_staging_db"
        )
        cursor = conn.cursor()

        # Validation query
        validation_query = """
        UPDATE raw_flight_prices
        SET is_valid = FALSE, validation_error = CASE
            WHEN airline IS NULL OR airline = '' THEN 'Missing airline'
            WHEN source IS NULL OR source = '' THEN 'Missing source'
            WHEN destination IS NULL OR destination = '' THEN 'Missing destination'
            WHEN base_fare IS NULL OR base_fare < 0 THEN 'Invalid base_fare'
            WHEN tax_surcharge IS NULL OR tax_surcharge < 0 THEN 'Invalid tax_surcharge'
            WHEN total_fare IS NULL OR total_fare < 0 THEN 'Invalid total_fare'
            WHEN source NOT IN ('Dhaka', 'Chittagong', 'Sylhet') THEN 'Invalid source city'
            WHEN destination NOT IN ('Dhaka', 'Chittagong', 'Sylhet') THEN 'Invalid destination city'
            ELSE NULL
        END
        WHERE is_valid = TRUE
        AND (
            airline IS NULL OR airline = '' OR
            source IS NULL OR source = '' OR
            destination IS NULL OR destination = '' OR
            base_fare IS NULL OR base_fare < 0 OR
            tax_surcharge IS NULL OR tax_surcharge < 0 OR
            total_fare IS NULL OR total_fare < 0 OR
            source NOT IN ('Dhaka', 'Chittagong', 'Sylhet') OR
            destination NOT IN ('Dhaka', 'Chittagong', 'Sylhet')
        );
        """
        cursor.execute(validation_query)
        conn.commit()

        cursor.execute("SELECT COUNT(*) FROM raw_flight_prices WHERE is_valid = FALSE")
        invalid_count = cursor.fetchone()[0]
        logging.info(f"Validation complete. Found {invalid_count} invalid records")
    except Exception as e:
        logging.error(f"Error validating data: {e}")
        raise
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()
