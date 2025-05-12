"""
Kafka Consumer for Heartbeat Monitoring Pipeline.

Consumes data from Kafka, validates and flags anomalies, and inserts into PostgreSQL.
"""

import json
import psycopg2
import psycopg2.extras
from confluent_kafka import Consumer
import time
import os
import logging

# Configure logging
logs_dir = os.getenv("LOGS_DIR", "logs")  # Default to 'logs' if not set
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] - %(message)s',
    handlers=[
        logging.FileHandler(os.path.join(logs_dir, "consumer.log")),
        logging.StreamHandler()  # Also output to console
    ]
)
logger = logging.getLogger(__name__)

def connect_db():
    """
    Establish a connection to PostgreSQL with retry logic.
    """
    logger.info("Attempting to connect to PostgreSQL...")
    for _ in range(5):
        try:
            conn = psycopg2.connect(
                dbname="heartbeats_db",
                user="postgres",
                password="postgres",
                host=os.getenv("POSTGRES_HOST", "localhost"),
                port="5432"
            )
            logger.info("Database connection successful!")
            return conn
        except psycopg2.OperationalError as e:
            logger.error(f"Connection failed: {e}. Retrying in 5 seconds...")
            time.sleep(5)
    raise Exception("Failed to connect to database after retries")

def is_anomalous(rate):
    """
    Determine if the heart rate is anomalous.
    """
    return rate < 50 or rate > 140

# Kafka consumer configuration
logger.info("Initializing Kafka consumer...")
consumer = Consumer({
    'bootstrap.servers': os.getenv("KAFKA_BROKER", "localhost:9092"),
    'group.id': 'heart_group',
    'auto.offset.reset': 'earliest'
})
logger.info("Subscribing to topic 'heart_rate_stream'...")
consumer.subscribe(['heart_rate_stream'])

# PostgreSQL connection
conn = connect_db()
cursor = conn.cursor()

batch = []
logger.info("Starting consumer loop...")

try:
    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            logger.error(f"Kafka error: {msg.error()}")
            continue
        try:
            record = json.loads(msg.value().decode('utf-8'))
            logger.info(f"Received message: {record}")
            anomaly = is_anomalous(record["heart_rate"])
            batch.append((record["customer_id"], record["timestamp"], record["heart_rate"], anomaly))
            logger.debug(f"Batch size: {len(batch)}")

            if len(batch) >= 10:
                logger.info("Inserting batch into PostgreSQL...")
                start_time = time.time()
                psycopg2.extras.execute_batch(cursor, """
                    INSERT INTO heartbeats (customer_id, timestamp, heart_rate, anomaly)
                    VALUES (%s, %s, %s, %s)
                """, batch)
                conn.commit()
                logger.info(f"Inserted {len(batch)} records in {time.time() - start_time:.2f} seconds")
                batch.clear()
        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse message: {e}")
            continue
except KeyboardInterrupt:
    logger.info("Stopping consumer...")
finally:
    if batch:
        logger.info("Committing remaining batch...")
        psycopg2.extras.execute_batch(cursor, """
            INSERT INTO heartbeats (customer_id, timestamp, heart_rate, anomaly)
            VALUES (%s, %s, %s, %s)
        """, batch)
        conn.commit()
    cursor.close()
    conn.close()
    consumer.close()