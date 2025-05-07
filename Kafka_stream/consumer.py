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

def connect_db():
    """
    Establish a connection to PostgreSQL with retry logic.
    """
    print("Attempting to connect to PostgreSQL...")
    for _ in range(5):
        try:
            conn = psycopg2.connect(
                dbname="heartbeats_db",
                user="postgres",
                password="postgres",
                host=os.getenv("POSTGRES_HOST", "localhost"),
                port="5432"
            )
            print("Database connection successful!")
            return conn
        except psycopg2.OperationalError as e:
            print(f"Connection failed: {e}. Retrying in 5 seconds...")
            time.sleep(5)
    raise Exception("Failed to connect to database after retries")

def is_anomalous(rate):
    """
    Determine if the heart rate is anomalous.
    """
    return rate < 50 or rate > 140

# Kafka consumer configuration
print("Initializing Kafka consumer...")
consumer = Consumer({
    'bootstrap.servers': os.getenv("KAFKA_BROKER", "localhost:9092"),
    'group.id': 'heart_group',
    'auto.offset.reset': 'earliest'
})
print("Subscribing to topic 'heart_rate_stream'...")
consumer.subscribe(['heart_rate_stream'])

# PostgreSQL connection
conn = connect_db()
cursor = conn.cursor()

batch = []
print("Starting consumer loop...")

try:
    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            print(f"Kafka error: {msg.error()}")
            continue
        try:
            record = json.loads(msg.value().decode('utf-8'))
            print(f"Received message: {record}")
            anomaly = is_anomalous(record["heart_rate"])
            batch.append((record["customer_id"], record["timestamp"], record["heart_rate"], anomaly))
            print(f"Batch size: {len(batch)}")

            if len(batch) >= 10:
                print("Inserting batch into PostgreSQL...")
                start_time = time.time()
                psycopg2.extras.execute_batch(cursor, """
                    INSERT INTO heartbeats (customer_id, timestamp, heart_rate, anomaly)
                    VALUES (%s, %s, %s, %s)
                """, batch)
                conn.commit()
                print(f"Inserted {len(batch)} records in {time.time() - start_time:.2f} seconds")
                batch.clear()
        except json.JSONDecodeError as e:
            print(f"Failed to parse message: {e}")
            continue
except KeyboardInterrupt:
    print("Stopping consumer...")
finally:
    if batch:
        print("Committing remaining batch...")
        psycopg2.extras.execute_batch(cursor, """
            INSERT INTO heartbeats (customer_id, timestamp, heart_rate, anomaly)
            VALUES (%s, %s, %s, %s)
        """, batch)
        conn.commit()
    cursor.close()
    conn.close()
    consumer.close()