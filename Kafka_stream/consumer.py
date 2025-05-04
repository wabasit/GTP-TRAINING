"""
Kafka Consumer for Heartbeat Monitoring Pipeline.

Consumes data from Kafka, validates and flags anomalies, and inserts into PostgreSQL.
"""

import json
import psycopg2
import psycopg2.extras
from confluent_kafka import Consumer

def is_anomalous(rate):
    """
    Determine if the heart rate is anomalous.
    """
    return rate < 50 or rate > 140

# Kafka consumer configuration
consumer = Consumer({
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'heart_group',
    'auto.offset.reset': 'earliest'
})
consumer.subscribe(['heartbeats'])

# PostgreSQL connection
conn = psycopg2.connect(dbname="heartbeats_db", user="heart_user", password="heart_pass", host="localhost")
cursor = conn.cursor()

batch = []

try:
    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        
        record = json.loads(msg.value())
        anomaly = is_anomalous(record["heart_rate"])
        batch.append((record["customer_id"], record["timestamp"], record["heart_rate"], anomaly))

        if len(batch) >= 10:
            # Batch insert to PostgreSQL
            psycopg2.extras.execute_batch(cursor, """
                INSERT INTO heartbeats (customer_id, timestamp, heart_rate, anomaly)
                VALUES (%s, %s, %s, %s)
            """, batch)
            conn.commit()
            batch.clear()
except KeyboardInterrupt:
    print("Stopping consumer...")
finally:
    consumer.close()
    conn.close()