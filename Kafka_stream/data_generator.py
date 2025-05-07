"""
Data Generator for Heartbeat Monitoring Pipeline.

Generates random heart rate data and sends to Kafka topic at configurable intervals.
"""

import json
import time
import random
import os
import argparse
from confluent_kafka import Producer

def generate_data():
    """
    Generate a random heartbeat record.
    """
    return {
        "customer_id": random.randint(1000, 9999),
        "timestamp": int(time.time()),
        "heart_rate": random.randint(60, 120)
    }

def delivery_report(err, msg):
    """
    Kafka delivery callback.
    """
    if err:
        print(f"Delivery failed: {err}")
    else:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}]")

def main(args):
    """
    Initialize Kafka producer and send heartbeat messages at given interval.
    """
    # Use environment variable for Kafka broker (set in docker-compose.yml)
    kafka_broker = os.getenv("KAFKA_BROKER", "localhost:9092")
    producer = Producer({'bootstrap.servers': kafka_broker})
    print(f"Sending data to Kafka topic 'heart_rate_stream' every {args.interval}s...")

    try:
        while True:
            data = generate_data()
            # Encode JSON to bytes for Kafka
            producer.produce(
                'heart_rate_stream',
                json.dumps(data).encode('utf-8'),
                callback=delivery_report
            )
            producer.flush()  # Ensure message is sent
            time.sleep(args.interval)
    except KeyboardInterrupt:
        print("Stopping producer...")
    finally:
        producer.flush()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Kafka Heartbeat Data Generator")
    parser.add_argument('--interval', type=float, default=0.5, help="Time interval between messages in seconds.")
    args = parser.parse_args()
    main(args)