"""
Data Generator for Heartbeat Monitoring Pipeline.

Generates random heart rate data and sends to Kafka topic at configurable intervals.
"""

import json
import time
import random
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

def main(interval):
    """
    Initialize Kafka producer and send heartbeat messages at given interval.
    """
    producer = Producer({'bootstrap.servers': 'localhost:9092'})
    print(f"Sending data to Kafka topic 'heartbeats' every {interval}s...")
    
    while True:
        data = generate_data()
        producer.produce('heartbeats', json.dumps(data), callback=delivery_report)
        producer.poll(0)
        time.sleep(interval)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--interval', type=float, default=0.5, help="Time interval between messages in seconds.")
    args = parser.parse_args()
    main(args.interval)