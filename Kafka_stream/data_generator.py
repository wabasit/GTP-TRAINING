"""
Data Generator for Heartbeat Monitoring Pipeline.

Generates random heart rate data and sends to Kafka topic at configurable intervals.
"""

import json
import time
from datetime import datetime
import random
import os
import logging
import argparse
from confluent_kafka import Producer

# Configure logging
logs_dir = os.getenv("LOGS_DIR", "logs")  # Default to 'logs' if not set
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] - %(message)s',
    handlers=[
        logging.FileHandler(os.path.join(logs_dir, "data_generator.log")),
        logging.StreamHandler()  # Also output to console
    ]
)
logger = logging.getLogger(__name__)

def generate_data():
    """
    Generate a random heartbeat record.
    """
    return {
        "customer_id": random.randint(1000, 9999),
        "timestamp": datetime.utcnow().isoformat() + "Z",
        "heart_rate": random.randint(60, 120)
    }

def delivery_report(err, msg):
    """
    Kafka delivery callback.
    """
    if err:
        logger.error(f"Delivery failed: {err}")
    else:
        logger.info(f"Message delivered to {msg.topic()} [{msg.partition()}]")

def main(args):
    """
    Initialize Kafka producer and send heartbeat messages at given interval.
    """
    # Use environment variable for Kafka broker (set in docker-compose.yml)
    kafka_broker = os.getenv("KAFKA_BROKER", "localhost:9092")
    producer = Producer({'bootstrap.servers': kafka_broker})
    logger.info(f"Sending data to Kafka topic 'heart_rate_stream' every {args.interval}s...")

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
        logger.info("Stopping producer...")
    finally:
        producer.flush()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Kafka Heartbeat Data Generator")
    parser.add_argument('--interval', type=float, default=0.5, help="Time interval between messages in seconds.")
    args = parser.parse_args()
    main(args)