import json, time, random, argparse
from confluent_kafka import Producer

def generate_data():
    return {
        "customer_id": random.randint(1000, 9999),
        "timestamp": int(time.time()),
        "heart_rate": random.randint(60, 120)
    }

def delivery_report(err, msg):
    if err:
        print(f"Delivery failed: {err}")
    else:
        print(f"Delivered to {msg.topic()} [{msg.partition()}]")

def main(interval):
    producer = Producer({'bootstrap.servers': 'localhost:9092'})
    while True:
        data = generate_data()
        producer.produce('heartbeats', json.dumps(data), callback=delivery_report)
        producer.poll(0)
        time.sleep(interval)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--interval', type=float, default=0.5)
    args = parser.parse_args()
    main(args.interval)
