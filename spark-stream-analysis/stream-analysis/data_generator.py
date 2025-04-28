import csv
import uuid
import random
from faker import Faker
from datetime import datetime

fake = Faker()
categories = ['Electronics', 'Books', 'Home', 'Clothing', 'Toys']
event_types = ['view', 'purchase']

def generate_event():
    event_type = random.choice(event_types)
    is_purchase = event_type == 'purchase'
    
    return {
        'event_id': str(uuid.uuid4()),
        'user_id': random.randint(1, 1000),
        'product_id': f"PROD{random.randint(1000, 9999)}",
        'event_type': event_type,
        'event_time': fake.date_time_between(
            start_date='-90d', 
            end_date='now'
        ).strftime('%Y-%m-%d %H:%M:%S'),
        'price': round(random.uniform(10.0, 500.0), 2) if is_purchase else None,
        'category': random.choice(categories),
        'user_device': random.choice(['Mobile', 'Desktop', 'Tablet']),
        'user_location': fake.country_code()
    }

# Generate 30,000 events
print("Generating 30,000 events...")
start_time = datetime.now()

with open('data/user_events.csv', 'w', newline='', encoding='utf-8') as csvfile:
    fieldnames = ['event_id', 'user_id', 'product_id', 'event_type', 
                 'event_time', 'price', 'category', 'user_device', 'user_location']
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
    
    writer.writeheader()
    
    # Batch writing for better memory management
    batch_size = 500
    for _ in range(0, 30000, batch_size):
        batch = [generate_event() for _ in range(batch_size)]
        writer.writerows(batch)
        print(f"Written {len(batch)} records (total: {_ + batch_size})")

print(f"\n Successfully generated 30,000 events in {(datetime.now() - start_time).total_seconds():.2f} seconds")
print("File saved to: user_events.csv")