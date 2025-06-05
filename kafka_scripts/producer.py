#!/usr/bin/env python3
"""
E-commerce Events Producer
--------------------------
This script simulates real-time user interactions on an e-commerce website,
generating events and sending them to a Kafka topic.
"""

import json
import time
import random
from datetime import datetime
from kafka import KafkaProducer
from faker import Faker

# Initialize Faker for generating fake data
fake = Faker()

# Configure Kafka producer
def create_producer():
    """Create and return a Kafka producer instance."""
    try:
        return KafkaProducer(
            bootstrap_servers=['kafka:9092'],
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
    except Exception as e:
        print(f"Error creating Kafka producer: {e}")
        return None

def generate_event():
    """Generate a simulated e-commerce event."""
    event_types = [
        'page_view',
        'product_view',
        'add_to_cart',
        'remove_from_cart',
        'checkout',
        'purchase',
        'search',
        'login',
        'logout',
        'signup'
    ]
    
    event_type = random.choice(event_types)
    
    # Base event data
    event = {
        'event_id': fake.uuid4(),
        'event_type': event_type,
        'timestamp': datetime.now().isoformat(),
        'user_id': fake.uuid4(),
        'session_id': fake.uuid4(),
        'device': random.choice(['mobile', 'desktop', 'tablet']),
        'browser': random.choice(['chrome', 'firefox', 'safari', 'edge']),
        'location': {
            'country': fake.country(),
            'city': fake.city()
        }
    }
    
    # Add event-specific data
    if event_type == 'page_view':
        event['page'] = random.choice(['home', 'category', 'product', 'cart', 'checkout', 'order_confirmation'])
        event['time_spent'] = random.randint(5, 300)
    
    elif event_type == 'product_view':
        event['product_id'] = fake.uuid4()
        event['product_name'] = fake.word() + ' ' + fake.word()
        event['category'] = random.choice(['electronics', 'clothing', 'home', 'beauty', 'toys'])
        event['price'] = round(random.uniform(5.99, 999.99), 2)
    
    elif event_type in ['add_to_cart', 'remove_from_cart']:
        event['product_id'] = fake.uuid4()
        event['quantity'] = random.randint(1, 5)
        event['price'] = round(random.uniform(5.99, 999.99), 2)
    
    elif event_type == 'checkout':
        event['cart_total'] = round(random.uniform(10.99, 1999.99), 2)
        event['items_count'] = random.randint(1, 10)
    
    elif event_type == 'purchase':
        event['order_id'] = fake.uuid4()
        event['order_total'] = round(random.uniform(10.99, 1999.99), 2)
        event['payment_method'] = random.choice(['credit_card', 'paypal', 'apple_pay', 'google_pay'])
    
    elif event_type == 'search':
        event['search_query'] = fake.word() + ' ' + fake.word()
        event['results_count'] = random.randint(0, 100)
    
    return event

def main():
    """Main function to run the producer."""
    producer = create_producer()
    if not producer:
        return
    
    print("Starting E-commerce Events Producer...")
    topic = 'ecommerce-events'
    
    try:
        while True:
            event = generate_event()
            producer.send(topic, event)
            print(f"Sent event: {event['event_type']}")
            time.sleep(random.uniform(0.5, 3))  # Random delay between events
    except KeyboardInterrupt:
        print("Producer stopped by user")
    except Exception as e:
        print(f"Error in producer: {e}")
    finally:
        producer.close()
        print("Producer closed")

if __name__ == "__main__":
    main() 