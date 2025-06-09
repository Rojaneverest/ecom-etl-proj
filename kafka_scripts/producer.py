#!/usr/bin/env python3
"""
E-commerce Event Producer - COMPLETE VERSION
Simulates various e-commerce events: add to cart, wishlist, product views, orders
"""

import json
import random
import time
import os
from datetime import datetime
from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError
from faker import Faker
import uuid
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Kafka configuration
KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
TOPIC_PRODUCT_VIEWS = os.getenv('KAFKA_TOPIC_PRODUCT_VIEWS', 'ecommerce.product.views')
TOPIC_CART_ADD = os.getenv('KAFKA_TOPIC_CART_ADD', 'ecommerce.cart.add')
TOPIC_WISHLIST_ADD = os.getenv('KAFKA_TOPIC_WISHLIST_ADD', 'ecommerce.wishlist.add')
TOPIC_ORDERS = os.getenv('KAFKA_TOPIC_ORDERS', 'ecommerce.orders.completed')

class ECommerceEventProducer:
    def __init__(self, bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS):
        print(f"🚀 Initializing Kafka producer with bootstrap servers: {bootstrap_servers}")
        
        # Create admin client to manage topics
        self.admin_client = KafkaAdminClient(
            bootstrap_servers=bootstrap_servers,
            client_id='ecommerce_admin'
        )
        
        # Ensure topics exist
        self.create_topics()
        
        try:
            self.producer = KafkaProducer(
                bootstrap_servers=bootstrap_servers,
                value_serializer=lambda v: json.dumps(v, default=str).encode('utf-8'),
                key_serializer=lambda k: str(k).encode('utf-8') if k else None,
                acks='all',  # Wait for all replicas to acknowledge
                retries=3,
                max_in_flight_requests_per_connection=1,  # Ensure ordering
                batch_size=0,  # Send immediately (no batching)
                linger_ms=0,   # Don't wait
                request_timeout_ms=30000,
                delivery_timeout_ms=60000
            )
            self.fake = Faker()
            print("✅ Successfully connected to Kafka")
        except Exception as e:
            print(f"❌ Failed to connect to Kafka: {e}")
            raise
        
        # Sample product data
        self.products = [
            {"id": "prod_001", "name": "iPhone 15 Pro", "category": "Electronics", "price": 999.99},
            {"id": "prod_002", "name": "Nike Air Max", "category": "Footwear", "price": 129.99},
            {"id": "prod_003", "name": "Samsung 4K TV", "category": "Electronics", "price": 799.99},
            {"id": "prod_004", "name": "Levi's Jeans", "category": "Clothing", "price": 79.99},
            {"id": "prod_005", "name": "MacBook Pro", "category": "Electronics", "price": 1999.99},
            {"id": "prod_006", "name": "Adidas Sneakers", "category": "Footwear", "price": 89.99},
            {"id": "prod_007", "name": "Coffee Maker", "category": "Appliances", "price": 149.99},
            {"id": "prod_008", "name": "Wireless Headphones", "category": "Electronics", "price": 199.99}
        ]
        
        self.users = [f"user_{i:03d}" for i in range(1, 101)]  # 100 sample users
        
    def create_topics(self):
        """Create Kafka topics if they don't exist"""
        topics = [
            TOPIC_PRODUCT_VIEWS,
            TOPIC_CART_ADD,
            TOPIC_WISHLIST_ADD,
            TOPIC_ORDERS
        ]
        
        topic_list = []
        for topic_name in topics:
            topic_list.append(NewTopic(
                name=topic_name,
                num_partitions=3,
                replication_factor=1
            ))
        
        try:
            fs = self.admin_client.create_topics(new_topics=topic_list, validate_only=False)
            for topic, f in fs.items():
                try:
                    f.result()  # The result itself is None
                    print(f"✅ Topic {topic} created successfully")
                except TopicAlreadyExistsError:
                    print(f"ℹ️ Topic {topic} already exists")
                except Exception as e:
                    print(f"⚠️ Failed to create topic {topic}: {e}")
        except Exception as e:
            print(f"⚠️ Error creating topics: {e}")
    
    def generate_product_view_event(self):
        """Generate a product view event"""
        product = random.choice(self.products)
        user_id = random.choice(self.users)
        
        event = {
            "event_id": str(uuid.uuid4()),
            "event_type": "product_view",
            "user_id": user_id,
            "product_id": product["id"],
            "product_name": product["name"],
            "product_category": product["category"],
            "product_price": product["price"],
            "timestamp": datetime.now().isoformat(),
            "session_id": f"session_{random.randint(1000, 9999)}",
            "page_url": f"/product/{product['id']}",
            "referrer": random.choice(["google", "facebook", "direct", "email"])
        }
        return event
    
    def generate_add_to_cart_event(self):
        """Generate an add to cart event"""
        product = random.choice(self.products)
        user_id = random.choice(self.users)
        quantity = random.randint(1, 3)
        
        event = {
            "event_id": str(uuid.uuid4()),
            "event_type": "add_to_cart",
            "user_id": user_id,
            "product_id": product["id"],
            "product_name": product["name"],
            "product_category": product["category"],
            "product_price": product["price"],
            "quantity": quantity,
            "total_amount": round(product["price"] * quantity, 2),
            "timestamp": datetime.now().isoformat(),
            "session_id": f"session_{random.randint(1000, 9999)}"
        }
        return event
    
    def generate_wishlist_event(self):
        """Generate a wishlist event"""
        product = random.choice(self.products)
        user_id = random.choice(self.users)
        
        event = {
            "event_id": str(uuid.uuid4()),
            "event_type": "add_to_wishlist",
            "user_id": user_id,
            "product_id": product["id"],
            "product_name": product["name"],
            "product_category": product["category"],
            "product_price": product["price"],
            "timestamp": datetime.now().isoformat(),
            "session_id": f"session_{random.randint(1000, 9999)}"
        }
        return event
    
    def generate_order_event(self):
        """Generate an order completion event"""
        user_id = random.choice(self.users)
        order_id = f"order_{uuid.uuid4().hex[:8]}"
        
        # Generate 1-5 items for the order
        num_items = random.randint(1, 5)
        items = []
        total_amount = 0.0
        
        for _ in range(num_items):
            product = random.choice(self.products)
            quantity = random.randint(1, 2)
            item_total = round(product["price"] * quantity, 2)
            total_amount += item_total
            
            items.append({
                "product_id": product["id"],
                "product_name": product["name"],
                "product_category": product["category"],
                "product_price": product["price"],
                "quantity": quantity,
                "item_total": item_total
            })
        
        event = {
            "event_id": str(uuid.uuid4()),
            "event_type": "order_completed",
            "user_id": user_id,
            "order_id": order_id,
            "items": items,
            "total_amount": round(total_amount, 2),
            "payment_method": random.choice(["credit_card", "debit_card", "paypal", "apple_pay", "google_pay"]),
            "shipping_address": {
                "street": self.fake.street_address(),
                "city": self.fake.city(),
                "state": self.fake.state(),
                "zip_code": self.fake.zipcode(),
                "country": "US"
            },
            "timestamp": datetime.now().isoformat(),
            "session_id": f"session_{random.randint(1000, 9999)}"
        }
        return event
    
    def send_event(self, topic, event, key=None):
        """Send an event to a Kafka topic"""
        try:
            future = self.producer.send(topic, value=event, key=key)
            record_metadata = future.get(timeout=10)
            print(f"✅ Sent to {topic}: {event['event_type']} (partition: {record_metadata.partition}, offset: {record_metadata.offset})")
            return True
        except Exception as e:
            print(f"❌ Failed to send event to {topic}: {e}")
            return False
    
    def simulate_traffic(self, duration_seconds=60, events_per_second=2):
        """Simulate realistic e-commerce traffic"""
        print(f"🚀 Starting traffic simulation for {duration_seconds} seconds at {events_per_second} events/second")
        
        total_events = 0
        start_time = time.time()
        
        try:
            while time.time() - start_time < duration_seconds:
                cycle_start = time.time()
                
                for _ in range(events_per_second):
                    # Weighted probabilities for different event types
                    event_type = random.choices(
                        ['view', 'cart', 'wishlist', 'order'],
                        weights=[70, 20, 8, 2],  # Views are most common, orders least common
                        k=1
                    )[0]
                    
                    try:
                        if event_type == 'view':
                            event = self.generate_product_view_event()
                            self.send_event(TOPIC_PRODUCT_VIEWS, event, key=event['user_id'])
                        
                        elif event_type == 'cart':
                            event = self.generate_add_to_cart_event()
                            self.send_event(TOPIC_CART_ADD, event, key=event['user_id'])
                        
                        elif event_type == 'wishlist':
                            event = self.generate_wishlist_event()
                            self.send_event(TOPIC_WISHLIST_ADD, event, key=event['user_id'])
                        
                        elif event_type == 'order':
                            event = self.generate_order_event()
                            self.send_event(TOPIC_ORDERS, event, key=event['user_id'])
                        
                        total_events += 1
                        
                    except Exception as e:
                        print(f"⚠️ Error generating {event_type} event: {e}")
                
                # Sleep to maintain the desired rate
                cycle_duration = time.time() - cycle_start
                sleep_time = max(0, 1.0 - cycle_duration)
                if sleep_time > 0:
                    time.sleep(sleep_time)
                
                # Progress update every 10 seconds
                elapsed = time.time() - start_time
                if int(elapsed) % 10 == 0 and elapsed > 0:
                    rate = total_events / elapsed
                    print(f"📊 Progress: {int(elapsed)}s elapsed, {total_events} events sent ({rate:.1f} events/sec)")
        
        except KeyboardInterrupt:
            print("\n🛑 Simulation interrupted by user")
        
        finally:
            elapsed = time.time() - start_time
            print(f"🏁 Simulation completed: {total_events} events in {elapsed:.1f} seconds ({total_events/elapsed:.1f} events/sec)")
            self.cleanup()
    
    def send_sample_events(self, count=50):
        """Send a batch of sample events for testing"""
        print(f"📤 Sending {count} sample events...")
        
        events_sent = 0
        
        try:
            for i in range(count):
                # Mix of different event types
                if i % 10 == 0:  # Every 10th event is an order
                    event = self.generate_order_event()
                    if self.send_event(TOPIC_ORDERS, event, key=event['user_id']):
                        events_sent += 1
                
                elif i % 7 == 0:  # Every 7th event is wishlist
                    event = self.generate_wishlist_event()
                    if self.send_event(TOPIC_WISHLIST_ADD, event, key=event['user_id']):
                        events_sent += 1
                
                elif i % 3 == 0:  # Every 3rd event is cart add
                    event = self.generate_add_to_cart_event()
                    if self.send_event(TOPIC_CART_ADD, event, key=event['user_id']):
                        events_sent += 1
                
                else:  # Most events are product views
                    event = self.generate_product_view_event()
                    if self.send_event(TOPIC_PRODUCT_VIEWS, event, key=event['user_id']):
                        events_sent += 1
                
                # Small delay between events
                time.sleep(0.1)
                
                if (i + 1) % 10 == 0:
                    print(f"📊 Sent {i + 1}/{count} events...")
        
        except KeyboardInterrupt:
            print("\n🛑 Batch sending interrupted")
        
        finally:
            print(f"✅ Batch complete: {events_sent} events sent successfully")
            self.cleanup()
    
    def cleanup(self):
        """Clean up producer resources"""
        try:
            if hasattr(self, 'producer') and self.producer:
                self.producer.flush()  # Ensure all messages are sent
                self.producer.close()
                print("✅ Producer closed successfully")
        except Exception as e:
            print(f"⚠️ Error closing producer: {e}")


def main():
    """Main function to run the producer"""
    try:
        print("🚀 Starting E-Commerce Event Producer")
        print(f"🔗 Kafka Bootstrap Servers: {KAFKA_BOOTSTRAP_SERVERS}")
        
        producer = ECommerceEventProducer()
        
        # Choose mode: sample batch or continuous simulation
        mode = os.getenv('PRODUCER_MODE', 'batch')  # 'batch' or 'simulation'
        
        if mode == 'simulation':
            duration = int(os.getenv('SIMULATION_DURATION', 60))
            rate = int(os.getenv('EVENTS_PER_SECOND', 2))
            producer.simulate_traffic(duration_seconds=duration, events_per_second=rate)
        else:
            # Default: send batch of sample events
            count = int(os.getenv('SAMPLE_COUNT', 50))
            producer.send_sample_events(count=count)
    
    except KeyboardInterrupt:
        print("\n🛑 Producer stopped by user")
    except Exception as e:
        print(f"❌ Error in producer: {e}")
        raise


if __name__ == "__main__":
    main()