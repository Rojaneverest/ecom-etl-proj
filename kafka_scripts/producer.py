#!/usr/bin/env python3
"""
E-commerce Event Producer
Simulates various e-commerce events: add to cart, wishlist, product views, orders
"""

import json
import random
import time
from datetime import datetime
from kafka import KafkaProducer
from faker import Faker
import uuid

class ECommerceEventProducer:
    def __init__(self, bootstrap_servers='localhost:9092'):
        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            key_serializer=lambda k: str(k).encode('utf-8')
        )
        self.fake = Faker()
        
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
            "total_amount": product["price"] * quantity,
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
        """Generate an order event"""
        user_id = random.choice(self.users)
        num_items = random.randint(1, 4)
        order_items = []
        total_amount = 0
        
        for _ in range(num_items):
            product = random.choice(self.products)
            quantity = random.randint(1, 2)
            item_total = product["price"] * quantity
            total_amount += item_total
            
            order_items.append({
                "product_id": product["id"],
                "product_name": product["name"],
                "product_category": product["category"],
                "price": product["price"],
                "quantity": quantity,
                "item_total": item_total
            })
        
        event = {
            "event_id": str(uuid.uuid4()),
            "event_type": "order_completed",
            "user_id": user_id,
            "order_id": f"order_{random.randint(10000, 99999)}",
            "items": order_items,
            "total_amount": round(total_amount, 2),
            "payment_method": random.choice(["credit_card", "paypal", "apple_pay", "google_pay"]),
            "timestamp": datetime.now().isoformat(),
            "session_id": f"session_{random.randint(1000, 9999)}",
            "shipping_address": {
                "city": self.fake.city(),
                "state": self.fake.state_abbr(),
                "zip_code": self.fake.zipcode()
            }
        }
        return event
    
    def send_event(self, topic, event):
        """Send event to Kafka topic"""
        try:
            future = self.producer.send(
                topic, 
                key=event["user_id"],
                value=event
            )
            record_metadata = future.get(timeout=10)
            print(f"✅ Sent {event['event_type']} to {topic} - Partition: {record_metadata.partition}, Offset: {record_metadata.offset}")
            return True
        except Exception as e:
            print(f"❌ Failed to send event: {e}")
            return False
    
    def simulate_events(self, duration_minutes=5, events_per_second=2):
        """Simulate e-commerce events for specified duration"""
        print(f"🚀 Starting e-commerce event simulation for {duration_minutes} minutes...")
        print(f"📊 Generating ~{events_per_second} events per second")
        
        start_time = time.time()
        end_time = start_time + (duration_minutes * 60)
        event_count = 0
        
        # Event type weights (product views are most common)
        event_types = [
            ("product_view", 0.6),      # 60% of events
            ("add_to_cart", 0.2),       # 20% of events
            ("add_to_wishlist", 0.15),  # 15% of events
            ("order_completed", 0.05)   # 5% of events
        ]
        
        try:
            while time.time() < end_time:
                # Generate events based on weights
                rand = random.random()
                cumulative = 0
                
                for event_type, weight in event_types:
                    cumulative += weight
                    if rand <= cumulative:
                        if event_type == "product_view":
                            event = self.generate_product_view_event()
                            topic = "ecommerce.product.views"
                        elif event_type == "add_to_cart":
                            event = self.generate_add_to_cart_event()
                            topic = "ecommerce.cart.add"
                        elif event_type == "add_to_wishlist":
                            event = self.generate_wishlist_event()
                            topic = "ecommerce.wishlist.add"
                        elif event_type == "order_completed":
                            event = self.generate_order_event()
                            topic = "ecommerce.orders.completed"
                        
                        if self.send_event(topic, event):
                            event_count += 1
                        break
                
                # Control event rate
                time.sleep(1 / events_per_second)
                
        except KeyboardInterrupt:
            print("\n⏹️  Simulation stopped by user")
        
        print(f"\n📈 Simulation completed! Generated {event_count} events")
        self.producer.close()

def main():
    try:
        producer = ECommerceEventProducer()
        
        # Run simulation
        print("Starting e-commerce event simulation...")
        print("Press Ctrl+C to stop\n")
        
        producer.simulate_events(duration_minutes=10, events_per_second=3)
        
    except KeyboardInterrupt:
        print("\nShutting down producer...")
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    main()