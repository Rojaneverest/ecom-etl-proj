#!/usr/bin/env python3
"""
E-commerce Event Consumer
Processes Kafka events and stores real-time metrics in Redis for dashboards
"""

import json
import redis
import time
from datetime import datetime, timedelta
from kafka import KafkaConsumer
from collections import defaultdict
import threading

class ECommerceEventConsumer:
    def __init__(self, bootstrap_servers='localhost:9092', redis_host='localhost', redis_port=6379):
        # Initialize Kafka consumer
        self.consumer = KafkaConsumer(
            'ecommerce.product.views',
            'ecommerce.cart.add', 
            'ecommerce.wishlist.add',
            'ecommerce.orders.completed',
            bootstrap_servers=bootstrap_servers,
            auto_offset_reset='latest',
            enable_auto_commit=True,
            group_id='ecommerce-analytics',
            value_deserializer=lambda m: json.loads(m.decode('utf-8'))
        )
        
        # Initialize Redis connection
        self.redis_client = redis.Redis(
            host=redis_host, 
            port=redis_port, 
            decode_responses=True
        )
        
        # Test Redis connection
        try:
            self.redis_client.ping()
            print("✅ Connected to Redis")
        except redis.ConnectionError:
            print("❌ Failed to connect to Redis")
            raise
        
        # Metrics storage
        self.metrics = {
            'total_views': 0,
            'total_cart_adds': 0,
            'total_wishlist_adds': 0,
            'total_orders': 0,
            'total_revenue': 0.0
        }
        
        # Start background metrics updater
        self.metrics_thread = threading.Thread(target=self.update_redis_metrics)
        self.metrics_thread.daemon = True
        self.running = True
        
    def process_product_view(self, event):
        """Process product view event"""
        print(f"👀 Product View: {event['product_name']} by {event['user_id']}")
        
        # Update counters
        self.metrics['total_views'] += 1
        
        # Store in Redis with expiration (24 hours)
        current_hour = datetime.now().strftime("%Y-%m-%d-%H")
        
        # Increment hourly view counts
        self.redis_client.hincrby("views:hourly", current_hour, 1)
        self.redis_client.expire("views:hourly", 86400)  # 24 hours
        
        # Product-specific views
        self.redis_client.hincrby("product:views", event['product_id'], 1)
        self.redis_client.expire("product:views", 86400)
        
        # Category views
        self.redis_client.hincrby("category:views", event['product_category'], 1)
        self.redis_client.expire("category:views", 86400)
        
        # User activity
        self.redis_client.hincrby("user:views", event['user_id'], 1)
        self.redis_client.expire("user:views", 86400)
        
        # Recent activity (last 100 views)
        recent_view = {
            "timestamp": event['timestamp'],
            "user_id": event['user_id'],
            "product_name": event['product_name'],
            "product_category": event['product_category']
        }
        self.redis_client.lpush("recent:views", json.dumps(recent_view))
        self.redis_client.ltrim("recent:views", 0, 99)  # Keep only last 100
    
    def process_cart_add(self, event):
        """Process add to cart event"""
        print(f"🛒 Cart Add: {event['quantity']}x {event['product_name']} by {event['user_id']} (${event['total_amount']})")
        
        # Update counters
        self.metrics['total_cart_adds'] += 1
        
        current_hour = datetime.now().strftime("%Y-%m-%d-%H")
        
        # Increment hourly cart adds
        self.redis_client.hincrby("cart:hourly", current_hour, 1)
        self.redis_client.expire("cart:hourly", 86400)
        
        # Product-specific cart adds
        self.redis_client.hincrby("product:cart_adds", event['product_id'], 1)
        self.redis_client.expire("product:cart_adds", 86400)
        
        # Cart value tracking
        self.redis_client.hincrbyfloat("cart:value:hourly", current_hour, event['total_amount'])
        self.redis_client.expire("cart:value:hourly", 86400)
        
        # Recent cart additions
        recent_cart = {
            "timestamp": event['timestamp'],
            "user_id": event['user_id'],
            "product_name": event['product_name'],
            "quantity": event['quantity'],
            "total_amount": event['total_amount']
        }
        self.redis_client.lpush("recent:cart_adds", json.dumps(recent_cart))
        self.redis_client.ltrim("recent:cart_adds", 0, 99)
    
    def process_wishlist_add(self, event):
        """Process wishlist add event"""
        print(f"❤️  Wishlist Add: {event['product_name']} by {event['user_id']}")
        
        # Update counters
        self.metrics['total_wishlist_adds'] += 1
        
        current_hour = datetime.now().strftime("%Y-%m-%d-%H")
        
        # Increment hourly wishlist adds
        self.redis_client.hincrby("wishlist:hourly", current_hour, 1)
        self.redis_client.expire("wishlist:hourly", 86400)
        
        # Product-specific wishlist adds
        self.redis_client.hincrby("product:wishlist_adds", event['product_id'], 1)
        self.redis_client.expire("product:wishlist_adds", 86400)
        
        # Recent wishlist additions
        recent_wishlist = {
            "timestamp": event['timestamp'],
            "user_id": event['user_id'],
            "product_name": event['product_name'],
            "product_category": event['product_category']
        }
        self.redis_client.lpush("recent:wishlist_adds", json.dumps(recent_wishlist))
        self.redis_client.ltrim("recent:wishlist_adds", 0, 99)
    
    def process_order(self, event):
        """Process order completion event"""
        print(f"💰 Order: {event['order_id']} by {event['user_id']} - ${event['total_amount']} ({len(event['items'])} items)")
        
        # Update counters
        self.metrics['total_orders'] += 1
        self.metrics['total_revenue'] += event['total_amount']
        
        current_hour = datetime.now().strftime("%Y-%m-%d-%H")
        
        # Increment hourly orders
        self.redis_client.hincrby("orders:hourly", current_hour, 1)
        self.redis_client.expire("orders:hourly", 86400)
        
        # Revenue tracking
        self.redis_client.hincrbyfloat("revenue:hourly", current_hour, event['total_amount'])
        self.redis_client.expire("revenue:hourly", 86400)
        
        # Process each item in the order
        for item in event['items']:
            # Product-specific order counts
            self.redis_client.hincrby("product:orders", item['product_id'], item['quantity'])
            self.redis_client.expire("product:orders", 86400)
            
            # Category revenue
            self.redis_client.hincrbyfloat("category:revenue", item['product_category'], item['item_total'])
            self.redis_client.expire("category:revenue", 86400)
        
        # Payment method tracking
        self.redis_client.hincrby("payment:methods", event['payment_method'], 1)
        self.redis_client.expire("payment:methods", 86400)
        
        # Recent orders
        recent_order = {
            "timestamp": event['timestamp'],
            "user_id": event['user_id'],
            "order_id": event['order_id'],
            "total_amount": event['total_amount'],
            "item_count": len(event['items']),
            "payment_method": event['payment_method']
        }
        self.redis_client.lpush("recent:orders", json.dumps(recent_order))
        self.redis_client.ltrim("recent:orders", 0, 99)
    
    def update_redis_metrics(self):
        """Update Redis with current metrics every 5 seconds"""
        while self.running:
            try:
                # Store current totals
                self.redis_client.hmset("metrics:totals", {
                    "total_views": self.metrics['total_views'],
                    "total_cart_adds": self.metrics['total_cart_adds'],
                    "total_wishlist_adds": self.metrics['total_wishlist_adds'],
                    "total_orders": self.metrics['total_orders'],
                    "total_revenue": round(self.metrics['total_revenue'], 2),
                    "last_updated": datetime.now().isoformat()
                })
                
                # Real-time metrics (last 5 minutes)
                now = datetime.now()
                five_min_ago = now - timedelta(minutes=5)
                
                # Store real-time activity indicator
                self.redis_client.set("metrics:last_activity", now.isoformat(), ex=300)  # 5 min expiry
                
                time.sleep(5)
            except Exception as e:
                print(f"Error updating metrics: {e}")
                time.sleep(5)
    
    def get_dashboard_data(self):
        """Get current dashboard data from Redis"""
        dashboard_data = {}
        
        try:
            # Get totals
            dashboard_data['totals'] = self.redis_client.hgetall("metrics:totals")
            
            # Get hourly data for last 24 hours
            dashboard_data['hourly_views'] = self.redis_client.hgetall("views:hourly")
            dashboard_data['hourly_orders'] = self.redis_client.hgetall("orders:hourly")
            dashboard_data['hourly_revenue'] = self.redis_client.hgetall("revenue:hourly")
            
            # Get top products
            dashboard_data['top_viewed_products'] = self.redis_client.hgetall("product:views")
            dashboard_data['top_ordered_products'] = self.redis_client.hgetall("product:orders")
            
            # Get recent activity
            recent_views = self.redis_client.lrange("recent:views", 0, 9)  # Last 10
            dashboard_data['recent_views'] = [json.loads(v) for v in recent_views]
            
            recent_orders = self.redis_client.lrange("recent:orders", 0, 9)  # Last 10
            dashboard_data['recent_orders'] = [json.loads(o) for o in recent_orders]
            
            return dashboard_data
            
        except Exception as e:
            print(f"Error getting dashboard data: {e}")
            return {}
    
    def print_dashboard_summary(self):
        """Print a summary of current metrics"""
        data = self.get_dashboard_data()
        
        print("\n" + "="*60)
        print("📊 REAL-TIME E-COMMERCE DASHBOARD")
        print("="*60)
        
        if 'totals' in data:
            totals = data['totals']
            print(f"👀 Total Views: {totals.get('total_views', 0)}")
            print(f"🛒 Total Cart Adds: {totals.get('total_cart_adds', 0)}")
            print(f"❤️  Total Wishlist Adds: {totals.get('total_wishlist_adds', 0)}")
            print(f"📦 Total Orders: {totals.get('total_orders', 0)}")
            print(f"💰 Total Revenue: ${totals.get('total_revenue', 0)}")
            print(f"🕐 Last Updated: {totals.get('last_updated', 'N/A')}")
        
        if 'recent_orders' in data and data['recent_orders']:
            print("\n🔥 Recent Orders:")
            for order in data['recent_orders'][:5]:
                print(f"  • {order['order_id']}: ${order['total_amount']} ({order['item_count']} items)")
        
        print("="*60)
    
    def consume_events(self):
        """Main event consumption loop"""
        print("🚀 Starting e-commerce event consumer...")
        print("📊 Real-time metrics will be stored in Redis")
        print("Press Ctrl+C to stop\n")
        
        # Start metrics updater thread
        self.metrics_thread.start()
        
        try:
            for message in self.consumer:
                event = message.value
                topic = message.topic
                
                # Process event based on topic
                if topic == 'ecommerce.product.views':
                    self.process_product_view(event)
                elif topic == 'ecommerce.cart.add':
                    self.process_cart_add(event)
                elif topic == 'ecommerce.wishlist.add':
                    self.process_wishlist_add(event)
                elif topic == 'ecommerce.orders.completed':
                    self.process_order(event)
                
                # Print dashboard summary every 50 events
                if (self.metrics['total_views'] + self.metrics['total_cart_adds'] + 
                    self.metrics['total_wishlist_adds'] + self.metrics['total_orders']) % 50 == 0:
                    self.print_dashboard_summary()
                    
        except KeyboardInterrupt:
            print("\n⏹️  Consumer stopped by user")
        finally:
            self.running = False
            self.consumer.close()
            self.redis_client.close()
            print("👋 Consumer shut down gracefully")

def main():
    try:
        consumer = ECommerceEventConsumer()
        consumer.consume_events()
        
    except KeyboardInterrupt:
        print("\nShutting down consumer...")
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    main()