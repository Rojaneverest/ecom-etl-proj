#!/usr/bin/env python3

"""
E-commerce Event Consumer - FIXED VERSION (Minutely Aggregation)
Processes Kafka events and stores real-time metrics in Redis for dashboards
"""

import json
import os
import time
import logging
from datetime import datetime, timedelta
from kafka import KafkaConsumer
from collections import defaultdict
import threading
import redis
from dotenv import load_dotenv
import signal
import sys

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('ecommerce-consumer')

# Load environment variables
load_dotenv()

# Kafka configuration
KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
KAFKA_GROUP_ID = os.getenv('KAFKA_GROUP_ID', 'ecommerce-analytics')
TOPIC_PRODUCT_VIEWS = os.getenv('KAFKA_TOPIC_PRODUCT_VIEWS', 'ecommerce.product.views')
TOPIC_CART_ADD = os.getenv('KAFKA_TOPIC_CART_ADD', 'ecommerce.cart.add')
TOPIC_WISHLIST_ADD = os.getenv('KAFKA_TOPIC_WISHLIST_ADD', 'ecommerce.wishlist.add')
TOPIC_ORDERS = os.getenv('KAFKA_TOPIC_ORDERS', 'ecommerce.orders.completed')

# Redis configuration
REDIS_HOST = os.getenv('REDIS_HOST', 'localhost')
REDIS_PORT = int(os.getenv('REDIS_PORT', 6379))
REDIS_DB = int(os.getenv('REDIS_DB', 0))

class ECommerceEventConsumer:
    def __init__(self, bootstrap_servers=None, redis_host=None, redis_port=None, reset_offset=False):
        # Set instance attributes from parameters or environment variables
        self.bootstrap_servers = bootstrap_servers or KAFKA_BOOTSTRAP_SERVERS
        self.redis_host = redis_host or REDIS_HOST
        self.redis_port = redis_port or REDIS_PORT
        self.running = True
        self.reset_offset = reset_offset
        
        print(f"🚀 Initializing Kafka consumer with bootstrap servers: {self.bootstrap_servers}")
        print(f"🔌 Connecting to Redis at {self.redis_host}:{self.redis_port}")
        print(f"🔄 Reset offset mode: {self.reset_offset}")

        # Initialize signal handlers for graceful shutdown
        signal.signal(signal.SIGTERM, self._signal_handler)
        signal.signal(signal.SIGINT, self._signal_handler)

        try:
            # Initialize Redis connection first
            self.redis_client = redis.Redis(
                host=self.redis_host,
                port=self.redis_port,
                db=int(os.getenv('REDIS_DB', 0)),
                decode_responses=True,
                socket_connect_timeout=5,
                socket_timeout=5
            )

            # Test Redis connection
            self.redis_client.ping()
            print("✅ Successfully connected to Redis")

            # Initialize Kafka consumer with proper offset strategy
            offset_reset = 'earliest' if self.reset_offset else 'latest'
            
            self.consumer = KafkaConsumer(
                TOPIC_PRODUCT_VIEWS,
                TOPIC_CART_ADD,
                TOPIC_WISHLIST_ADD,
                TOPIC_ORDERS,
                bootstrap_servers=self.bootstrap_servers,
                auto_offset_reset=offset_reset,
                enable_auto_commit=True,
                auto_commit_interval_ms=1000,  # Commit offsets every second
                group_id=KAFKA_GROUP_ID,
                value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                consumer_timeout_ms=30000,  # Increased timeout
                session_timeout_ms=30000,
                heartbeat_interval_ms=10000,
                max_poll_records=10,  # Process smaller batches
                fetch_min_bytes=1,  # Don't wait for large batches
                fetch_max_wait_ms=500  # Short wait time
            )
            print("✅ Successfully connected to Kafka")

        except Exception as e:
            print(f"❌ Error initializing consumer: {e}")
            raise

        # Initialize or reset metrics based on mode
        if self.reset_offset:
            self.reset_all_metrics()
        else:
            self.load_existing_metrics()

    def _signal_handler(self, signum, frame):
        """Handle shutdown signals gracefully"""
        print(f"\n🛑 Received signal {signum}, shutting down...")
        self.running = False

    def reset_all_metrics(self):
        """Reset all metrics to zero (for fresh start)"""
        try:
            print("🧹 Resetting all metrics to zero...")
            
            # Clear all Redis keys related to metrics
            keys_to_delete = [
                "metrics:totals",
                "views:minutely",  # Changed from hourly
                "cart:minutely",   # Changed from hourly
                "wishlist:minutely", # Changed from hourly
                "orders:minutely",   # Changed from hourly
                "revenue:minutely",  # Changed from hourly
                "cart:value:minutely", # Changed from hourly
                "product:views",
                "product:cart_adds",
                "product:wishlist_adds",
                "product:orders",
                "category:views",
                "category:revenue",
                "payment:methods",
                "user:views",
                "recent:views",
                "recent:cart_adds",
                "recent:wishlist_adds",
                "recent:orders",
                "metrics:last_activity"
            ]
            
            for key in keys_to_delete:
                self.redis_client.delete(key)
            
            # Initialize fresh metrics
            self.metrics = {
                'total_views': 0,
                'total_cart_adds': 0,
                'total_wishlist_adds': 0,
                'total_orders': 0,
                'total_revenue': 0.0
            }
            
            # Store initial empty state
            self.update_metrics_in_redis()
            print("✅ All metrics reset successfully")
            
        except Exception as e:
            print(f"⚠️ Error resetting metrics: {e}")
            self.metrics = {
                'total_views': 0,
                'total_cart_adds': 0,
                'total_wishlist_adds': 0,
                'total_orders': 0,
                'total_revenue': 0.0
            }

    def load_existing_metrics(self):
        """Load existing metrics from Redis to maintain state"""
        try:
            existing_metrics = self.redis_client.hgetall("metrics:totals")
            self.metrics = {
                'total_views': int(existing_metrics.get('total_views', 0)),
                'total_cart_adds': int(existing_metrics.get('total_cart_adds', 0)),
                'total_wishlist_adds': int(existing_metrics.get('total_wishlist_adds', 0)),
                'total_orders': int(existing_metrics.get('total_orders', 0)),
                'total_revenue': float(existing_metrics.get('total_revenue', 0.0))
            }
            print(f"📊 Loaded existing metrics: {self.metrics}")
        except Exception as e:
            print(f"⚠️ Could not load existing metrics, starting fresh: {e}")
            self.metrics = {
                'total_views': 0,
                'total_cart_adds': 0,
                'total_wishlist_adds': 0,
                'total_orders': 0,
                'total_revenue': 0.0
            }

    def process_product_view(self, event):
        """Process product view event"""
        print(f"👀 Product View: {event['product_name']} by {event['user_id']}")

        # Update counters
        self.metrics['total_views'] += 1

        # Use minutely timestamp for aggregation
        current_minute = datetime.now().strftime("%Y-%m-%d-%H-%M") # Changed to minutely

        # Increment minutely view counts
        self.redis_client.hincrby("views:minutely", current_minute, 1) # Changed Redis key
        self.redis_client.expire("views:minutely", 86400) # 24 hours (still keep 24h of minutes)

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
        self.redis_client.ltrim("recent:views", 0, 99) # Keep only last 100

        # Update totals in Redis immediately
        self.update_metrics_in_redis()

    def process_cart_add(self, event):
        """Process add to cart event"""
        print(f"🛒 Cart Add: {event['quantity']}x {event['product_name']} by {event['user_id']} (${event['total_amount']})")

        # Update counters
        self.metrics['total_cart_adds'] += 1

        # Use minutely timestamp for aggregation
        current_minute = datetime.now().strftime("%Y-%m-%d-%H-%M") # Changed to minutely

        # Increment minutely cart adds
        self.redis_client.hincrby("cart:minutely", current_minute, 1) # Changed Redis key
        self.redis_client.expire("cart:minutely", 86400)

        # Product-specific cart adds
        self.redis_client.hincrby("product:cart_adds", event['product_id'], 1)
        self.redis_client.expire("product:cart_adds", 86400)

        # Cart value tracking
        self.redis_client.hincrbyfloat("cart:value:minutely", current_minute, event['total_amount']) # Changed Redis key
        self.redis_client.expire("cart:value:minutely", 86400)

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

        # Update totals in Redis immediately
        self.update_metrics_in_redis()

    def process_wishlist_add(self, event):
        """Process wishlist add event"""
        print(f"❤️ Wishlist Add: {event['product_name']} by {event['user_id']}")

        # Update counters
        self.metrics['total_wishlist_adds'] += 1

        # Use minutely timestamp for aggregation
        current_minute = datetime.now().strftime("%Y-%m-%d-%H-%M") # Changed to minutely

        # Increment minutely wishlist adds
        self.redis_client.hincrby("wishlist:minutely", current_minute, 1) # Changed Redis key
        self.redis_client.expire("wishlist:minutely", 86400)

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

        # Update totals in Redis immediately
        self.update_metrics_in_redis()

    def process_order(self, event):
        """Process order completion event"""
        print(f"💰 Order: {event['order_id']} by {event['user_id']} - ${event['total_amount']} ({len(event['items'])} items)")

        # Update counters
        self.metrics['total_orders'] += 1
        self.metrics['total_revenue'] += event['total_amount']

        # Use minutely timestamp for aggregation
        current_minute = datetime.now().strftime("%Y-%m-%d-%H-%M") # Changed to minutely

        # Increment minutely orders
        self.redis_client.hincrby("orders:minutely", current_minute, 1) # Changed Redis key
        self.redis_client.expire("orders:minutely", 86400)

        # Revenue tracking
        self.redis_client.hincrbyfloat("revenue:minutely", current_minute, event['total_amount']) # Changed Redis key
        self.redis_client.expire("revenue:minutely", 86400)

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

        # Update totals in Redis immediately
        self.update_metrics_in_redis()

    def update_metrics_in_redis(self):
        """Update Redis with current metrics immediately"""
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

            # Store real-time activity indicator
            self.redis_client.set("metrics:last_activity", datetime.now().isoformat(), ex=300)

        except Exception as e:
            print(f"Error updating metrics in Redis: {e}")

    def get_dashboard_data(self):
        """Get current dashboard data from Redis"""
        dashboard_data = {}

        try:
            # Get totals
            dashboard_data['totals'] = self.redis_client.hgetall("metrics:totals")

            # Get minutely data (changed from hourly)
            dashboard_data['minutely_views'] = self.redis_client.hgetall("views:minutely") # Changed Redis key
            dashboard_data['minutely_orders'] = self.redis_client.hgetall("orders:minutely") # Changed Redis key
            dashboard_data['minutely_revenue'] = self.redis_client.hgetall("revenue:minutely") # Changed Redis key

            # Get top products
            dashboard_data['top_viewed_products'] = self.redis_client.hgetall("product:views")
            dashboard_data['top_ordered_products'] = self.redis_client.hgetall("product:orders")

            # Get recent activity
            recent_views = self.redis_client.lrange("recent:views", 0, 9) # Last 10
            dashboard_data['recent_views'] = [json.loads(v) for v in recent_views] if recent_views else []

            recent_orders = self.redis_client.lrange("recent:orders", 0, 9) # Last 10
            dashboard_data['recent_orders'] = [json.loads(o) for o in recent_orders] if recent_orders else []

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

        # Print in-memory metrics
        print(f"👀 Total Views: {self.metrics['total_views']}")
        print(f"🛒 Total Cart Adds: {self.metrics['total_cart_adds']}")
        print(f"❤️ Total Wishlist Adds: {self.metrics['total_wishlist_adds']}")
        print(f"📦 Total Orders: {self.metrics['total_orders']}")
        print(f"💰 Total Revenue: ${self.metrics['total_revenue']:.2f}")
        print(f"🕐 Last Updated: {datetime.now().isoformat()}")

        if 'recent_orders' in data and data['recent_orders']:
            print("\n🔥 Recent Orders:")
            for order in data['recent_orders'][:5]:
                print(f" • {order['order_id']}: ${order['total_amount']} ({order['item_count']} items)")

        print("="*60)

    def consume_events(self, max_messages=100, timeout_seconds=60):
        """Start consuming events with timeout"""
        print("🚀 Starting Kafka consumer...")
        print(f"📊 Subscribed to Topics: {list(self.consumer.subscription())}")
        print(f"🔍 Will process up to {max_messages} messages or run for {timeout_seconds} seconds")

        # Wait a moment for producer to send messages
        print("⏳ Waiting 3 seconds for messages to be produced...")
        time.sleep(3)

        message_count = 0
        start_time = time.time()
        last_message_time = start_time
        consecutive_empty_polls = 0
        max_empty_polls = 3  # Exit after 6 consecutive empty polls (30 seconds)

        try:
            while self.running:
                current_time = time.time()

                if current_time - start_time > timeout_seconds:
                    print(f"⏰ Timeout reached ({timeout_seconds}s), stopping consumer")
                    break

                if message_count >= max_messages:
                    print(f"✅ Processed {message_count} messages, stopping consumer")
                    break

                try:
                    # Poll with shorter timeout for more responsive behavior
                    message_batch = self.consumer.poll(timeout_ms=5000)

                    if not message_batch:
                        consecutive_empty_polls += 1
                        elapsed_since_last = current_time - last_message_time
                        
                        print(f"⏸️ No messages received (poll #{consecutive_empty_polls})")
                        
                        if consecutive_empty_polls >= max_empty_polls:
                            print(f"🏁 No messages for {max_empty_polls} consecutive polls, assuming completion")
                            break
                            
                        continue
                    
                    # Reset empty poll counter when we get messages
                    consecutive_empty_polls = 0

                    for topic_partition, messages in message_batch.items():
                        for message in messages:
                            if not self.running:
                                break

                            try:
                                # Process messages based on topic
                                if message.topic == TOPIC_PRODUCT_VIEWS:
                                    self.process_product_view(message.value)
                                elif message.topic == TOPIC_CART_ADD:
                                    self.process_cart_add(message.value)
                                elif message.topic == TOPIC_WISHLIST_ADD:
                                    self.process_wishlist_add(message.value)
                                elif message.topic == TOPIC_ORDERS:
                                    self.process_order(message.value)
                                else:
                                    print(f"⚠️ Unknown topic: {message.topic}")

                                message_count += 1
                                last_message_time = time.time()

                                if message_count % 5 == 0:
                                    elapsed = time.time() - start_time
                                    print(f"📊 Processed {message_count} messages in {elapsed:.1f} seconds")

                                if message_count >= max_messages:
                                    break

                            except Exception as e:
                                print(f"⚠️ Error processing message: {e}")
                                continue

                except Exception as e:
                    print(f"⚠️ Error polling messages: {e}")
                    time.sleep(1)
                    continue

        except Exception as e:
            print(f"❌ Error in consumer loop: {e}")

        finally:
            self.cleanup()
            elapsed = time.time() - start_time
            print(f"👋 Consumer completed after processing {message_count} messages in {elapsed:.1f} seconds")
            self.print_dashboard_summary()

    def cleanup(self):
        """Clean up resources"""
        self.running = False

        # Final metrics update
        self.update_metrics_in_redis()

        try:
            if hasattr(self, 'consumer') and self.consumer:
                self.consumer.close()
                print("✅ Kafka consumer closed")
        except Exception as e:
            print(f"⚠️ Error closing Kafka consumer: {e}")

        try:
            if hasattr(self, 'redis_client') and self.redis_client:
                self.redis_client.close()
                print("✅ Redis connection closed")
        except Exception as e:
            print(f"⚠️ Error closing Redis connection: {e}")


def main():
    try:
        print("🚀 Starting E-Commerce Event Consumer")
        print(f"🔗 Kafka: {os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:29092')}")
        print(f"🔴 Redis: {os.getenv('REDIS_HOST', 'redis')}:{os.getenv('REDIS_PORT', 6379)}")

        # Check if we should reset offsets (for fresh start)
        reset_mode = os.getenv('RESET_KAFKA_OFFSETS', 'false').lower() == 'true'
        
        # Create and start consumer
        consumer = ECommerceEventConsumer(reset_offset=reset_mode)
        consumer.consume_events(max_messages=100, timeout_seconds=90)

    except KeyboardInterrupt:
        print("\n🛑 Shutdown requested, stopping consumer...")
    except Exception as e:
        print(f"❌ Error in main: {e}")
        raise


if __name__ == "__main__":
    main()
