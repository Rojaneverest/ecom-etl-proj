#!/usr/bin/env python3
"""
E-commerce Events Consumer
-------------------------
This script consumes events from the Kafka topic, processes them,
and stores the results in Redis for real-time analytics.
"""

import json
import time
import redis
from kafka import KafkaConsumer

def create_consumer():
    """Create and return a Kafka consumer instance."""
    try:
        return KafkaConsumer(
            'ecommerce-events',
            bootstrap_servers=['kafka:9092'],
            auto_offset_reset='latest',
            enable_auto_commit=True,
            group_id='ecommerce-analytics-group',
            value_deserializer=lambda x: json.loads(x.decode('utf-8'))
        )
    except Exception as e:
        print(f"Error creating Kafka consumer: {e}")
        return None

def get_redis_connection():
    """Create and return a Redis connection."""
    try:
        return redis.Redis(host='redis', port=6379, db=0)
    except Exception as e:
        print(f"Error connecting to Redis: {e}")
        return None

def process_event(event, redis_conn):
    """Process an event and update Redis metrics."""
    if not redis_conn:
        return
    
    event_type = event.get('event_type')
    timestamp = event.get('timestamp')
    
    # Increment event count
    redis_conn.hincrby('event_counts', event_type, 1)
    
    # Track active users in the last minute (using a sorted set)
    if 'user_id' in event:
        redis_conn.zadd('active_users', {event['user_id']: time.time()})
    
    # Process specific event types
    if event_type == 'page_view':
        # Track page views
        redis_conn.hincrby('page_views', event.get('page', 'unknown'), 1)
    
    elif event_type == 'product_view':
        # Track popular products
        product_id = event.get('product_id')
        if product_id:
            redis_conn.zincrby('popular_products', 1, product_id)
    
    elif event_type == 'add_to_cart':
        # Track cart additions
        product_id = event.get('product_id')
        if product_id:
            redis_conn.zincrby('cart_additions', 1, product_id)
    
    elif event_type == 'purchase':
        # Track revenue
        order_total = event.get('order_total', 0)
        redis_conn.incrby('total_revenue_cents', int(order_total * 100))
        redis_conn.hincrby('purchases_by_payment', event.get('payment_method', 'unknown'), 1)

def cleanup_stale_data(redis_conn):
    """Clean up stale data from Redis (e.g., inactive users)."""
    if not redis_conn:
        return
    
    # Remove users inactive for more than 5 minutes
    cutoff_time = time.time() - 300
    redis_conn.zremrangebyscore('active_users', 0, cutoff_time)

def main():
    """Main function to run the consumer."""
    consumer = create_consumer()
    if not consumer:
        return
    
    redis_conn = get_redis_connection()
    if not redis_conn:
        consumer.close()
        return
    
    print("Starting E-commerce Events Consumer...")
    
    try:
        # Reset metrics on startup
        redis_conn.flushdb()
        
        last_cleanup = time.time()
        
        for message in consumer:
            event = message.value
            process_event(event, redis_conn)
            print(f"Processed event: {event.get('event_type')}")
            
            # Periodically clean up stale data
            current_time = time.time()
            if current_time - last_cleanup > 60:  # Every minute
                cleanup_stale_data(redis_conn)
                last_cleanup = current_time
                
    except KeyboardInterrupt:
        print("Consumer stopped by user")
    except Exception as e:
        print(f"Error in consumer: {e}")
    finally:
        consumer.close()
        print("Consumer closed")

if __name__ == "__main__":
    main() 