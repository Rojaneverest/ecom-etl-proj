from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.exceptions import AirflowException
import logging
import subprocess
import redis # For Redis connectivity check
from kafka.admin import KafkaAdminClient # Import KafkaAdminClient
from kafka.errors import NoBrokersAvailable # Import specific Kafka error for no brokers
import os

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Define default arguments with optimized retry settings
default_args = {
    'owner': 'data-engineering-team',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': False,  # Don't email on every retry
    'retries': 2,  # Reduced retries since scripts have their own retry logic
    'retry_delay': timedelta(minutes=1),
    'retry_exponential_backoff': True,
    'max_retry_delay': timedelta(minutes=3),
    'execution_timeout': timedelta(minutes=20),  # Overall task timeout
}

# Define the DAG with improved settings
dag = DAG(
    'kafka_ecommerce_pipeline',
    default_args=default_args,
    description='E-commerce Kafka producer simulation and Redis analytics verification',
    schedule_interval='*/1 * * * *',  # Run every 1 minutes (more reasonable for demo)
    start_date=days_ago(1),
    catchup=False,
    max_active_runs=1,  # Prevent overlapping runs
    tags=['kafka', 'ecommerce', 'analytics', 'redis'],
    concurrency=3,  # Allow up to 3 tasks to run in parallel
    dagrun_timeout=timedelta(minutes=25),  # Total DAG runtime limit
)

# Environment variables for services (consistent with docker-compose)
REDIS_HOST = os.getenv('REDIS_HOST', 'redis')
REDIS_PORT = int(os.getenv('REDIS_PORT', 6379))
KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:29092') # Changed from localhost to kafka

# Task 1: Check if Redis and Kafka are reachable
def check_services_health(**kwargs):
    logger.info("🔍 Checking service health...")

    # Check Redis connectivity
    try:
        r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, socket_connect_timeout=5)
        r.ping()
        logger.info(f"✅ Redis is reachable at {REDIS_HOST}:{REDIS_PORT}")
    except redis.exceptions.ConnectionError as e:
        logger.error(f"❌ Redis connectivity check failed: {e}")
        raise AirflowException(f"Redis connectivity check failed: {e}")
    except Exception as e:
        logger.error(f"❌ Unexpected error during Redis check: {e}")
        raise AirflowException(f"Unexpected error during Redis check: {e}")

    # Check Kafka connectivity using kafka-python client
    kafka_admin_client = None
    try:
        kafka_admin_client = KafkaAdminClient(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            client_id='airflow_health_check',
            request_timeout_ms=5000 # 5 seconds timeout for requests
        )
        # Attempt to list topics to verify connectivity
        # This will raise NoBrokersAvailable if Kafka isn't reachable
        kafka_admin_client.list_topics()
        logger.info(f"✅ Kafka is reachable at {KAFKA_BOOTSTRAP_SERVERS}")
    except NoBrokersAvailable as e:
        logger.error(f"❌ Kafka connectivity check failed: No brokers available at {KAFKA_BOOTSTRAP_SERVERS}. Error: {e}")
        raise AirflowException(f"Kafka connectivity check failed: No brokers available. Error: {e}")
    except Exception as e:
        logger.error(f"❌ Could not test Kafka connectivity: {e}")
        raise AirflowException(f"Kafka connectivity check failed: {e}")
    finally:
        if kafka_admin_client:
            try:
                kafka_admin_client.close()
                logger.info("ℹ️ KafkaAdminClient closed.")
            except Exception as e:
                logger.warning(f"⚠️ Error closing KafkaAdminClient: {e}")


health_check = PythonOperator(
    task_id='check_services_health',
    python_callable=check_services_health,
    dag=dag,
    execution_timeout=timedelta(minutes=1)
)

# Task 2: Run the Kafka Producer (for simulation)
run_producer = BashOperator(
    task_id='run_kafka_producer',
    # Command to run the producer script
    bash_command='python /app/kafka_scripts/producer.py',
    env={
        'KAFKA_BOOTSTRAP_SERVERS': 'kafka:29092',
        'PRODUCER_MODE': 'simulation',
        'SIMULATION_DURATION': '60', # Run for 60 seconds
        'EVENTS_PER_SECOND': '5', # 5 events per second
        'KAFKA_TOPIC_PRODUCT_VIEWS': 'ecommerce.product.views',
        'KAFKA_TOPIC_CART_ADD': 'ecommerce.cart.add',
        'KAFKA_TOPIC_WISHLIST_ADD': 'ecommerce.wishlist.add',
        'KAFKA_TOPIC_ORDERS': 'ecommerce.orders.completed',
        'PYTHONPATH': '/app/kafka_scripts'  # Ensure python finds modules correctly
    },
    dag=dag,
    execution_timeout=timedelta(minutes=2)
)

# Task 3: Wait for producer to send enough data
wait_for_producer = BashOperator(
    task_id='wait_for_producer_data',
    bash_command='echo "Waiting for producer to generate data..."; sleep 10;', # Wait 10 seconds
    dag=dag,
    execution_timeout=timedelta(minutes=1)
)

# Task 4: Verify results in Redis (Consumer is assumed to be running continuously)
def verify_pipeline_results(**kwargs):
    logger.info("✅ Verifying pipeline results in Redis...")
    try:
        r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
        total_views = r.hget('metrics:totals', 'total_views')
        total_revenue = r.hget('metrics:totals', 'total_revenue')

        if total_views and int(total_views) > 0:
            logger.info(f"📈 Total Views in Redis: {total_views}")
        else:
            raise AirflowException("Total views not found or 0 in Redis - data pipeline might have issues.")

        if total_revenue and float(total_revenue) > 0:
            logger.info(f"💰 Total Revenue in Redis: ${float(total_revenue):,.2f}")
        else:
            logger.warning("Total revenue not found or 0 in Redis - check if orders were processed.")

        # Check for minutely metrics keys
        minutely_keys = r.keys('metrics:minutely:*')
        if minutely_keys:
            logger.info(f"📊 Found {len(minutely_keys)} minutely aggregation keys in Redis.")
        else:
            logger.warning("No minutely aggregation keys found in Redis.")

        # Check for recent activities
        recent_orders_count = r.llen('recent:orders')
        recent_views_count = r.llen('recent:views')
        if recent_orders_count > 0 or recent_views_count > 0:
            logger.info(f"🧾 Found {recent_orders_count} recent orders and {recent_views_count} recent views.")
        else:
            logger.warning("No recent orders or views found.")

    except redis.exceptions.ConnectionError as e:
        logger.error(f"❌ Redis connection error during verification: {e}")
        raise AirflowException(f"Redis verification failed: {e}")
    except Exception as e:
        logger.error(f"❌ Error verifying pipeline results: {e}")
        raise

verify_results = PythonOperator(
    task_id='verify_pipeline_results',
    python_callable=verify_pipeline_results,
    dag=dag,
    execution_timeout=timedelta(minutes=3)
)

# Task 5: Cleanup and final status
cleanup_task = BashOperator(
    task_id='cleanup_and_complete',
    bash_command='''
echo "🧹 Performing cleanup..."
echo "📈 Kafka E-commerce Pipeline completed successfully at $(date)"
echo "🎯 Check Redis for real-time analytics data (Consumer is always running)"
echo "✅ DAG execution finished"
''',
    dag=dag,
    execution_timeout=timedelta(minutes=1),
    trigger_rule='all_done',  # Run regardless of upstream task status
)

# Set task dependencies with proper sequencing
health_check >> run_producer
run_producer >> wait_for_producer >> verify_results # Consumer task removed
verify_results >> cleanup_task

# Optional: Add parallel monitoring task
monitor_resources = BashOperator(
    task_id='monitor_system_resources',
    bash_command='''
echo "💻 System Resource Usage:"
echo "Memory: $(free -h | grep Mem | awk '{print $3 "/" $2}')"
echo "Disk: $(df -h / | tail -1 | awk '{print $3 "/" $2 " (" $5 " used)"}')"
echo "Load: $(uptime | awk -F'load average:' '{print $2}')"
''',
    dag=dag,
    execution_timeout=timedelta(minutes=1),
    trigger_rule='all_done', # Can run in parallel or after other tasks, good for general info
)

# You can connect monitor_resources to any task, or run it in parallel if needed
# For simplicity, let's just make it dependent on the health check
health_check >> monitor_resources