from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator  # Updated import
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.exceptions import AirflowException
import logging

# Configure logging
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
    description='E-commerce Kafka producer and consumer pipeline with Redis analytics',
    schedule_interval='*/1 * * * *',  # Run every 1 minutes (more reasonable for demo)
    start_date=days_ago(1),
    catchup=False,
    max_active_runs=1,  # Prevent overlapping runs
    tags=['kafka', 'ecommerce', 'analytics', 'redis'],
    concurrency=3,  # Allow up to 3 tasks to run in parallel
    dagrun_timeout=timedelta(minutes=25),  # Total DAG timeout
    doc_md="""
    ## E-commerce Kafka Data Pipeline

    This DAG simulates an e-commerce data pipeline using Kafka and Redis:

    1. **Producer**: Generates realistic e-commerce events (views, cart adds, wishlist, orders)
    2. **Consumer**: Processes events and stores analytics in Redis
    3. **Health Check**: Verifies pipeline completion and data integrity

    ### Environment Variables Required:
    - KAFKA_BOOTSTRAP_SERVERS
    - REDIS_HOST, REDIS_PORT
    - Various Kafka topic names
    """,
)

# Define the working directory
project_dir = '/opt/airflow/ecom_etl/'

# Common environment variables
common_env = {
    'KAFKA_BOOTSTRAP_SERVERS': 'kafka:29092',
    'KAFKA_TOPIC_PRODUCT_VIEWS': 'ecommerce.product.views',
    'KAFKA_TOPIC_CART_ADD': 'ecommerce.cart.add',
    'KAFKA_TOPIC_WISHLIST_ADD': 'ecommerce.wishlist.add',
    'KAFKA_TOPIC_ORDERS': 'ecommerce.orders.completed',
    'REDIS_HOST': 'redis',
    'REDIS_PORT': '6379',
    'REDIS_DB': '0',
    'PYTHONUNBUFFERED': '1',  # Ensure logs are flushed immediately
    'PYTHONPATH': project_dir,  # Add project directory to Python path
}

# Task 1: Health check for Kafka and Redis connectivity
def check_services_health():
    """Check if Kafka and Redis services are available"""
    import subprocess
    import time
    import os

    # Set environment variables in the function scope
    os.environ.update(common_env)

    logger.info("🔍 Checking service health...")

    # Simple connectivity test using netcat (if available) or python
    services = [
        ("Kafka", "kafka", "29092"),
        ("Redis", "redis", "6379")
    ]

    for service_name, host, port in services:
        try:
            # Use telnet-like check with timeout
            result = subprocess.run(
                ["timeout", "5", "bash", "-c", f"echo > /dev/tcp/{host}/{port}"],
                capture_output=True,
                text=True,
                timeout=10
            )
            if result.returncode == 0:
                logger.info(f"✅ {service_name} is reachable at {host}:{port}")
            else:
                logger.warning(f"⚠️ {service_name} connectivity test failed")
        except Exception as e:
            logger.warning(f"⚠️ Could not test {service_name} connectivity: {e}")

    logger.info("🏥 Health check completed")

health_check = PythonOperator(
    task_id='check_services_health',
    python_callable=check_services_health,
    dag=dag,
    execution_timeout=timedelta(minutes=2),
)

# Task 2: Run Kafka Producer with optimized settings
run_producer = BashOperator(
    task_id='run_kafka_producer',
    bash_command=f'cd {project_dir} && python kafka_scripts/producer.py',
    dag=dag,
    execution_timeout=timedelta(minutes=8),  # Reasonable timeout for producer
    retries=2,
    retry_delay=timedelta(minutes=1),
    env=common_env,
    do_xcom_push=False,  # Don't store large outputs in XCom
)

# Task 3: Wait a bit to ensure producer has started sending messages
wait_for_producer = BashOperator(
    task_id='wait_for_producer_warmup',
    bash_command='echo "⏳ Waiting for producer to warm up..." && sleep 10',
    dag=dag,
    execution_timeout=timedelta(minutes=1),
)

# Task 4: Run Kafka Consumer with optimized settings
run_consumer = BashOperator(
    task_id='run_kafka_consumer',
    bash_command=f'cd {project_dir} && python kafka_scripts/consumer.py',
    dag=dag,
    execution_timeout=timedelta(minutes=12),  # Consumer needs more time to process
    retries=2,
    retry_delay=timedelta(minutes=1),
    env={
        **common_env,
        'KAFKA_GROUP_ID': 'ecommerce-analytics-{{ ds_nodash }}',  # Unique group per DAG run
    },
    do_xcom_push=False,
)

# Task 5: Verify pipeline results
def verify_pipeline_results():
    """Verify that the pipeline processed data correctly"""
    import redis
    import json
    import os

    # Set environment variables in the function scope
    os.environ.update(common_env)

    try:
        # Connect to Redis
        redis_client = redis.Redis(
            host=os.getenv('REDIS_HOST', 'redis'),
            port=int(os.getenv('REDIS_PORT', 6379)),
            db=int(os.getenv('REDIS_DB', 0)),
            decode_responses=True,
            socket_connect_timeout=5
        )

        # Check if data was processed
        totals = redis_client.hgetall("metrics:totals")

        if not totals:
            logger.warning("⚠️ No metrics found in Redis")
            return

        logger.info("📊 Pipeline Results:")
        logger.info(f" 👀 Total Views: {totals.get('total_views', 0)}")
        logger.info(f" 🛒 Total Cart Adds: {totals.get('total_cart_adds', 0)}")
        logger.info(f" ❤️ Total Wishlist Adds: {totals.get('total_wishlist_adds', 0)}")
        logger.info(f" 📦 Total Orders: {totals.get('total_orders', 0)}")
        logger.info(f" 💰 Total Revenue: ${totals.get('total_revenue', 0)}")
        logger.info(f" 🕐 Last Updated: {totals.get('last_updated', 'N/A')}")

        # Check for recent activity
        recent_orders = redis_client.lrange("recent:orders", 0, 4)
        if recent_orders:
            logger.info("🔥 Recent Orders:")
            for order_json in recent_orders:
                order = json.loads(order_json)
                logger.info(f" • {order['order_id']}: ${order['total_amount']} ({order['item_count']} items)")

        # Verify data freshness
        last_activity = redis_client.get("metrics:last_activity")
        if last_activity:
            logger.info(f"✅ Pipeline completed successfully - Last activity: {last_activity}")
        else:
            logger.warning("⚠️ No recent activity indicator found")

    except Exception as e:
        logger.error(f"❌ Error verifying pipeline results: {e}")
        raise

verify_results = PythonOperator(
    task_id='verify_pipeline_results',
    python_callable=verify_pipeline_results,
    dag=dag,
    execution_timeout=timedelta(minutes=3)
)

# Task 6: Cleanup and final status
cleanup_task = BashOperator(
    task_id='cleanup_and_complete',
    bash_command='''
echo "🧹 Performing cleanup..."
echo "📈 Kafka E-commerce Pipeline completed successfully at $(date)"
echo "🎯 Check Redis for real-time analytics data"
echo "✅ DAG execution finished"
''',
    dag=dag,
    execution_timeout=timedelta(minutes=1),
    trigger_rule='all_done',  # Run regardless of upstream task status
)

# Set task dependencies with proper sequencing
health_check >> run_producer
run_producer >> wait_for_producer >> run_consumer
run_consumer >> verify_results >> cleanup_task

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
    trigger_rule='all_done',
)

# Add monitoring to run in parallel with cleanup
[verify_results, run_consumer] >> monitor_resources
monitor_resources >> cleanup_task