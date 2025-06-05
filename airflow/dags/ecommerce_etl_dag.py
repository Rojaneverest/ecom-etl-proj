from datetime import datetime, timedelta
import os
import sys
import logging
import glob

# Add the path to the directory containing your ingestion script
# This ensures Python can find and import your module
sys.path.insert(0, "/opt/airflow/ingestion")

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from airflow.utils.dates import days_ago
from airflow.sensors.filesystem import FileSensor

# Import functions from your ingestion script
from ingestion_script import process_csv_file, create_s3_client, ensure_s3_bucket_exists

# Define default arguments for the DAG
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# Create a DAG instance
dag = DAG(
    "ecommerce_data_ingestion",
    default_args=default_args,
    description="Process and ingest e-commerce data into S3",
    schedule_interval="0 1 * * *",  # Run daily at 1 AM
    start_date=days_ago(1),
    catchup=False,
    tags=["ecommerce", "data-ingestion"],
)

# Get configuration from Airflow Variables or use defaults
try:
    DATA_DIR = Variable.get(
        "ecommerce_data_dir",
        default_var="/opt/airflow/data/extracted_data",
    )
    S3_BUCKET = Variable.get(
        "ecommerce_s3_bucket", default_var="rj-ecommerce-data-lake"
    )
except Exception as e:
    # Fallback configuration
    DATA_DIR = "/opt/airflow/data/extracted_data"
    S3_BUCKET = "rj-ecommerce-data-lake"


def process_file(file_path, **kwargs):
    """Process a single CSV file using the imported function from ingestion script."""
    logging.info(f"Processing file: {file_path}")

    # Create S3 client
    s3_client = create_s3_client()

    # Process the file
    success = process_csv_file(file_path, s3_client)

    if success:
        return f"Successfully processed {os.path.basename(file_path)}"
    else:
        raise Exception(f"Failed to process {os.path.basename(file_path)}")


def setup_s3_bucket(**kwargs):
    """Setup and verify S3 bucket exists."""
    s3_client = create_s3_client()
    ensure_s3_bucket_exists(s3_client)
    return f"S3 bucket {S3_BUCKET} ready for ingestion"


# Task to check and setup S3 bucket
setup_task = PythonOperator(
    task_id="setup_s3_bucket",
    python_callable=setup_s3_bucket,
    dag=dag,
)

datasets = [
    "olist_orders_dataset",
    "olist_customers_dataset",
    "olist_order_items_dataset",
    "olist_order_payments_dataset",
    "olist_products_dataset",
    "olist_order_reviews_dataset",
    "olist_sellers_dataset",
    "olist_geolocation_dataset",
    "product_category_name_translation",
]

# Create file sensors and processing tasks for each dataset
file_sensors = {}
processing_tasks = {}

for dataset in datasets:
    file_path = os.path.join(DATA_DIR, f"{dataset}.csv")

    # Create file sensor
    file_sensor = FileSensor(
        task_id=f"wait_for_{dataset}",
        filepath=file_path,
        poke_interval=300,  # Check every 5 minutes
        timeout=60 * 60 * 12,  # Time out after 12 hours
        mode="poke",  # Use reschedule mode for long-running DAGs
        soft_fail=False,  # Fail if file is not found, for debugging
        dag=dag,
    )
    file_sensors[dataset] = file_sensor

    # Create processing task
    process_task = PythonOperator(
        task_id=f"process_{dataset}",
        python_callable=process_file,
        op_kwargs={"file_path": file_path},
        dag=dag,
    )
    processing_tasks[dataset] = process_task

    # Set dependencies
    setup_task >> file_sensor >> process_task


# Add a task to process any additional CSV files not explicitly listed
def process_additional_files(**kwargs):
    """Process any additional CSV files found in the data directory."""
    known_files = [f"{dataset}.csv" for dataset in datasets]
    all_csv_files = glob.glob(os.path.join(DATA_DIR, "*.csv"))

    # Filter out already processed files
    additional_files = [
        f for f in all_csv_files if os.path.basename(f) not in known_files
    ]

    if not additional_files:
        logging.info("No additional CSV files to process")
        return

    s3_client = create_s3_client()
    processed = 0

    for file_path in additional_files:
        logging.info(f"Processing additional file: {file_path}")
        if process_csv_file(file_path, s3_client):
            processed += 1

    return f"Processed {processed} additional CSV files"


# Add task for any additional files
process_additional_task = PythonOperator(
    task_id="process_additional_files",
    python_callable=process_additional_files,
    dag=dag,
)

# Set dependencies for additional files task
# This will run after all other processing tasks
for dataset in datasets:
    processing_tasks[dataset] >> process_additional_task
