from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import days_ago

# Define default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

# Define the DAG
dag = DAG(
    'kafka_data_pipeline',
    default_args=default_args,
    description='Kafka producer and consumer pipeline',
    schedule_interval='*/2 * * * *',  # Run every 2 minutes
    start_date=days_ago(1),
    catchup=False,
    tags=['kafka', 'data-pipeline'],
)

# Define the working directory
project_dir = '/opt/airflow/ecom_etl/'

# Task 1: Run Kafka Producer
run_producer = BashOperator(
    task_id='run_kafka_producer',
    bash_command=f'python {project_dir}kafka_scripts/producer.py',
    dag=dag,
)

# Task 2: Run Kafka Consumer
run_consumer = BashOperator(
    task_id='run_kafka_consumer',
    bash_command=f'python {project_dir}kafka_scripts/consumer.py',
    dag=dag,
)

# Set task dependencies
run_producer >> run_consumer
