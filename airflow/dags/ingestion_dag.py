from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

# Inside Docker container, the project is mounted at /opt/airflow
PROJECT_ROOT = '/opt/airflow'

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 6, 7),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='ingestion_dag',
    default_args=default_args,
    description='DAG for data ingestion',
    schedule_interval=timedelta(days=1), # Adjust as needed
    catchup=False,
    tags=['ingestion'],
) as dag:

    run_ingestion_script = BashOperator(
        task_id='run_ingestion_script',
        bash_command='cd /opt/airflow/ingestion && python ingestion_script.py',
    )
