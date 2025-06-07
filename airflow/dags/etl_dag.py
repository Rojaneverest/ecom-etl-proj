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
    dag_id='etl_dag',
    default_args=default_args,
    description='DAG for ODS, Staging, Target, and Drop Staging layers',
    schedule_interval=timedelta(days=1), # Adjust as needed
    catchup=False,
    tags=['etl'],
) as dag:

    run_ods_layer = BashOperator(
        task_id='run_ods_layer',
        bash_command='cd /opt/airflow/ecom_etl && python ods_layer.py',
    )

    run_staging_layer = BashOperator(
        task_id='run_staging_layer',
        bash_command='cd /opt/airflow/ecom_etl && python staging_layer.py',
    )

    run_target_layer = BashOperator(
        task_id='run_target_layer',
        bash_command='cd /opt/airflow/ecom_etl && python target_layer.py',
    )

    run_truncate_stg = BashOperator(
        task_id='run_truncate_stg',
        bash_command='cd /opt/airflow/ecom_etl && python truncate_stg.py',
    )

    run_ods_layer >> run_staging_layer >> run_target_layer >> run_truncate_stg
