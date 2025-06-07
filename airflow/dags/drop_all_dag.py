from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 6, 7),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Create the DAG
with DAG(
    dag_id='drop_all_dag',
    default_args=default_args,
    description='DAG to drop all ODS, STG, and TARGET schemas',
    schedule_interval=None,  # Manual trigger only
    catchup=False,
    tags=['maintenance', 'cleanup'],
) as dag:
    
    # Task to drop ODS schema
    drop_ods = BashOperator(
        task_id='drop_ods',
        bash_command='cd /opt/airflow/ecom_etl && python drop_ods.py',
    )
    
    # Task to drop STG schema
    drop_stg = BashOperator(
        task_id='drop_stg',
        bash_command='cd /opt/airflow/ecom_etl && python drop_stg.py',
    )
    
    # Task to drop TARGET schema
    drop_target = BashOperator(
        task_id='drop_target',
        bash_command='cd /opt/airflow/ecom_etl && python drop_target.py',
    )
    
    # Set the dependencies between tasks
    drop_ods >> drop_stg >> drop_target
