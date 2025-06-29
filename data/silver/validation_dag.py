from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import os

# Default arguments for the DAG
default_args = {
    'owner': 'zoo-data-team',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
dag = DAG(
    'silver_layer_validation',
    default_args=default_args,
    description='Daily validation of silver layer data quality',
    schedule_interval='0 2 * * *',  # Run daily at 2 AM
    catchup=False,
    tags=['zoo', 'silver-layer', 'validation', 'data-quality'],
)

# Get the path to the SQL file
sql_file_path = os.path.join(
    os.path.dirname(__file__), 
    'validation_job.sql'
)

# Task to run the validation SQL script
validate_silver_data = PostgresOperator(
    task_id='validate_silver_data',
    postgres_conn_id='zoo_postgres',
    sql=sql_file_path,
    autocommit=True,
    dag=dag,
)

# Task to log validation completion
def log_validation_completion(**context):
    """Log completion of validation job"""
    print("Silver layer validation completed successfully")
    print(f"Task instance: {context['task_instance']}")
    print(f"Execution date: {context['execution_date']}")

log_completion = PythonOperator(
    task_id='log_validation_completion',
    python_callable=log_validation_completion,
    provide_context=True,
    dag=dag,
)

# Set task dependencies
validate_silver_data >> log_completion 